"""Tests for the metrics-tracking instrumentation.

Covers:
- GigaChatClient.extract_usage (parsing of OpenAI-style usage field).
- LLMNormalizer.call_store wiring (token rows persisted on both ok and error).
- FallbackNormalizer.last_path (automaton / regex_fallback / none).
- NormalizationHandler -> task_path_store.set_normalizer_path flow.
- queries._percentile pure helper.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest

from app.api.queries import _percentile
from app.models.schemas import (
    EventType,
    LocationSchema,
    NormalizedEventSchema,
    ParsedRecordSchema,
)
from app.normalization.automaton import AutomatonNormalizer, FallbackNormalizer
from app.normalization.gigachat_client import GigaChatClient
from app.normalization.llm import LLMNormalizer
from app.workers.normalizer import NormalizationHandler
from app.workers.queue import Task, TaskType

# ---------------------------------------------------------------------------
# GigaChatClient.extract_usage
# ---------------------------------------------------------------------------


class TestExtractUsage:
    def test_full_usage_block(self):
        response = {"usage": {"prompt_tokens": 120, "completion_tokens": 45, "total_tokens": 165}}
        assert GigaChatClient.extract_usage(response) == {
            "prompt_tokens": 120,
            "completion_tokens": 45,
            "total_tokens": 165,
        }

    def test_missing_usage_returns_zeros(self):
        assert GigaChatClient.extract_usage({"choices": [{"message": {"content": "x"}}]}) == {
            "prompt_tokens": 0,
            "completion_tokens": 0,
            "total_tokens": 0,
        }

    def test_partial_usage_fills_missing_total(self):
        response = {"usage": {"prompt_tokens": 10, "completion_tokens": 5}}
        # total_tokens auto-derived from prompt + completion when absent
        assert GigaChatClient.extract_usage(response)["total_tokens"] == 15

    def test_non_dict_input_safe(self):
        assert GigaChatClient.extract_usage(None) == {  # type: ignore[arg-type]
            "prompt_tokens": 0,
            "completion_tokens": 0,
            "total_tokens": 0,
        }


# ---------------------------------------------------------------------------
# LLMNormalizer with injected call_store
# ---------------------------------------------------------------------------


@dataclass
class FakeCallStore:
    records: list[dict] = field(default_factory=list)

    async def record(
        self,
        *,
        model: str,
        prompt_tokens: int,
        completion_tokens: int,
        total_tokens: int,
        duration_ms: int,
        status: str,
        task_id: UUID | None = None,
        trace_id: UUID | None = None,
    ) -> None:
        self.records.append(
            {
                "model": model,
                "prompt_tokens": prompt_tokens,
                "completion_tokens": completion_tokens,
                "total_tokens": total_tokens,
                "duration_ms": duration_ms,
                "status": status,
                "task_id": task_id,
                "trace_id": trace_id,
            }
        )


class FakeGigaChatClient:
    def __init__(self, payload: dict | Exception, model: str = "GigaChat-test") -> None:
        self._payload = payload
        self._model = model
        self.calls = 0

    @property
    def model_name(self) -> str:
        return self._model

    async def chat_completion(self, **_kwargs):
        self.calls += 1
        if isinstance(self._payload, Exception):
            raise self._payload
        return self._payload


def _parsed_for_llm() -> ParsedRecordSchema:
    return ParsedRecordSchema(
        id=uuid4(),
        raw_record_id=uuid4(),
        source_id=uuid4(),
        external_id="ext",
        start_time=datetime(2026, 5, 12, 3, 0, tzinfo=UTC),
        end_time=datetime(2026, 5, 12, 9, 0, tzinfo=UTC),
        location_city="Томск",
        location_district=None,
        location_street="улица Кирова",
        location_region_code="RU-TOM",
        reason="Ремонт",
        extra={"houses": "12"},
        trace_id=uuid4(),
        extracted_at=datetime(2026, 5, 12, 0, 0, tzinfo=UTC),
    )


_HAPPY = {
    "choices": [
        {
            "message": {
                "content": json.dumps(
                    {
                        "event_type": "power_outage",
                        "start_time": "2026-05-12T03:00:00Z",
                        "end_time": "2026-05-12T09:00:00Z",
                        "location": {
                            "raw": "Томск, Кирова, 12",
                            "city": "Томск",
                            "street": "улица Кирова",
                            "building": "12",
                        },
                        "reason": "Ремонт",
                        "confidence": 0.9,
                    }
                )
            }
        }
    ],
    "usage": {"prompt_tokens": 312, "completion_tokens": 88, "total_tokens": 400},
}


async def test_llm_normalizer_records_call_on_success():
    store = FakeCallStore()
    client = FakeGigaChatClient(_HAPPY)
    normalizer = LLMNormalizer(client=client, call_store=store)  # type: ignore[arg-type]
    record = _parsed_for_llm()

    event = await normalizer.normalize(record)
    assert event is not None

    assert len(store.records) == 1
    call = store.records[0]
    assert call["status"] == "ok"
    assert call["model"] == "GigaChat-test"
    assert call["prompt_tokens"] == 312
    assert call["completion_tokens"] == 88
    assert call["total_tokens"] == 400
    assert call["trace_id"] == record.trace_id
    assert call["duration_ms"] >= 0


async def test_llm_normalizer_records_call_on_invalid_json():
    from app.normalization.gigachat_client import GigaChatInvalidResponseError

    store = FakeCallStore()
    client = FakeGigaChatClient(GigaChatInvalidResponseError("boom"))
    normalizer = LLMNormalizer(client=client, call_store=store)  # type: ignore[arg-type]

    result = await normalizer.normalize(_parsed_for_llm())
    assert result is None
    assert len(store.records) == 1
    assert store.records[0]["status"] == "error"
    # No tokens for an errored request
    assert store.records[0]["total_tokens"] == 0


# ---------------------------------------------------------------------------
# FallbackNormalizer.last_path
# ---------------------------------------------------------------------------


@dataclass
class FakeFallback:
    return_value: NormalizedEventSchema | None
    calls: list[ParsedRecordSchema] = field(default_factory=list)

    async def normalize(self, record: ParsedRecordSchema) -> NormalizedEventSchema | None:
        self.calls.append(record)
        return self.return_value


def _llm_event(parsed_id: UUID, raw_id: UUID) -> NormalizedEventSchema:
    return NormalizedEventSchema(
        event_id=uuid4(),
        parsed_record_id=parsed_id,
        event_type=EventType.POWER_OUTAGE,
        start_time=datetime(2026, 5, 12, 3, 0, tzinfo=UTC),
        end_time=None,
        location=LocationSchema(raw="x", normalized="x|x|x", city="x", street="x", building="x"),
        reason=None,
        sources=[raw_id],
        confidence=0.9,
    )


async def test_fallback_last_path_automaton_when_confidence_high():
    record = _parsed_for_llm()
    normalizer = FallbackNormalizer(AutomatonNormalizer(), FakeFallback(None), threshold=0.6)

    await normalizer.normalize(record)
    assert normalizer.last_path == FallbackNormalizer.PATH_AUTOMATON


async def test_fallback_last_path_regex_when_confidence_below_threshold():
    record = _parsed_for_llm()
    fallback_event = _llm_event(record.id, record.raw_record_id)
    normalizer = FallbackNormalizer(
        AutomatonNormalizer(), FakeFallback(fallback_event), threshold=1.0
    )
    # Force low confidence: glue region into city so the FSA flags it.
    bad_record = _parsed_for_llm()
    bad_record = bad_record.model_copy(
        update={"location_city": "Томская область Томский район Томск"}
    )

    await normalizer.normalize(bad_record)
    assert normalizer.last_path == FallbackNormalizer.PATH_REGEX_FALLBACK


async def test_fallback_last_path_none_when_both_fail():
    record = _parsed_for_llm()
    bad_record = record.model_copy(update={"location_street": None})
    normalizer = FallbackNormalizer(AutomatonNormalizer(), FakeFallback(None), threshold=0.6)

    event = await normalizer.normalize(bad_record)
    assert event is None
    assert normalizer.last_path == FallbackNormalizer.PATH_NONE


# ---------------------------------------------------------------------------
# NormalizationHandler persists normalizer_path
# ---------------------------------------------------------------------------


@dataclass
class FakeParsedStore:
    record: object

    async def get_by_id(self, parsed_id: UUID):
        return self.record


@dataclass
class FakeNormalizedStore:
    saved: list = field(default_factory=list)

    async def save(self, event, trace_id):
        self.saved.append((event, trace_id))
        return event.event_id


@dataclass
class FakeTaskPathStore:
    updates: list[tuple[UUID, str]] = field(default_factory=list)

    async def set_normalizer_path(self, task_id: UUID, path: str) -> None:
        self.updates.append((task_id, path))


@dataclass
class FakePathAwareNormalizer:
    event: NormalizedEventSchema | None
    last_path: str | None = "automaton"

    async def normalize(self, record):
        return self.event


@dataclass
class FakeParsedRecord:
    id: UUID = field(default_factory=uuid4)
    raw_record_id: UUID = field(default_factory=uuid4)
    source_id: UUID = field(default_factory=uuid4)
    external_id: str | None = "ext-1"
    start_time: datetime | None = field(
        default_factory=lambda: datetime(2026, 5, 12, 3, 0, tzinfo=UTC)
    )
    end_time: datetime | None = field(
        default_factory=lambda: datetime(2026, 5, 12, 9, 0, tzinfo=UTC)
    )
    location_city: str | None = "Томск"
    location_district: str | None = None
    location_street: str | None = "улица Кирова"
    location_region_code: str | None = "RU-TOM"
    reason: str | None = "ремонт"
    extra: dict = field(default_factory=lambda: {"houses": "12"})
    trace_id: UUID = field(default_factory=uuid4)
    extracted_at: datetime = field(default_factory=lambda: datetime.now(UTC))


def _task(parsed_id: UUID) -> Task:
    return Task(
        task_type=TaskType.NORMALIZE_EVENT,
        payload={"parsed_record_id": str(parsed_id)},
        trace_id=uuid4(),
    )


async def test_normalization_handler_persists_normalizer_path():
    parsed = FakeParsedRecord()
    event = _llm_event(parsed.id, parsed.raw_record_id)
    path_store = FakeTaskPathStore()
    handler = NormalizationHandler(
        FakeParsedStore(parsed),
        FakeNormalizedStore(),
        FakePathAwareNormalizer(event=event, last_path="automaton"),
        task_path_store=path_store,
    )
    task = _task(parsed.id)

    await handler.handle(task)

    assert path_store.updates == [(task.task_id, "automaton")]


async def test_normalization_handler_skips_path_when_normalizer_has_no_last_path():
    parsed = FakeParsedRecord()
    event = _llm_event(parsed.id, parsed.raw_record_id)
    # Use a normalizer without `last_path` attribute (e.g. raw LLMNormalizer in
    # a test config). The handler should not blow up — and should not write.

    @dataclass
    class PathlessNormalizer:
        e: NormalizedEventSchema

        async def normalize(self, record):
            return self.e

    path_store = FakeTaskPathStore()
    handler = NormalizationHandler(
        FakeParsedStore(parsed),
        FakeNormalizedStore(),
        PathlessNormalizer(e=event),
        task_path_store=path_store,
    )

    await handler.handle(_task(parsed.id))

    assert path_store.updates == []


# ---------------------------------------------------------------------------
# _percentile helper
# ---------------------------------------------------------------------------


class TestPercentile:
    def test_empty_returns_zero(self):
        assert _percentile([], 0.5) == 0

    def test_single_value(self):
        assert _percentile([42], 0.95) == 42

    def test_p50_of_uniform_range(self):
        # 11 values [0..10]; nearest-rank p50 = round(0.5 * 10) = 5
        assert _percentile(list(range(11)), 0.50) == 5

    def test_p95_of_uniform_range(self):
        # 21 values [0..20]; nearest-rank p95 = round(0.95 * 20) = 19
        assert _percentile(list(range(21)), 0.95) == 19

    def test_p100_returns_max(self):
        assert _percentile([1, 5, 10], 1.0) == 10


pytest.importorskip("pydantic", reason="schemas require pydantic")  # safety net
