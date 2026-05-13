from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest

from app.models.schemas import (
    EventType,
    LocationSchema,
    NormalizedEventSchema,
    ParsedRecordSchema,
)
from app.normalization.llm import _build_event
from app.workers.normalizer import NormalizationHandler
from app.workers.queue import Task, TaskType


@dataclass
class FakeParsedRecord:
    id: UUID = field(default_factory=uuid4)
    raw_record_id: UUID = field(default_factory=uuid4)
    source_id: UUID | None = field(default_factory=uuid4)
    external_id: str | None = "external-1"
    start_time: datetime | None = field(
        default_factory=lambda: datetime(2026, 5, 12, 3, 0, tzinfo=UTC)
    )
    end_time: datetime | None = field(
        default_factory=lambda: datetime(2026, 5, 12, 9, 0, tzinfo=UTC)
    )
    location_city: str | None = "Томск"
    location_district: str | None = "Томский р-н"
    location_street: str | None = "ул. Весенняя"
    location_region_code: str | None = "Томская обл"
    reason: str | None = "Ремонтные работы"
    extra: dict = field(default_factory=dict)
    trace_id: UUID = field(default_factory=uuid4)
    extracted_at: datetime = field(default_factory=lambda: datetime.now(UTC))


@dataclass
class FakeParsedStore:
    record: FakeParsedRecord | None

    async def get_by_id(self, parsed_id: UUID):
        return self.record


@dataclass
class FakeNormalizedStore:
    saved: list[tuple[NormalizedEventSchema, UUID]] = field(default_factory=list)

    async def save(self, event: NormalizedEventSchema, trace_id: UUID) -> UUID:
        self.saved.append((event, trace_id))
        return event.event_id


@dataclass
class FakeNormalizer:
    result: NormalizedEventSchema | None
    seen: list[ParsedRecordSchema] = field(default_factory=list)

    async def normalize(self, record: ParsedRecordSchema) -> NormalizedEventSchema | None:
        self.seen.append(record)
        return self.result


def _task(parsed_id: UUID) -> Task:
    return Task(
        task_type=TaskType.NORMALIZE_EVENT,
        payload={"parsed_record_id": str(parsed_id)},
        trace_id=uuid4(),
    )


def _event(parsed_record_id: UUID, raw_record_id: UUID) -> NormalizedEventSchema:
    return NormalizedEventSchema(
        event_id=uuid4(),
        parsed_record_id=parsed_record_id,
        event_type=EventType.POWER_OUTAGE,
        start_time=datetime(2026, 5, 12, 3, 0, tzinfo=UTC),
        end_time=datetime(2026, 5, 12, 9, 0, tzinfo=UTC),
        location=LocationSchema(
            raw="Томская обл, Томск, улица Весенняя",
            normalized="томск, улица весенняя",
            city="Томск",
            street="улица Весенняя",
        ),
        reason="Ремонтные работы",
        sources=[raw_record_id],
        confidence=0.86,
    )


async def test_normalization_handler_saves_normalized_event():
    parsed = FakeParsedRecord()
    expected = _event(parsed.id, parsed.raw_record_id)
    store = FakeNormalizedStore()
    normalizer = FakeNormalizer(expected)
    submitted: list[Task] = []

    async def submit(task: Task) -> None:
        submitted.append(task)

    handler = NormalizationHandler(FakeParsedStore(parsed), store, normalizer, submit)
    task = _task(parsed.id)

    await handler.handle(task)

    assert len(normalizer.seen) == 1
    assert normalizer.seen[0].id == parsed.id
    assert store.saved == [(expected, task.trace_id)]
    assert len(submitted) == 1
    assert submitted[0].task_type == TaskType.DEDUPLICATE_EVENT
    assert submitted[0].payload == {"event_id": str(expected.event_id)}


async def test_normalization_handler_raises_when_parsed_missing():
    handler = NormalizationHandler(
        FakeParsedStore(None),
        FakeNormalizedStore(),
        FakeNormalizer(None),
    )
    with pytest.raises(ValueError, match="ParsedRecord not found"):
        await handler.handle(_task(uuid4()))


async def test_normalization_handler_skips_when_llm_returns_none():
    parsed = FakeParsedRecord()
    store = FakeNormalizedStore()
    handler = NormalizationHandler(FakeParsedStore(parsed), store, FakeNormalizer(None))

    await handler.handle(_task(parsed.id))

    assert store.saved == []


def test_build_event_uses_llm_address_and_clamps_confidence():
    parsed = ParsedRecordSchema(
        id=uuid4(),
        raw_record_id=uuid4(),
        source_id=uuid4(),
        start_time=datetime(2026, 5, 12, 3, 0, tzinfo=UTC),
        end_time=datetime(2026, 5, 12, 9, 0, tzinfo=UTC),
        location_city="Томск",
        location_district="Томский р-н",
        location_street="ул. Весенняя",
        location_region_code="Томская обл",
        reason="Ремонтные работы",
        extra={},
        trace_id=uuid4(),
        extracted_at=datetime.now(UTC),
    )

    event = _build_event(
        parsed,
        {
            "event_type": "maintenance",
            "location": {
                "raw": "Томская обл, Томск, ул. Весенняя",
                "normalized": "томск, улица весенняя",
                "city": "Томск",
                "street": "улица Весенняя",
                "building": "",
            },
            "confidence": 2,
        },
    )

    assert event is not None
    assert event.parsed_record_id == parsed.id
    assert event.event_type == EventType.MAINTENANCE
    assert event.location.normalized == "томск, улица весенняя"
    assert event.location.building is None
    assert event.confidence == 1.0
