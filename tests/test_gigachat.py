"""Tests for the GigaChat transport client and the LLMNormalizer wrapper.

NormalizationHandler tests live in tests/test_normalizer.py — they don't care
about the underlying provider and use a FakeNormalizer.
"""

from __future__ import annotations

import base64
import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest

from app.models.schemas import EventType, ParsedRecordSchema
from app.normalization.gigachat_client import (
    GigaChatAuthError,
    GigaChatClient,
    GigaChatError,
    GigaChatHTTPError,
    GigaChatInvalidResponseError,
)
from app.normalization.llm import LLMNormalizer, _strip_json_fences

# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


class FakeGigaChatClient:
    """Stand-in for GigaChatClient. Yields canned content from a queue."""

    def __init__(self, content: str | list[str | Exception] | Exception) -> None:
        if isinstance(content, list):
            self._queue: list[str | Exception] = list(content)
        else:
            self._queue = [content]
        self.calls: list[list[dict]] = []

    async def chat_completion(
        self,
        *,
        messages: list[dict],
        temperature: float = 0.0,
        max_tokens: int | None = None,
    ) -> dict:
        self.calls.append(messages)
        if not self._queue:
            raise AssertionError("FakeGigaChatClient: no more canned responses queued")
        item = self._queue.pop(0)
        if isinstance(item, Exception):
            raise item
        return {"choices": [{"message": {"content": item}}]}


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


def _to_schema(record: FakeParsedRecord) -> ParsedRecordSchema:
    return ParsedRecordSchema(
        id=record.id,
        raw_record_id=record.raw_record_id,
        source_id=record.source_id,
        external_id=record.external_id,
        start_time=record.start_time,
        end_time=record.end_time,
        location_city=record.location_city,
        location_district=record.location_district,
        location_street=record.location_street,
        location_region_code=record.location_region_code,
        reason=record.reason,
        extra=record.extra,
        trace_id=record.trace_id,
        extracted_at=record.extracted_at,
    )


_HAPPY_REPLY = json.dumps(
    {
        "event_type": "maintenance",
        "start_time": "2026-05-12T03:00:00Z",
        "end_time": "2026-05-12T09:00:00Z",
        "location": {
            "raw": "Томская обл, Томск, ул. Весенняя",
            "normalized": "томск, улица весенняя",
            "city": "Томск",
            "street": "улица Весенняя",
            "building": None,
        },
        "reason": "Ремонтные работы",
        "confidence": 0.86,
    }
)


# ---------------------------------------------------------------------------
# GigaChatClient — constructor guard / helpers
# ---------------------------------------------------------------------------


class TestGigaChatClientGuard:
    _kwargs: dict = dict(  # noqa: RUF012
        scope="GIGACHAT_API_PERS",
        base_url="https://example",
        oauth_url="https://example/oauth",
        model="GigaChat-2",
    )

    def test_no_credentials_raises(self):
        with pytest.raises(GigaChatAuthError, match="credentials missing"):
            GigaChatClient(**self._kwargs)

    def test_only_client_id_without_secret_raises(self):
        with pytest.raises(GigaChatAuthError, match="credentials missing"):
            GigaChatClient(client_id="abc", client_secret="", **self._kwargs)

    def test_only_client_secret_without_id_raises(self):
        with pytest.raises(GigaChatAuthError, match="credentials missing"):
            GigaChatClient(client_id="", client_secret="xyz", **self._kwargs)

    def test_auth_key_alone_works(self):
        client = GigaChatClient(auth_key="precomputed-base64", **self._kwargs)
        assert client._auth_key == "precomputed-base64"

    def test_client_id_and_secret_encoded_to_base64(self):
        client = GigaChatClient(client_id="my-id", client_secret="my-secret", **self._kwargs)
        expected = base64.b64encode(b"my-id:my-secret").decode("ascii")
        assert client._auth_key == expected

    def test_auth_key_wins_when_both_supplied(self):
        client = GigaChatClient(
            auth_key="explicit-key",
            client_id="ignored",
            client_secret="ignored",
            **self._kwargs,
        )
        assert client._auth_key == "explicit-key"

    def test_extract_message_content_success(self):
        response = {"choices": [{"message": {"content": "hello"}}]}
        assert GigaChatClient.extract_message_content(response) == "hello"

    def test_extract_message_content_bad_shape_raises(self):
        with pytest.raises(GigaChatInvalidResponseError, match="unexpected"):
            GigaChatClient.extract_message_content({"error": "boom"})


# ---------------------------------------------------------------------------
# JSON fence stripping
# ---------------------------------------------------------------------------


class TestStripJsonFences:
    def test_plain_json_passthrough(self):
        assert _strip_json_fences('{"a": 1}') == '{"a": 1}'

    def test_strips_json_fence(self):
        assert _strip_json_fences('```json\n{"a": 1}\n```') == '{"a": 1}'

    def test_strips_bare_fence(self):
        assert _strip_json_fences('```\n{"a": 1}\n```') == '{"a": 1}'

    def test_strips_with_whitespace(self):
        assert _strip_json_fences('   ```json\n{"a": 1}\n```   ') == '{"a": 1}'


# ---------------------------------------------------------------------------
# LLMNormalizer behaviour against a fake GigaChat client
# ---------------------------------------------------------------------------


class TestLLMNormalizerHappyPath:
    async def test_returns_event_for_valid_json(self):
        client = FakeGigaChatClient(_HAPPY_REPLY)
        normalizer = LLMNormalizer(client=client)
        parsed = _to_schema(FakeParsedRecord())

        event = await normalizer.normalize(parsed)

        assert event is not None
        assert event.event_type == EventType.MAINTENANCE
        assert event.parsed_record_id == parsed.id
        assert event.location.normalized == "томск|весенняя|"
        assert event.confidence == pytest.approx(0.86)
        # sources is auto-filled from the parsed record's raw_record_id
        assert event.sources == [parsed.raw_record_id]

    async def test_strips_markdown_fences(self):
        wrapped = f"```json\n{_HAPPY_REPLY}\n```"
        client = FakeGigaChatClient(wrapped)
        normalizer = LLMNormalizer(client=client)
        event = await normalizer.normalize(_to_schema(FakeParsedRecord()))
        assert event is not None
        assert event.location.normalized == "томск|весенняя|"


class TestLLMNormalizerFailureModes:
    async def test_invalid_json_returns_none(self):
        client = FakeGigaChatClient("this is not json")
        normalizer = LLMNormalizer(client=client)
        result = await normalizer.normalize(_to_schema(FakeParsedRecord()))
        assert result is None

    async def test_non_object_json_returns_none(self):
        client = FakeGigaChatClient('["not", "an", "object"]')
        normalizer = LLMNormalizer(client=client)
        result = await normalizer.normalize(_to_schema(FakeParsedRecord()))
        assert result is None

    async def test_bad_chat_shape_returns_none(self):
        # client.chat_completion raises during extract — we catch and return None
        client = FakeGigaChatClient(GigaChatInvalidResponseError("garbage"))
        normalizer = LLMNormalizer(client=client)
        result = await normalizer.normalize(_to_schema(FakeParsedRecord()))
        assert result is None

    async def test_http_error_reraises(self):
        # Transport-level failures bubble up so Dispatcher's retry logic kicks in
        client = FakeGigaChatClient(GigaChatHTTPError("HTTP 500"))
        normalizer = LLMNormalizer(client=client)
        with pytest.raises(GigaChatError):
            await normalizer.normalize(_to_schema(FakeParsedRecord()))

    async def test_no_start_time_returns_none(self):
        # If neither LLM nor the record provides start_time → cannot build event
        parsed = FakeParsedRecord(start_time=None)
        reply = json.dumps(
            {
                "event_type": "power_outage",
                "start_time": None,
                "end_time": None,
                "location": {"raw": "x"},
                "confidence": 0.5,
            }
        )
        client = FakeGigaChatClient(reply)
        normalizer = LLMNormalizer(client=client)
        result = await normalizer.normalize(_to_schema(parsed))
        assert result is None


class TestLLMNormalizerLazyInit:
    async def test_no_credentials_returns_none_without_crashing(self, monkeypatch):
        # Wipe credentials so the lazy client construction fails
        import app.normalization.llm as llm

        monkeypatch.setattr(llm.settings, "gigachat_auth_key", "")
        monkeypatch.setattr(llm.settings, "gigachat_client_id", "")
        monkeypatch.setattr(llm.settings, "gigachat_client_secret", "")

        normalizer = LLMNormalizer()  # no client injected, will try settings
        result = await normalizer.normalize(_to_schema(FakeParsedRecord()))
        assert result is None
