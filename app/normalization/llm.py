"""LLM-based normalizer for parsed outage records.

Uses Sber GigaChat as the underlying chat API. Transport-level concerns
(OAuth, HTTP, JSON parsing) live in `gigachat_client.py`; this module owns
the prompt, the request shape, and the conversion of the LLM reply into
a validated `NormalizedEventSchema`.

The class exposes the same `NormalizerProtocol` contract that
`app.workers.normalizer.NormalizationHandler` expects, so swapping the
underlying provider later is a one-class change.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import time
from collections import deque
from datetime import UTC, datetime
from typing import Any, Protocol
from uuid import UUID, uuid4

from app.config import settings
from app.models.schemas import (
    EventType,
    LocationSchema,
    NormalizedEventSchema,
    ParsedRecordSchema,
)
from app.normalization.address import canonical_key
from app.normalization.gigachat_client import (
    GigaChatClient,
    GigaChatError,
    GigaChatInvalidResponseError,
)


class LLMCallStoreProtocol(Protocol):
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
    ) -> None: ...


logger = logging.getLogger(__name__)

_SYSTEM_PROMPT = """You normalize Russian planned power outage records.
Return ONLY a valid JSON object. No prose, no markdown, no ``` fences.
Do not invent missing house numbers or addresses.
Use UTC ISO-8601 datetimes.
Classify event_type as one of: power_outage, maintenance, infrastructure_failure, other.
Set confidence from 0.0 to 1.0. Lower it when city, street, or time is missing.
Return structured address parts; text-level normalization (abbreviations, casing,
canonical key) happens deterministically downstream — keep `city`, `street`,
`building` as you read them, just split them apart.

Expected JSON shape:
{
  "event_type": "...",
  "start_time": "ISO8601 | null",
  "end_time": "ISO8601 | null",
  "location": {
    "raw": "string",
    "city": "string | null",
    "street": "string | null",
    "building": "string | null"
  },
  "reason": "string | null",
  "confidence": 0.0
}
"""


class LLMNormalizer:
    """Normalizes parsed outage records via Sber GigaChat.

    GigaChat is required to come up via settings (`gigachat_*` fields). The
    client is constructed lazily so that import-time failures (missing key)
    don't break the rest of the app.
    """

    def __init__(
        self,
        client: GigaChatClient | None = None,
        *,
        call_store: LLMCallStoreProtocol | None = None,
        rate_limit_per_minute: int | None = None,
    ) -> None:
        self._client = client
        self._call_store = call_store
        self._rate_limiter = AsyncRateLimiter(
            rate_limit_per_minute
            if rate_limit_per_minute is not None
            else settings.llm_normalization_rate_per_minute
        )

    def _get_client(self) -> GigaChatClient:
        if self._client is None:
            self._client = GigaChatClient(
                auth_key=settings.gigachat_auth_key,
                client_id=settings.gigachat_client_id,
                client_secret=settings.gigachat_client_secret,
                scope=settings.gigachat_scope,
                base_url=settings.gigachat_base_url,
                oauth_url=settings.gigachat_oauth_url,
                model=settings.gigachat_model,
                verify_ssl=settings.gigachat_verify_ssl,
            )
        return self._client

    async def _record_call(
        self,
        *,
        client: GigaChatClient,
        response: dict | None,
        duration_ms: int,
        status: str,
        trace_id: UUID | None,
    ) -> None:
        if self._call_store is None:
            return
        usage = (
            GigaChatClient.extract_usage(response)
            if response is not None
            else {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
        )
        try:
            await self._call_store.record(
                model=client.model_name,
                prompt_tokens=usage["prompt_tokens"],
                completion_tokens=usage["completion_tokens"],
                total_tokens=usage["total_tokens"],
                duration_ms=duration_ms,
                status=status,
                trace_id=trace_id,
            )
        except Exception:  # noqa: BLE001 — metrics must never break normalization
            logger.exception("LLMNormalizer  call_store.record failed (metric drop)")

    async def normalize(self, record: ParsedRecordSchema) -> NormalizedEventSchema | None:
        payload = _record_payload(record)
        logger.debug("LLMNormalizer  request  parsed_record_id=%s", record.id)

        try:
            client = self._get_client()
        except GigaChatError as exc:
            # Misconfiguration is fatal for every call until fixed; log once per call
            # but do not crash the handler.
            logger.error(
                "LLMNormalizer  client init failed  parsed_record_id=%s  error=%s",
                record.id,
                exc,
            )
            return None

        started = time.perf_counter()
        response: dict | None = None
        try:
            await self._rate_limiter.wait()
            response = await client.chat_completion(
                messages=[
                    {"role": "system", "content": _SYSTEM_PROMPT},
                    {
                        "role": "user",
                        "content": (
                            "Normalize this parsed outage record. Return JSON with keys: "
                            "event_type, start_time, end_time, location, reason, confidence.\n\n"
                            f"{json.dumps(payload, ensure_ascii=False, default=str)}"
                        ),
                    },
                ],
                temperature=0.0,
            )
        except GigaChatInvalidResponseError as exc:
            duration_ms = int((time.perf_counter() - started) * 1000)
            await self._record_call(
                client=client,
                response=None,
                duration_ms=duration_ms,
                status="error",
                trace_id=record.trace_id,
            )
            logger.warning(
                "LLMNormalizer  invalid response shape  parsed_record_id=%s  error=%s",
                record.id,
                exc,
            )
            return None
        except GigaChatError as exc:
            duration_ms = int((time.perf_counter() - started) * 1000)
            await self._record_call(
                client=client,
                response=None,
                duration_ms=duration_ms,
                status="error",
                trace_id=record.trace_id,
            )
            logger.error(
                "LLMNormalizer  API error  parsed_record_id=%s  error=%s",
                record.id,
                exc,
            )
            raise

        duration_ms = int((time.perf_counter() - started) * 1000)
        await self._record_call(
            client=client,
            response=response,
            duration_ms=duration_ms,
            status="ok",
            trace_id=record.trace_id,
        )

        try:
            content = GigaChatClient.extract_message_content(response)
        except GigaChatInvalidResponseError as exc:
            logger.warning(
                "LLMNormalizer  bad chat shape  parsed_record_id=%s  error=%s",
                record.id,
                exc,
            )
            return None

        cleaned = _strip_json_fences(content)
        try:
            data = json.loads(cleaned)
        except json.JSONDecodeError:
            logger.warning(
                "LLMNormalizer  invalid JSON  parsed_record_id=%s  content=%r",
                record.id,
                content[:500],
            )
            return None

        if not isinstance(data, dict):
            logger.warning(
                "LLMNormalizer  JSON is not an object  parsed_record_id=%s  type=%s",
                record.id,
                type(data).__name__,
            )
            return None

        return _build_event(record, data)


class AsyncRateLimiter:
    """Tiny process-local sliding-window limiter for paid LLM calls."""

    def __init__(self, max_calls_per_minute: int) -> None:
        self._max_calls = max(0, max_calls_per_minute)
        self._calls: deque[float] = deque()
        self._lock = asyncio.Lock()

    async def wait(self) -> None:
        if self._max_calls <= 0:
            return

        while True:
            async with self._lock:
                now = time.monotonic()
                cutoff = now - 60.0
                while self._calls and self._calls[0] <= cutoff:
                    self._calls.popleft()
                if len(self._calls) < self._max_calls:
                    self._calls.append(now)
                    return
                sleep_for = max(0.05, 60.0 - (now - self._calls[0]))
            await asyncio.sleep(sleep_for)


def _record_payload(record: ParsedRecordSchema) -> dict[str, Any]:
    return {
        "parsed_record_id": str(record.id),
        "raw_record_id": str(record.raw_record_id),
        "source_id": str(record.source_id) if record.source_id else None,
        "external_id": record.external_id,
        "start_time": record.start_time.isoformat() if record.start_time else None,
        "end_time": record.end_time.isoformat() if record.end_time else None,
        "location": {
            "region": record.location_region_code,
            "district": record.location_district,
            "city": record.location_city,
            "street": record.location_street,
            "extra": record.extra,
        },
        "reason": record.reason,
    }


def _strip_json_fences(text: str) -> str:
    """Remove leading ```json / ``` fences if the model added them despite instructions."""
    s = text.strip()
    if s.startswith("```"):
        s = re.sub(r"^```(?:json)?\s*", "", s)
        s = re.sub(r"\s*```\s*$", "", s)
    return s.strip()


def _build_event(record: ParsedRecordSchema, data: dict[str, Any]) -> NormalizedEventSchema | None:
    start_time = _parse_dt(data.get("start_time")) or record.start_time
    if start_time is None:
        logger.warning("LLMNormalizer  no start_time  parsed_record_id=%s", record.id)
        return None

    location = data.get("location") if isinstance(data.get("location"), dict) else {}
    raw_location = location.get("raw") or _raw_location(record)
    city = _clean(location.get("city")) or record.location_city
    street = _clean(location.get("street")) or record.location_street
    building = _clean(location.get("building"))

    return NormalizedEventSchema(
        event_id=uuid4(),
        parsed_record_id=record.id,
        event_type=_event_type(data.get("event_type")),
        start_time=start_time,
        end_time=_parse_dt(data.get("end_time")) or record.end_time,
        location=LocationSchema(
            raw=raw_location,
            # Deterministic key, computed from structured fields. LLM's own
            # `normalized` string is dropped — different runs produced
            # different casings/spacings and dedup compared them with `==`.
            normalized=canonical_key(city, street, building),
            city=city,
            street=street,
            building=building,
        ),
        reason=_clean(data.get("reason")) or record.reason,
        sources=[record.raw_record_id],
        confidence=_confidence(data.get("confidence")),
    )


def _parse_dt(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    candidate = value.strip().replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(candidate)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _event_type(value: Any) -> EventType:
    try:
        return EventType(str(value))
    except ValueError:
        return EventType.POWER_OUTAGE


def _confidence(value: Any) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return 0.0
    return max(0.0, min(1.0, number))


def _clean(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _raw_location(record: ParsedRecordSchema) -> str:
    parts = [
        record.location_region_code,
        record.location_district,
        record.location_city,
        record.location_street,
    ]
    houses = record.extra.get("houses") if isinstance(record.extra, dict) else None
    if houses:
        parts.append(str(houses))
    return ", ".join(part.strip() for part in parts if part and part.strip())
