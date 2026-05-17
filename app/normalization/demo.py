from __future__ import annotations

import re
from datetime import UTC, datetime, timedelta
from uuid import uuid4

from app.models.schemas import (
    EventType,
    LocationSchema,
    NormalizedEventSchema,
    ParsedRecordSchema,
)
from app.normalization.address import canonical_key

_HOUSE_RE = re.compile(r"(?<!\d)(\d{1,4})(?:/\d{1,4})?[a-zа-я]?", re.IGNORECASE)


class DemoNormalizer:
    """Deterministic normalizer for local end-to-end demo runs.

    It keeps the same NormalizerProtocol as the production fallback but avoids external
    credentials and network calls, so `--demo-e2e` is repeatable.
    """

    async def normalize(self, record: ParsedRecordSchema) -> NormalizedEventSchema | None:
        start_time = record.start_time or datetime.now(UTC) + timedelta(hours=2)
        end_time = record.end_time or start_time + timedelta(hours=4)
        street, building = _street_and_building(record.location_street, record.extra)
        raw_location = _raw_location(record, street, building)

        return NormalizedEventSchema(
            event_id=uuid4(),
            parsed_record_id=record.id,
            event_type=EventType.POWER_OUTAGE,
            start_time=start_time,
            end_time=end_time,
            location=LocationSchema(
                raw=raw_location,
                normalized=canonical_key(record.location_city, street, building),
                city=record.location_city,
                street=street,
                building=building,
            ),
            reason=record.reason,
            sources=[record.raw_record_id],
            confidence=0.95 if record.location_city and street else 0.7,
        )


def _street_and_building(street: str | None, extra: dict) -> tuple[str | None, str | None]:
    building = _first_house(extra.get("houses") if isinstance(extra, dict) else None)
    if street is None:
        return None, building

    cleaned = street.strip()
    if building:
        return _strip_house(cleaned, building), building

    matches = list(_HOUSE_RE.finditer(cleaned))
    if not matches:
        return cleaned, None

    match = matches[-1]
    building = match.group(0)
    return cleaned[: match.start()].strip(" ,;"), building


def _first_house(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if text.casefold() in {"", "б/н", "бн", "без номера"}:
        return None
    match = _HOUSE_RE.search(text)
    return match.group(0) if match else None


def _strip_house(street: str, building: str) -> str:
    return re.sub(rf"[,;\s]*(?:дом|д\.)?\s*{re.escape(building)}\b", "", street, flags=re.I).strip(
        " ,;"
    )


def _raw_location(
    record: ParsedRecordSchema,
    street: str | None,
    building: str | None,
) -> str:
    parts = [record.location_city, street]
    if building:
        parts.append(f"дом {building}")
    return ", ".join(part.strip() for part in parts if part and part.strip())
