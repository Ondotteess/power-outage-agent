from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol
from uuid import uuid4

from app.models.schemas import NormalizedEventSchema, ParsedRecordSchema


class NormalizerProtocol(Protocol):
    async def normalize(self, record: ParsedRecordSchema) -> NormalizedEventSchema | None: ...


@dataclass(frozen=True)
class QualityCase:
    name: str
    parsed: ParsedRecordSchema
    expected_event_type: str
    expected_canonical_key: str | None
    min_confidence: float = 0.0


async def evaluate_normalizer(
    normalizer: NormalizerProtocol,
    cases: list[QualityCase],
) -> dict:
    total = len(cases)
    normalized = 0
    event_type_hits = 0
    address_hits = 0
    confidence_hits = 0
    failures: list[dict] = []

    for case in cases:
        event = await normalizer.normalize(case.parsed)
        if event is None:
            failures.append({"case": case.name, "reason": "normalizer returned None"})
            continue

        normalized += 1
        event_type_ok = str(event.event_type) == case.expected_event_type
        address_ok = event.location.normalized == case.expected_canonical_key
        confidence_ok = event.confidence >= case.min_confidence

        event_type_hits += int(event_type_ok)
        address_hits += int(address_ok)
        confidence_hits += int(confidence_ok)

        if not (event_type_ok and address_ok and confidence_ok):
            failures.append(
                {
                    "case": case.name,
                    "expected_event_type": case.expected_event_type,
                    "actual_event_type": str(event.event_type),
                    "expected_canonical_key": case.expected_canonical_key,
                    "actual_canonical_key": event.location.normalized,
                    "min_confidence": case.min_confidence,
                    "actual_confidence": event.confidence,
                }
            )

    return {
        "total": total,
        "normalized": normalized,
        "normalization_rate": _rate(normalized, total),
        "event_type_accuracy": _rate(event_type_hits, total),
        "address_accuracy": _rate(address_hits, total),
        "confidence_pass_rate": _rate(confidence_hits, total),
        "failures": failures,
    }


def load_quality_cases(path: str | Path) -> list[QualityCase]:
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    cases: list[QualityCase] = []
    for item in raw:
        parsed = item["parsed"]
        cases.append(
            QualityCase(
                name=item["name"],
                parsed=ParsedRecordSchema(
                    id=uuid4(),
                    raw_record_id=uuid4(),
                    source_id=None,
                    external_id=parsed.get("external_id"),
                    start_time=_parse_dt(parsed.get("start_time")),
                    end_time=_parse_dt(parsed.get("end_time")),
                    location_city=parsed.get("location_city"),
                    location_district=parsed.get("location_district"),
                    location_street=parsed.get("location_street"),
                    location_region_code=parsed.get("location_region_code"),
                    reason=parsed.get("reason"),
                    extra=parsed.get("extra") or {},
                    trace_id=uuid4(),
                    extracted_at=datetime.now(UTC),
                ),
                expected_event_type=item["expected"]["event_type"],
                expected_canonical_key=item["expected"].get("canonical_key"),
                min_confidence=float(item["expected"].get("min_confidence", 0.0)),
            )
        )
    return cases


def _parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _rate(numerator: int, denominator: int) -> float:
    if denominator == 0:
        return 0.0
    return round(numerator / denominator, 4)
