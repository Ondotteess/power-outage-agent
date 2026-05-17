from __future__ import annotations

import argparse
import asyncio
import json
from dataclasses import asdict, dataclass

from sqlalchemy import select

from app.config import settings
from app.db.engine import async_session_factory
from app.db.models import ParsedRecord
from app.models.schemas import ParsedRecordSchema
from app.normalization.automaton import AutomatonNormalizer, FallbackNormalizer, RegexNormalizer


@dataclass(frozen=True)
class Sample:
    parsed_id: str
    city: str | None
    street: str | None
    extra: dict
    automaton_key: str | None
    regex_key: str | None
    regex_city: str | None
    regex_street: str | None
    regex_building: str | None


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compare automaton-only vs automaton+regex")
    parser.add_argument("--limit", type=int, default=0, help="Max parsed records to compare; 0 = all")
    parser.add_argument(
        "--threshold",
        type=float,
        default=settings.normalizer_fallback_threshold,
        help="Fallback threshold used by FallbackNormalizer",
    )
    parser.add_argument("--samples", type=int, default=8, help="Number of changed samples to print")
    return parser.parse_args()


async def _load_records(limit: int) -> list[ParsedRecord]:
    stmt = select(ParsedRecord).order_by(ParsedRecord.extracted_at.desc())
    if limit > 0:
        stmt = stmt.limit(limit)
    async with async_session_factory() as session:
        result = await session.execute(stmt)
        return list(result.scalars().all())


def _to_schema(record: ParsedRecord) -> ParsedRecordSchema:
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
        extra=record.extra or {},
        trace_id=record.trace_id,
        extracted_at=record.extracted_at,
    )


async def main() -> int:
    args = _parse_args()
    records = await _load_records(args.limit)
    automaton = AutomatonNormalizer()
    combined = FallbackNormalizer(AutomatonNormalizer(), RegexNormalizer(), threshold=args.threshold)

    automaton_normalized = 0
    combined_normalized = 0
    regex_path = 0
    none_before = 0
    none_after = 0
    added_by_regex = 0
    changed_key = 0
    added_samples: list[Sample] = []
    changed_samples: list[Sample] = []

    for row in records:
        parsed = _to_schema(row)
        auto_event = automaton.parse(parsed).event
        combined_event = await combined.normalize(parsed)

        if auto_event is not None:
            automaton_normalized += 1
        else:
            none_before += 1

        if combined_event is not None:
            combined_normalized += 1
        else:
            none_after += 1

        if combined.last_path == FallbackNormalizer.PATH_REGEX_FALLBACK:
            regex_path += 1

        auto_key = auto_event.location.normalized if auto_event else None
        regex_key = combined_event.location.normalized if combined_event else None
        if auto_event is None and combined_event is not None:
            added_by_regex += 1
        if auto_event is not None and combined_event is not None and auto_key != regex_key:
            changed_key += 1

        sample = Sample(
            parsed_id=str(parsed.id),
            city=parsed.location_city,
            street=parsed.location_street,
            extra=parsed.extra,
            automaton_key=auto_key,
            regex_key=regex_key,
            regex_city=combined_event.location.city if combined_event else None,
            regex_street=combined_event.location.street if combined_event else None,
            regex_building=combined_event.location.building if combined_event else None,
        )
        if auto_event is None and combined_event is not None and len(added_samples) < args.samples:
            added_samples.append(sample)
        elif (
            auto_event is not None
            and combined_event is not None
            and auto_key != regex_key
            and len(changed_samples) < args.samples
        ):
            changed_samples.append(sample)

    report = {
        "threshold": args.threshold,
        "total": len(records),
        "automaton_only": {
            "normalized": automaton_normalized,
            "none": none_before,
            "normalization_rate": _rate(automaton_normalized, len(records)),
        },
        "automaton_plus_regex": {
            "normalized": combined_normalized,
            "none": none_after,
            "normalization_rate": _rate(combined_normalized, len(records)),
            "regex_path": regex_path,
            "added_by_regex": added_by_regex,
            "changed_key": changed_key,
        },
        "added_samples": [asdict(sample) for sample in added_samples],
        "changed_samples": [asdict(sample) for sample in changed_samples],
    }
    print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
    return 0


def _rate(value: int, total: int) -> float:
    return round(value / total, 4) if total else 0.0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
