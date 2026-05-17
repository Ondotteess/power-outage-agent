from __future__ import annotations

import argparse
import asyncio
import sys
from datetime import UTC, datetime, timedelta
from uuid import uuid4

from sqlalchemy import delete, func, or_, select

from app.db.engine import async_session_factory, init_db
from app.db.models import NormalizedEvent, Office, OfficeImpact
from app.matching.office_matcher import MatchableEvent, MatchableOffice, OfficeMatcher


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Rebuild office_impacts for normalized events with the current matcher."
    )
    parser.add_argument(
        "--future-days",
        type=int,
        default=7,
        help="Only rebuild non-expired events starting within this horizon.",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Rebuild all normalized events instead of only the upcoming horizon.",
    )
    parser.add_argument("--limit", type=int, default=None)
    return parser.parse_args()


async def _load_inputs(
    *,
    future_days: int,
    all_events: bool,
    limit: int | None,
) -> tuple[list[Office], list[NormalizedEvent]]:
    now = datetime.now(UTC)
    event_stmt = select(NormalizedEvent).order_by(NormalizedEvent.start_time)
    if not all_events:
        event_stmt = event_stmt.where(
            NormalizedEvent.start_time <= now + timedelta(days=future_days)
        ).where(or_(NormalizedEvent.end_time.is_(None), NormalizedEvent.end_time >= now))
    if limit is not None:
        event_stmt = event_stmt.limit(max(0, limit))

    async with async_session_factory() as session:
        offices_result = await session.execute(select(Office).where(Office.is_active.is_(True)))
        events_result = await session.execute(event_stmt)
        return list(offices_result.scalars().all()), list(events_result.scalars().all())


async def _active_or_future_impacts() -> int:
    now = datetime.now(UTC)
    async with async_session_factory() as session:
        result = await session.execute(
            select(func.count(OfficeImpact.id)).where(
                or_(OfficeImpact.impact_end.is_(None), OfficeImpact.impact_end >= now)
            )
        )
        return result.scalar() or 0


async def main() -> int:
    args = _parse_args()
    await init_db()

    offices, events = await _load_inputs(
        future_days=args.future_days,
        all_events=args.all,
        limit=args.limit,
    )
    before = await _active_or_future_impacts()

    matcher = OfficeMatcher([_to_matchable_office(office) for office in offices])
    trace_id = uuid4()
    detected_at = datetime.now(UTC)
    rows: list[OfficeImpact] = []
    event_ids = [event.event_id for event in events]
    for index, event in enumerate(events, start=1):
        for match in matcher.match(_to_matchable_event(event), now=detected_at):
            rows.append(
                OfficeImpact(
                    id=uuid4(),
                    office_id=match.office.id,
                    event_id=event.event_id,
                    impact_start=event.start_time,
                    impact_end=event.end_time,
                    impact_level=str(match.impact_level),
                    match_strategy=match.match_strategy,
                    match_score=match.match_score,
                    match_explanation=list(match.explanation),
                    trace_id=trace_id,
                    detected_at=detected_at,
                )
            )
        if index % 500 == 0:
            print(f"  matched_events: {index}/{len(events)}", flush=True)

    async with async_session_factory() as session:
        if event_ids:
            await session.execute(delete(OfficeImpact).where(OfficeImpact.event_id.in_(event_ids)))
        session.add_all(rows)
        await session.commit()

    after = await _active_or_future_impacts()
    print("Office impacts rebuild complete")
    print(f"  events_rebuilt: {len(events)}")
    print(f"  impacts_inserted: {len(rows)}")
    print(f"  active_or_future_impacts_before: {before}")
    print(f"  active_or_future_impacts_after: {after}")
    return 0


def _to_matchable_office(office: Office) -> MatchableOffice:
    return MatchableOffice(
        id=office.id,
        name=office.name,
        city=office.city,
        address=office.address,
        region=office.region,
    )


def _to_matchable_event(event: NormalizedEvent) -> MatchableEvent:
    return MatchableEvent(
        event_id=event.event_id,
        event_type=event.event_type,
        start_time=event.start_time,
        end_time=event.end_time,
        location_raw=event.location_raw,
        location_normalized=event.location_normalized,
        location_city=event.location_city,
        location_street=event.location_street,
        location_building=event.location_building,
    )


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
