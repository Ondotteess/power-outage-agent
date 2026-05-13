"""Read-only DB queries for the admin API.

Kept separate from `app/db/repositories.py` so the pipeline's write-path code
is not touched. All functions take an `AsyncSession` (injected via FastAPI
dependency in `app/api/deps.py`).
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import UUID

from sqlalchemy import and_, desc, distinct, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import (
    DedupEvent,
    NormalizedEvent,
    Notification,
    Office,
    OfficeImpact,
    ParsedRecord,
    RawRecord,
    Source,
    TaskRecord,
)

SYSTEM_ABANDONED_TASK_ERROR = "abandoned by previous pipeline process"


def _exclude_system_failures(stmt):
    return stmt.where(
        or_(
            TaskRecord.status != "failed",
            TaskRecord.error.is_(None),
            TaskRecord.error != SYSTEM_ABANDONED_TASK_ERROR,
        )
    )


async def list_sources(session: AsyncSession) -> list[Source]:
    result = await session.execute(select(Source).order_by(Source.created_at.desc()))
    return list(result.scalars().all())


async def get_source(session: AsyncSession, source_id: UUID) -> Source | None:
    result = await session.execute(select(Source).where(Source.id == source_id))
    return result.scalar_one_or_none()


async def count_sources(session: AsyncSession, *, active_only: bool = False) -> int:
    stmt = select(func.count(Source.id))
    if active_only:
        stmt = stmt.where(Source.is_active.is_(True))
    result = await session.execute(stmt)
    return result.scalar() or 0


async def list_raw(
    session: AsyncSession, *, limit: int = 50, offset: int = 0, source_id: UUID | None = None
) -> list[RawRecord]:
    stmt = select(RawRecord).order_by(desc(RawRecord.fetched_at)).limit(limit).offset(offset)
    if source_id:
        stmt = stmt.where(RawRecord.source_id == source_id)
    result = await session.execute(stmt)
    return list(result.scalars().all())


async def count_raw_since(session: AsyncSession, since: datetime) -> int:
    result = await session.execute(
        select(func.count(RawRecord.id)).where(RawRecord.fetched_at >= since)
    )
    return result.scalar() or 0


async def count_raw_per_source_since(session: AsyncSession, since: datetime) -> dict[UUID, int]:
    result = await session.execute(
        select(RawRecord.source_id, func.count(RawRecord.id))
        .where(RawRecord.fetched_at >= since)
        .group_by(RawRecord.source_id)
    )
    return {row[0]: row[1] for row in result.all() if row[0] is not None}


async def last_fetch_per_source(session: AsyncSession) -> dict[UUID, datetime]:
    result = await session.execute(
        select(RawRecord.source_id, func.max(RawRecord.fetched_at)).group_by(RawRecord.source_id)
    )
    return {row[0]: row[1] for row in result.all() if row[0] is not None}


async def list_parsed(
    session: AsyncSession,
    *,
    limit: int = 50,
    offset: int = 0,
    source_id: UUID | None = None,
    city: str | None = None,
) -> list[ParsedRecord]:
    stmt = (
        select(ParsedRecord)
        .order_by(desc(ParsedRecord.start_time).nulls_last(), desc(ParsedRecord.extracted_at))
        .limit(limit)
        .offset(offset)
    )
    if source_id:
        stmt = stmt.where(ParsedRecord.source_id == source_id)
    if city:
        stmt = stmt.where(ParsedRecord.location_city.ilike(f"%{city}%"))
    result = await session.execute(stmt)
    return list(result.scalars().all())


async def count_parsed_since(session: AsyncSession, since: datetime) -> int:
    result = await session.execute(
        select(func.count(ParsedRecord.id)).where(ParsedRecord.extracted_at >= since)
    )
    return result.scalar() or 0


async def list_normalized(
    session: AsyncSession,
    *,
    limit: int = 50,
    offset: int = 0,
) -> list[NormalizedEvent]:
    stmt = (
        select(NormalizedEvent)
        .order_by(desc(NormalizedEvent.start_time))
        .limit(limit)
        .offset(offset)
    )
    result = await session.execute(stmt)
    return list(result.scalars().all())


async def list_offices(session: AsyncSession) -> list[Office]:
    result = await session.execute(select(Office).order_by(Office.name))
    return list(result.scalars().all())


async def list_office_impacts(
    session: AsyncSession,
    *,
    limit: int = 100,
    offset: int = 0,
) -> list[tuple[OfficeImpact, Office]]:
    result = await session.execute(
        select(OfficeImpact, Office)
        .join(Office, Office.id == OfficeImpact.office_id)
        .order_by(desc(OfficeImpact.detected_at))
        .limit(limit)
        .offset(offset)
    )
    return [(impact, office) for impact, office in result.all()]


async def list_map_office_rows(
    session: AsyncSession,
    *,
    now: datetime,
) -> list[tuple[Office, OfficeImpact | None, NormalizedEvent | None]]:
    result = await session.execute(
        select(Office, OfficeImpact, NormalizedEvent)
        .outerjoin(
            OfficeImpact,
            and_(
                OfficeImpact.office_id == Office.id,
                OfficeImpact.impact_start <= now,
                or_(OfficeImpact.impact_end.is_(None), OfficeImpact.impact_end >= now),
            ),
        )
        .outerjoin(NormalizedEvent, NormalizedEvent.event_id == OfficeImpact.event_id)
        .order_by(Office.name, desc(OfficeImpact.impact_start))
    )
    return [(office, impact, event) for office, impact, event in result.all()]


async def list_notifications(
    session: AsyncSession,
    *,
    limit: int = 100,
    offset: int = 0,
) -> list[tuple[Notification, Office]]:
    result = await session.execute(
        select(Notification, Office)
        .join(Office, Office.id == Notification.office_id)
        .order_by(desc(Notification.emitted_at))
        .limit(limit)
        .offset(offset)
    )
    return [(notification, office) for notification, office in result.all()]


async def count_active_office_impacts(session: AsyncSession, now: datetime) -> int:
    result = await session.execute(
        select(func.count(distinct(OfficeImpact.office_id))).where(
            OfficeImpact.impact_start <= now,
            or_(OfficeImpact.impact_end.is_(None), OfficeImpact.impact_end >= now),
        )
    )
    return result.scalar() or 0


async def count_dedup_events_since(session: AsyncSession, since: datetime) -> int:
    result = await session.execute(
        select(func.count(DedupEvent.id)).where(DedupEvent.created_at >= since)
    )
    return result.scalar() or 0


async def confidence_distribution(session: AsyncSession) -> dict[str, int]:
    """Return counts of normalized events bucketed by confidence."""
    result = await session.execute(select(NormalizedEvent.confidence))
    values = [row[0] or 0.0 for row in result.all()]
    high = sum(1 for v in values if v >= 0.8)
    medium = sum(1 for v in values if 0.5 <= v < 0.8)
    low = sum(1 for v in values if v < 0.5)
    return {"high": high, "medium": medium, "low": low, "total": len(values)}


async def list_tasks(
    session: AsyncSession,
    *,
    status: str | None = None,
    limit: int = 100,
    offset: int = 0,
    include_system_failures: bool = False,
) -> list[TaskRecord]:
    stmt = select(TaskRecord).order_by(desc(TaskRecord.updated_at)).limit(limit).offset(offset)
    if status:
        stmt = stmt.where(TaskRecord.status == status)
    if not include_system_failures:
        stmt = _exclude_system_failures(stmt)
    result = await session.execute(stmt)
    return list(result.scalars().all())


async def get_task(session: AsyncSession, task_id: UUID) -> TaskRecord | None:
    result = await session.execute(select(TaskRecord).where(TaskRecord.id == task_id))
    return result.scalar_one_or_none()


async def count_tasks_by_status(session: AsyncSession) -> dict[str, int]:
    stmt = _exclude_system_failures(
        select(TaskRecord.status, func.count(TaskRecord.id)).group_by(TaskRecord.status)
    )
    result = await session.execute(stmt)
    return {row[0]: row[1] for row in result.all()}


async def count_tasks_by_type_status(session: AsyncSession) -> dict[tuple[str, str], int]:
    stmt = _exclude_system_failures(
        select(TaskRecord.task_type, TaskRecord.status, func.count(TaskRecord.id)).group_by(
            TaskRecord.task_type, TaskRecord.status
        )
    )
    result = await session.execute(stmt)
    return {(row[0], row[1]): row[2] for row in result.all()}


async def recent_task_updates(session: AsyncSession, *, limit: int = 30) -> list[TaskRecord]:
    result = await session.execute(
        select(TaskRecord).order_by(desc(TaskRecord.updated_at)).limit(limit)
    )
    return list(result.scalars().all())


def utc_day_start() -> datetime:
    now = datetime.now(UTC)
    return now.replace(hour=0, minute=0, second=0, microsecond=0)


def utc_window(hours: int) -> datetime:
    return datetime.now(UTC) - timedelta(hours=hours)
