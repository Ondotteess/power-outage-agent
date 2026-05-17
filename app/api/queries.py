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
    EventLog,
    LLMCall,
    NormalizedEvent,
    Notification,
    Office,
    OfficeImpact,
    ParsedRecord,
    QueueDepthSnapshot,
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


async def silent_sources(
    session: AsyncSession,
    *,
    multiplier: float = 3.0,
    now: datetime | None = None,
) -> list[dict]:
    """Return active sources whose most recent RawRecord is older than
    `multiplier × poll_interval_seconds` (or which have never produced one).

    Used by the ParserHealthWatchdog — a source that stops emitting is
    almost always a parser breakage (selector drift / API contract change),
    and the DLQ alone doesn't surface it because fetches still "succeed"
    structurally, just yield zero records.
    """
    when = now or datetime.now(UTC)
    result = await session.execute(select(Source).where(Source.is_active.is_(True)))
    sources = list(result.scalars().all())

    raw_fetch = await last_fetch_per_source(session)
    silent: list[dict] = []
    for source in sources:
        threshold = timedelta(seconds=source.poll_interval_seconds * multiplier)
        last = raw_fetch.get(source.id)
        if last is None or (when - last) > threshold:
            silent.append(
                {
                    "id": source.id,
                    "name": source.name,
                    "url": source.url,
                    "poll_interval_seconds": source.poll_interval_seconds,
                    "last_fetched_at": last,
                    "silent_for_seconds": int((when - last).total_seconds()) if last else None,
                }
            )
    return silent


async def source_success_rates(session: AsyncSession, since: datetime) -> dict[UUID, float]:
    result = await session.execute(
        select(TaskRecord.payload, TaskRecord.status)
        .where(TaskRecord.task_type == "fetch_source")
        .where(TaskRecord.updated_at >= since)
    )
    totals: dict[UUID, int] = {}
    successes: dict[UUID, int] = {}
    for payload, status in result.all():
        if not isinstance(payload, dict) or not payload.get("source_id"):
            continue
        try:
            source_id = UUID(str(payload["source_id"]))
        except ValueError:
            continue
        totals[source_id] = totals.get(source_id, 0) + 1
        if status == "done":
            successes[source_id] = successes.get(source_id, 0) + 1
    return {source_id: successes.get(source_id, 0) / total for source_id, total in totals.items()}


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
    horizon_until: datetime,
) -> list[
    tuple[
        Office,
        OfficeImpact | None,
        NormalizedEvent | None,
        ParsedRecord | None,
        RawRecord | None,
        Source | None,
    ]
]:
    result = await session.execute(
        select(Office, OfficeImpact, NormalizedEvent, ParsedRecord, RawRecord, Source)
        .outerjoin(
            OfficeImpact,
            and_(
                OfficeImpact.office_id == Office.id,
                OfficeImpact.impact_start <= horizon_until,
                or_(OfficeImpact.impact_end.is_(None), OfficeImpact.impact_end >= now),
            ),
        )
        .outerjoin(NormalizedEvent, NormalizedEvent.event_id == OfficeImpact.event_id)
        .outerjoin(ParsedRecord, ParsedRecord.id == NormalizedEvent.parsed_record_id)
        .outerjoin(RawRecord, RawRecord.id == ParsedRecord.raw_record_id)
        .outerjoin(Source, Source.id == RawRecord.source_id)
        .order_by(Office.name, desc(OfficeImpact.impact_start))
    )
    return [
        (office, impact, event, parsed, raw, source)
        for office, impact, event, parsed, raw, source in result.all()
    ]


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


async def count_active_office_impacts(
    session: AsyncSession,
    now: datetime,
    *,
    horizon_until: datetime | None = None,
) -> int:
    until = horizon_until or now
    result = await session.execute(
        select(func.count(distinct(OfficeImpact.office_id))).where(
            OfficeImpact.impact_start <= until,
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


# ---------------------------------------------------------------------------
# Metrics: per-stage timings, LLM cost, normalizer path mix
# ---------------------------------------------------------------------------


async def stage_timings(session: AsyncSession, *, since: datetime | None = None) -> list[dict]:
    """Per-task-type timing aggregates.

    Computed in Python over the matching rows because the SQL flavours we
    care about (Postgres + SQLite for tests) disagree on percentile syntax —
    keeping it portable is cheaper than two query paths. Volumes at MVP are
    in the thousands per day, well within an in-memory sort.
    """
    stmt = (
        select(TaskRecord.task_type, TaskRecord.duration_ms)
        .where(TaskRecord.duration_ms.is_not(None))
        .where(TaskRecord.status.in_(("done", "failed")))
    )
    if since is not None:
        stmt = stmt.where(TaskRecord.completed_at >= since)
    result = await session.execute(stmt)

    by_type: dict[str, list[int]] = {}
    for task_type, duration in result.all():
        if duration is None:
            continue
        by_type.setdefault(str(task_type), []).append(int(duration))

    rows: list[dict] = []
    for task_type, durations in by_type.items():
        durations.sort()
        n = len(durations)
        rows.append(
            {
                "task_type": task_type,
                "count": n,
                "avg_ms": int(sum(durations) / n) if n else 0,
                "p50_ms": _percentile(durations, 0.50),
                "p95_ms": _percentile(durations, 0.95),
                "max_ms": durations[-1] if durations else 0,
            }
        )
    rows.sort(key=lambda r: r["task_type"])
    return rows


def _percentile(sorted_values: list[int], pct: float) -> int:
    if not sorted_values:
        return 0
    if len(sorted_values) == 1:
        return sorted_values[0]
    # Nearest-rank — simple, monotonic, doesn't need numpy.
    idx = min(len(sorted_values) - 1, max(0, int(round(pct * (len(sorted_values) - 1)))))
    return sorted_values[idx]


async def llm_totals(session: AsyncSession, *, since: datetime | None = None) -> dict:
    """Aggregate token / call counts for the LLM normalizer.

    Returns raw token sums; the caller multiplies by the configured tariff to
    get a RUB estimate, so the query stays storage-only and the route stays
    config-aware.
    """
    stmt = select(
        func.count(LLMCall.id),
        func.coalesce(func.sum(LLMCall.prompt_tokens), 0),
        func.coalesce(func.sum(LLMCall.completion_tokens), 0),
        func.coalesce(func.sum(LLMCall.total_tokens), 0),
        func.coalesce(func.avg(LLMCall.duration_ms), 0),
        func.coalesce(func.max(LLMCall.duration_ms), 0),
    ).where(LLMCall.status == "ok")
    if since is not None:
        stmt = stmt.where(LLMCall.created_at >= since)
    error_stmt = select(func.count(LLMCall.id)).where(LLMCall.status == "error")
    if since is not None:
        error_stmt = error_stmt.where(LLMCall.created_at >= since)

    row = (await session.execute(stmt)).one()
    errors = (await session.execute(error_stmt)).scalar() or 0
    return {
        "calls_ok": int(row[0] or 0),
        "calls_error": int(errors),
        "prompt_tokens": int(row[1] or 0),
        "completion_tokens": int(row[2] or 0),
        "total_tokens": int(row[3] or 0),
        "avg_duration_ms": int(row[4] or 0),
        "max_duration_ms": int(row[5] or 0),
    }


async def llm_recent_calls(session: AsyncSession, *, limit: int = 20) -> list[LLMCall]:
    result = await session.execute(select(LLMCall).order_by(desc(LLMCall.created_at)).limit(limit))
    return list(result.scalars().all())


async def normalizer_path_counts(
    session: AsyncSession, *, since: datetime | None = None
) -> dict[str, int]:
    """Count NORMALIZE_EVENT tasks by which normalizer produced their output."""
    stmt = (
        select(TaskRecord.normalizer_path, func.count(TaskRecord.id))
        .where(TaskRecord.task_type == "normalize_event")
        .where(TaskRecord.normalizer_path.is_not(None))
        .group_by(TaskRecord.normalizer_path)
    )
    if since is not None:
        stmt = stmt.where(TaskRecord.completed_at >= since)
    result = await session.execute(stmt)
    return {str(row[0]): int(row[1]) for row in result.all()}


async def list_queue_depth_snapshots(
    session: AsyncSession,
    *,
    since: datetime,
) -> list[QueueDepthSnapshot]:
    result = await session.execute(
        select(QueueDepthSnapshot)
        .where(QueueDepthSnapshot.created_at >= since)
        .order_by(QueueDepthSnapshot.created_at)
    )
    return list(result.scalars().all())


async def list_event_logs(
    session: AsyncSession,
    *,
    limit: int = 100,
    offset: int = 0,
    severity: str | None = None,
) -> list[EventLog]:
    stmt = select(EventLog).order_by(desc(EventLog.created_at)).limit(limit).offset(offset)
    if severity:
        stmt = stmt.where(EventLog.severity == severity.upper())
    result = await session.execute(stmt)
    return list(result.scalars().all())
