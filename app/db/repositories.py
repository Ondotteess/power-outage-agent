from __future__ import annotations

import logging
from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.db.models import (
    NormalizedEvent,
    Notification,
    Office,
    OfficeImpact,
    ParsedRecord,
    RawRecord,
    Source,
    TaskRecord,
)
from app.models.schemas import (
    NormalizedEventSchema,
    NotificationSchema,
    OfficeImpactSchema,
    ParsedRecordSchema,
    RawRecordSchema,
)
from app.workers.queue import Task

logger = logging.getLogger(__name__)


class TaskStore:
    """Persists queue task lifecycle into the `tasks` table.

    DLQ = rows with status='failed'.
    """

    def __init__(self, session_factory: async_sessionmaker) -> None:
        self._sf = session_factory

    async def upsert(self, task: Task, status: str, error: str | None = None) -> None:
        logger.debug(
            "TaskStore  upsert  task_id=%s  type=%s  status=%s  attempt=%d  error=%s",
            task.task_id,
            task.task_type,
            status,
            task.attempt,
            error,
        )
        record = TaskRecord(
            id=task.task_id,
            task_type=str(task.task_type),
            input_hash=task.input_hash,
            status=status,
            attempt=task.attempt,
            payload=task.payload,
            error=error,
            trace_id=task.trace_id,
        )
        async with self._sf() as session:
            await session.merge(record)
            await session.commit()
        logger.debug("TaskStore  upsert done  task_id=%s  status=%s", task.task_id, status)

    async def fail_incomplete(self, reason: str) -> int:
        """Mark tasks abandoned by a previous process as failed.

        The runtime queue is in memory, so pending/running rows cannot be resumed
        after a restart yet. Keeping them as active makes the admin UI look stuck.
        """
        async with self._sf() as session:
            result = await session.execute(
                update(TaskRecord)
                .where(TaskRecord.status.in_(("pending", "running")))
                .values(status="failed", error=reason, updated_at=datetime.now(UTC))
            )
            await session.commit()
        return result.rowcount or 0


class RawStore:
    def __init__(self, session_factory: async_sessionmaker) -> None:
        self._sf = session_factory

    async def get_id_by_hash(self, content_hash: str) -> UUID | None:
        logger.debug("RawStore  get_id_by_hash  hash=%s", content_hash)
        async with self._sf() as session:
            result = await session.execute(
                select(RawRecord.id).where(RawRecord.content_hash == content_hash)
            )
            return result.scalar_one_or_none()

    async def get_by_id(self, raw_id: UUID) -> RawRecord | None:
        logger.debug("RawStore  get_by_id  raw_id=%s", raw_id)
        async with self._sf() as session:
            result = await session.execute(select(RawRecord).where(RawRecord.id == raw_id))
            return result.scalar_one_or_none()

    async def save(self, raw: RawRecordSchema, source_id: UUID | None) -> None:
        logger.debug(
            "RawStore  save  raw_id=%s  source_id=%s  hash=%s  size=%d",
            raw.id,
            source_id,
            raw.content_hash,
            len(raw.raw_content),
        )
        async with self._sf() as session:
            session.add(
                RawRecord(
                    id=raw.id,
                    source_id=source_id,
                    source_url=raw.source_url,
                    source_type=str(raw.source_type),
                    raw_content=raw.raw_content,
                    content_hash=raw.content_hash,
                    fetched_at=raw.fetched_at,
                    trace_id=raw.trace_id,
                )
            )
            await session.commit()
        logger.info("RawStore  saved  raw_id=%s  url=%s", raw.id, raw.source_url)


class SourceStore:
    def __init__(self, session_factory: async_sessionmaker) -> None:
        self._sf = session_factory

    async def list_active(self) -> list[Source]:
        logger.debug("SourceStore  list_active")
        async with self._sf() as session:
            result = await session.execute(select(Source).where(Source.is_active.is_(True)))
            sources = list(result.scalars().all())
        logger.info("SourceStore  list_active  found=%d", len(sources))
        return sources

    async def list_all(self) -> list[Source]:
        logger.debug("SourceStore  list_all")
        async with self._sf() as session:
            result = await session.execute(select(Source).order_by(Source.created_at))
            sources = list(result.scalars().all())
        logger.info("SourceStore  list_all  found=%d", len(sources))
        return sources

    async def get_by_id(self, source_id: UUID) -> Source | None:
        logger.debug("SourceStore  get_by_id  source_id=%s", source_id)
        async with self._sf() as session:
            result = await session.execute(select(Source).where(Source.id == source_id))
            return result.scalar_one_or_none()

    async def seed_if_empty(self, defaults: list[dict]) -> None:
        logger.debug("SourceStore  seed_if_empty  defaults=%d", len(defaults))
        async with self._sf() as session:
            result = await session.execute(select(func.count(Source.id)))
            count = result.scalar() or 0
            if count > 0:
                logger.debug("SourceStore  seed skipped  existing_rows=%d", count)
                return
            for payload in defaults:
                session.add(Source(**payload))
            await session.commit()
        logger.info("SourceStore  seeded %d source(s)", len(defaults))


class ParsedStore:
    def __init__(self, session_factory: async_sessionmaker) -> None:
        self._sf = session_factory

    async def save_many(self, records: list[ParsedRecordSchema]) -> None:
        if not records:
            return
        logger.debug("ParsedStore  save_many  count=%d", len(records))
        async with self._sf() as session:
            for r in records:
                session.add(
                    ParsedRecord(
                        id=r.id,
                        raw_record_id=r.raw_record_id,
                        source_id=r.source_id,
                        external_id=r.external_id,
                        start_time=r.start_time,
                        end_time=r.end_time,
                        location_city=r.location_city,
                        location_district=r.location_district,
                        location_street=r.location_street,
                        location_region_code=r.location_region_code,
                        reason=r.reason,
                        extra=r.extra,
                        trace_id=r.trace_id,
                        extracted_at=r.extracted_at,
                    )
                )
            await session.commit()
        logger.info("ParsedStore  saved %d parsed record(s)", len(records))

    async def get_by_id(self, parsed_id: UUID) -> ParsedRecord | None:
        logger.debug("ParsedStore  get_by_id  parsed_id=%s", parsed_id)
        async with self._sf() as session:
            result = await session.execute(select(ParsedRecord).where(ParsedRecord.id == parsed_id))
            return result.scalar_one_or_none()


class NormalizedEventStore:
    def __init__(self, session_factory: async_sessionmaker) -> None:
        self._sf = session_factory

    async def save(self, event: NormalizedEventSchema, trace_id: UUID) -> UUID:
        logger.debug(
            "NormalizedEventStore  save  event_id=%s  parsed_record_id=%s",
            event.event_id,
            event.parsed_record_id,
        )
        async with self._sf() as session:
            sources = _event_source_strings(event)
            existing = await _find_existing_normalized_event(session, event)
            if existing is not None:
                _merge_normalized_event(existing, event, trace_id, sources)
                await session.commit()
                logger.info(
                    "NormalizedEventStore  dedup merged  incoming_event_id=%s  existing_event_id=%s",
                    event.event_id,
                    existing.event_id,
                )
                return existing.event_id

            session.add(_new_normalized_event(event, trace_id, sources))
            await session.commit()
        logger.info("NormalizedEventStore  saved  event_id=%s", event.event_id)
        return event.event_id

    async def get_by_id(self, event_id: UUID) -> NormalizedEvent | None:
        logger.debug("NormalizedEventStore  get_by_id  event_id=%s", event_id)
        async with self._sf() as session:
            result = await session.execute(
                select(NormalizedEvent).where(NormalizedEvent.event_id == event_id)
            )
            return result.scalar_one_or_none()


class OfficeStore:
    def __init__(self, session_factory: async_sessionmaker) -> None:
        self._sf = session_factory

    async def list_active(self) -> list[Office]:
        logger.debug("OfficeStore  list_active")
        async with self._sf() as session:
            result = await session.execute(
                select(Office).where(Office.is_active.is_(True)).order_by(Office.name)
            )
            return list(result.scalars().all())

    async def get_by_id(self, office_id: UUID) -> Office | None:
        logger.debug("OfficeStore  get_by_id  office_id=%s", office_id)
        async with self._sf() as session:
            result = await session.execute(select(Office).where(Office.id == office_id))
            return result.scalar_one_or_none()

    async def seed_if_empty(self, defaults: list[dict]) -> None:
        logger.debug("OfficeStore  seed_if_empty  defaults=%d", len(defaults))
        async with self._sf() as session:
            result = await session.execute(select(func.count(Office.id)))
            count = result.scalar() or 0
            if count > 0:
                logger.debug("OfficeStore  seed skipped  existing_rows=%d", count)
                return
            for payload in defaults:
                session.add(Office(**payload))
            await session.commit()
        logger.info("OfficeStore  seeded %d office(s)", len(defaults))


class OfficeImpactStore:
    def __init__(self, session_factory: async_sessionmaker) -> None:
        self._sf = session_factory

    async def save_many(self, impacts: list[OfficeImpactSchema], trace_id: UUID) -> int:
        if not impacts:
            return 0

        async with self._sf() as session:
            saved = 0
            for impact in impacts:
                result = await session.execute(
                    select(OfficeImpact)
                    .where(OfficeImpact.office_id == impact.office_id)
                    .where(OfficeImpact.event_id == impact.event_id)
                    .limit(1)
                )
                existing = result.scalars().first()
                if existing is None:
                    session.add(
                        OfficeImpact(
                            id=impact.id,
                            office_id=impact.office_id,
                            event_id=impact.event_id,
                            impact_start=impact.impact_start,
                            impact_end=impact.impact_end,
                            impact_level=str(impact.impact_level),
                            match_strategy=impact.match_strategy,
                            match_score=impact.match_score,
                            trace_id=trace_id,
                            detected_at=impact.detected_at,
                        )
                    )
                    saved += 1
                    continue

                existing.impact_start = impact.impact_start
                existing.impact_end = impact.impact_end
                existing.impact_level = str(impact.impact_level)
                if impact.match_score >= (existing.match_score or 0.0):
                    existing.match_strategy = impact.match_strategy
                    existing.match_score = impact.match_score
                existing.trace_id = trace_id
                existing.detected_at = impact.detected_at
                saved += 1
            await session.commit()

        logger.info("OfficeImpactStore  saved %d impact(s)", saved)
        return saved


class NotificationStore:
    def __init__(self, session_factory: async_sessionmaker) -> None:
        self._sf = session_factory

    async def save(
        self,
        notification: NotificationSchema,
        trace_id: UUID,
        *,
        channel: str,
        status: str,
    ) -> UUID:
        async with self._sf() as session:
            result = await session.execute(
                select(Notification)
                .where(Notification.office_id == notification.office_id)
                .where(Notification.event_id == notification.event_id)
                .where(Notification.channel == channel)
                .limit(1)
            )
            existing = result.scalars().first()
            if existing is not None:
                existing.status = status
                existing.severity = str(notification.severity)
                existing.summary = notification.source_summary
                existing.trace_id = trace_id
                existing.emitted_at = notification.emitted_at
                await session.commit()
                logger.info(
                    "NotificationStore  updated  notification_id=%s  channel=%s",
                    existing.id,
                    channel,
                )
                return existing.id

            row = Notification(
                id=notification.notification_id,
                office_id=notification.office_id,
                event_id=notification.event_id,
                channel=channel,
                status=status,
                severity=str(notification.severity),
                summary=notification.source_summary,
                trace_id=trace_id,
                emitted_at=notification.emitted_at,
            )
            session.add(row)
            await session.commit()
            logger.info(
                "NotificationStore  saved  notification_id=%s  channel=%s",
                notification.notification_id,
                channel,
            )
            return notification.notification_id


async def _find_existing_normalized_event(
    session: AsyncSession,
    event: NormalizedEventSchema,
) -> NormalizedEvent | None:
    if event.parsed_record_id is not None:
        result = await session.execute(
            select(NormalizedEvent)
            .where(NormalizedEvent.parsed_record_id == event.parsed_record_id)
            .limit(1)
        )
        existing = result.scalars().first()
        if existing is not None:
            return existing

    stmt = (
        select(NormalizedEvent)
        .where(NormalizedEvent.event_type == str(event.event_type))
        .where(NormalizedEvent.start_time == event.start_time)
        .limit(1)
    )
    if event.end_time is None:
        stmt = stmt.where(NormalizedEvent.end_time.is_(None))
    else:
        stmt = stmt.where(NormalizedEvent.end_time == event.end_time)

    if event.location.normalized:
        stmt = stmt.where(NormalizedEvent.location_normalized == event.location.normalized)
    else:
        stmt = stmt.where(NormalizedEvent.location_raw == event.location.raw)

    result = await session.execute(stmt)
    return result.scalars().first()


def _new_normalized_event(
    event: NormalizedEventSchema,
    trace_id: UUID,
    sources: list[str],
) -> NormalizedEvent:
    return NormalizedEvent(
        event_id=event.event_id,
        parsed_record_id=event.parsed_record_id,
        event_type=str(event.event_type),
        start_time=event.start_time,
        end_time=event.end_time,
        location_raw=event.location.raw,
        location_normalized=event.location.normalized,
        location_city=event.location.city,
        location_street=event.location.street,
        location_building=event.location.building,
        reason=event.reason,
        sources=sources,
        confidence=event.confidence,
        trace_id=trace_id,
    )


def _merge_normalized_event(
    existing: NormalizedEvent,
    event: NormalizedEventSchema,
    trace_id: UUID,
    sources: list[str],
) -> None:
    existing.sources = _merge_source_lists(existing.sources, sources)
    existing.trace_id = trace_id
    existing.normalized_at = datetime.now(UTC)

    if existing.parsed_record_id is None:
        existing.parsed_record_id = event.parsed_record_id

    incoming_confidence = event.confidence or 0.0
    current_confidence = existing.confidence or 0.0
    if incoming_confidence >= current_confidence:
        existing.event_type = str(event.event_type)
        existing.start_time = event.start_time
        existing.end_time = event.end_time
        existing.location_raw = event.location.raw
        existing.location_normalized = event.location.normalized
        existing.location_city = event.location.city
        existing.location_street = event.location.street
        existing.location_building = event.location.building
        existing.reason = event.reason
        existing.confidence = event.confidence
    elif existing.reason is None and event.reason is not None:
        existing.reason = event.reason


def _event_source_strings(event: NormalizedEventSchema) -> list[str]:
    return [str(source_id) for source_id in event.sources]


def _merge_source_lists(existing: object, incoming: list[str]) -> list[str]:
    merged: list[str] = []
    seen: set[str] = set()
    values = existing if isinstance(existing, list) else []
    for value in [*values, *incoming]:
        text = str(value)
        if text not in seen:
            seen.add(text)
            merged.append(text)
    return merged
