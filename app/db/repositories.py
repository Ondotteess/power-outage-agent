from __future__ import annotations

import hashlib
import json
import logging
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

from sqlalchemy import delete, desc, func, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.db.models import (
    DedupEvent,
    EventLog,
    LLMCall,
    NormalizedEvent,
    Notification,
    Office,
    OfficeImpact,
    ParsedRecord,
    PollRequest,
    QueueDepthSnapshot,
    RawRecord,
    RetryRequest,
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
from app.workers.queue import Task, TaskType

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
        now = datetime.now(UTC)
        async with self._sf() as session:
            existing = await session.get(TaskRecord, task.task_id)
            if existing is None:
                record = TaskRecord(
                    id=task.task_id,
                    task_type=str(task.task_type),
                    input_hash=task.input_hash,
                    status=status,
                    attempt=task.attempt,
                    max_attempts=task.max_attempts,
                    payload=task.payload,
                    error=error,
                    trace_id=task.trace_id,
                    next_run_at=task.available_at,
                )
                session.add(record)
            else:
                existing.status = status
                existing.attempt = task.attempt
                existing.max_attempts = task.max_attempts
                existing.payload = task.payload
                existing.error = error
                existing.updated_at = now
                existing.next_run_at = task.available_at
                # Per-stage timing. `started_at` is reset on each "running"
                # transition so retries measure the latest attempt, not the
                # very first one. `completed_at` + `duration_ms` finalize on
                # done/failed; duration is the wall time between started_at
                # and now (excludes queue wait + backoff).
                if status == "running":
                    existing.started_at = now
                    existing.completed_at = None
                    existing.duration_ms = None
                elif status in ("done", "failed"):
                    existing.completed_at = now
                    if existing.started_at is not None:
                        existing.duration_ms = int(
                            (now - existing.started_at).total_seconds() * 1000
                        )
            await session.commit()
        logger.debug("TaskStore  upsert done  task_id=%s  status=%s", task.task_id, status)

    async def claim_next(self) -> Task | None:
        """Atomically claim the next runnable pending task for a DB-backed worker."""
        now = datetime.now(UTC)
        async with self._sf() as session, session.begin():
            result = await session.execute(
                select(TaskRecord)
                .where(TaskRecord.status == "pending")
                .where(or_(TaskRecord.next_run_at.is_(None), TaskRecord.next_run_at <= now))
                .order_by(TaskRecord.created_at)
                .limit(1)
                .with_for_update(skip_locked=True)
            )
            record = result.scalar_one_or_none()
            if record is None:
                return None

            record.status = "running"
            record.started_at = now
            record.completed_at = None
            record.duration_ms = None
            record.updated_at = now

            return Task(
                task_type=TaskType(record.task_type),
                payload=dict(record.payload or {}),
                trace_id=record.trace_id,
                task_id=record.id,
                attempt=record.attempt,
                max_attempts=record.max_attempts,
                available_at=record.next_run_at,
            )

    async def count_active(self) -> int:
        async with self._sf() as session:
            result = await session.execute(
                select(func.count(TaskRecord.id)).where(
                    TaskRecord.status.in_(("pending", "running"))
                )
            )
            return result.scalar() or 0

    async def seconds_until_next_pending(self) -> float | None:
        now = datetime.now(UTC)
        async with self._sf() as session:
            result = await session.execute(
                select(TaskRecord.next_run_at)
                .where(TaskRecord.status == "pending")
                .where(TaskRecord.next_run_at.is_not(None))
                .where(TaskRecord.next_run_at > now)
                .order_by(TaskRecord.next_run_at)
                .limit(1)
            )
            next_run_at = result.scalar_one_or_none()
        if next_run_at is None:
            return None
        return max(0.0, (next_run_at - now).total_seconds())

    async def set_normalizer_path(self, task_id: UUID, path: str) -> None:
        """Record which normalizer (`automaton` or `llm_fallback`) produced the
        event for a NORMALIZE_EVENT task. Used by the Metrics page to show the
        FSA-vs-LLM hit ratio without scanning every LLMCall row."""
        async with self._sf() as session:
            await session.execute(
                update(TaskRecord)
                .where(TaskRecord.id == task_id)
                .values(normalizer_path=path, updated_at=datetime.now(UTC))
            )
            await session.commit()

    async def fail_incomplete(self, reason: str) -> int:
        """Legacy/manual escape hatch: move active tasks to DLQ."""
        async with self._sf() as session:
            result = await session.execute(
                update(TaskRecord)
                .where(TaskRecord.status.in_(("pending", "running")))
                .values(status="failed", error=reason, updated_at=datetime.now(UTC))
            )
            await session.commit()
        return result.rowcount or 0

    async def requeue_incomplete(self, reason: str) -> int:
        """Make tasks abandoned by a previous process runnable again."""
        now = datetime.now(UTC)
        async with self._sf() as session:
            result = await session.execute(
                update(TaskRecord)
                .where(TaskRecord.status.in_(("pending", "running")))
                .values(
                    status="pending",
                    error=reason,
                    started_at=None,
                    completed_at=None,
                    duration_ms=None,
                    next_run_at=now,
                    updated_at=now,
                )
            )
            await session.commit()
        return result.rowcount or 0

    async def get_by_id(self, task_id: UUID) -> TaskRecord | None:
        logger.debug("TaskStore  get_by_id  task_id=%s", task_id)
        async with self._sf() as session:
            result = await session.execute(select(TaskRecord).where(TaskRecord.id == task_id))
            return result.scalar_one_or_none()


class LLMCallStore:
    """Writes one row per chat-completion call (success or failure)."""

    def __init__(self, session_factory: async_sessionmaker) -> None:
        self._sf = session_factory

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
    ) -> None:
        async with self._sf() as session:
            session.add(
                LLMCall(
                    model=model,
                    prompt_tokens=prompt_tokens,
                    completion_tokens=completion_tokens,
                    total_tokens=total_tokens,
                    duration_ms=duration_ms,
                    status=status,
                    task_id=task_id,
                    trace_id=trace_id,
                )
            )
            await session.commit()


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

    async def save_many(self, records: list[ParsedRecordSchema]) -> list[ParsedRecordSchema]:
        if not records:
            return []
        logger.debug("ParsedStore  save_many  count=%d", len(records))
        persisted: list[ParsedRecordSchema] = []
        async with self._sf() as session:
            for r in records:
                fingerprint = _parsed_fingerprint(r)
                result = await session.execute(
                    select(ParsedRecord).where(ParsedRecord.fingerprint == fingerprint).limit(1)
                )
                existing = result.scalars().first()
                if existing is not None:
                    existing.raw_record_id = r.raw_record_id
                    existing.source_id = r.source_id
                    existing.external_id = r.external_id
                    existing.start_time = r.start_time
                    existing.end_time = r.end_time
                    existing.location_city = r.location_city
                    existing.location_district = r.location_district
                    existing.location_street = r.location_street
                    existing.location_region_code = r.location_region_code
                    existing.reason = r.reason
                    existing.extra = r.extra
                    existing.trace_id = r.trace_id
                    existing.extracted_at = r.extracted_at
                    persisted.append(r.model_copy(update={"id": existing.id}))
                    continue

                session.add(
                    ParsedRecord(
                        id=r.id,
                        raw_record_id=r.raw_record_id,
                        source_id=r.source_id,
                        external_id=r.external_id,
                        fingerprint=fingerprint,
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
                persisted.append(r)
            await session.commit()
        logger.info("ParsedStore  persisted %d parsed record(s)", len(persisted))
        return persisted

    async def get_by_id(self, parsed_id: UUID) -> ParsedRecord | None:
        logger.debug("ParsedStore  get_by_id  parsed_id=%s", parsed_id)
        async with self._sf() as session:
            result = await session.execute(select(ParsedRecord).where(ParsedRecord.id == parsed_id))
            return result.scalar_one_or_none()


class EventLogStore:
    def __init__(self, session_factory: async_sessionmaker) -> None:
        self._sf = session_factory

    async def record(
        self,
        *,
        event_type: str,
        severity: str,
        message: str,
        source: str | None = None,
        task_id: UUID | None = None,
        trace_id: UUID | None = None,
        payload: dict | None = None,
    ) -> None:
        async with self._sf() as session:
            session.add(
                EventLog(
                    event_type=event_type,
                    severity=severity,
                    message=message,
                    source=source,
                    task_id=task_id,
                    trace_id=trace_id,
                    payload=payload or {},
                )
            )
            await session.commit()


class QueueSnapshotStore:
    def __init__(self, session_factory: async_sessionmaker) -> None:
        self._sf = session_factory

    async def record_current(self) -> None:
        async with self._sf() as session:
            result = await session.execute(
                select(TaskRecord.status, func.count(TaskRecord.id)).group_by(TaskRecord.status)
            )
            counts = {status: int(count) for status, count in result.all()}
            session.add(
                QueueDepthSnapshot(
                    pending=counts.get("pending", 0),
                    running=counts.get("running", 0),
                    failed=counts.get("failed", 0),
                )
            )
            await session.commit()


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
                strategy = _dedup_strategy(existing, event)
                _merge_normalized_event(existing, event, trace_id, sources)
                session.add(
                    DedupEvent(
                        incoming_event_id=event.event_id,
                        existing_event_id=existing.event_id,
                        strategy=strategy,
                        trace_id=trace_id,
                    )
                )
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

    async def get_by_parsed_record_id(self, parsed_record_id: UUID) -> NormalizedEvent | None:
        logger.debug(
            "NormalizedEventStore  get_by_parsed_record_id  parsed_record_id=%s",
            parsed_record_id,
        )
        async with self._sf() as session:
            result = await session.execute(
                select(NormalizedEvent).where(NormalizedEvent.parsed_record_id == parsed_record_id)
            )
            return result.scalar_one_or_none()

    async def add_sources(self, event_id: UUID, sources: list[UUID], trace_id: UUID) -> None:
        async with self._sf() as session:
            event = await session.get(NormalizedEvent, event_id)
            if event is None:
                return
            event.sources = _merge_source_lists(event.sources, [str(source) for source in sources])
            event.trace_id = trace_id
            event.normalized_at = datetime.now(UTC)
            await session.commit()


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

    async def replace_all(self, defaults: list[dict]) -> None:
        logger.warning("OfficeStore  replacing office registry  defaults=%d", len(defaults))
        async with self._sf() as session:
            await session.execute(delete(Notification))
            await session.execute(delete(OfficeImpact))
            await session.execute(delete(Office))
            for payload in defaults:
                session.add(Office(**payload))
            await session.commit()
        logger.info("OfficeStore  replaced office registry with %d office(s)", len(defaults))

    async def upsert_many(self, rows: list[dict]) -> tuple[int, int]:
        """Idempotent import: match by (name, city, address) — the existing
        unique constraint — and either update or insert. Returns
        (inserted, updated) counts so the API can return them to the caller.

        Rows without the three required keys are silently skipped; the caller
        is expected to validate at the request boundary."""
        if not rows:
            return (0, 0)
        inserted = 0
        updated = 0
        async with self._sf() as session:
            for payload in rows:
                name = payload.get("name")
                city = payload.get("city")
                address = payload.get("address")
                if not (name and city and address):
                    continue
                result = await session.execute(
                    select(Office)
                    .where(Office.name == name)
                    .where(Office.city == city)
                    .where(Office.address == address)
                    .limit(1)
                )
                existing = result.scalars().first()
                if existing is None:
                    session.add(Office(**payload))
                    inserted += 1
                else:
                    existing.region = payload.get("region", existing.region)
                    if "is_active" in payload:
                        existing.is_active = bool(payload["is_active"])
                    if "latitude" in payload:
                        existing.latitude = payload["latitude"]
                    if "longitude" in payload:
                        existing.longitude = payload["longitude"]
                    if "extra" in payload:
                        existing.extra = payload["extra"] or {}
                    updated += 1
            await session.commit()
        logger.info(
            "OfficeStore  upsert_many  inserted=%d  updated=%d  total=%d",
            inserted,
            updated,
            len(rows),
        )
        return (inserted, updated)


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
                            match_explanation=impact.match_explanation,
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
                    existing.match_explanation = impact.match_explanation
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


class PollRequestStore:
    def __init__(self, session_factory: async_sessionmaker) -> None:
        self._sf = session_factory

    async def create(self, source_id: UUID, *, trace_id: UUID | None = None) -> UUID:
        request_id = uuid4()
        request = PollRequest(id=request_id, source_id=source_id, trace_id=trace_id or request_id)
        async with self._sf() as session:
            session.add(request)
            await session.commit()
        logger.info("PollRequestStore  created  request_id=%s source_id=%s", request.id, source_id)
        return request.id

    async def claim_pending(self, *, limit: int = 20) -> list[PollRequest]:
        async with self._sf() as session, session.begin():
            result = await session.execute(
                select(PollRequest)
                .where(PollRequest.status == "pending")
                .order_by(PollRequest.created_at)
                .limit(limit)
                .with_for_update(skip_locked=True)
            )
            requests = list(result.scalars().all())
            if not requests:
                return []
            now = datetime.now(UTC)
            for request in requests:
                request.status = "processing"
                request.updated_at = now
        return requests

    async def mark_done(self, request_id: UUID, *, task_id: UUID) -> None:
        await self._mark(request_id, status="done", task_id=task_id)

    async def mark_failed(self, request_id: UUID, *, error: str) -> None:
        await self._mark(request_id, status="failed", error=error)

    async def fail_incomplete(self, reason: str) -> int:
        async with self._sf() as session:
            result = await session.execute(
                update(PollRequest)
                .where(PollRequest.status == "processing")
                .values(status="failed", error=reason, updated_at=datetime.now(UTC))
            )
            await session.commit()
        return result.rowcount or 0

    async def _mark(
        self,
        request_id: UUID,
        *,
        status: str,
        task_id: UUID | None = None,
        error: str | None = None,
    ) -> None:
        values = {
            "status": status,
            "error": error,
            "updated_at": datetime.now(UTC),
            "processed_at": datetime.now(UTC),
        }
        if task_id is not None:
            values["task_id"] = task_id
        async with self._sf() as session:
            await session.execute(
                update(PollRequest).where(PollRequest.id == request_id).values(**values)
            )
            await session.commit()


class RetryRequestStore:
    def __init__(self, session_factory: async_sessionmaker) -> None:
        self._sf = session_factory

    async def create(self, task_id: UUID, *, trace_id: UUID | None = None) -> UUID:
        request_id = uuid4()
        request = RetryRequest(id=request_id, task_id=task_id, trace_id=trace_id or request_id)
        async with self._sf() as session:
            session.add(request)
            await session.commit()
        logger.info("RetryRequestStore  created  request_id=%s task_id=%s", request.id, task_id)
        return request.id

    async def claim_pending(self, *, limit: int = 20) -> list[RetryRequest]:
        async with self._sf() as session, session.begin():
            result = await session.execute(
                select(RetryRequest)
                .where(RetryRequest.status == "pending")
                .order_by(RetryRequest.created_at)
                .limit(limit)
                .with_for_update(skip_locked=True)
            )
            requests = list(result.scalars().all())
            if not requests:
                return []
            now = datetime.now(UTC)
            for request in requests:
                request.status = "processing"
                request.updated_at = now
        return requests

    async def mark_done(self, request_id: UUID, *, new_task_id: UUID) -> None:
        await self._mark(request_id, status="done", new_task_id=new_task_id)

    async def mark_failed(self, request_id: UUID, *, error: str) -> None:
        await self._mark(request_id, status="failed", error=error)

    async def fail_incomplete(self, reason: str) -> int:
        async with self._sf() as session:
            result = await session.execute(
                update(RetryRequest)
                .where(RetryRequest.status == "processing")
                .values(status="failed", error=reason, updated_at=datetime.now(UTC))
            )
            await session.commit()
        return result.rowcount or 0

    async def _mark(
        self,
        request_id: UUID,
        *,
        status: str,
        new_task_id: UUID | None = None,
        error: str | None = None,
    ) -> None:
        values = {
            "status": status,
            "error": error,
            "updated_at": datetime.now(UTC),
            "processed_at": datetime.now(UTC),
        }
        if new_task_id is not None:
            values["new_task_id"] = new_task_id
        async with self._sf() as session:
            await session.execute(
                update(RetryRequest).where(RetryRequest.id == request_id).values(**values)
            )
            await session.commit()


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

    if event.location.normalized:
        stmt = (
            select(NormalizedEvent)
            .where(NormalizedEvent.location_normalized == event.location.normalized)
            .order_by(desc(NormalizedEvent.start_time))
            .limit(50)
        )
    else:
        stmt = (
            select(NormalizedEvent)
            .where(NormalizedEvent.location_raw == event.location.raw)
            .order_by(desc(NormalizedEvent.start_time))
            .limit(50)
        )

    result = await session.execute(stmt)
    for existing in result.scalars().all():
        if _compatible_event_type(existing.event_type, str(event.event_type)) and _windows_match(
            existing, event
        ):
            return existing
    return None


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


def _dedup_strategy(existing: NormalizedEvent, event: NormalizedEventSchema) -> str:
    if event.parsed_record_id is not None and existing.parsed_record_id == event.parsed_record_id:
        return "parsed_record"
    if (
        _compatible_event_type(existing.event_type, str(event.event_type))
        and existing.start_time == event.start_time
        and existing.end_time == event.end_time
        and (
            existing.location_normalized == event.location.normalized
            or existing.location_raw == event.location.raw
        )
    ):
        return "exact_window"
    if _windows_overlap(existing, event):
        return "overlap_window"
    if _windows_match(existing, event):
        return "time_tolerance"
    return "composite"


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

    merged_start = min(existing.start_time, event.start_time)
    merged_end = _merged_end_time(existing.end_time, event.end_time)
    incoming_confidence = event.confidence or 0.0
    current_confidence = existing.confidence or 0.0
    if incoming_confidence >= current_confidence:
        existing.event_type = str(event.event_type)
        existing.start_time = merged_start
        existing.end_time = merged_end
        existing.location_raw = event.location.raw
        existing.location_normalized = event.location.normalized
        existing.location_city = event.location.city
        existing.location_street = event.location.street
        existing.location_building = event.location.building
        existing.reason = event.reason
        existing.confidence = event.confidence
        return

    existing.start_time = merged_start
    existing.end_time = merged_end
    if existing.reason is None and event.reason is not None:
        existing.reason = event.reason


_DEDUP_TIME_TOLERANCE = timedelta(minutes=15)
_OUTAGE_COMPATIBLE_TYPES = {"power_outage", "maintenance", "infrastructure_failure"}


def _compatible_event_type(existing: str, incoming: str) -> bool:
    if existing == incoming:
        return True
    return existing in _OUTAGE_COMPATIBLE_TYPES and incoming in _OUTAGE_COMPATIBLE_TYPES


def _windows_match(existing: NormalizedEvent, event: NormalizedEventSchema) -> bool:
    return _windows_overlap(existing, event) or (
        abs(existing.start_time - event.start_time) <= _DEDUP_TIME_TOLERANCE
        and _nullable_dt_close(existing.end_time, event.end_time)
    )


def _windows_overlap(existing: NormalizedEvent, event: NormalizedEventSchema) -> bool:
    existing_end = existing.end_time or existing.start_time
    incoming_end = event.end_time or event.start_time
    return (
        existing.start_time <= incoming_end + _DEDUP_TIME_TOLERANCE
        and event.start_time <= existing_end + _DEDUP_TIME_TOLERANCE
    )


def _nullable_dt_close(existing: datetime | None, incoming: datetime | None) -> bool:
    if existing is None or incoming is None:
        return existing is None and incoming is None
    return abs(existing - incoming) <= _DEDUP_TIME_TOLERANCE


def _merged_end_time(existing: datetime | None, incoming: datetime | None) -> datetime | None:
    if existing is None or incoming is None:
        return existing or incoming
    return max(existing, incoming)


def _event_source_strings(event: NormalizedEventSchema) -> list[str]:
    return [str(source_id) for source_id in event.sources]


def _parsed_fingerprint(record: ParsedRecordSchema) -> str:
    """Stable parsed-record key used to make reparsing idempotent."""
    if record.source_id is not None and record.external_id:
        payload = {
            "source_id": str(record.source_id),
            "external_id": record.external_id,
        }
    else:
        payload = {
            "source_id": str(record.source_id) if record.source_id else None,
            "start_time": record.start_time.isoformat() if record.start_time else None,
            "end_time": record.end_time.isoformat() if record.end_time else None,
            "city": record.location_city,
            "district": record.location_district,
            "street": record.location_street,
            "region": record.location_region_code,
            "reason": record.reason,
            "extra": record.extra,
        }
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(raw.encode()).hexdigest()


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
