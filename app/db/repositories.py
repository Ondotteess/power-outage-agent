from __future__ import annotations

import logging
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import async_sessionmaker

from app.db.models import RawRecord, Source, TaskRecord
from app.models.schemas import RawRecordSchema
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


class RawStore:
    def __init__(self, session_factory: async_sessionmaker) -> None:
        self._sf = session_factory

    async def exists_by_hash(self, content_hash: str) -> bool:
        logger.debug("RawStore  exists_by_hash  hash=%s", content_hash)
        async with self._sf() as session:
            result = await session.execute(
                select(RawRecord.id).where(RawRecord.content_hash == content_hash)
            )
            exists = result.scalar_one_or_none() is not None
        logger.debug("RawStore  exists_by_hash  hash=%s  result=%s", content_hash, exists)
        return exists

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
