from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from uuid import UUID

from app.db.repositories import RawStore
from app.models.schemas import SourceType
from app.parsers.base import BaseCollector
from app.parsers.html_collector import HtmlCollector
from app.workers.queue import Task, TaskType

logger = logging.getLogger(__name__)

Submit = Callable[[Task], Awaitable[None]]


def default_collectors() -> dict[str, BaseCollector]:
    return {SourceType.HTML: HtmlCollector()}


class CollectorHandler:
    """Handles FETCH_SOURCE tasks: fetches raw content, persists it, enqueues parse."""

    def __init__(
        self,
        submit: Submit,
        raw_store: RawStore,
        collectors: dict[str, BaseCollector] | None = None,
    ) -> None:
        self._submit = submit
        self._raw_store = raw_store
        self._collectors = collectors if collectors is not None else default_collectors()

    async def handle(self, task: Task) -> None:
        source_type = task.payload.get("source_type", SourceType.HTML)
        url = task.payload["url"]
        source_id_str = task.payload.get("source_id")
        source_id = UUID(source_id_str) if source_id_str else None

        logger.debug(
            "Collector  start  task_id=%s  source_type=%s  url=%s  source_id=%s  trace=%s",
            task.task_id,
            source_type,
            url,
            source_id,
            task.trace_id,
        )

        collector = self._collectors.get(source_type)
        if collector is None:
            logger.error(
                "Collector  unknown source_type=%s  task_id=%s  trace=%s",
                source_type,
                task.task_id,
                task.trace_id,
            )
            raise ValueError(f"No collector for source type: {source_type!r}")

        raw = await collector.fetch(url=url, trace_id=task.trace_id)
        logger.info(
            "Collector  fetched  %d bytes  url=%s  content_hash=%s  trace=%s",
            len(raw.raw_content),
            url,
            raw.content_hash,
            task.trace_id,
        )

        if await self._raw_store.exists_by_hash(raw.content_hash):
            logger.info(
                "Collector  duplicate skipped  content_hash=%s  url=%s  trace=%s",
                raw.content_hash,
                url,
                task.trace_id,
            )
            return

        await self._raw_store.save(raw, source_id=source_id)
        logger.info(
            "Collector  raw saved  raw_id=%s  source_id=%s  url=%s  trace=%s",
            raw.id,
            source_id,
            url,
            task.trace_id,
        )

        parse_task = Task(
            task_type=TaskType.PARSE_CONTENT,
            payload={"raw_record_id": str(raw.id)},
            trace_id=task.trace_id,
        )
        logger.debug(
            "Collector  enqueue PARSE_CONTENT  parse_task_id=%s  raw_id=%s  trace=%s",
            parse_task.task_id,
            raw.id,
            task.trace_id,
        )
        await self._submit(parse_task)
