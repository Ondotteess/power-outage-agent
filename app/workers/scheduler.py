from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from uuid import UUID, uuid4

from app.workers.queue import Task, TaskType

logger = logging.getLogger(__name__)

Submit = Callable[[Task], Awaitable[None]]


@dataclass
class SourceConfig:
    source_id: UUID
    url: str
    source_type: str
    poll_interval_seconds: int


class Scheduler:
    def __init__(self, submit: Submit) -> None:
        self._submit = submit
        self._sources: list[SourceConfig] = []

    def add_source(self, source: SourceConfig) -> None:
        self._sources.append(source)
        logger.debug(
            "Scheduler  source added  id=%s  type=%s  interval=%ds  url=%s",
            source.source_id,
            source.source_type,
            source.poll_interval_seconds,
            source.url,
        )

    async def run(self) -> None:
        if not self._sources:
            logger.warning("Scheduler started with no sources — nothing to poll")
            return
        logger.info("Scheduler started with %d source(s)", len(self._sources))
        await asyncio.gather(*[self._poll(s) for s in self._sources])

    async def _poll(self, source: SourceConfig) -> None:
        logger.info(
            "Scheduler  poller started  id=%s  url=%s  interval=%ds",
            source.source_id,
            source.url,
            source.poll_interval_seconds,
        )
        while True:
            task = Task(
                task_type=TaskType.FETCH_SOURCE,
                payload={
                    "source_id": str(source.source_id),
                    "url": source.url,
                    "source_type": source.source_type,
                },
                trace_id=uuid4(),
            )
            logger.debug(
                "Scheduler  submitting FETCH_SOURCE  task_id=%s  trace=%s  source=%s",
                task.task_id,
                task.trace_id,
                source.source_id,
            )
            await self._submit(task)
            logger.info(
                "Scheduler  sleeping %ds before next poll of source %s",
                source.poll_interval_seconds,
                source.source_id,
            )
            await asyncio.sleep(source.poll_interval_seconds)
