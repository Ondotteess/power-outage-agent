import asyncio
import logging
from dataclasses import dataclass
from uuid import uuid4

from app.workers.queue import Task, TaskQueue, TaskType

logger = logging.getLogger(__name__)


@dataclass
class SourceConfig:
    source_id: str
    url: str
    source_type: str
    poll_interval_seconds: int


class Scheduler:
    def __init__(self, queue: TaskQueue) -> None:
        self._queue = queue
        self._sources: list[SourceConfig] = []

    def add_source(self, source: SourceConfig) -> None:
        self._sources.append(source)

    async def run(self) -> None:
        if not self._sources:
            logger.warning("Scheduler started with no sources configured")
            return
        await asyncio.gather(*[self._poll(s) for s in self._sources])

    async def _poll(self, source: SourceConfig) -> None:
        while True:
            task = Task(
                task_type=TaskType.FETCH_SOURCE,
                payload={
                    "source_id": source.source_id,
                    "url": source.url,
                    "source_type": source.source_type,
                },
                trace_id=uuid4(),
            )
            await self._queue.put(task)
            logger.debug("Queued %s for source %s", task.task_type, source.source_id)
            await asyncio.sleep(source.poll_interval_seconds)
