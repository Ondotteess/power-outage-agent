import asyncio
import logging

from app.models.schemas import RawRecordSchema, SourceType
from app.parsers.base import BaseCollector
from app.parsers.html_collector import HtmlCollector
from app.workers.queue import Task, TaskQueue, TaskType

logger = logging.getLogger(__name__)

_BACKOFF_BASE = 2
_BACKOFF_MAX = 600  # 10 min


def _get_collector(source_type: str) -> BaseCollector:
    if source_type == SourceType.HTML:
        return HtmlCollector()
    raise ValueError(f"No collector for source type: {source_type!r}")


class CollectorWorker:
    def __init__(self, queue: TaskQueue) -> None:
        self._queue = queue

    async def run(self) -> None:
        logger.info("CollectorWorker started")
        while True:
            task = await self._queue.get()
            if task.task_type != TaskType.FETCH_SOURCE:
                await self._queue.put(task)
                await asyncio.sleep(0)
                continue
            try:
                await self._handle(task)
            finally:
                self._queue.task_done()

    async def _handle(self, task: Task) -> None:
        source_type = task.payload.get("source_type", SourceType.HTML)
        url = task.payload["url"]
        try:
            collector = _get_collector(source_type)
            raw: RawRecordSchema = await collector.fetch(url=url, trace_id=task.trace_id)
            logger.info(
                "Fetched %d bytes from %s [trace=%s]",
                len(raw.raw_content),
                url,
                task.trace_id,
            )
            await self._on_fetched(task, raw)
        except Exception as exc:
            await self._retry(task, exc)

    async def _on_fetched(self, task: Task, raw: RawRecordSchema) -> None:
        # TODO week-1: persist raw_record to DB via RawRecord ORM model
        # TODO week-1: enqueue TaskType.PARSE_CONTENT with payload={"raw_record_id": str(raw.id)}
        pass

    async def _retry(self, task: Task, exc: Exception) -> None:
        task.attempt += 1
        if task.attempt >= task.max_attempts:
            self._queue.move_to_dlq(task)
            return
        backoff = min(_BACKOFF_BASE**task.attempt, _BACKOFF_MAX)
        logger.warning(
            "Task %s failed (attempt %d/%d): %s — retry in %ds",
            task.task_id,
            task.attempt,
            task.max_attempts,
            exc,
            backoff,
        )
        await asyncio.sleep(backoff)
        await self._queue.put(task)
