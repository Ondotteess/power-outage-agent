from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from uuid import UUID, uuid4

logger = logging.getLogger(__name__)


class TaskType(StrEnum):
    FETCH_SOURCE = "fetch_source"
    PARSE_CONTENT = "parse_content"
    NORMALIZE_EVENT = "normalize_event"
    DEDUPLICATE_EVENT = "deduplicate_event"
    MATCH_OFFICES = "match_offices"
    EMIT_EVENT = "emit_event"


@dataclass
class Task:
    task_type: TaskType
    payload: dict
    trace_id: UUID
    task_id: UUID = field(default_factory=uuid4)
    attempt: int = 0
    max_attempts: int = 5
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    @property
    def input_hash(self) -> str:
        raw = json.dumps(
            {"task_type": str(self.task_type), "payload": self.payload}, sort_keys=True
        )
        return hashlib.sha256(raw.encode()).hexdigest()


class TaskQueue:
    def __init__(self) -> None:
        self._queue: asyncio.Queue[Task] = asyncio.Queue()

    async def put(self, task: Task) -> None:
        logger.debug(
            "Queue  PUT  task_id=%s  type=%-20s  attempt=%d  trace=%s",
            task.task_id,
            task.task_type,
            task.attempt,
            task.trace_id,
        )
        await self._queue.put(task)

    async def get(self) -> Task:
        task = await self._queue.get()
        logger.debug(
            "Queue  GET  task_id=%s  type=%-20s  attempt=%d  trace=%s",
            task.task_id,
            task.task_type,
            task.attempt,
            task.trace_id,
        )
        return task

    def task_done(self) -> None:
        self._queue.task_done()

    async def join(self) -> None:
        await self._queue.join()

    @property
    def size(self) -> int:
        return self._queue.qsize()

    @property
    def unfinished_tasks(self) -> int:
        return self._queue._unfinished_tasks
