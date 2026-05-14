from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import Protocol
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
    available_at: datetime | None = None

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


class DurableTaskStoreProtocol(Protocol):
    async def claim_next(self) -> Task | None: ...

    async def count_active(self) -> int: ...

    async def seconds_until_next_pending(self) -> float | None: ...


class DatabaseTaskQueue:
    """Queue facade backed by the persisted `tasks` table.

    `Dispatcher.submit()` writes pending tasks through `TaskStore.upsert`; this
    facade only wakes workers and claims runnable rows. It keeps the same small
    interface as `TaskQueue`, so handlers and the dispatcher stay unchanged.
    """

    supports_delayed_claims = True

    def __init__(self, store: DurableTaskStoreProtocol, *, poll_interval: float = 1.0) -> None:
        self._store = store
        self._poll_interval = poll_interval
        self._wake = asyncio.Event()
        self._inflight = 0

    async def put(self, task: Task) -> None:
        logger.debug(
            "DBQueue  WAKE  task_id=%s  type=%-20s  attempt=%d  trace=%s",
            task.task_id,
            task.task_type,
            task.attempt,
            task.trace_id,
        )
        self._wake.set()

    async def get(self) -> Task:
        while True:
            task = await self._store.claim_next()
            if task is not None:
                self._inflight += 1
                logger.debug(
                    "DBQueue  CLAIM  task_id=%s  type=%-20s  attempt=%d  trace=%s",
                    task.task_id,
                    task.task_type,
                    task.attempt,
                    task.trace_id,
                )
                return task

            timeout = self._poll_interval
            until_next = await self._store.seconds_until_next_pending()
            if until_next is not None:
                timeout = max(0.05, min(timeout, until_next))

            self._wake.clear()
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._wake.wait(), timeout=timeout)

    def task_done(self) -> None:
        self._inflight = max(0, self._inflight - 1)
        self._wake.set()

    async def join(self) -> None:
        while True:
            if self._inflight == 0 and await self._store.count_active() == 0:
                return
            self._wake.clear()
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._wake.wait(), timeout=self._poll_interval)

    @property
    def size(self) -> int:
        return 0

    @property
    def unfinished_tasks(self) -> int:
        return self._inflight
