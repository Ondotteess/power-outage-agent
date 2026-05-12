from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from typing import Protocol

from app.workers.queue import Task, TaskQueue, TaskType

logger = logging.getLogger(__name__)

Handler = Callable[[Task], Awaitable[None]]

_BACKOFF_BASE = 2
_BACKOFF_MAX = 600  # 10 min


class TaskStoreProtocol(Protocol):
    async def upsert(self, task: Task, status: str, error: str | None = None) -> None: ...


class Dispatcher:
    """Pulls tasks from the queue and routes them to registered handlers.

    Handles retry with exponential backoff and pushes lifecycle updates
    (pending/running/done/failed) into the task store. Tasks with
    status='failed' are the DLQ.
    """

    def __init__(
        self,
        queue: TaskQueue,
        store: TaskStoreProtocol,
        *,
        backoff_base: int = _BACKOFF_BASE,
        backoff_max: int = _BACKOFF_MAX,
    ) -> None:
        self._queue = queue
        self._store = store
        self._handlers: dict[TaskType, Handler] = {}
        self._backoff_base = backoff_base
        self._backoff_max = backoff_max
        self._delayed_retries: set[asyncio.Task[None]] = set()

    def register(self, task_type: TaskType, handler: Handler) -> None:
        self._handlers[task_type] = handler
        logger.debug("Dispatcher  handler registered  type=%s  handler=%s", task_type, handler)

    async def submit(self, task: Task) -> None:
        """Write 'pending' row to DB then put on the queue."""
        logger.debug(
            "Dispatcher  submit  task_id=%s  type=%s  trace=%s  payload=%s",
            task.task_id,
            task.task_type,
            task.trace_id,
            task.payload,
        )
        await self._store.upsert(task, status="pending")
        await self._queue.put(task)

    async def join(self) -> None:
        """Wait until the queue and delayed retry backlog are fully drained."""
        while True:
            await self._queue.join()
            pending_retries = [task for task in self._delayed_retries if not task.done()]
            if not pending_retries:
                await asyncio.sleep(0)
                if self._queue.unfinished_tasks == 0:
                    return
                continue
            await asyncio.wait(pending_retries, return_when=asyncio.FIRST_COMPLETED)

    async def run(self) -> None:
        logger.info(
            "Dispatcher started  handlers=%s",
            [str(t) for t in self._handlers],
        )
        while True:
            task = await self._queue.get()
            try:
                await self._process(task)
            finally:
                self._queue.task_done()

    async def _process(self, task: Task) -> None:
        handler = self._handlers.get(task.task_type)
        if handler is None:
            logger.warning(
                "Dispatcher  no handler for type=%s  task_id=%s  trace=%s — dropped",
                task.task_type,
                task.task_id,
                task.trace_id,
            )
            return

        logger.debug(
            "Dispatcher  processing  task_id=%s  type=%s  attempt=%d/%d  trace=%s",
            task.task_id,
            task.task_type,
            task.attempt,
            task.max_attempts,
            task.trace_id,
        )
        await self._store.upsert(task, status="running")
        try:
            await handler(task)
        except Exception as exc:  # noqa: BLE001
            await self._on_error(task, exc)
        else:
            logger.info(
                "Dispatcher  done  task_id=%s  type=%s  trace=%s",
                task.task_id,
                task.task_type,
                task.trace_id,
            )
            await self._store.upsert(task, status="done")

    async def _on_error(self, task: Task, exc: Exception) -> None:
        task.attempt += 1

        if task.attempt >= task.max_attempts:
            logger.error(
                "Dispatcher  FAILED (DLQ)  task_id=%s  type=%s  attempts=%d  trace=%s  error=%s",
                task.task_id,
                task.task_type,
                task.attempt,
                task.trace_id,
                exc,
            )
            await self._store.upsert(task, status="failed", error=str(exc))
            return

        backoff = min(self._backoff_base**task.attempt, self._backoff_max)
        logger.warning(
            "Dispatcher  retry  task_id=%s  type=%s  attempt=%d/%d  backoff=%ds  trace=%s  error=%s",
            task.task_id,
            task.task_type,
            task.attempt,
            task.max_attempts,
            backoff,
            task.trace_id,
            exc,
        )
        await self._store.upsert(task, status="pending", error=str(exc))
        retry = asyncio.create_task(self._requeue_after_delay(task, backoff))
        self._delayed_retries.add(retry)
        retry.add_done_callback(self._delayed_retries.discard)

    async def _requeue_after_delay(self, task: Task, backoff: int) -> None:
        await asyncio.sleep(backoff)
        await self._queue.put(task)
