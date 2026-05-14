from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from typing import Protocol
from uuid import UUID

from app.workers.queue import Task, TaskType

logger = logging.getLogger(__name__)

Submit = Callable[[Task], Awaitable[None]]


class SourceStoreProtocol(Protocol):
    async def get_by_id(self, source_id: UUID): ...


class TaskStoreProtocol(Protocol):
    async def get_by_id(self, task_id: UUID): ...


class PollRequestStoreProtocol(Protocol):
    async def claim_pending(self, *, limit: int = 20): ...

    async def mark_done(self, request_id: UUID, *, task_id: UUID) -> None: ...

    async def mark_failed(self, request_id: UUID, *, error: str) -> None: ...


class RetryRequestStoreProtocol(Protocol):
    async def claim_pending(self, *, limit: int = 20): ...

    async def mark_done(self, request_id: UUID, *, new_task_id: UUID) -> None: ...

    async def mark_failed(self, request_id: UUID, *, error: str) -> None: ...


class RequestWatcher:
    """Turns admin API DB requests into durable pipeline tasks."""

    def __init__(
        self,
        *,
        submit: Submit,
        source_store: SourceStoreProtocol,
        task_store: TaskStoreProtocol,
        poll_requests: PollRequestStoreProtocol,
        retry_requests: RetryRequestStoreProtocol,
        interval_seconds: float = 2.0,
        batch_size: int = 20,
    ) -> None:
        self._submit = submit
        self._source_store = source_store
        self._task_store = task_store
        self._poll_requests = poll_requests
        self._retry_requests = retry_requests
        self._interval_seconds = interval_seconds
        self._batch_size = batch_size

    async def run(self) -> None:
        logger.info("RequestWatcher started  interval=%.1fs", self._interval_seconds)
        while True:
            await self.process_once()
            await asyncio.sleep(self._interval_seconds)

    async def process_once(self) -> None:
        await self._process_poll_requests()
        await self._process_retry_requests()

    async def _process_poll_requests(self) -> None:
        requests = await self._poll_requests.claim_pending(limit=self._batch_size)
        for request in requests:
            try:
                source = await self._source_store.get_by_id(request.source_id)
                if source is None:
                    raise ValueError(f"Source not found: {request.source_id}")
                if not source.is_active:
                    raise ValueError(f"Source is inactive: {request.source_id}")

                task = Task(
                    task_type=TaskType.FETCH_SOURCE,
                    payload={
                        "source_id": str(source.id),
                        "url": source.url,
                        "source_type": source.source_type,
                        "manual_poll": True,
                    },
                    trace_id=request.trace_id,
                )
                await self._submit(task)
                await self._poll_requests.mark_done(request.id, task_id=task.task_id)
                logger.info(
                    "RequestWatcher  poll submitted  request_id=%s  task_id=%s  source_id=%s",
                    request.id,
                    task.task_id,
                    source.id,
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "RequestWatcher  poll failed  request_id=%s  source_id=%s  error=%s",
                    request.id,
                    request.source_id,
                    exc,
                )
                await self._poll_requests.mark_failed(request.id, error=str(exc))

    async def _process_retry_requests(self) -> None:
        requests = await self._retry_requests.claim_pending(limit=self._batch_size)
        for request in requests:
            try:
                row = await self._task_store.get_by_id(request.task_id)
                if row is None:
                    raise ValueError(f"Task not found: {request.task_id}")
                if row.status != "failed":
                    raise ValueError(f"Task is not failed: {request.task_id} status={row.status}")

                task = Task(
                    task_type=TaskType(row.task_type),
                    payload=dict(row.payload or {}),
                    trace_id=row.trace_id,
                    task_id=row.id,
                    attempt=0,
                )
                await self._submit(task)
                await self._retry_requests.mark_done(request.id, new_task_id=task.task_id)
                logger.info(
                    "RequestWatcher  retry submitted  request_id=%s  task_id=%s  type=%s",
                    request.id,
                    task.task_id,
                    task.task_type,
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "RequestWatcher  retry failed  request_id=%s  task_id=%s  error=%s",
                    request.id,
                    request.task_id,
                    exc,
                )
                await self._retry_requests.mark_failed(request.id, error=str(exc))
