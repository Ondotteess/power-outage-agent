from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from typing import Protocol
from uuid import UUID

from app.workers.queue import Task, TaskType

logger = logging.getLogger(__name__)


class NormalizedEventStoreProtocol(Protocol):
    async def get_by_id(self, event_id: UUID): ...


class DeduplicationHandler:
    """Marks the explicit dedup stage and passes deduped events to matching.

    Exact dedup/merge already happens inside NormalizedEventStore.save. This
    handler keeps the task graph honest, so the dashboard can show the stage.
    """

    def __init__(
        self,
        normalized_store: NormalizedEventStoreProtocol,
        submit: Callable[[Task], Awaitable[None]],
    ) -> None:
        self._normalized_store = normalized_store
        self._submit = submit

    async def handle(self, task: Task) -> None:
        event_id = UUID(task.payload["event_id"])
        event = await self._normalized_store.get_by_id(event_id)
        if event is None:
            raise ValueError(f"NormalizedEvent not found: {event_id}")

        logger.info(
            "DeduplicationHandler  pass  event_id=%s  trace=%s",
            event_id,
            task.trace_id,
        )
        await self._submit(
            Task(
                task_type=TaskType.MATCH_OFFICES,
                payload={"event_id": str(event_id)},
                trace_id=task.trace_id,
            )
        )
