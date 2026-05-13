from __future__ import annotations

from dataclasses import dataclass, field
from uuid import UUID, uuid4

import pytest

from app.workers.deduplicator import DeduplicationHandler
from app.workers.queue import Task, TaskType


@dataclass
class FakeEvent:
    event_id: UUID = field(default_factory=uuid4)


@dataclass
class FakeNormalizedStore:
    event: FakeEvent | None

    async def get_by_id(self, event_id: UUID):
        return self.event


def _task(event_id: UUID) -> Task:
    return Task(
        task_type=TaskType.DEDUPLICATE_EVENT,
        payload={"event_id": str(event_id)},
        trace_id=uuid4(),
    )


async def test_deduplication_handler_enqueues_match_task():
    event = FakeEvent()
    submitted: list[Task] = []

    async def submit(task: Task) -> None:
        submitted.append(task)

    handler = DeduplicationHandler(FakeNormalizedStore(event), submit)

    await handler.handle(_task(event.event_id))

    assert len(submitted) == 1
    assert submitted[0].task_type == TaskType.MATCH_OFFICES
    assert submitted[0].payload == {"event_id": str(event.event_id)}


async def test_deduplication_handler_raises_when_event_missing():
    async def submit(task: Task) -> None:
        raise AssertionError("submit must not be called")

    handler = DeduplicationHandler(FakeNormalizedStore(None), submit)

    with pytest.raises(ValueError, match="NormalizedEvent not found"):
        await handler.handle(_task(uuid4()))
