from __future__ import annotations

import asyncio
import contextlib
from uuid import uuid4

from app.workers.queue import Task, TaskType
from app.workers.scheduler import Scheduler, SourceConfig


async def test_scheduler_submits_fetch_source_for_each_source():
    submitted: list[Task] = []

    async def submit(task: Task) -> None:
        submitted.append(task)

    scheduler = Scheduler(submit)
    source = SourceConfig(
        source_id=uuid4(),
        url="https://example.com",
        source_type="html",
        poll_interval_seconds=3600,
    )
    scheduler.add_source(source)

    task = asyncio.create_task(scheduler.run())
    # let scheduler tick once
    await asyncio.sleep(0.05)
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task

    assert len(submitted) >= 1
    submitted_task = submitted[0]
    assert submitted_task.task_type == TaskType.FETCH_SOURCE
    assert submitted_task.payload["url"] == source.url
    assert submitted_task.payload["source_id"] == str(source.source_id)


async def test_scheduler_with_no_sources_exits_immediately():
    async def submit(task: Task) -> None:  # pragma: no cover — should not be called
        raise AssertionError("submit must not be called")

    scheduler = Scheduler(submit)
    await asyncio.wait_for(scheduler.run(), timeout=0.1)
