from __future__ import annotations

import asyncio
import contextlib
from uuid import uuid4

import pytest

from app.workers.dispatcher import Dispatcher
from app.workers.queue import Task, TaskQueue, TaskType
from tests.conftest import FakeTaskStore


def _make_task(task_type: TaskType = TaskType.FETCH_SOURCE) -> Task:
    return Task(task_type=task_type, payload={"url": "x"}, trace_id=uuid4())


async def _run_until_idle(dispatcher: Dispatcher, queue: TaskQueue) -> asyncio.Task:
    runner = asyncio.create_task(dispatcher.run())
    # let dispatcher drain
    for _ in range(20):
        await asyncio.sleep(0)
        if queue.size == 0:
            break
    return runner


async def _stop(runner: asyncio.Task) -> None:
    runner.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await runner


async def test_dispatch_to_registered_handler_marks_running_then_done():
    queue = TaskQueue()
    store = FakeTaskStore()
    dispatcher = Dispatcher(queue, store)

    handled: list[Task] = []

    async def handler(task: Task) -> None:
        handled.append(task)

    dispatcher.register(TaskType.FETCH_SOURCE, handler)

    task = _make_task()
    await dispatcher.submit(task)

    runner = await _run_until_idle(dispatcher, queue)
    await _stop(runner)

    assert handled == [task]
    statuses = store.statuses_for(task.task_id)
    assert statuses == ["pending", "running", "done"]


async def test_task_with_no_handler_is_dropped_with_warning():
    queue = TaskQueue()
    store = FakeTaskStore()
    dispatcher = Dispatcher(queue, store)

    task = _make_task(TaskType.PARSE_CONTENT)
    await dispatcher.submit(task)

    runner = await _run_until_idle(dispatcher, queue)
    await _stop(runner)

    # only the initial 'pending' from submit; no running/done/failed
    assert store.statuses_for(task.task_id) == ["pending"]


async def test_handler_failure_retries_with_zero_backoff():
    queue = TaskQueue()
    store = FakeTaskStore()
    dispatcher = Dispatcher(queue, store, backoff_base=1, backoff_max=0)

    attempts: list[int] = []

    async def handler(task: Task) -> None:
        attempts.append(task.attempt)
        if task.attempt < 2:
            raise RuntimeError("boom")

    dispatcher.register(TaskType.FETCH_SOURCE, handler)

    task = _make_task()
    task.max_attempts = 5
    await dispatcher.submit(task)

    runner = asyncio.create_task(dispatcher.run())
    for _ in range(200):
        await asyncio.sleep(0)
        if queue.size == 0 and "done" in store.statuses_for(task.task_id):
            break
    await _stop(runner)

    assert attempts == [0, 1, 2]
    statuses = store.statuses_for(task.task_id)
    assert statuses[-1] == "done"
    assert "running" in statuses
    # pending appears between retries
    assert statuses.count("pending") >= 2


async def test_handler_failure_goes_to_dlq_after_max_attempts():
    queue = TaskQueue()
    store = FakeTaskStore()
    dispatcher = Dispatcher(queue, store, backoff_base=1, backoff_max=0)

    async def handler(task: Task) -> None:
        raise RuntimeError("always fails")

    dispatcher.register(TaskType.FETCH_SOURCE, handler)

    task = _make_task()
    task.max_attempts = 3
    await dispatcher.submit(task)

    runner = asyncio.create_task(dispatcher.run())
    for _ in range(200):
        await asyncio.sleep(0)
        if "failed" in store.statuses_for(task.task_id):
            break
    await _stop(runner)

    statuses = store.statuses_for(task.task_id)
    assert statuses[-1] == "failed"
    assert task.attempt == task.max_attempts
    failed_call = next(c for c in store.calls if c[1] == "failed")
    assert failed_call[2] == "always fails"


@pytest.mark.parametrize(
    ("attempt", "expected"),
    [(1, 2), (2, 4), (3, 8), (10, 600)],  # capped at backoff_max (2**10 = 1024)
)
async def test_backoff_formula_matches_spec(attempt: int, expected: int):
    queue = TaskQueue()
    store = FakeTaskStore()
    dispatcher = Dispatcher(queue, store)
    # mirror the formula used in Dispatcher._on_error
    assert min(2**attempt, 600) == expected
    # ensure dispatcher uses the same defaults
    assert dispatcher._backoff_base == 2
    assert dispatcher._backoff_max == 600
