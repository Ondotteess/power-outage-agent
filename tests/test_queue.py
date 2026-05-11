from uuid import uuid4

from app.workers.queue import Task, TaskQueue, TaskType


def test_input_hash_is_stable_for_same_inputs():
    payload = {"url": "https://x", "source_id": "1"}
    a = Task(task_type=TaskType.FETCH_SOURCE, payload=payload, trace_id=uuid4())
    b = Task(task_type=TaskType.FETCH_SOURCE, payload=payload, trace_id=uuid4())
    assert a.input_hash == b.input_hash


def test_input_hash_ignores_key_order():
    a = Task(
        task_type=TaskType.FETCH_SOURCE,
        payload={"a": 1, "b": 2},
        trace_id=uuid4(),
    )
    b = Task(
        task_type=TaskType.FETCH_SOURCE,
        payload={"b": 2, "a": 1},
        trace_id=uuid4(),
    )
    assert a.input_hash == b.input_hash


def test_input_hash_differs_for_different_task_types():
    payload = {"x": 1}
    a = Task(task_type=TaskType.FETCH_SOURCE, payload=payload, trace_id=uuid4())
    b = Task(task_type=TaskType.PARSE_CONTENT, payload=payload, trace_id=uuid4())
    assert a.input_hash != b.input_hash


async def test_queue_put_get_roundtrip():
    queue = TaskQueue()
    task = Task(task_type=TaskType.FETCH_SOURCE, payload={"url": "x"}, trace_id=uuid4())
    await queue.put(task)
    assert queue.size == 1
    got = await queue.get()
    assert got is task
    queue.task_done()
