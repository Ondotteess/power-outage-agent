from __future__ import annotations

from dataclasses import dataclass, field
from uuid import UUID, uuid4

from app.workers.queue import Task, TaskType
from app.workers.requests import RequestWatcher


@dataclass
class FakeSource:
    id: UUID = field(default_factory=uuid4)
    url: str = "https://example.test/outages"
    source_type: str = "json"
    is_active: bool = True


@dataclass
class FakePollRequest:
    id: UUID = field(default_factory=uuid4)
    source_id: UUID = field(default_factory=uuid4)
    trace_id: UUID = field(default_factory=uuid4)


@dataclass
class FakeRetryRequest:
    id: UUID = field(default_factory=uuid4)
    task_id: UUID = field(default_factory=uuid4)
    trace_id: UUID = field(default_factory=uuid4)


@dataclass
class FakeTaskRow:
    id: UUID = field(default_factory=uuid4)
    task_type: str = "fetch_source"
    payload: dict = field(default_factory=lambda: {"source_id": str(uuid4())})
    status: str = "failed"
    trace_id: UUID = field(default_factory=uuid4)


class FakeSourceStore:
    def __init__(self, source: FakeSource | None) -> None:
        self.source = source

    async def get_by_id(self, source_id: UUID):
        return self.source if self.source and self.source.id == source_id else None


class FakeTaskStore:
    def __init__(self, row: FakeTaskRow | None) -> None:
        self.row = row

    async def get_by_id(self, task_id: UUID):
        return self.row if self.row and self.row.id == task_id else None


class FakePollStore:
    def __init__(self, requests: list[FakePollRequest] | None = None) -> None:
        self.requests = requests or []
        self.done: list[tuple[UUID, UUID]] = []
        self.failed: list[tuple[UUID, str]] = []

    async def claim_pending(self, *, limit: int = 20):
        return self.requests[:limit]

    async def mark_done(self, request_id: UUID, *, task_id: UUID) -> None:
        self.done.append((request_id, task_id))

    async def mark_failed(self, request_id: UUID, *, error: str) -> None:
        self.failed.append((request_id, error))


class FakeRetryStore:
    def __init__(self, requests: list[FakeRetryRequest] | None = None) -> None:
        self.requests = requests or []
        self.done: list[tuple[UUID, UUID]] = []
        self.failed: list[tuple[UUID, str]] = []

    async def claim_pending(self, *, limit: int = 20):
        return self.requests[:limit]

    async def mark_done(self, request_id: UUID, *, new_task_id: UUID) -> None:
        self.done.append((request_id, new_task_id))

    async def mark_failed(self, request_id: UUID, *, error: str) -> None:
        self.failed.append((request_id, error))


async def test_request_watcher_turns_poll_request_into_fetch_task():
    source = FakeSource()
    request = FakePollRequest(source_id=source.id)
    poll_store = FakePollStore([request])
    submitted: list[Task] = []

    async def submit(task: Task) -> None:
        submitted.append(task)

    watcher = RequestWatcher(
        submit=submit,
        source_store=FakeSourceStore(source),
        task_store=FakeTaskStore(None),
        poll_requests=poll_store,
        retry_requests=FakeRetryStore(),
    )

    await watcher.process_once()

    assert len(submitted) == 1
    assert submitted[0].task_type == TaskType.FETCH_SOURCE
    assert submitted[0].payload["source_id"] == str(source.id)
    assert submitted[0].payload["manual_poll"] is True
    assert poll_store.done == [(request.id, submitted[0].task_id)]
    assert poll_store.failed == []


async def test_request_watcher_turns_retry_request_into_failed_task_retry():
    row = FakeTaskRow(payload={"raw_record_id": str(uuid4())}, task_type="parse_content")
    request = FakeRetryRequest(task_id=row.id)
    retry_store = FakeRetryStore([request])
    submitted: list[Task] = []

    async def submit(task: Task) -> None:
        submitted.append(task)

    watcher = RequestWatcher(
        submit=submit,
        source_store=FakeSourceStore(None),
        task_store=FakeTaskStore(row),
        poll_requests=FakePollStore(),
        retry_requests=retry_store,
    )

    await watcher.process_once()

    assert len(submitted) == 1
    assert submitted[0].task_type == TaskType.PARSE_CONTENT
    assert submitted[0].task_id == row.id
    assert submitted[0].attempt == 0
    assert retry_store.done == [(request.id, row.id)]
    assert retry_store.failed == []


async def test_request_watcher_rejects_retry_for_non_failed_task():
    row = FakeTaskRow(status="done")
    request = FakeRetryRequest(task_id=row.id)
    retry_store = FakeRetryStore([request])

    async def submit(task: Task) -> None:
        raise AssertionError("submit must not be called")

    watcher = RequestWatcher(
        submit=submit,
        source_store=FakeSourceStore(None),
        task_store=FakeTaskStore(row),
        poll_requests=FakePollStore(),
        retry_requests=retry_store,
    )

    await watcher.process_once()

    assert retry_store.done == []
    assert retry_store.failed
    assert "not failed" in retry_store.failed[0][1]
