from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from datetime import UTC, datetime
from uuid import UUID, uuid4

from app.models.schemas import RawRecordSchema, SourceType
from app.parsers.base import BaseCollector
from app.workers.collector import CollectorHandler
from app.workers.queue import Task, TaskType


class FakeHtmlCollector(BaseCollector):
    def __init__(self, body: str = "<html>ok</html>") -> None:
        self._body = body
        self.calls: list[tuple[str, UUID]] = []

    async def fetch(self, url: str, trace_id: UUID) -> RawRecordSchema:
        self.calls.append((url, trace_id))
        return RawRecordSchema(
            id=uuid4(),
            source_url=url,
            source_type=SourceType.HTML,
            raw_content=self._body,
            content_hash=hashlib.sha256(self._body.encode()).hexdigest(),
            fetched_at=datetime.now(UTC),
            trace_id=trace_id,
        )


@dataclass
class FakeRawStore:
    seen_hashes: set[str] = field(default_factory=set)
    saved: list[tuple[RawRecordSchema, UUID | None]] = field(default_factory=list)

    async def exists_by_hash(self, content_hash: str) -> bool:
        return content_hash in self.seen_hashes

    async def save(self, raw: RawRecordSchema, source_id: UUID | None) -> None:
        self.saved.append((raw, source_id))
        self.seen_hashes.add(raw.content_hash)


def _fetch_task(source_id: UUID) -> Task:
    return Task(
        task_type=TaskType.FETCH_SOURCE,
        payload={
            "url": "https://example.com/outages",
            "source_id": str(source_id),
            "source_type": "html",
        },
        trace_id=uuid4(),
    )


async def test_collector_persists_raw_and_enqueues_parse():
    submitted: list[Task] = []

    async def submit(task: Task) -> None:
        submitted.append(task)

    raw_store = FakeRawStore()
    collector = FakeHtmlCollector(body="<html>fresh</html>")
    handler = CollectorHandler(submit, raw_store, collectors={"html": collector})

    source_id = uuid4()
    task = _fetch_task(source_id)
    await handler.handle(task)

    assert collector.calls == [(task.payload["url"], task.trace_id)]
    assert len(raw_store.saved) == 1
    saved_raw, saved_source_id = raw_store.saved[0]
    assert saved_source_id == source_id
    assert saved_raw.source_url == task.payload["url"]

    assert len(submitted) == 1
    parse_task = submitted[0]
    assert parse_task.task_type == TaskType.PARSE_CONTENT
    assert parse_task.payload["raw_record_id"] == str(saved_raw.id)
    assert parse_task.trace_id == task.trace_id


async def test_collector_skips_duplicate_content_hash():
    submitted: list[Task] = []

    async def submit(task: Task) -> None:
        submitted.append(task)

    body = "<html>same</html>"
    raw_store = FakeRawStore(seen_hashes={hashlib.sha256(body.encode()).hexdigest()})
    handler = CollectorHandler(submit, raw_store, collectors={"html": FakeHtmlCollector(body=body)})

    await handler.handle(_fetch_task(uuid4()))

    assert raw_store.saved == []
    assert submitted == []


async def test_collector_rejects_unknown_source_type():
    submitted: list[Task] = []

    async def submit(task: Task) -> None:
        submitted.append(task)

    handler = CollectorHandler(submit, FakeRawStore(), collectors={"html": FakeHtmlCollector()})
    task = Task(
        task_type=TaskType.FETCH_SOURCE,
        payload={"url": "x", "source_type": "telegram"},
        trace_id=uuid4(),
    )

    try:
        await handler.handle(task)
    except ValueError as e:
        assert "telegram" in str(e)
    else:  # pragma: no cover
        raise AssertionError("expected ValueError")
