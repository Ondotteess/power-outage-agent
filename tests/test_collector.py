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

    async def fetch(self, url: str, trace_id: UUID, verify_ssl: bool = True) -> RawRecordSchema:
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


@dataclass
class FakeSource:
    id: UUID = field(default_factory=uuid4)
    parser_profile: dict = field(default_factory=dict)


@dataclass
class FakeSourceStore:
    source: FakeSource | None = None
    requested_ids: list[UUID] = field(default_factory=list)

    async def get_by_id(self, source_id: UUID):
        self.requested_ids.append(source_id)
        return self.source


async def test_collector_paginates_with_parser_profile():
    submitted: list[Task] = []

    async def submit(task: Task) -> None:
        submitted.append(task)

    source_id = uuid4()
    source = FakeSource(
        id=source_id,
        parser_profile={"paginate": {"param": "PAGEN_1", "max_pages": 3}},
    )
    collector = FakeHtmlCollector(body="<html>page</html>")
    # Make every fetch produce a different content_hash so all pages get saved
    fetched_urls: list[str] = []
    original_fetch = collector.fetch

    async def varying_fetch(url: str, trace_id: UUID, verify_ssl: bool = True):
        fetched_urls.append(url)
        raw = await original_fetch(url, trace_id, verify_ssl=verify_ssl)
        # rewrite hash to be url-dependent
        import hashlib
        raw.content_hash = hashlib.sha256(url.encode()).hexdigest()
        return raw

    collector.fetch = varying_fetch  # type: ignore[assignment]

    handler = CollectorHandler(
        submit,
        FakeRawStore(),
        source_store=FakeSourceStore(source=source),
        collectors={"html": collector},
    )

    await handler.handle(
        Task(
            task_type=TaskType.FETCH_SOURCE,
            payload={"url": "https://x/list", "source_id": str(source_id), "source_type": "html"},
            trace_id=uuid4(),
        )
    )

    # 3 pages → 3 PARSE_CONTENT tasks
    assert len(submitted) == 3
    assert fetched_urls == [
        "https://x/list?PAGEN_1=1",
        "https://x/list?PAGEN_1=2",
        "https://x/list?PAGEN_1=3",
    ]


async def test_collector_injects_date_params_from_profile():
    """date_params templates (today, today+window) resolved into URL query params."""
    from datetime import date, timedelta

    fetched_urls: list[str] = []

    class CapturingCollector(FakeHtmlCollector):
        async def fetch(self, url: str, trace_id: UUID, verify_ssl: bool = True):
            fetched_urls.append(url)
            return await super().fetch(url, trace_id, verify_ssl=verify_ssl)

    source_id = uuid4()
    source = FakeSource(
        id=source_id,
        parser_profile={
            "date_filter_days": 4,
            "date_params": {"date_start": "today", "date_end": "today+window"},
        },
    )

    handler = CollectorHandler(
        lambda _t: _noop(),
        FakeRawStore(),
        source_store=FakeSourceStore(source=source),
        collectors={"html": CapturingCollector()},
    )

    await handler.handle(
        Task(
            task_type=TaskType.FETCH_SOURCE,
            payload={"url": "https://x/list", "source_id": str(source_id), "source_type": "html"},
            trace_id=uuid4(),
        )
    )

    today = date.today().strftime("%d.%m.%Y")
    cutoff = (date.today() + timedelta(days=4)).strftime("%d.%m.%Y")
    assert fetched_urls == [f"https://x/list?date_start={today}&date_end={cutoff}"]


async def _noop():
    return None


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
