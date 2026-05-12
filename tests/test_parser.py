from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import date, timedelta
from uuid import UUID, uuid4

import pytest

from app.models.schemas import ParsedRecordSchema
from app.parsers.rosseti_sib import RossetiSibParser
from app.workers.parser import ParseHandler
from app.workers.queue import Task, TaskType

# ---------------------------------------------------------------------------
# Fixtures / fakes
# ---------------------------------------------------------------------------


def _today_str(offset: int = 0) -> str:
    d = date.today() + timedelta(days=offset)
    return d.strftime("%d.%m.%Y")


def _make_item(
    id: str = "1",
    date_start: str | None = None,
    date_finish: str | None = None,
    time_start: str = "10:00",
    time_finish: str = "14:00",
    gorod: str = "г Новосибирск",
    raion: str = "Ленинский р-н",
    street: str = "ул Ленина",
    region: str = "54",
    res: str = "Новосибирский РЭС\r\n",
    f_otkl: str = "1",
) -> dict:
    ds = date_start or _today_str(1)
    return {
        "id": id,
        "date_start": ds,
        "date_finish": date_finish or ds,
        "time_start": time_start,
        "time_finish": time_finish,
        "gorod": gorod,
        "raion": raion,
        "street": street,
        "region": region,
        "res": res,
        "f_otkl": f_otkl,
    }


@dataclass
class FakeRawRecord:
    id: UUID = field(default_factory=uuid4)
    source_id: UUID | None = None
    raw_content: str = "[]"
    source_type: str = "json"


@dataclass
class FakeSource:
    id: UUID = field(default_factory=uuid4)
    parser_profile: dict = field(
        default_factory=lambda: {"parser": "rosseti_sib", "date_filter_days": 4}
    )


@dataclass
class FakeRawStore:
    record: FakeRawRecord | None = None

    async def get_by_id(self, raw_id: UUID) -> FakeRawRecord | None:
        return self.record


@dataclass
class FakeSourceStore:
    source: FakeSource | None = None

    async def get_by_id(self, source_id: UUID) -> FakeSource | None:
        return self.source


@dataclass
class FakeParsedStore:
    saved: list[list[ParsedRecordSchema]] = field(default_factory=list)

    async def save_many(self, records: list[ParsedRecordSchema]) -> None:
        self.saved.append(records)


# ---------------------------------------------------------------------------
# RossetiSibParser unit tests
# ---------------------------------------------------------------------------


class TestRossetiSibParser:
    def setup_method(self):
        self.parser = RossetiSibParser()
        self.raw_id = uuid4()
        self.source_id = uuid4()
        self.trace_id = uuid4()
        self.profile = {"date_filter_days": 4}

    def _parse(self, items: list[dict]) -> list[ParsedRecordSchema]:
        return self.parser.parse(
            raw_content=json.dumps(items),
            raw_record_id=self.raw_id,
            source_id=self.source_id,
            trace_id=self.trace_id,
            parser_profile=self.profile,
        )

    def test_parses_record_within_window(self):
        items = [_make_item(date_start=_today_str(1))]
        results = self._parse(items)
        assert len(results) == 1
        r = results[0]
        assert r.raw_record_id == self.raw_id
        assert r.source_id == self.source_id
        assert r.external_id == "1"
        assert r.location_city == "г Новосибирск"
        assert r.location_district == "Ленинский р-н"
        assert r.location_street == "ул Ленина"
        assert r.location_region_code == "54"
        assert r.reason == "Новосибирский РЭС"  # \r\n stripped

    def test_filters_out_past_dates(self):
        items = [_make_item(date_start=_today_str(-1))]
        assert self._parse(items) == []

    def test_filters_out_dates_beyond_window(self):
        items = [_make_item(date_start=_today_str(5))]
        assert self._parse(items) == []

    def test_includes_today(self):
        items = [_make_item(date_start=_today_str(0))]
        assert len(self._parse(items)) == 1

    def test_includes_exact_cutoff_day(self):
        items = [_make_item(date_start=_today_str(4))]
        assert len(self._parse(items)) == 1

    def test_start_time_converted_to_utc(self):
        items = [_make_item(date_start=_today_str(1), time_start="10:00")]
        r = self._parse(items)[0]
        # UTC+7 → UTC: 10:00 local = 03:00 UTC
        assert r.start_time is not None
        assert r.start_time.hour == 3

    def test_end_time_parsed(self):
        items = [_make_item(date_start=_today_str(1), time_finish="17:00")]
        r = self._parse(items)[0]
        assert r.end_time is not None
        assert r.end_time.hour == 10  # 17:00 UTC+7 = 10:00 UTC

    def test_skips_records_with_missing_date(self):
        items = [{"id": "99", "gorod": "Москва", "street": "ул Ленина"}]
        assert self._parse(items) == []

    def test_handles_invalid_json(self):
        results = self.parser.parse(
            raw_content="not json",
            raw_record_id=self.raw_id,
            source_id=self.source_id,
            trace_id=self.trace_id,
            parser_profile=self.profile,
        )
        assert results == []

    def test_handles_multiple_records(self):
        items = [
            _make_item(id="1", date_start=_today_str(1)),
            _make_item(id="2", date_start=_today_str(2)),
            _make_item(id="3", date_start=_today_str(10)),  # beyond window
        ]
        results = self._parse(items)
        assert len(results) == 2
        assert {r.external_id for r in results} == {"1", "2"}

    def test_res_field_stripped(self):
        items = [_make_item(date_start=_today_str(1), res="Баргузинский участок\r\n")]
        r = self._parse(items)[0]
        assert r.reason == "Баргузинский участок"

    def test_extra_contains_f_otkl(self):
        items = [_make_item(date_start=_today_str(1), f_otkl="1")]
        r = self._parse(items)[0]
        assert r.extra.get("f_otkl") == "1"


# ---------------------------------------------------------------------------
# ParseHandler integration tests
# ---------------------------------------------------------------------------


class TestParseHandler:
    def _make_handler(self, raw_record, source=None, **handler_kwargs):
        submitted = []

        async def submit(task: Task) -> None:
            submitted.append(task)

        raw_store = FakeRawStore(record=raw_record)
        source_store = FakeSourceStore(source=source)
        parsed_store = FakeParsedStore()
        handler = ParseHandler(submit, raw_store, source_store, parsed_store, **handler_kwargs)
        return handler, submitted, parsed_store

    def _make_task(self, raw_id: UUID) -> Task:
        return Task(
            task_type=TaskType.PARSE_CONTENT,
            payload={"raw_record_id": str(raw_id)},
            trace_id=uuid4(),
        )

    async def test_saves_records_and_enqueues_normalize(self):
        source = FakeSource()
        items = [
            _make_item(id="10", date_start=_today_str(1)),
            _make_item(id="11", date_start=_today_str(2)),
        ]
        raw = FakeRawRecord(source_id=source.id, raw_content=json.dumps(items))
        handler, submitted, parsed_store = self._make_handler(raw, source)

        await handler.handle(self._make_task(raw.id))

        assert len(parsed_store.saved) == 1
        assert len(parsed_store.saved[0]) == 2
        assert len(submitted) == 2
        assert all(t.task_type == TaskType.NORMALIZE_EVENT for t in submitted)

    async def test_raises_when_raw_record_missing(self):
        handler, _, _ = self._make_handler(raw_record=None)
        with pytest.raises(ValueError, match="not found"):
            await handler.handle(self._make_task(uuid4()))

    async def test_raises_on_unknown_parser(self):
        source = FakeSource(parser_profile={"parser": "nonexistent"})
        raw = FakeRawRecord(source_id=source.id, raw_content="[]")
        handler, _, _ = self._make_handler(raw, source)
        with pytest.raises(ValueError, match="nonexistent"):
            await handler.handle(self._make_task(raw.id))

    async def test_no_tasks_enqueued_when_nothing_in_window(self):
        source = FakeSource()
        items = [_make_item(id="1", date_start=_today_str(10))]  # beyond window
        raw = FakeRawRecord(source_id=source.id, raw_content=json.dumps(items))
        handler, submitted, parsed_store = self._make_handler(raw, source)

        await handler.handle(self._make_task(raw.id))

        assert submitted == []
        assert parsed_store.saved == []

    async def test_normalization_can_be_disabled_by_profile(self):
        source = FakeSource(parser_profile={"parser": "rosseti_sib", "normalize_enabled": False})
        raw = FakeRawRecord(source_id=source.id, raw_content=json.dumps([_make_item()]))
        handler, submitted, parsed_store = self._make_handler(raw, source)

        await handler.handle(self._make_task(raw.id))

        assert len(parsed_store.saved) == 1
        assert submitted == []

    async def test_normalization_limit_caps_enqueued_tasks(self):
        source = FakeSource(parser_profile={"parser": "rosseti_sib", "normalize_limit": 1})
        raw = FakeRawRecord(
            source_id=source.id,
            raw_content=json.dumps(
                [
                    _make_item(id="1", date_start=_today_str(1)),
                    _make_item(id="2", date_start=_today_str(1)),
                ]
            ),
        )
        handler, submitted, parsed_store = self._make_handler(raw, source)

        await handler.handle(self._make_task(raw.id))

        assert len(parsed_store.saved[0]) == 2
        assert len(submitted) == 1

    async def test_global_llm_normalization_flag_disables_enqueue(self):
        source = FakeSource(parser_profile={"parser": "rosseti_sib"})
        raw = FakeRawRecord(source_id=source.id, raw_content=json.dumps([_make_item()]))
        handler, submitted, parsed_store = self._make_handler(
            raw,
            source,
            llm_normalization_enabled=False,
        )

        await handler.handle(self._make_task(raw.id))

        assert len(parsed_store.saved) == 1
        assert submitted == []

    async def test_global_llm_normalization_cap_limits_enqueue(self):
        source = FakeSource(parser_profile={"parser": "rosseti_sib"})
        raw = FakeRawRecord(
            source_id=source.id,
            raw_content=json.dumps(
                [
                    _make_item(id="1", date_start=_today_str(1)),
                    _make_item(id="2", date_start=_today_str(1)),
                    _make_item(id="3", date_start=_today_str(1)),
                ]
            ),
        )
        handler, submitted, parsed_store = self._make_handler(
            raw,
            source,
            llm_normalization_max_per_raw=2,
        )

        await handler.handle(self._make_task(raw.id))

        assert len(parsed_store.saved[0]) == 3
        assert len(submitted) == 2

    async def test_parser_profile_override_enables_limited_normalization(self):
        source = FakeSource(parser_profile={"parser": "rosseti_sib", "normalize_enabled": False})
        raw = FakeRawRecord(
            source_id=source.id,
            raw_content=json.dumps(
                [
                    _make_item(id="1", date_start=_today_str(1)),
                    _make_item(id="2", date_start=_today_str(1)),
                    _make_item(id="3", date_start=_today_str(1)),
                ]
            ),
        )
        handler, submitted, parsed_store = self._make_handler(
            raw,
            source,
            parser_profile_override={"normalize_enabled": True, "normalize_limit": 2},
        )

        await handler.handle(self._make_task(raw.id))

        assert len(parsed_store.saved[0]) == 3
        assert len(submitted) == 2

    async def test_per_source_normalization_budget_spans_multiple_raw_records(self):
        source = FakeSource(parser_profile={"parser": "rosseti_sib"})
        raw1 = FakeRawRecord(
            source_id=source.id,
            raw_content=json.dumps(
                [
                    _make_item(id="1", date_start=_today_str(1)),
                    _make_item(id="2", date_start=_today_str(1)),
                ]
            ),
        )
        handler, submitted, _parsed_store = self._make_handler(
            raw1,
            source,
            llm_normalization_max_per_source=3,
        )

        await handler.handle(self._make_task(raw1.id))

        raw2 = FakeRawRecord(
            source_id=source.id,
            raw_content=json.dumps(
                [
                    _make_item(id="3", date_start=_today_str(1)),
                    _make_item(id="4", date_start=_today_str(1)),
                ]
            ),
        )
        handler._raw_store.record = raw2
        await handler.handle(self._make_task(raw2.id))

        assert len(submitted) == 3
