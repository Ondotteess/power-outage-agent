from __future__ import annotations

import json
from datetime import date, timedelta
from uuid import uuid4

from app.parsers.eseti import EsetiParser


def _today_iso(days_offset: int = 0, hour: int = 10, minute: int = 0) -> str:
    d = date.today() + timedelta(days=days_offset)
    return f"{d.isoformat()}T{hour:02d}:{minute:02d}:00"


def _item(
    *,
    shutdown: str | None = None,
    restore: str | None = None,
    street: str = "ул Тестовая",
    region: str = "",
    city: str = "",
    houses: str = "б/н",
    type_: str = "Плановая",
) -> dict:
    return {
        "region": region,
        "city": city,
        "street": street,
        "commaSeparatedHouses": houses,
        "shutdownDate": shutdown or _today_iso(1),
        "restoreDate": restore or _today_iso(1, hour=17),
        "type": type_,
    }


class TestEsetiParser:
    def setup_method(self):
        self.parser = EsetiParser()
        self.raw_id = uuid4()
        self.source_id = uuid4()
        self.trace_id = uuid4()
        self.profile = {"date_filter_days": 4}

    def _parse(self, items):
        return self.parser.parse(
            raw_content=json.dumps(items),
            raw_record_id=self.raw_id,
            source_id=self.source_id,
            trace_id=self.trace_id,
            parser_profile=self.profile,
        )

    def test_parses_record_in_window(self):
        items = [_item(shutdown=_today_iso(1), street="«Возрождение» сельхозкооператив")]
        results = self._parse(items)
        assert len(results) == 1
        r = results[0]
        assert r.location_street == "«Возрождение» сельхозкооператив"
        assert r.reason == "Плановая"
        assert r.extra == {"houses": "б/н"}

    def test_start_time_converted_to_utc(self):
        items = [_item(shutdown=_today_iso(1, hour=10, minute=0))]
        r = self._parse(items)[0]
        # naive 10:00 in UTC+7 → 03:00 UTC
        assert r.start_time is not None
        assert r.start_time.hour == 3
        assert r.start_time.minute == 0

    def test_end_time_parsed(self):
        items = [_item(restore=_today_iso(1, hour=17, minute=30))]
        r = self._parse(items)[0]
        assert r.end_time is not None
        assert r.end_time.hour == 10  # 17:30 UTC+7 → 10:30 UTC
        assert r.end_time.minute == 30

    def test_filters_past_dates(self):
        items = [_item(shutdown=_today_iso(-1))]
        assert self._parse(items) == []

    def test_filters_beyond_window(self):
        items = [_item(shutdown=_today_iso(5))]
        assert self._parse(items) == []

    def test_includes_today_and_cutoff(self):
        items = [_item(shutdown=_today_iso(0)), _item(shutdown=_today_iso(4))]
        assert len(self._parse(items)) == 2

    def test_skips_record_without_date(self):
        items = [{"street": "ул X", "shutdownDate": None, "type": "Плановая"}]
        assert self._parse(items) == []

    def test_skips_record_with_invalid_date(self):
        items = [_item(shutdown="not a date")]
        assert self._parse(items) == []

    def test_empty_region_and_city_mapped_to_none(self):
        items = [_item(region="", city="", street="ул X")]
        r = self._parse(items)[0]
        assert r.location_region_code is None
        assert r.location_city is None
        assert r.location_street == "ул X"

    def test_handles_invalid_json(self):
        results = self.parser.parse(
            raw_content="<not json>",
            raw_record_id=self.raw_id,
            source_id=self.source_id,
            trace_id=self.trace_id,
            parser_profile=self.profile,
        )
        assert results == []

    def test_handles_non_list_json(self):
        results = self.parser.parse(
            raw_content='{"items": []}',
            raw_record_id=self.raw_id,
            source_id=self.source_id,
            trace_id=self.trace_id,
            parser_profile=self.profile,
        )
        assert results == []

    def test_extra_empty_when_no_houses(self):
        items = [_item(houses="")]
        r = self._parse(items)[0]
        assert r.extra == {}
