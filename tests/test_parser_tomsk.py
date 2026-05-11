from __future__ import annotations

from datetime import date, timedelta
from uuid import uuid4

from app.parsers.rosseti_tomsk import RossetiTomskParser, _split_locality


def _today_str(offset: int = 0) -> str:
    return (date.today() + timedelta(days=offset)).strftime("%d.%m.%Y")


def _row_html(
    *,
    locality: str = "Томская обл, Томский р-н, деревня Нелюбино",
    address: str = "ул. Весенняя",
    date_str: str | None = None,
    time_str: str = "с 10:00 до 16:00",
    reason: str = "Ремонтные работы",
    equipment: str = "ТП Н-15-4",
    td_id: str = "bx_3218110189_32408",
) -> str:
    date_str = date_str or _today_str(1)
    return f"""
    <tr><td id="{td_id}">
      <p class="t1"><label>Населенный пункт:</label>{locality}</p>
      <p class="t2"><label>Адрес:</label>{address}</p>
      <p class="t3"><label>Дата:</label>{date_str}</p>
      <p class="t3"><label>Время:</label>{time_str}</p>
      <p class="t4"><label>Причина:</label>{reason}</p>
      <p class="t5"><label>Оборудование:</label>{equipment}</p>
    </td></tr>
    """


def _wrap(rows_html: str) -> str:
    return f"""
    <html><body>
      <table class="shuthown_table">
        <tbody>{rows_html}</tbody>
      </table>
    </body></html>
    """


class TestRossetiTomskParser:
    def setup_method(self):
        self.parser = RossetiTomskParser()
        self.raw_id = uuid4()
        self.source_id = uuid4()
        self.trace_id = uuid4()
        self.profile = {"date_filter_days": 4}

    def _parse(self, html: str):
        return self.parser.parse(
            raw_content=html,
            raw_record_id=self.raw_id,
            source_id=self.source_id,
            trace_id=self.trace_id,
            parser_profile=self.profile,
        )

    def test_parses_full_row(self):
        html = _wrap(_row_html(date_str=_today_str(1)))
        results = self._parse(html)
        assert len(results) == 1
        r = results[0]
        assert r.external_id == "bx_3218110189_32408"
        assert r.location_region_code == "Томская обл"
        assert r.location_district == "Томский р-н"
        assert r.location_city == "деревня Нелюбино"
        assert r.location_street == "ул. Весенняя"
        assert r.reason == "Ремонтные работы"
        assert r.extra["equipment"] == "ТП Н-15-4"

    def test_start_and_end_time_utc(self):
        html = _wrap(_row_html(date_str=_today_str(1), time_str="с 10:00 до 16:00"))
        r = self._parse(html)[0]
        # UTC+7 → UTC: 10:00 → 03:00, 16:00 → 09:00
        assert r.start_time is not None and r.start_time.hour == 3
        assert r.end_time is not None and r.end_time.hour == 9

    def test_filters_past_dates(self):
        html = _wrap(_row_html(date_str=_today_str(-1)))
        assert self._parse(html) == []

    def test_filters_beyond_window(self):
        html = _wrap(_row_html(date_str=_today_str(5)))
        assert self._parse(html) == []

    def test_includes_today_and_cutoff(self):
        html = _wrap(_row_html(date_str=_today_str(0)) + _row_html(date_str=_today_str(4)))
        assert len(self._parse(html)) == 2

    def test_returns_empty_when_table_missing(self):
        html = "<html><body><p>no table here</p></body></html>"
        assert self._parse(html) == []

    def test_handles_two_part_locality(self):
        html = _wrap(_row_html(locality="г Томск, село Тимирязевское", date_str=_today_str(1)))
        r = self._parse(html)[0]
        assert r.location_region_code is None  # 'г Томск' is not region
        assert r.location_district is None  # no 'р-н' marker
        assert r.location_city == "село Тимирязевское"

    def test_handles_unparseable_time(self):
        html = _wrap(_row_html(date_str=_today_str(1), time_str="всю ночь"))
        r = self._parse(html)[0]
        assert r.start_time is None
        assert r.end_time is None

    def test_skips_row_with_invalid_date(self):
        html = _wrap(_row_html(date_str="не дата"))
        assert self._parse(html) == []


class TestLocalitySplit:
    def test_three_parts(self):
        assert _split_locality("Томская обл, Томский р-н, деревня Нелюбино") == (
            "Томская обл",
            "Томский р-н",
            "деревня Нелюбино",
        )

    def test_two_parts_with_region(self):
        assert _split_locality("Красноярский край, село X") == (
            "Красноярский край",
            None,
            "село X",
        )

    def test_two_parts_with_district_only(self):
        # no region marker on first part — first becomes city, not region
        assert _split_locality("Томский р-н, деревня X") == (
            None,
            "Томский р-н",
            "деревня X",
        )

    def test_single_part(self):
        assert _split_locality("г Томск") == (None, None, "г Томск")

    def test_empty(self):
        assert _split_locality("") == (None, None, None)
