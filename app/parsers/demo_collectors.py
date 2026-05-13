from __future__ import annotations

import hashlib
import json
from datetime import UTC, date, datetime, timedelta
from uuid import UUID, uuid4

from app.models.schemas import RawRecordSchema, SourceType
from app.parsers.base import BaseCollector

_DATE_FMT = "%d.%m.%Y"

_SAMPLES = [
    {
        "city": "село Бичура",
        "street": "улица Кирова, 12",
        "houses": "12",
        "region": "RU-BU",
        "locality": "Республика Бурятия, Бичурский р-н, село Бичура",
        "reason": "Демо: плановые работы",
    },
    {
        "city": "село Бичура",
        "street": "улица Гагарина, 8",
        "houses": "8",
        "region": "RU-BU",
        "locality": "Республика Бурятия, Бичурский р-н, село Бичура",
        "reason": "Демо: замена оборудования",
    },
    {
        "city": "село Тимирязевское",
        "street": "улица Октябрьская, 70",
        "houses": "70",
        "region": "RU-TOM",
        "locality": "Томская обл, Томский р-н, село Тимирязевское",
        "reason": "Демо: ремонтные работы",
    },
    {
        "city": "Богашево",
        "street": "улица Киевская, 24",
        "houses": "24",
        "region": "RU-TOM",
        "locality": "Томская обл, Томский р-н, Богашево",
        "reason": "Демо: профилактика линии",
    },
    {
        "city": "деревня Губино",
        "street": "улица Весенняя, 3",
        "houses": "3",
        "region": "RU-TOM",
        "locality": "Томская обл, Томский р-н, деревня Губино",
        "reason": "Демо: переключение нагрузки",
    },
]


class DemoJsonCollector(BaseCollector):
    def __init__(self, records_per_source: int, run_id: str | None = None) -> None:
        self._records_per_source = max(1, records_per_source)
        self._run_id = run_id or str(uuid4())

    async def fetch(self, url: str, trace_id: UUID, verify_ssl: bool = True) -> RawRecordSchema:
        if "eseti" in url.casefold():
            content = json.dumps(
                _eseti_items(self._records_per_source, self._run_id), ensure_ascii=False
            )
        else:
            content = json.dumps(
                _rosseti_sib_items(self._records_per_source, self._run_id),
                ensure_ascii=False,
            )
        return _raw(url, SourceType.JSON, content, trace_id)


class DemoHtmlCollector(BaseCollector):
    def __init__(self, records_per_source: int, run_id: str | None = None) -> None:
        self._records_per_source = max(1, records_per_source)
        self._run_id = run_id or str(uuid4())

    async def fetch(self, url: str, trace_id: UUID, verify_ssl: bool = True) -> RawRecordSchema:
        content = _tomsk_html(self._records_per_source, self._run_id)
        return _raw(url, SourceType.HTML, content, trace_id)


def _rosseti_sib_items(limit: int, run_id: str) -> list[dict]:
    items: list[dict] = []
    for i, sample in enumerate(_take(limit)):
        d = _day(i)
        items.append(
            {
                "id": f"demo-sib-{run_id}-{i}",
                "date_start": d.strftime(_DATE_FMT),
                "date_finish": d.strftime(_DATE_FMT),
                "time_start": "10:00",
                "time_finish": "14:00",
                "gorod": sample["city"],
                "raion": "",
                "street": sample["street"],
                "region": sample["region"],
                "res": sample["reason"],
                "f_otkl": "demo",
            }
        )
    return items


def _eseti_items(limit: int, run_id: str) -> list[dict]:
    items: list[dict] = []
    for i, sample in enumerate(_take(limit)):
        d = _day(i)
        items.append(
            {
                "demoRunId": run_id,
                "region": sample["region"],
                "city": sample["city"],
                "street": sample["street"].split(",")[0],
                "commaSeparatedHouses": sample["houses"],
                "shutdownDate": f"{d.isoformat()}T11:00:00",
                "restoreDate": f"{d.isoformat()}T15:00:00",
                "type": sample["reason"],
            }
        )
    return items


def _tomsk_html(limit: int, run_id: str) -> str:
    rows = []
    for i, sample in enumerate(_take(limit)):
        d = _day(i).strftime(_DATE_FMT)
        rows.append(
            f"""
            <tr><td id="demo-tomsk-{run_id}-{i}">
              <p class="t1"><label>Населенный пункт:</label>{sample["locality"]}</p>
              <p class="t2"><label>Адрес:</label>{sample["street"]}</p>
              <p class="t3"><label>Дата:</label>{d}</p>
              <p class="t3"><label>Время:</label>с 09:00 до 13:00</p>
              <p class="t4"><label>Причина:</label>{sample["reason"]}</p>
              <p class="t5"><label>Оборудование:</label>demo-{i}</p>
            </td></tr>
            """
        )
    return f"""
    <html><body>
      <!-- demo-run-id: {run_id} -->
      <table class="shuthown_table"><tbody>{"".join(rows)}</tbody></table>
    </body></html>
    """


def _take(limit: int) -> list[dict]:
    if limit <= len(_SAMPLES):
        return _SAMPLES[:limit]
    return [_SAMPLES[i % len(_SAMPLES)] for i in range(limit)]


def _day(index: int) -> date:
    return date.today() + timedelta(days=index % 4)


def _raw(url: str, source_type: SourceType, content: str, trace_id: UUID) -> RawRecordSchema:
    return RawRecordSchema(
        id=uuid4(),
        source_url=url,
        source_type=source_type,
        raw_content=content,
        content_hash=hashlib.sha256(f"{url}\n{content}".encode()).hexdigest(),
        fetched_at=datetime.now(UTC),
        trace_id=trace_id,
    )
