from __future__ import annotations

import hashlib
import json
from datetime import UTC, date, datetime, timedelta, timezone
from uuid import UUID, uuid4

from app.models.schemas import RawRecordSchema, SourceType
from app.parsers.base import BaseCollector

_DATE_FMT = "%d.%m.%Y"
_DEMO_TZ = timezone(timedelta(hours=7))
_START_TIME = "00:00"
_END_TIME = "23:59"

_THREAT_SAMPLES = [
    {
        "city": "Кемерово",
        "street": "проспект Ленина, 90",
        "houses": "90",
        "region": "RU-KEM",
        "locality": "Кемеровская область - Кузбасс, Кемерово",
        "reason": "Демо: плановое переключение питающей линии",
    },
    {
        "city": "Новокузнецк",
        "street": "улица Кирова, 55",
        "houses": "55",
        "region": "RU-KEM",
        "locality": "Кемеровская область - Кузбасс, Новокузнецк",
        "reason": "Демо: повреждение кабельной муфты",
    },
    {
        "city": "Прокопьевск",
        "street": "проспект Гагарина, 21",
        "houses": "21",
        "region": "RU-KEM",
        "locality": "Кемеровская область - Кузбасс, Прокопьевск",
        "reason": "Демо: ремонт оборудования на подстанции",
    },
    {
        "city": "Юрга",
        "street": "проспект Победы, 38",
        "houses": "38",
        "region": "RU-KEM",
        "locality": "Кемеровская область - Кузбасс, Юрга",
        "reason": "Демо: аварийная разгрузка фидера",
    },
    {
        "city": "Новосибирск",
        "street": "Красный проспект, 77",
        "houses": "77",
        "region": "RU-NVS",
        "locality": "Новосибирская область, Новосибирск",
        "reason": "Демо: реконфигурация городской сети",
    },
    {
        "city": "Бердск",
        "street": "улица Ленина, 33",
        "houses": "33",
        "region": "RU-NVS",
        "locality": "Новосибирская область, Бердск",
        "reason": "Демо: расчистка просеки рядом с ВЛ",
    },
    {
        "city": "Искитим",
        "street": "Южный микрорайон, 12",
        "houses": "12",
        "region": "RU-NVS",
        "locality": "Новосибирская область, Искитим",
        "reason": "Демо: замена силового трансформатора",
    },
    {
        "city": "Томск",
        "street": "проспект Ленина, 120",
        "houses": "120",
        "region": "RU-TOM",
        "locality": "Томская область, Томск",
        "reason": "Демо: профилактические работы на распределительном пункте",
    },
    {
        "city": "Северск",
        "street": "Коммунистический проспект, 45",
        "houses": "45",
        "region": "RU-TOM",
        "locality": "Томская область, Северск",
        "reason": "Демо: проверка релейной защиты",
    },
    {
        "city": "Колпашево",
        "street": "улица Кирова, 19",
        "houses": "19",
        "region": "RU-TOM",
        "locality": "Томская область, Колпашево",
        "reason": "Демо: осмотр линии после неблагоприятной погоды",
    },
    # --- Address-normalization stress samples ---------------------------------
    # The next three records intentionally mangle abbreviations / locality
    # prefixes / punctuation so that, before canonical_key, dedup saw them as
    # distinct events. They now share a key with the clean samples above and
    # the matcher hits the same offices. Keep them paired with the clean
    # variants — moving an office requires updating both lines.
    {
        "city": "Новокузнецк",
        "street": "ул. Кирова, д. 55",
        "houses": "55",
        "region": "RU-KEM",
        "locality": "Кемеровская область - Кузбасс, г. Новокузнецк",
        "reason": "Демо: проверка нормализации сокращений ул./д.",
    },
    {
        "city": "Кемерово",
        "street": "пр-т Ленина, 90",
        "houses": "90",
        "region": "RU-KEM",
        "locality": "Кемеровская область - Кузбасс, Кемерово",
        "reason": "Демо: проверка нормализации пр-т → проспект",
    },
    {
        "city": "г. Томск",
        "street": "пр. Ленина 120",
        "houses": "120",
        "region": "RU-TOM",
        "locality": "Томская область, г. Томск",
        "reason": "Демо: проверка нормализации без запятой и с префиксом г.",
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
    event_date = _demo_day()
    for i, sample in enumerate(_take(limit)):
        items.append(
            {
                "id": f"demo-sib-{run_id}-{i}",
                "date_start": event_date.strftime(_DATE_FMT),
                "date_finish": event_date.strftime(_DATE_FMT),
                "time_start": _START_TIME,
                "time_finish": _END_TIME,
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
    event_date = _demo_day()
    for sample in _take(limit):
        items.append(
            {
                "demoRunId": run_id,
                "region": sample["region"],
                "city": sample["city"],
                "street": sample["street"].split(",")[0],
                "commaSeparatedHouses": sample["houses"],
                "shutdownDate": f"{event_date.isoformat()}T{_START_TIME}:00",
                "restoreDate": f"{event_date.isoformat()}T{_END_TIME}:00",
                "type": sample["reason"],
            }
        )
    return items


def _tomsk_html(limit: int, run_id: str) -> str:
    rows = []
    event_date = _demo_day().strftime(_DATE_FMT)
    for i, sample in enumerate(_take(limit)):
        rows.append(
            f"""
            <tr><td id="demo-tomsk-{run_id}-{i}">
              <p class="t1"><label>Населенный пункт:</label>{sample["locality"]}</p>
              <p class="t2"><label>Адрес:</label>{sample["street"]}</p>
              <p class="t3"><label>Дата:</label>{event_date}</p>
              <p class="t3"><label>Время:</label>с {_START_TIME} до {_END_TIME}</p>
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
    if limit <= len(_THREAT_SAMPLES):
        return _THREAT_SAMPLES[:limit]
    return [_THREAT_SAMPLES[i % len(_THREAT_SAMPLES)] for i in range(limit)]


def _demo_day() -> date:
    return datetime.now(UTC).astimezone(_DEMO_TZ).date()


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
