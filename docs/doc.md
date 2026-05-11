# Power Outage Agent — Code Documentation

Документация по коду. Описывает структуру проекта, зависимости, контракты модулей и поток данных. Соответствует спецификации в `docs/spec.md`.

---

## 1. Обзор

Система реализована как асинхронный event-driven pipeline на `asyncio`. Точка входа — `app/main.py`. Все компоненты связаны через единственную in-memory очередь задач (`TaskQueue`) и центральный `Dispatcher`, который роутит задачи по типу к зарегистрированным хендлерам.

**Архитектурный слой (после закрытия реестра источников, конец Week 2):**

```
Scheduler ──submit──▶ Dispatcher ──▶ TaskStore (tasks: pending/running/done/failed)
                          │
                          ├─▶ CollectorHandler (FETCH_SOURCE)
                          │      ├─▶ {Html, Json}Collector.fetch
                          │      ├─▶ RawStore.save (raw_records, dedup по content_hash)
                          │      └─▶ submit(PARSE_CONTENT) × N (если пагинация)
                          │
                          └─▶ ParseHandler (PARSE_CONTENT)
                                 ├─▶ парсер по parser_profile.parser
                                 │      (RossetiSibParser / RossetiTomskParser / EsetiParser)
                                 ├─▶ ParsedStore.save_many (parsed_records)
                                 └─▶ submit(NORMALIZE_EVENT) × N  ← ждёт нормализатор в Week 3
```

DLQ = строки в `tasks` со `status='failed'`. Никаких внешних брокеров (Redis/Celery/Kafka).

**Реализовано (конец Week 2):**

- Scheduler, TaskQueue, Dispatcher с retry+DLQ.
- CollectorHandler с пагинацией (`PAGEN_1`-style), подстановкой дат в URL (`date_start`/`date_end`) и опциональным отключением SSL-валидации — всё через `parser_profile` в БД.
- HtmlCollector + JsonCollector (на `httpx.AsyncClient`).
- ParseHandler с реестром парсеров (`rosseti_sib`, `rosseti_tomsk`, `eseti`).
- RossetiSibParser: парсит JSON-массив до 18к записей, фильтрует по окну `today..today+N`.
- RossetiTomskParser: парсит HTML-таблицу `table.shuthown_table > td > p.tN`, разбивает локалити на регион/район/город, парсит время в UTC.
- EsetiParser: парсит JSON DotNetNuke `/API/Shutdown` (~2700 записей), ISO-даты в UTC+7, маппит `street`/`commaSeparatedHouses` (там же часто кадастровые номера).
- ORM-модели и репозитории: `Source`, `RawRecord`, `ParsedRecord`, `TaskRecord` + `TaskStore`, `RawStore`, `SourceStore`, `ParsedStore`.

**Не реализовано:** Normalizer (LLM), Dedup на уровне событий, Office Matcher, Notifier, FastAPI-эндпоинты, Alembic-миграции.

---

## 2. Структура проекта

```
power-outage-agent/
├── app/
│   ├── __init__.py
│   ├── config.py                 — настройки (pydantic-settings)
│   ├── main.py                   — точка входа (asyncio.run)
│   │
│   ├── db/
│   │   ├── engine.py             — async engine, Base, init_db()
│   │   ├── models.py             — ORM: Source, RawRecord, ParsedRecord, TaskRecord
│   │   └── repositories.py       — TaskStore, RawStore, ParsedStore, SourceStore
│   │
│   ├── models/
│   │   └── schemas.py            — Pydantic-схемы (по spec §7 + ParsedRecordSchema)
│   │
│   ├── workers/
│   │   ├── queue.py              — Task, TaskType, TaskQueue
│   │   ├── scheduler.py          — Scheduler, SourceConfig
│   │   ├── dispatcher.py         — Dispatcher (retry + DLQ + lifecycle)
│   │   ├── collector.py          — CollectorHandler (FETCH_SOURCE, пагинация, date_params)
│   │   └── parser.py             — ParseHandler (PARSE_CONTENT) + реестр парсеров
│   │
│   ├── parsers/
│   │   ├── base.py               — BaseCollector (ABC)
│   │   ├── html_collector.py     — HtmlCollector (httpx, опц. verify_ssl)
│   │   ├── json_collector.py     — JsonCollector (httpx, опц. verify_ssl)
│   │   ├── rosseti_sib.py        — RossetiSibParser (JSON → ParsedRecord)
│   │   ├── rosseti_tomsk.py      — RossetiTomskParser (HTML → ParsedRecord)
│   │   └── eseti.py              — EsetiParser (JSON DNN → ParsedRecord)
│   │
│   ├── normalization/
│   │   └── base.py               — StubNormalizer (placeholder, Week 3)
│   │
│   ├── alerts/                   — пусто (Week 4)
│   └── api/                      — пусто (Week 4)
│
├── tests/
│   ├── conftest.py               — FakeTaskStore и общие фикстуры
│   ├── test_queue.py
│   ├── test_scheduler.py
│   ├── test_dispatcher.py
│   ├── test_collector.py         — + пагинация, date_params, verify_ssl
│   ├── test_parser.py            — RossetiSibParser + ParseHandler
│   ├── test_parser_tomsk.py      — RossetiTomskParser + locality split
│   └── test_parser_eseti.py      — EsetiParser
│
├── docs/
│   ├── spec.md                   — определения и соглашения
│   ├── arch.md                   — архитектурные решения
│   ├── doc.md                    — этот файл
│   └── commands.md               — шпаргалка по командам
│
├── pyproject.toml
├── docker-compose.yml
├── Dockerfile
├── .env.example
└── .gitignore
```

---

## 3. Зависимости

Объявлены в `pyproject.toml`:

| Пакет               | Версия    | Назначение                                          |
| ------------------- | --------- | --------------------------------------------------- |
| `fastapi`           | ≥ 0.115   | HTTP API (дашборд, Week 4)                          |
| `uvicorn`           | ≥ 0.32    | ASGI-сервер                                         |
| `pydantic`          | ≥ 2.10    | Валидация и сериализация                            |
| `pydantic-settings` | ≥ 2.7     | Загрузка конфига из `.env`                          |
| `sqlalchemy`        | ≥ 2.0     | ORM с поддержкой `asyncio`                          |
| `alembic`           | ≥ 1.14    | Миграции БД (пока не используется)                  |
| `asyncpg`           | ≥ 0.30    | Async-драйвер PostgreSQL                            |
| `httpx`             | ≥ 0.28    | Async HTTP-клиент                                   |
| `beautifulsoup4`    | ≥ 4.12    | Парсинг HTML                                        |
| `openai`            | ≥ 1.58    | OpenAI-совместимый клиент (Week 3)                  |

Dev-extras (`pip install -e ".[dev]"`):

| Пакет             | Версия    | Назначение                            |
| ----------------- | --------- | ------------------------------------- |
| `pytest`          | ≥ 8.3     | Тест-раннер                           |
| `pytest-asyncio`  | ≥ 0.24    | Async-тесты (`asyncio_mode = "auto"`) |
| `ruff`            | ≥ 0.8     | Линтер + форматтер                    |

**Внешние сервисы:**

- PostgreSQL 16+ (через docker-compose).
- LLM API (DeepSeek или GigaChat) — Week 3.
- Telethon (userbot) — Week 3+.

---

## 4. Конфигурация

`app/config.py` определяет глобальный объект `settings: Settings`, наследник `BaseSettings`. Значения подгружаются из переменных окружения и/или файла `.env`.

| Поле                 | Дефолт                                                              | Описание                                |
| -------------------- | ------------------------------------------------------------------- | --------------------------------------- |
| `database_url`       | `postgresql+asyncpg://postgres:postgres@localhost:5432/outage_agent` | DSN для async SQLAlchemy                |
| `llm_base_url`       | `https://api.deepseek.com`                                          | Base URL OpenAI-совместимого LLM API    |
| `llm_api_key`        | `""`                                                                | API-ключ LLM-провайдера                 |
| `llm_model`          | `deepseek-chat`                                                     | Имя модели                              |
| `telegram_bot_token` | `""`                                                                | Bot API token (placeholder)             |
| `log_level`          | `INFO`                                                              | Уровень логирования                     |

`.env.example` — шаблон. Файл `.env` в git не коммитится.

---

## 5. БД и схемы

### 5.1 ORM-модели (`app/db/models.py`)

| Таблица          | Поля                                                                                                                                                                                              | Назначение                                       |
| ---------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------ |
| `sources`        | `id`, `name`, `url`, `source_type`, `poll_interval_seconds`, `is_active`, `parser_profile (JSON)`, `created_at`. UNIQUE(`source_type`, `url`)                                                     | Реестр источников                                |
| `raw_records`    | `id`, `source_id (FK)`, `source_url`, `source_type`, `raw_content`, `content_hash (UNIQUE)`, `fetched_at`, `trace_id`                                                                             | Иммутабельное хранилище сырых ответов            |
| `parsed_records` | `id`, `raw_record_id (FK)`, `source_id (FK)`, `external_id (varchar 64)`, `start_time`, `end_time`, `location_city (varchar 255)`, `location_district (varchar 255)`, `location_street (text)`, `location_region_code (varchar 128)`, `reason (text)`, `extra (JSON)`, `trace_id`, `extracted_at` | Структурированные записи до нормализации |
| `tasks`          | `id`, `task_type`, `input_hash (idx)`, `status`, `attempt`, `payload (JSON)`, `error`, `trace_id`, `created_at`, `updated_at`                                                                     | Лог задач очереди (DLQ как `status='failed'`)    |

Все таймстемпы — UTC (`datetime.now(timezone.utc)`), колонки `DateTime(timezone=True)`. Индексы: `parsed_records (raw_record_id)`, `parsed_records (source_id, external_id)`.

### 5.2 Pydantic-схемы (`app/models/schemas.py`)

| Класс                   | Описание                                                                    |
| ----------------------- | --------------------------------------------------------------------------- |
| `SourceType` (Enum)     | `html` / `rss` / `telegram` / `json` / `other`                              |
| `EventType` (Enum)      | `power_outage` / `maintenance` / `infrastructure_failure` / `other`         |
| `ImpactLevel` (Enum)    | `low` / `medium` / `high`                                                   |
| `SourceSchema`          | Описание источника                                                          |
| `RawRecordSchema`       | Сырой ответ источника + метаданные (spec §7.1)                              |
| `ParsedRecordSchema`    | Извлечённая структура до нормализации                                       |
| `LocationSchema`        | Адрес                                                                       |
| `NormalizedEventSchema` | Нормализованное событие (spec §7.2)                                         |
| `OfficeImpactSchema`    | Сопоставление события с офисом (spec §7.3)                                  |
| `NotificationSchema`    | Сообщение для отправки в канал (spec §7.4)                                  |

### 5.3 Репозитории (`app/db/repositories.py`)

- **`TaskStore.upsert(task, status, error=None)`** — `session.merge` записи `TaskRecord` по `task.task_id`. Используется Dispatcher на каждом переходе lifecycle.
- **`RawStore.exists_by_hash(content_hash)`** — дедуп raw-контента.
- **`RawStore.save(raw, source_id)`** — INSERT в `raw_records`.
- **`RawStore.get_by_id(raw_id)`** — нужен ParseHandler для загрузки raw перед парсингом.
- **`SourceStore.list_active()`** — выбирает активные источники.
- **`SourceStore.get_by_id(source_id)`** — нужен CollectorHandler для чтения `parser_profile`.
- **`SourceStore.seed_if_empty(defaults)`** — идемпотентный seed.
- **`ParsedStore.save_many(records)`** — батч-вставка `ParsedRecord`.

---

## 6. `parser_profile` — конфиг источника

Поле `parser_profile` (JSON) в таблице `sources` описывает специфику источника. Читается **CollectorHandler** (для fetch-логики) и **ParseHandler** (для выбора парсера).

| Ключ                 | Тип    | Назначение                                                                                                  |
| -------------------- | ------ | ----------------------------------------------------------------------------------------------------------- |
| `parser`             | str    | Имя парсера в реестре (`rosseti_sib`, `rosseti_tomsk`, `eseti`)                                             |
| `date_filter_days`   | int    | Окно фильтра дат на стороне парсера: `today..today+N` (по умолчанию 4)                                      |
| `verify_ssl`         | bool   | Проверять SSL-сертификат (по умолчанию `true`). Используем `false` для сайтов на российских корневых УЦ     |
| `paginate.param`     | str    | Имя URL-параметра пагинации (например `PAGEN_1` для Bitrix)                                                 |
| `paginate.max_pages` | int    | Сколько страниц подряд фетчить максимум                                                                     |
| `date_params`        | object | Шаблоны подстановки в URL: `{"date_start": "today", "date_end": "today+window"}`. Значения форматируются в `DD.MM.YYYY` |

Пример (Россети Томск из `_DEFAULT_SOURCES`):

```python
{
    "parser": "rosseti_tomsk",
    "date_filter_days": 4,
    "verify_ssl": False,
    "paginate": {"param": "PAGEN_1", "max_pages": 2},
}
```

Добавить новый источник = INSERT строка в `sources` + (опц.) написать парсер и зарегистрировать его в `_PARSER_REGISTRY` в `app/workers/parser.py`.

---

## 7. Модули

### 7.1 `app.workers.queue`

In-memory очередь задач на `asyncio.Queue`. `TaskType` (StrEnum): `fetch_source`, `parse_content`, `normalize_event`, `deduplicate_event`, `match_offices`, `emit_event`.

**`Task` (dataclass):** `task_type`, `payload`, `trace_id`, `task_id` (autogen), `attempt`, `max_attempts=5`, `created_at`. `input_hash` — SHA256 от `(task_type, payload)`, индекс идемпотентности в `tasks`.

**`TaskQueue`** — тонкая обёртка над `asyncio.Queue`. DLQ — в БД, не в памяти.

### 7.2 `app.workers.scheduler`

`SourceConfig`: `source_id (UUID)`, `url`, `source_type`, `poll_interval_seconds`. `Scheduler` принимает callable `submit` (обычно `Dispatcher.submit`). Для каждого источника запускает корутину `_poll(source)`: в цикле формирует `Task(FETCH_SOURCE, ...)` и вызывает `submit`, затем спит `poll_interval_seconds`.

### 7.3 `app.workers.dispatcher`

**`Dispatcher`** — центральный роутер. Параметры: `queue`, `task_store`, `backoff_base=2`, `backoff_max=600`.

Lifecycle одной задачи:
1. `submit` → `pending`
2. dispatcher берёт задачу → `running`
3. handler вернулся без исключения → `done`
4. handler бросил исключение:
   - `attempt += 1`
   - если `attempt < max_attempts`: `pending` + `error`, `asyncio.sleep(min(2**attempt, 600))`, обратно в очередь
   - иначе: `failed` + `error` (DLQ).

Если хендлер для `task_type` не зарегистрирован — задача дропается с warning (запись `pending` от submit остаётся в БД).

### 7.4 `app.workers.collector` — CollectorHandler (FETCH_SOURCE)

Параметры конструктора: `submit`, `raw_store`, `source_store` (опционально), `collectors: dict[str, BaseCollector]` (по умолчанию `{html: HtmlCollector(), json: JsonCollector()}`).

Логика `handle(task)`:

1. Извлекает `url`, `source_type`, `source_id` из `task.payload`.
2. Если есть `source_store` и `source_id` — подгружает `parser_profile` из БД.
3. Из профиля строит список URL'ов для фетча:
   - Подставляет даты в URL по `date_params` (`today`, `today+window`).
   - Если есть `paginate` — генерирует `[base?param=1, base?param=2, …, base?param=max_pages]`.
4. Для каждого URL: `collector.fetch(url, trace_id, verify_ssl)` → `RawStore.exists_by_hash` → `RawStore.save` → `submit(PARSE_CONTENT)`.

Дубликаты по `content_hash` пропускаются без записи и без enqueue парсинга.

### 7.5 `app.workers.parser` — ParseHandler (PARSE_CONTENT)

Реестр парсеров — модуль-уровневый dict:

```python
_PARSER_REGISTRY = {
    "rosseti_sib": RossetiSibParser(),
    "rosseti_tomsk": RossetiTomskParser(),
    "eseti": EsetiParser(),
}
```

Логика `handle(task)`:

1. Из payload берёт `raw_record_id`, грузит `RawRecord` через `RawStore.get_by_id`.
2. Через `SourceStore.get_by_id` получает `parser_profile` источника.
3. Выбирает парсер по `parser_profile["parser"]`. Если не найден — `ValueError` → retry → DLQ.
4. Вызывает `parser.parse(raw_content, raw_record_id, source_id, trace_id, parser_profile)` → `list[ParsedRecordSchema]`.
5. Батч-вставка через `ParsedStore.save_many`.
6. Для каждого `ParsedRecord` ставит `Task(NORMALIZE_EVENT, payload={parsed_record_id})`.

Контракт парсера: чистая функция (никаких побочных эффектов в БД/сеть), детерминированная относительно `(raw_content, parser_profile, today)`.

### 7.6 `app.parsers.base` / `html_collector` / `json_collector`

**`BaseCollector.fetch(url, trace_id, verify_ssl=True) -> RawRecordSchema`** — контракт. HTTP-ошибки бросаются и попадают в retry-loop Dispatcher.

- `HtmlCollector` — `httpx.AsyncClient(timeout=30, headers=_HEADERS, verify=verify_ssl)`, content_type `html`.
- `JsonCollector` — то же самое, плюс заголовок `Accept: application/json`, `Referer`, content_type `json`.

`verify_ssl=False` — для сайтов на корневых сертификатах, которых нет в стандартном `certifi`-бандле (например, российский Минцифры root CA на rosseti-tomsk.ru).

### 7.7 `app.parsers.rosseti_sib` — RossetiSibParser

Парсит ответ `data.php` Россетей Сибирь. Структура — плоский JSON-массив до 18к записей:

```json
{"id": "119490890", "region": "03", "raion": "Баргузинский р-н",
 "gorod": "с Баргузин", "street": "ул Братьев Козулиных",
 "date_start": "27.05.2026", "date_finish": "27.05.2026",
 "time_start": "11:00", "time_finish": "17:00",
 "f_otkl": "1", "res": "Баргузинский участок\r\n"}
```

Маппинг → `ParsedRecord`:
- `id` → `external_id`
- `region` → `location_region_code`, `raion` → `location_district`, `gorod` → `location_city`, `street` → `location_street`
- `date_start` + `time_start` (UTC+7) → `start_time` (UTC)
- `date_finish` + `time_finish` (UTC+7) → `end_time` (UTC)
- `res` (с обрезанным `\r\n`) → `reason`
- `f_otkl` → `extra.f_otkl`

Фильтр: оставляет записи где `today ≤ date_start ≤ today + date_filter_days`. Битый JSON или не-список → `[]` с warning.

### 7.8 `app.parsers.rosseti_tomsk` — RossetiTomskParser

Парсит HTML-страницу `planovie_otklucheniya.php` (Bitrix). Таблица `table.shuthown_table`, каждая строка `<tr>` содержит один `<td>` с пятью `<p class="tN">`, где значение идёт после `<label>`:

```html
<td id="bx_3218110189_32408">
  <p class="t1"><label>Населенный пункт:</label>Томская обл, Томский р-н, деревня Нелюбино</p>
  <p class="t2"><label>Адрес:</label>ул. Весенняя</p>
  <p class="t3"><label>Дата:</label>28.05.2026</p>
  <p class="t3"><label>Время:</label>с 10:00 до 16:00</p>
  <p class="t4"><label>Причина:</label>Ремонтные работы</p>
  <p class="t5"><label>Оборудование:</label>ТП Н-15-4</p>
</td>
```

`t3` встречается дважды (первый раз — дата, второй — время).

Маппинг → `ParsedRecord`:
- `td.id` → `external_id`
- Локалити сплитится по запятым: первый компонент с маркером `обл|край|респ|ао` → `location_region_code`; следующий с маркером `р-н|район` → `location_district`; последний → `location_city`. Без маркеров — всё уходит в `location_city`.
- `t2` → `location_street`
- Дата (DD.MM.YYYY) + время (с HH:MM до HH:MM), TZ = UTC+7 → `start_time`/`end_time` в UTC
- `t4` → `reason`, `t5` → `extra.equipment`

Сервер сортирует таблицу по убыванию даты, поэтому первые страницы покрывают `today+window` без необходимости серверного date-фильтра (он, кстати, на этом сайте работает странно — отдаёт пустые ответы).

### 7.9 `app.parsers.eseti` — EsetiParser

Парсит JSON-ответ DotNetNuke WebApi `https://www.eseti.ru/DesktopModules/ResWebApi/API/Shutdown`. Один запрос отдаёт весь массив (~2700 записей), пагинация на странице — клиентская в Angular Material:

```json
{
  "region": "",
  "city": "",
  "street": "«Возрождение» сельхозкооператив",
  "commaSeparatedHouses": "б/н",
  "shutdownDate": "2026-05-15T10:00:00",
  "restoreDate": "2026-05-15T16:00:00",
  "type": "Плановая"
}
```

Маппинг → `ParsedRecord`:
- `region` → `location_region_code` (часто пусто)
- `city` → `location_city` (часто пусто)
- `street` → `location_street` (свободный текст: улицы, кадастровые номера, описания участков)
- `commaSeparatedHouses` → `extra.houses` (номера домов или кадастры)
- `shutdownDate` (наивный ISO, считаем UTC+7 — Новосибирск) → `start_time` (UTC)
- `restoreDate` → `end_time`
- `type` → `reason` (значения вроде "Плановая")
- `external_id` = None (API не отдаёт ID)

Фильтр окна работает на стороне парсера: `today ≤ shutdownDate.date() ≤ today + date_filter_days`. Битый JSON или не-список → пустой результат + warning.

### 7.10 `app.normalization.base`

`StubNormalizer.normalize(raw) -> None` — заглушка. Реальный LLM-нормализатор появится в Week 3.

### 7.11 `app.main`

CLI-аргумент `--log-level {DEBUG, INFO, WARNING, ERROR}` (default — `settings.log_level` из `.env`).

Последовательность старта:

1. `_parse_args()`.
2. `_setup_logging(level)` — `logging.basicConfig(force=True, …)`; глушит `httpx`/`httpcore` до WARNING, `sqlalchemy.engine` до INFO на DEBUG / WARNING иначе.
3. `await init_db()`. `OSError` (Postgres недоступен) перехватывается и выводится понятным сообщением.
4. Создание `TaskQueue`, `TaskStore`, `RawStore`, `SourceStore`, `ParsedStore`.
5. `Dispatcher(queue, task_store)`.
6. Регистрация `CollectorHandler` на `FETCH_SOURCE` (с `source_store`) и `ParseHandler` на `PARSE_CONTENT`.
7. `Scheduler(dispatcher.submit)`, `await _bootstrap_sources()` — seed + load из БД.
8. `asyncio.gather(scheduler.run(), dispatcher.run())`.

Дефолтные источники в `_DEFAULT_SOURCES`:

- **Россети Сибирь** — JSON-API `https://www.rosseti-sib.ru/.../data.php`, `source_type=json`, `parser=rosseti_sib`, `date_filter_days=4`.
- **Россети Томск** — HTML-страница, `source_type=html`, `parser=rosseti_tomsk`, пагинация `PAGEN_1` (`max_pages=2`), `verify_ssl=False`.
- **eseti.ru** — JSON-API `https://www.eseti.ru/DesktopModules/ResWebApi/API/Shutdown`, `source_type=json`, `parser=eseti`, `date_filter_days=4`.

---

## 8. Поток данных

```
[Scheduler._poll]
   ↓ Task(FETCH_SOURCE, payload={url, source_id, source_type})
[Dispatcher.submit]
   ├─▶ TaskStore.upsert("pending")
   └─▶ TaskQueue.put
        ↓
[Dispatcher.run / _process]
   ├─▶ TaskStore.upsert("running")
   ├─▶ CollectorHandler.handle(task)
   │        ├─▶ SourceStore.get_by_id  → parser_profile
   │        ├─▶ build_urls (date_params + paginate)
   │        └─▶ for page in urls:
   │               ├─▶ Collector.fetch(url, verify_ssl)  → RawRecordSchema
   │               ├─▶ RawStore.exists_by_hash  (skip if dup)
   │               ├─▶ RawStore.save
   │               └─▶ submit(Task(PARSE_CONTENT, payload={raw_record_id}))
   └─▶ TaskStore.upsert("done")

[Dispatcher.run]
   ├─▶ ParseHandler.handle(task)
   │        ├─▶ RawStore.get_by_id  → RawRecord
   │        ├─▶ SourceStore.get_by_id  → parser_profile
   │        ├─▶ _PARSER_REGISTRY[parser_profile.parser].parse(...)  → list[ParsedRecordSchema]
   │        ├─▶ ParsedStore.save_many
   │        └─▶ for record: submit(Task(NORMALIZE_EVENT, payload={parsed_record_id}))
   └─▶ TaskStore.upsert("done")
```

На ошибке:

```
handler raises
   ↓
[Dispatcher._on_error]
   ├─ attempt += 1
   ├─ < max_attempts: TaskStore.upsert("pending", error); sleep(backoff); TaskQueue.put
   └─ ≥ max_attempts:  TaskStore.upsert("failed", error)   ← DLQ row
```

`NORMALIZE_EVENT` пока не имеет хендлера — Dispatcher логирует warning и дропает задачу.

---

## 9. Запуск

**Требования:** Python 3.11+, Docker Desktop.

```bash
cp .env.example .env                  # 1. Конфиг
docker compose up db -d               # 2. PostgreSQL
pip install -e ".[dev]"               # 3. Зависимости
python -m app.main --log-level INFO   # 4. Pipeline
```

Полный стек в Docker:

```bash
docker compose up --build
```

Тесты и линтер:

```bash
pytest                # все тесты (61)
ruff check .          # линт
ruff format .         # форматирование
```

Полный набор команд — см. [commands.md](commands.md).

---

## 10. Соответствие спецификации

| Раздел spec                  | Реализация                                                                         |
| ---------------------------- | ---------------------------------------------------------------------------------- |
| §3.1 Scheduler               | `app/workers/scheduler.py`                                                         |
| §3.2 Task System             | `app/workers/queue.py` + `app/workers/dispatcher.py`                               |
| §3.3 Collectors              | `app/parsers/base.py`, `html_collector.py`, `json_collector.py`                    |
| §3.4 Adaptive Parser         | `parser_profile` + `_PARSER_REGISTRY` в `app/workers/parser.py`; LLM-fallback — Week 3 |
| §3.5 Raw Storage             | `app/db/models.py::RawRecord` + `RawStore`                                         |
| §3.6 Normalization           | `app/normalization/base.py::StubNormalizer` (заглушка, Week 3)                     |
| §3.7 Dedup Engine            | RAW-уровень — по `content_hash`; событийный dedup — Week 3                         |
| §3.8 Office Matcher          | Week 3                                                                             |
| §3.9 Notifier                | Week 4                                                                             |
| §6.1 Форматы данных          | UTC-таймстемпы, UUIDv4, snake_case                                                 |
| §6.2 Retry / идемпотентность | `Dispatcher._on_error`, `Task.input_hash`, `tasks.status`                          |
| §6.3 Логирование / трейсинг  | `trace_id` пробрасывается через `Task` → `RawRecordSchema` → `ParsedRecordSchema` → лог |
| §7 Схемы                     | `app/models/schemas.py`                                                            |

---

## 11. Логирование

Уровень задаётся CLI-аргументом `--log-level {DEBUG, INFO, WARNING, ERROR}`.

**Формат записи:**

```
2026-05-12 11:53:26 INFO     app.workers.dispatcher                   Dispatcher started ...
└── timestamp        └─level └── module (40 chars)                    └── message
```

**Что показывает каждый уровень:**

| Уровень   | Что видно                                                                                                         |
| --------- | ----------------------------------------------------------------------------------------------------------------- |
| `DEBUG`   | Всё: `Queue PUT/GET`, `TaskStore upsert`, HTTP request/response, payload задач, SQL-запросы.                      |
| `INFO`    | Нормальный поток: запуск, `Collector fetched`, `RossetiSibParser total=N in_window=M`, `ParsedStore saved N`, `done`. |
| `WARNING` | Retry; неизвестный `task_type`; парсинг с ошибкой в одной строке; пустой реестр источников.                       |
| `ERROR`   | DLQ-fail после исчерпания попыток; неизвестный `source_type` / парсер; БД недоступна.                             |

**Сквозной `trace_id`** — UUID, создаётся в `Scheduler._poll` и пробрасывается через `Task` → `RawRecord` → `ParsedRecord` → лог. По нему собирается вся цепочка обработки одного fetch.

**Шумные библиотеки:** `httpx` и `httpcore` зажаты до WARNING даже на DEBUG. `sqlalchemy.engine` показывает SQL только на DEBUG.

---

## 12. Известные ограничения и TODO

- LLM-нормализация (DeepSeek/GigaChat) + DaData — Week 3.
- Нет Alembic-миграций — схема создаётся через `Base.metadata.create_all`.
- Нет дедупа на уровне нормализованных событий, нет Office Matcher, нет Notifier.
- Нет FastAPI-эндпоинтов и дашборда — Week 4.
- Для перезапуска: pending-задачи остаются в `tasks`, но из очереди стираются — re-enqueue из БД пока не реализован.
- При большом числе записей (Сибирь — 8к в окне) `submit(NORMALIZE_EVENT)` создаёт 8к INSERT в `tasks` подряд. Когда появится нормализатор — батчить или ограничить очередь.
- В `RossetiTomskParser` сплит локалити по запятым — простая эвристика, не покрывает все варианты (улицы с запятыми внутри, нестандартные форматы). Должен уехать в LLM-нормализатор.
- Для `rosseti-tomsk.ru` `verify_ssl=False` — компромисс. Корректное решение — установить Russian Trusted Root CA в `certifi`-bundle или системный cert store.
