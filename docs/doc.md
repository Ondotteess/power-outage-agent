# Power Outage Agent — Code Documentation

Документация по коду MVP-скелета. Описывает структуру проекта, зависимости, контракты модулей и поток данных. Соответствует спецификации в `docs/spec.md`.

---

## 1. Обзор

Система реализована как асинхронный event-driven pipeline на `asyncio`. Точка входа — `app/main.py`. Все компоненты связаны через единственную in-memory очередь задач (`TaskQueue`) и центральный `Dispatcher`, который роутит задачи по типу к зарегистрированным хендлерам.

**Архитектурный слой (текущая итерация):**

```
Scheduler ──submit──▶ Dispatcher ──▶ TaskStore (tasks: pending/running/done/failed)
                          │
                          └─▶ CollectorHandler (FETCH_SOURCE)
                                   │
                                   ├─▶ HtmlCollector.fetch
                                   ├─▶ RawStore.save (raw_records, dedup по content_hash)
                                   └─▶ submit(PARSE_CONTENT)  ← ждёт ParseHandler в Week 2
```

DLQ = строки в `tasks` со `status='failed'`. Никаких внешних брокеров (Redis/Celery/Kafka).

Реализовано (конец Week 1 / начало Week 2):
- Scheduler, TaskQueue, Dispatcher с retry+DLQ;
- CollectorHandler с записью `RawRecord` и постановкой `PARSE_CONTENT`;
- ORM-модели на SQLAlchemy 2.0 (Uuid + DateTime(tz=True));
- репозитории TaskStore / RawStore / SourceStore;
- загрузка активных источников из БД, идемпотентный seed.

Не реализовано: ParseHandler, Normalizer (LLM), Dedup, Matcher, Notifier, FastAPI-эндпоинты.

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
│   │   ├── models.py             — ORM-модели: Source, RawRecord, TaskRecord
│   │   └── repositories.py       — TaskStore, RawStore, SourceStore
│   │
│   ├── models/
│   │   └── schemas.py            — Pydantic-схемы (по spec §7)
│   │
│   ├── workers/
│   │   ├── queue.py              — Task, TaskType, TaskQueue
│   │   ├── scheduler.py          — Scheduler, SourceConfig
│   │   ├── dispatcher.py         — Dispatcher (retry + DLQ + lifecycle)
│   │   └── collector.py          — CollectorHandler (хендлер FETCH_SOURCE)
│   │
│   ├── parsers/
│   │   ├── base.py               — BaseCollector (ABC)
│   │   └── html_collector.py     — HtmlCollector (httpx)
│   │
│   ├── normalization/
│   │   └── base.py               — StubNormalizer (placeholder, Week 2)
│   │
│   ├── alerts/                   — пусто (Week 4)
│   └── api/                      — пусто (Week 4)
│
├── tests/
│   ├── conftest.py               — FakeTaskStore и общие фикстуры
│   ├── test_queue.py
│   ├── test_scheduler.py
│   ├── test_dispatcher.py
│   └── test_collector.py
│
├── docs/
│   ├── spec.md                   — определения и соглашения
│   ├── arch.md                   — основной источник архитектурных решений
│   ├── doc.md                    — этот файл (тех. док к коду)
│   └── commands.md               — шпаргалка по командам (запуск, тесты, БД)
│
├── pyproject.toml                — зависимости (single source of truth), ruff, pytest config
├── docker-compose.yml            — postgres + app
├── Dockerfile
├── .env.example
└── .gitignore
```

---

## 3. Зависимости

Объявлены в `pyproject.toml`:

| Пакет               | Версия    | Назначение                                          |
| ------------------- | --------- | --------------------------------------------------- |
| `fastapi`           | ≥ 0.115   | HTTP API (дашборд, эндпоинты для админ-операций)    |
| `uvicorn`           | ≥ 0.32    | ASGI-сервер для FastAPI                             |
| `pydantic`          | ≥ 2.10    | Валидация и сериализация (схемы событий)            |
| `pydantic-settings` | ≥ 2.7     | Загрузка конфига из `.env`                          |
| `sqlalchemy`        | ≥ 2.0     | ORM с поддержкой `asyncio`                          |
| `alembic`           | ≥ 1.14    | Миграции БД                                         |
| `asyncpg`           | ≥ 0.30    | Async-драйвер PostgreSQL                            |
| `httpx`             | ≥ 0.28    | Async HTTP-клиент для коллекторов                   |
| `beautifulsoup4`    | ≥ 4.12    | Парсинг HTML                                        |
| `openai`            | ≥ 1.58    | OpenAI-совместимый клиент (DeepSeek / GigaChat)     |

Dev-extras (`pip install .[dev]`):

| Пакет             | Версия    | Назначение                            |
| ----------------- | --------- | ------------------------------------- |
| `pytest`          | ≥ 8.3     | Тест-раннер                           |
| `pytest-asyncio`  | ≥ 0.24    | Async-тесты (`asyncio_mode = "auto"`) |
| `ruff`            | ≥ 0.8     | Линтер + форматтер                    |

**Внешние сервисы:**

- PostgreSQL 16+ (через docker-compose)
- LLM API (DeepSeek или GigaChat — оба OpenAI-совместимы; подключение в Week 2)
- Telethon (userbot, Week 2+) — для чтения публичных Telegram-каналов

---

## 4. Граф зависимостей модулей

```
main.py
  ├── config.settings
  ├── db.engine (init_db, async_session_factory)
  ├── db.repositories (TaskStore, RawStore, SourceStore)
  ├── workers.queue (TaskQueue, TaskType)
  ├── workers.dispatcher.Dispatcher
  ├── workers.collector.CollectorHandler
  └── workers.scheduler.Scheduler, SourceConfig

workers.dispatcher
  └── workers.queue (Task, TaskQueue, TaskType)

workers.collector
  ├── workers.queue (Task, TaskType)
  ├── parsers.base.BaseCollector
  ├── parsers.html_collector.HtmlCollector
  ├── db.repositories.RawStore
  └── models.schemas (RawRecordSchema, SourceType)

workers.scheduler
  └── workers.queue (Task, TaskType)

db.repositories
  ├── db.models (Source, RawRecord, TaskRecord)
  ├── models.schemas (RawRecordSchema)
  └── workers.queue (Task)

db.models
  └── db.engine.Base

db.engine
  └── config.settings
```

Связи проходят через `models/schemas` (DTO-слой) и `workers/queue` (транспорт). Хендлеры не зависят друг от друга — между ними только очередь.

---

## 5. Конфигурация

`app/config.py` определяет глобальный объект `settings: Settings`, наследник `BaseSettings`. Значения подгружаются из переменных окружения и/или файла `.env`.

| Поле                 | Дефолт                                                              | Описание                                |
| -------------------- | ------------------------------------------------------------------- | --------------------------------------- |
| `database_url`       | `postgresql+asyncpg://postgres:postgres@localhost:5432/outage_agent` | DSN для async SQLAlchemy                |
| `llm_base_url`       | `https://api.deepseek.com`                                          | Base URL OpenAI-совместимого LLM API    |
| `llm_api_key`        | `""`                                                                | API-ключ LLM-провайдера                 |
| `llm_model`          | `deepseek-chat`                                                     | Имя модели                              |
| `telegram_bot_token` | `""`                                                                | Bot API token (placeholder)             |
| `log_level`          | `INFO`                                                              | Уровень логирования                     |

`.env.example` — шаблон для копирования в `.env`. Файл `.env` в git не коммитится.

---

## 6. Модули

### 6.1. `app.models.schemas`

Pydantic-модели — единый DTO-слой между компонентами pipeline. Соответствуют spec §7.

| Класс                     | Описание                                                                    |
| ------------------------- | --------------------------------------------------------------------------- |
| `SourceType` (Enum)       | `html` / `rss` / `telegram` / `json` / `other`                              |
| `EventType` (Enum)        | `power_outage` / `maintenance` / `infrastructure_failure` / `other`         |
| `ImpactLevel` (Enum)      | `low` / `medium` / `high`                                                   |
| `SourceSchema`            | Описание источника                                                          |
| `RawRecordSchema`         | Сырой ответ источника + метаданные (spec §7.1)                              |
| `LocationSchema`          | Адрес                                                                       |
| `NormalizedEventSchema`   | Нормализованное событие (spec §7.2)                                         |
| `OfficeImpactSchema`      | Сопоставление события с офисом (spec §7.3)                                  |
| `NotificationSchema`      | Сообщение для отправки в канал (spec §7.4)                                  |

### 6.2. `app.db.engine`

Инфраструктура async SQLAlchemy.

| Объект                  | Тип                       | Назначение                                                 |
| ----------------------- | ------------------------- | ---------------------------------------------------------- |
| `engine`                | `AsyncEngine`             | Единый engine, читает `settings.database_url`              |
| `async_session_factory` | `async_sessionmaker`      | Фабрика сессий, `expire_on_commit=False`                   |
| `Base`                  | `DeclarativeBase`         | Базовый класс для ORM-моделей                              |
| `get_session()`         | async generator           | DI-провайдер сессии                                        |
| `init_db()`             | async function            | Создаёт все таблицы через `Base.metadata.create_all`       |

В production-режиме `init_db()` будет заменён на Alembic-миграции.

### 6.3. `app.db.models`

ORM-модели (SQLAlchemy 2.x style с `Mapped[...]`, явные `Uuid` и `DateTime(timezone=True)`).

| Таблица        | Поля                                                                                                            | Назначение                                       |
| -------------- | --------------------------------------------------------------------------------------------------------------- | ------------------------------------------------ |
| `sources`      | `id`, `name`, `url`, `source_type`, `poll_interval_seconds`, `is_active`, `parser_profile (JSON)`, `created_at`. UNIQUE(`source_type`, `url`) | Реестр источников                                |
| `raw_records`  | `id`, `source_id (FK)`, `source_url`, `source_type`, `raw_content`, `content_hash (UNIQUE)`, `fetched_at`, `trace_id`         | Иммутабельное хранилище сырых ответов            |
| `tasks`        | `id`, `task_type`, `input_hash (idx)`, `status`, `attempt`, `payload (JSON)`, `error`, `trace_id`, `created_at`, `updated_at` | Лог задач очереди (включая DLQ как `status='failed'`) |

Все таймстемпы — UTC (`datetime.now(timezone.utc)`), колонки `DateTime(timezone=True)`.

### 6.4. `app.db.repositories`

- **`TaskStore.upsert(task, status, error=None)`** — `session.merge` записи `TaskRecord` по `task.task_id`. Используется Dispatcher на каждом переходе `pending → running → done / failed`.
- **`RawStore.exists_by_hash(content_hash)`** — проверка дедупа raw-контента.
- **`RawStore.save(raw, source_id)`** — INSERT в `raw_records`. Unique-индекс по `content_hash` страхует от гонок.
- **`SourceStore.list_active()`** — выбирает `Source.is_active = True`.
- **`SourceStore.seed_if_empty(defaults)`** — если таблица пуста, вставляет seed-записи. Идемпотентен.

### 6.5. `app.workers.queue`

In-memory очередь задач на `asyncio.Queue`.

**`TaskType` (StrEnum):** значения соответствуют spec §3.2 — `fetch_source`, `parse_content`, `normalize_event`, `deduplicate_event`, `match_offices`, `emit_event`.

**`Task` (dataclass):**

| Поле           | Тип        | Описание                                              |
| -------------- | ---------- | ----------------------------------------------------- |
| `task_type`    | `TaskType` | Тип задачи                                            |
| `payload`      | `dict`     | Произвольные данные                                   |
| `trace_id`     | `UUID`     | Сквозной trace для логов и событий                    |
| `task_id`      | `UUID`     | Уникальный ID задачи (autogen)                        |
| `attempt`      | `int`      | Номер попытки                                         |
| `max_attempts` | `int`      | Лимит retry, по умолчанию 5                           |
| `created_at`   | `datetime` | Момент создания (UTC)                                 |

`input_hash` — SHA256 от `(task_type, payload)`. Используется как индекс идемпотентности в `tasks`.

**`TaskQueue`** — тонкая обёртка над `asyncio.Queue`: `put`, `get`, `task_done`, `size`. DLQ — в БД, не в памяти.

### 6.6. `app.workers.scheduler`

`SourceConfig`: `source_id (UUID)`, `url`, `source_type`, `poll_interval_seconds`.

`Scheduler` принимает callable `submit: Callable[[Task], Awaitable[None]]` (обычно `Dispatcher.submit`). Для каждого источника запускает корутину `_poll(source)`: в цикле формирует `Task(FETCH_SOURCE, ...)` и вызывает `submit`, затем спит `poll_interval_seconds`.

Источники добавляются через `add_source()`. На старте `main.py` они загружаются из таблицы `sources` через `SourceStore.list_active()`.

### 6.7. `app.workers.dispatcher`

**`Dispatcher`** — центральный роутер. Параметры: `queue`, `task_store`, `backoff_base=2`, `backoff_max=600`.

Методы:

- `register(task_type, handler)` — регистрация хендлера (корутины `async (Task) -> None`).
- `submit(task)` — записывает `status='pending'` в `tasks` и кладёт в очередь.
- `run()` — основной цикл: `queue.get` → роутит по `task_type`.

Lifecycle одной задачи:
1. `submit` → `pending`
2. dispatcher берёт задачу → `running`
3. handler возвращается без исключения → `done`
4. handler бросил исключение:
   - `attempt += 1`
   - если `attempt < max_attempts`: `pending` + `error`, `asyncio.sleep(min(2**attempt, 600))`, обратно в очередь
   - иначе: `failed` + `error` (DLQ).

Если хендлер для `task_type` не зарегистрирован — задача дропается с warning (запись `pending` от submit остаётся в БД, в очередь не возвращается).

### 6.8. `app.workers.collector`

`CollectorHandler` — хендлер `FETCH_SOURCE`. Параметры: `submit`, `raw_store`, опционально `collectors: dict[str, BaseCollector]` (по умолчанию — `{"html": HtmlCollector()}`; используется для DI в тестах).

Логика `handle(task)`:

1. Извлекает `source_type`, `url`, `source_id` из payload.
2. Выбирает коллектор из словаря, вызывает `collector.fetch(url, trace_id)` → `RawRecordSchema`.
3. Если `raw_store.exists_by_hash(content_hash)` — пропускает (RAW-dedup).
4. Иначе — `raw_store.save(raw, source_id)`.
5. Ставит новую задачу `PARSE_CONTENT(raw_record_id=...)` через `submit`.

### 6.9. `app.parsers.base` / `app.parsers.html_collector`

`BaseCollector.fetch(url, trace_id) -> RawRecordSchema` — контракт. `HtmlCollector` — реализация на `httpx.AsyncClient` (timeout 30s, redirects on, кастомный User-Agent). HTTP-ошибки бросаются и попадают в retry-loop Dispatcher.

### 6.10. `app.normalization.base`

`StubNormalizer.normalize(raw) -> None` — заглушка. Реальный нормализатор появится в Итерации 3 (LLM + DaData).

### 6.11. `app.main`

Точка входа. CLI-аргумент `--log-level {DEBUG, INFO, WARNING, ERROR}` (default — `settings.log_level` из `.env`). Помощь: `python -m app.main --help`.

Последовательность:

1. `_parse_args()` — argparse.
2. `_setup_logging(level)` — `logging.basicConfig(force=True, ...)` (force нужен потому что библиотеки могли уже навесить handlers); глушит `httpx`/`httpcore` до WARNING, `sqlalchemy.engine` до INFO на DEBUG / WARNING иначе.
3. `await init_db()` — создание таблиц. `OSError` (Postgres недоступен) перехватывается и выводится понятным сообщением вместо стектрейса.
4. Создание `TaskQueue`, `TaskStore`, `RawStore`.
5. `Dispatcher(queue, task_store)`; регистрация `CollectorHandler` на `FETCH_SOURCE`.
6. `Scheduler(dispatcher.submit)`; `await _bootstrap_sources()` (seed + load из БД).
7. `asyncio.gather(scheduler.run(), dispatcher.run())`.

---

## 7. Поток данных (current)

```
[Scheduler._poll]
   ↓ Task(FETCH_SOURCE, payload={url, source_id, source_type})
[Dispatcher.submit]
   ├─▶ TaskStore.upsert(status="pending")
   └─▶ TaskQueue.put
        ↓
[Dispatcher.run / _process]
   ├─▶ TaskStore.upsert(status="running")
   ├─▶ CollectorHandler.handle(task)
   │        ├─▶ HtmlCollector.fetch  → RawRecordSchema
   │        ├─▶ RawStore.exists_by_hash  (skip if dup)
   │        ├─▶ RawStore.save
   │        └─▶ submit(Task(PARSE_CONTENT, payload={raw_record_id}))
   └─▶ TaskStore.upsert(status="done")
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

`PARSE_CONTENT` пока не имеет хендлера — Dispatcher логирует warning и дропает задачу из очереди (запись `pending` в БД остаётся, при перезапуске её можно перезабрать в Итерации 2).

---

## 8. Запуск

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
pytest                # все тесты
ruff check .          # линт
ruff format .         # форматирование
```

Полный набор команд — см. [commands.md](commands.md).

---

## 9. Соответствие спецификации

| Раздел spec                  | Реализация                                                       |
| ---------------------------- | ---------------------------------------------------------------- |
| §3.1 Scheduler               | `app/workers/scheduler.py`                                       |
| §3.2 Task System             | `app/workers/queue.py` + `app/workers/dispatcher.py`             |
| §3.3 Collectors              | `app/parsers/base.py`, `app/parsers/html_collector.py`           |
| §3.4 Adaptive Parser         | архитектурно (словарь коллекторов в `CollectorHandler`); LLM-fallback — Week 2 |
| §3.5 Raw Storage             | `app/db/models.py::RawRecord` + `RawStore`                       |
| §3.6 Normalization           | `app/normalization/base.py::StubNormalizer` (заглушка)           |
| §3.7 Dedup Engine            | RAW-уровень — по `content_hash`; событийный dedup — Week 2       |
| §3.8 Office Matcher          | Week 3                                                           |
| §3.9 Notifier                | Week 4                                                           |
| §6.1 Форматы данных          | UTC-таймстемпы, UUIDv4, snake_case                               |
| §6.2 Retry / идемпотентность | `Dispatcher._on_error`, `Task.input_hash`, `tasks.status`        |
| §6.3 Логирование / трейсинг  | `trace_id` пробрасывается через `Task` → `RawRecordSchema` → лог |
| §7 Схемы                     | `app/models/schemas.py`                                          |

---

## 10. Логирование

Уровень задаётся CLI-аргументом `--log-level {DEBUG, INFO, WARNING, ERROR}` (default — `settings.log_level` из `.env`).

**Формат записи:**

```
2026-05-11 11:53:26 INFO     app.workers.dispatcher                   Dispatcher started ...
└── timestamp        └─level └── module (40 chars)                    └── message
```

**Что показывает каждый уровень:**

| Уровень   | Что видно                                                                                                         |
| --------- | ----------------------------------------------------------------------------------------------------------------- |
| `DEBUG`   | Всё: каждый `Queue PUT/GET`, `TaskStore upsert`, HTTP request/response с кодом и Content-Type, регистрация источников, payload задач, SQL-запросы (sqlalchemy.engine на INFO). |
| `INFO`    | Нормальный поток: запуск pipeline, число загруженных источников, `Collector fetched N bytes`, `done` задачи, следующий poll через N сек. |
| `WARNING` | Retry с номером попытки и backoff; неизвестный `task_type` (нет handler); пустой реестр источников.               |
| `ERROR`   | DLQ-fail после исчерпания попыток (с `task_id` / `attempts` / `trace_id` / error); неизвестный `source_type`; невозможность подключиться к БД. |

**Сквозной `trace_id`** — UUID, который создаётся в `Scheduler._poll` и пробрасывается через `Task` → `RawRecordSchema` → все лог-записи. По нему можно собрать всю цепочку обработки одного fetch.

**Шумные библиотеки:** `httpx` и `httpcore` зажаты до WARNING даже при `--log-level DEBUG` (иначе каждый HTTP-вызов засорит лог). `sqlalchemy.engine` показывает SQL только на DEBUG.

Технический момент: `logging.basicConfig(force=True, ...)` нужен потому что библиотеки могут добавить root-handlers при импорте — без `force` наш уровень и формат были бы проигнорированы.

---

## 11. Известные ограничения и TODO

- `PARSE_CONTENT` не имеет хендлера — Итерация 2.
- LLM-нормализация (DeepSeek/GigaChat) + DaData — Итерация 3.
- Нет Alembic-миграций — схема создаётся через `Base.metadata.create_all`.
- Нет реализации dedup на уровне нормализованных событий, matcher, notifier.
- Нет FastAPI-эндпоинтов и дашборда — Итерация 4.
- Для перезапуска: pending-задачи остаются в `tasks`, но из очереди стираются — re-enqueue из БД пока не реализован.
