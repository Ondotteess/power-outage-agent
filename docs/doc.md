# Power Outage Agent — Code Documentation

Документация по коду MVP-скелета. Описывает структуру проекта, зависимости, контракты модулей и поток данных. Соответствует спецификации в `docs/spec.md`.

---

## 1. Обзор

Система реализована как асинхронный event-driven pipeline на `asyncio`. Точка входа — `app/main.py`. Все компоненты межмодульно связаны через единственную in-memory очередь задач (`TaskQueue`), без внешних брокеров (Redis/Celery/Kafka).

**Архитектурный слой:**

```
Scheduler → TaskQueue → Worker(s) → Collector → RawRecord (→ DB)
                                  ↓
                              Normalizer (LLM, TBD)
                                  ↓
                              Dedup → Office Matcher → Notifier
```

На текущей итерации (Week 1) реализованы: Scheduler, TaskQueue, CollectorWorker, BaseCollector + HtmlCollector, ORM-модели и Pydantic-схемы. Normalizer — заглушка. Dedup, Matcher, Notifier — ещё не реализованы.

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
│   │   ├── __init__.py
│   │   ├── engine.py             — async engine, Base, init_db()
│   │   └── models.py             — ORM-модели: Source, RawRecord, TaskRecord
│   │
│   ├── models/
│   │   ├── __init__.py
│   │   └── schemas.py            — Pydantic-схемы (по spec §7)
│   │
│   ├── workers/
│   │   ├── __init__.py
│   │   ├── queue.py              — Task, TaskType, TaskQueue
│   │   ├── scheduler.py          — Scheduler, SourceConfig
│   │   └── collector_worker.py   — CollectorWorker (retry + backoff)
│   │
│   ├── parsers/
│   │   ├── __init__.py
│   │   ├── base.py               — BaseCollector (ABC)
│   │   └── html_collector.py     — HtmlCollector (httpx)
│   │
│   ├── normalization/
│   │   ├── __init__.py
│   │   └── base.py               — StubNormalizer (placeholder)
│   │
│   ├── alerts/                   — пусто (Week 4)
│   └── api/                      — пусто (Week 4)
│
├── data/
│   ├── raw/                      — артефакты для аудита (опционально)
│   └── processed/
│
├── docs/
│   ├── spec.md                   — техническая спецификация
│   ├── arch.md                   — ссылка на схему архитектуры
│   └── doc.md                    — этот файл
│
├── pyproject.toml                — описание проекта (uv)
├── requirements.txt              — pip-зависимости
├── docker-compose.yml            — postgres + app
├── Dockerfile
├── .env.example
└── .gitignore
```

---

## 3. Зависимости

| Пакет             | Версия    | Назначение                                          |
| ----------------- | --------- | --------------------------------------------------- |
| `fastapi`         | ≥ 0.115   | HTTP API (дашборд, эндпоинты для админ-операций)    |
| `uvicorn`         | ≥ 0.32    | ASGI-сервер для FastAPI                             |
| `pydantic`        | ≥ 2.10    | Валидация и сериализация (схемы событий)            |
| `pydantic-settings` | ≥ 2.7   | Загрузка конфига из `.env`                          |
| `sqlalchemy`      | ≥ 2.0     | ORM с поддержкой `asyncio`                          |
| `alembic`         | ≥ 1.14    | Миграции БД                                         |
| `asyncpg`         | ≥ 0.30    | Async-драйвер PostgreSQL                            |
| `httpx`           | ≥ 0.28    | Async HTTP-клиент для коллекторов                   |
| `beautifulsoup4`  | ≥ 4.12    | Парсинг HTML-таблиц                                 |
| `openai`          | ≥ 1.58    | OpenAI-совместимый клиент (DeepSeek / GigaChat)     |

**Внешние сервисы:**

- PostgreSQL 16+ (через docker-compose)
- LLM API (DeepSeek или GigaChat — оба OpenAI-совместимы)
- Telegram Bot API (для приёма из публичных каналов; будет добавлен в Week 2)

---

## 4. Граф зависимостей модулей

```
main.py
  ├── config.settings
  ├── db.engine.init_db
  ├── workers.queue.TaskQueue
  ├── workers.scheduler.Scheduler, SourceConfig
  └── workers.collector_worker.CollectorWorker

workers.collector_worker
  ├── workers.queue (Task, TaskQueue, TaskType)
  ├── parsers.base.BaseCollector
  ├── parsers.html_collector.HtmlCollector
  └── models.schemas (RawRecordSchema, SourceType)

workers.scheduler
  └── workers.queue (Task, TaskQueue, TaskType)

parsers.html_collector
  ├── parsers.base.BaseCollector
  └── models.schemas (RawRecordSchema, SourceType)

db.models
  └── db.engine.Base

db.engine
  └── config.settings
```

Зависимости направлены **только сверху вниз**. `workers/`, `parsers/`, `normalization/` не знают друг о друге напрямую — связи проходят через `models/schemas` (DTO-слой) и `workers/queue` (транспорт).

---

## 5. Конфигурация

`app/config.py` определяет глобальный объект `settings: Settings`, наследник `BaseSettings`. Значения подгружаются из переменных окружения и/или файла `.env`.

| Поле                    | Дефолт                                                          | Описание                                |
| ----------------------- | --------------------------------------------------------------- | --------------------------------------- |
| `database_url`          | `postgresql+asyncpg://postgres:postgres@localhost:5432/outage_agent` | DSN для async SQLAlchemy           |
| `llm_base_url`          | `https://api.deepseek.com`                                      | Base URL OpenAI-совместимого LLM API    |
| `llm_api_key`           | `""`                                                            | API-ключ LLM-провайдера                 |
| `llm_model`             | `deepseek-chat`                                                 | Имя модели                              |
| `telegram_bot_token`    | `""`                                                            | Bot API token                           |
| `log_level`             | `INFO`                                                          | Уровень логирования                     |

`.env.example` — шаблон для копирования в `.env`. Файл `.env` в git не коммитится (`.gitignore`).

---

## 6. Модули

### 6.1. `app.models.schemas`

Pydantic-модели — единый DTO-слой между компонентами pipeline. Соответствуют spec §7.

| Класс                     | Описание                                                                    |
| ------------------------- | --------------------------------------------------------------------------- |
| `SourceType` (Enum)       | `html` / `rss` / `telegram` / `json` / `other`                              |
| `EventType` (Enum)        | `power_outage` / `maintenance` / `infrastructure_failure` / `other`         |
| `ImpactLevel` (Enum)      | `low` / `medium` / `high`                                                   |
| `SourceSchema`            | Описание источника (id, url, тип, частота опроса, профиль парсера)          |
| `RawRecordSchema`         | Сырой ответ источника + метаданные (spec §7.1)                              |
| `LocationSchema`          | Адрес: raw, normalized, city, street, building                              |
| `NormalizedEventSchema`   | Нормализованное событие отключения (spec §7.2)                              |
| `OfficeImpactSchema`      | Сопоставление события с офисом (spec §7.3)                                  |
| `NotificationSchema`      | Сообщение для отправки в канал (spec §7.4)                                  |

Используются как контракты между слоями (collector → worker → normalizer → matcher → notifier).

### 6.2. `app.db.engine`

Инфраструктура async SQLAlchemy.

| Объект                  | Тип                       | Назначение                                                 |
| ----------------------- | ------------------------- | ---------------------------------------------------------- |
| `engine`                | `AsyncEngine`             | Единый engine, читает `settings.database_url`              |
| `async_session_factory` | `async_sessionmaker`      | Фабрика сессий, `expire_on_commit=False`                   |
| `Base`                  | `DeclarativeBase`         | Базовый класс для ORM-моделей                              |
| `get_session()`         | async generator           | DI-провайдер сессии (для FastAPI Depends)                  |
| `init_db()`             | async function            | Создаёт все таблицы через `Base.metadata.create_all`       |

`init_db()` импортирует `app.db.models` лениво, чтобы избежать циклических импортов и при этом гарантировать регистрацию моделей в `Base.metadata`.

В production-режиме `init_db()` будет заменён на Alembic-миграции.

### 6.3. `app.db.models`

ORM-модели (SQLAlchemy 2.x style с `Mapped[...]`).

| Таблица        | Поля                                                                                                          | Назначение                              |
| -------------- | ------------------------------------------------------------------------------------------------------------- | --------------------------------------- |
| `sources`      | `id`, `name`, `url`, `source_type`, `poll_interval_seconds`, `is_active`, `parser_profile (JSON)`, `created_at` | Реестр активных источников              |
| `raw_records`  | `id`, `source_id`, `source_url`, `source_type`, `raw_content`, `content_hash` (idx), `fetched_at`, `trace_id`   | Иммутабельное хранилище сырых ответов   |
| `tasks`        | `id`, `task_type`, `input_hash` (idx), `status`, `attempt`, `payload (JSON)`, `error`, `trace_id`, `created_at`, `updated_at` | Лог задач очереди (для идемпотентности и аудита) |

Все таймстемпы — UTC (`datetime.now(timezone.utc)`).

### 6.4. `app.workers.queue`

In-memory очередь задач на `asyncio.Queue` + список DLQ.

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

`input_hash` — SHA256 от `(task_type, payload)`. Используется как ключ идемпотентности (spec §6.2).

**`TaskQueue`** — обёртка над `asyncio.Queue`:

| Метод               | Описание                                  |
| ------------------- | ----------------------------------------- |
| `put(task)`         | Поставить в очередь                       |
| `get() -> Task`     | Забрать (await)                           |
| `task_done()`       | Отметить выполнение                       |
| `move_to_dlq(task)` | Переместить в Dead Letter Queue           |
| `dlq` (property)    | Текущее содержимое DLQ                    |
| `size` (property)   | Размер основной очереди                   |

### 6.5. `app.workers.scheduler`

**`SourceConfig` (dataclass):** `source_id`, `url`, `source_type`, `poll_interval_seconds`.

**`Scheduler`:** для каждого источника запускает корутину `_poll(source)`, которая в бесконечном цикле:

1. Создаёт `Task(FETCH_SOURCE, payload={...}, trace_id=uuid4())`
2. Кладёт в очередь
3. Спит `poll_interval_seconds` (asyncio.sleep)

Все источники опрашиваются параллельно через `asyncio.gather`. Источники добавляются через `add_source()`. На текущей итерации — статически из `main.py`; в Week 1 (TODO) будет загружаться из таблицы `sources`.

### 6.6. `app.workers.collector_worker`

Обрабатывает задачи `FETCH_SOURCE`. Цикл `run()`:

1. `await queue.get()`
2. Если `task_type != FETCH_SOURCE` — кладём обратно (другие воркеры обработают)
3. Иначе — `_handle(task)`

`_handle`:

1. Выбирает коллектор через `_get_collector(source_type)` (factory; пока поддерживается только `html`)
2. Вызывает `collector.fetch(url, trace_id)` → `RawRecordSchema`
3. `_on_fetched(task, raw)` — TODO Week 1: записать в БД + поставить `PARSE_CONTENT`
4. При исключении — `_retry(task, exc)`

`_retry` — экспоненциальный backoff (spec §6.2):

- `attempt += 1`
- Если `attempt >= max_attempts` → DLQ
- Иначе спит `min(2^attempt, 600)` секунд и кладёт задачу обратно в очередь

Реальные значения backoff: 2с → 4с → 8с → 16с → 32с (max 600).

### 6.7. `app.parsers.base`

```python
class BaseCollector(ABC):
    @abstractmethod
    async def fetch(self, url: str, trace_id: UUID) -> RawRecordSchema: ...
```

Контракт коллектора: получить URL и trace_id, вернуть `RawRecordSchema`. Никакой бизнес-логики, никакой интерпретации (spec §3.3).

### 6.8. `app.parsers.html_collector`

`HtmlCollector(BaseCollector)` — реализация для HTML-источников через `httpx.AsyncClient`:

- `follow_redirects=True`, `timeout=30s`, кастомный `User-Agent`
- `response.raise_for_status()` — ошибки HTTP пробрасываются и попадают в retry-loop
- `content_hash` = SHA256 от `response.text`

В будущем для других типов источников появятся `RssCollector`, `JsonCollector`, `TelegramCollector` — все через тот же `BaseCollector`.

### 6.9. `app.normalization.base`

`StubNormalizer.normalize(raw) -> None` — заглушка. Реальный нормализатор появится в Week 2 (LLM с OpenAI-совместимым клиентом, prompt извлекает поля по схеме `NormalizedEventSchema`).

### 6.10. `app.main`

Точка входа. Последовательность:

1. Настройка логирования по `settings.log_level`
2. `await init_db()` — создание таблиц
3. Создание `TaskQueue`
4. Создание `Scheduler`, добавление источников (пока хардкод)
5. Создание `CollectorWorker`
6. `asyncio.gather(scheduler.run(), worker.run())` — параллельный запуск

---

## 7. Поток данных (current)

```
[Scheduler]
   ↓ Task(FETCH_SOURCE, payload={url, source_id, source_type})
[TaskQueue]
   ↓
[CollectorWorker.run()]
   ↓ _handle(task)
[HtmlCollector.fetch()]
   ↓ httpx.get(url) → RawRecordSchema
[CollectorWorker._on_fetched()]    ← TODO: persist + enqueue PARSE_CONTENT
```

При ошибке HTTP/network:

```
[CollectorWorker._handle] → exception
   ↓ _retry(task, exc)
[asyncio.sleep(backoff)]
   ↓ task.attempt < max_attempts
[TaskQueue.put(task)]              ← retry
   |
   └─ task.attempt >= max_attempts
[TaskQueue.move_to_dlq(task)]      ← terminal
```

---

## 8. Запуск

**Требования:** Python 3.11+, Docker Desktop.

```bash
# 1. Конфиг
cp .env.example .env

# 2. PostgreSQL
docker-compose up db -d

# 3. Зависимости
pip install -r requirements.txt

# 4. Pipeline
python -m app.main
```

Полный стек в Docker:

```bash
docker-compose up --build
```

---

## 9. Соответствие спецификации

| Раздел spec                | Реализация                                                       |
| -------------------------- | ---------------------------------------------------------------- |
| §3.1 Scheduler             | `app/workers/scheduler.py`                                       |
| §3.2 Task System           | `app/workers/queue.py` (типы задач, retry, DLQ)                  |
| §3.3 Collectors            | `app/parsers/base.py`, `app/parsers/html_collector.py`           |
| §3.4 Adaptive Parser       | архитектурно (factory `_get_collector`); LLM-fallback — Week 2   |
| §3.5 Raw Storage           | `app/db/models.py::RawRecord` (запись — TODO)                    |
| §3.6 Normalization         | `app/normalization/base.py::StubNormalizer` (заглушка)           |
| §3.7 Dedup Engine          | не реализовано (Week 1/2)                                        |
| §3.8 Office Matcher        | не реализовано (Week 3)                                          |
| §3.9 Notifier              | не реализовано (Week 4)                                          |
| §6.1 Форматы данных        | UTC-таймстемпы, UUIDv4, snake_case (`app/models/schemas.py`)     |
| §6.2 Retry / идемпотентность | `CollectorWorker._retry`, `Task.input_hash`                    |
| §6.3 Логирование / трейсинг | `trace_id` пробрасывается через `Task` и `RawRecordSchema`      |
| §7 Схемы                   | `app/models/schemas.py`                                          |

---

## 10. Известные ограничения и TODO

- `CollectorWorker._on_fetched` не сохраняет `RawRecord` в БД и не порождает `PARSE_CONTENT`
- Источники в `Scheduler` загружаются хардкодом из `main.py`, а не из таблицы `sources`
- Нет Alembic-миграций — схема создаётся через `Base.metadata.create_all`
- `StubNormalizer` ничего не делает (LLM-интеграция в Week 2)
- Нет реализации dedup, matcher, notifier
- Нет тестов (директория `test/` пустая)
- Нет FastAPI-эндпоинтов (директория `app/api/` пустая)
