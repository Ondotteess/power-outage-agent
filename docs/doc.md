# Power Outage Agent — Code Documentation

Документация по текущему коду. Описывает структуру проекта, зависимости, контракты модулей и фактический поток данных.

---

## 1. Обзор

Система реализована как асинхронный event-driven pipeline на `asyncio`. Точка входа — `app/main.py`. Runtime-компоненты связаны через durable DB-backed очередь (`tasks`) и центральный `Dispatcher`, который роутит задачи по `TaskType`; in-memory `TaskQueue` оставлен для unit-тестов.

Текущий pipeline:

```text
Scheduler
  └─▶ Task(FETCH_SOURCE)
        └─▶ CollectorHandler
              ├─▶ HtmlCollector / JsonCollector
              ├─▶ RawStore.save                 → raw_records
              └─▶ Task(PARSE_CONTENT)
                    └─▶ ParseHandler
                          ├─▶ RossetiSibParser / RossetiTomskParser / EsetiParser
                          ├─▶ ParsedStore.save_many       → parsed_records
                          └─▶ Task(NORMALIZE_EVENT)       → controlled by parser_profile
                                └─▶ NormalizationHandler
                                      ├─▶ LLMNormalizer
                                      ├─▶ NormalizedEventStore.save → normalized_events
                                      └─▶ Task(DEDUPLICATE_EVENT)
                                            └─▶ DeduplicationHandler
                                                  └─▶ Task(MATCH_OFFICES)
                                                        └─▶ OfficeMatchHandler
                                                              ├─▶ OfficeImpactStore.save_many → office_impacts
                                                              └─▶ Task(EMIT_EVENT)
                                                                    └─▶ NotificationHandler
                                                                          └─▶ NotificationStore.save → notifications
```

DLQ = строки в `tasks` со `status='failed'`. Внешних брокеров нет: Redis/Celery/Kafka не используются.
Admin API не трогает worker-процесс напрямую: `POST /api/sources/{id}/poll`
и `POST /api/tasks/{id}/retry` пишут заявки в `poll_requests` / `retry_requests`,
а долгоживущий pipeline-процесс забирает их через `RequestWatcher`.

---

## 2. Что реализовано

- Scheduler, TaskQueue, Dispatcher с retry + DLQ.
- CollectorHandler с пагинацией, date params и `verify_ssl` через `parser_profile`.
- HtmlCollector и JsonCollector на `httpx.AsyncClient`.
- ParseHandler с реестром парсеров `rosseti_sib`, `rosseti_tomsk`, `eseti`.
- Три источника энергокомпаний:
  - Россети Сибирь: JSON API, около 18k записей за запрос.
  - Россети Томск: HTML + Bitrix-пагинация `PAGEN_1`.
  - eseti.ru: DotNetNuke JSON API.
- `ParsedRecord` как промежуточная структурированная запись до LLM.
- LLM-нормализация через Sber GigaChat (`GigaChat-2` по умолчанию).
- Транспорт GigaChat: OAuth (Basic auth → access_token, кеш 30 мин), chat completions на `httpx`.
- Таблица `normalized_events`.
- Защита от массовых LLM-вызовов через `normalize_enabled` и `normalize_limit`.
- Dedup нормализованных событий по parsed/composite keys.
- Таблица `dedup_events` для KPI по merge/skip дубликатов.
- DB-backed IPC для ручных `Run poll now` и `Retry`: `poll_requests` / `retry_requests`
  + `RequestWatcher`.
- Office registry (`offices`) с ручными координатами `latitude` / `longitude`.
- Office Matcher, который создаёт `office_impacts`.
- NotificationHandler и таблица `notifications`.
- FastAPI Admin API (`/api/*`) и Vite/React admin UI.
- Карта офисов `/map` на Leaflet через `GET /api/map/offices`.

Не реализовано: persistent queue-depth time series, лог-стрим из БД/observability-системы.

---

## 3. Структура проекта

```text
power-outage-agent/
├── app/
│   ├── config.py                 — настройки через pydantic-settings
│   ├── main.py                   — точка входа
│   ├── db/
│   │   ├── engine.py             — async engine, Base, init_db()
│   │   ├── models.py             — Source, RawRecord, ParsedRecord, NormalizedEvent, Office, OfficeImpact, Notification, TaskRecord
│   │   └── repositories.py       — stores для задач, raw, parsed, normalized, offices, impacts, notifications, sources
│   ├── models/
│   │   └── schemas.py            — Pydantic-схемы
│   ├── workers/
│   │   ├── queue.py              — Task, TaskType, TaskQueue
│   │   ├── scheduler.py          — Scheduler
│   │   ├── dispatcher.py         — retry, DLQ, routing
│   │   ├── requests.py           — RequestWatcher для poll/retry заявок из Admin API
│   │   ├── collector.py          — FETCH_SOURCE
│   │   ├── parser.py             — PARSE_CONTENT
│   │   ├── normalizer.py         — NORMALIZE_EVENT
│   │   ├── deduplicator.py       — DEDUPLICATE_EVENT
│   │   ├── matcher.py            — MATCH_OFFICES
│   │   └── notifier.py           — EMIT_EVENT
│   ├── matching/
│   │   ├── defaults.py           — seed офисов с ручными координатами
│   │   └── office_matcher.py     — эвристики address/street/house matching
│   ├── parsers/
│   │   ├── base.py               — BaseCollector contract
│   │   ├── html_collector.py
│   │   ├── json_collector.py
│   │   ├── rosseti_sib.py
│   │   ├── rosseti_tomsk.py
│   │   └── eseti.py
│   ├── normalization/
│   │   ├── llm.py                — LLMNormalizer
│   │   ├── gigachat_client.py    — транспорт GigaChat
│   │   └── demo.py               — deterministic demo normalizer
│   ├── alerts/
│   │   └── telegram.py
│   ├── tools/
│   │   └── smoke_check.py       — проверка demo/smoke результата через БД и API
│   └── api/
│       ├── app.py                — FastAPI app
│       ├── schemas.py            — UI/API response schemas
│       ├── queries.py            — read-only admin queries
│       └── routers/              — dashboard, records, offices, map, notifications, ...
├── web/                          — Vite + React admin UI
│   └── src/
│       ├── pages/                — Dashboard, OfficeMatcher, OfficeMap, ...
│       ├── components/           — layout/ui/charts/activity
│       └── lib/api/              — ApiClient + real/mock providers
├── tests/
│   ├── test_collector.py
│   ├── test_dispatcher.py
│   ├── test_normalizer.py
│   ├── test_parser.py
│   ├── test_parser_eseti.py
│   ├── test_parser_tomsk.py
│   ├── test_queue.py
│   ├── test_map_api.py
│   └── test_scheduler.py
├── docs/
├── pyproject.toml
├── docker-compose.yml
├── Dockerfile
├── .env.example
└── .gitignore
```

---

## 4. Конфигурация

`Settings` читает переменные окружения и файлы `.env`, `.env.local`. `.env.local` предназначен для секретов и игнорируется git.

| Поле | Дефолт | Описание |
| --- | --- | --- |
| `database_url` | `postgresql+asyncpg://postgres:postgres@localhost:5432/outage_agent` | DSN для локального запуска |
| `llm_base_url` | `https://api.deepseek.com` | OpenAI-compatible baseline. **Не используется** активным GigaChat-нормализатором, зарезервировано под будущее переключение |
| `llm_api_key` | `""` | API key провайдера baseline |
| `llm_model` | `deepseek-chat` | Модель baseline |
| `gigachat_auth_key` | `""` | Authorization Key из ЛК GigaChat (base64 `client_id:client_secret`). Альтернатива — пара ниже |
| `gigachat_client_id` | `""` | Client ID. С `client_secret` будет автоматически собран в base64 |
| `gigachat_client_secret` | `""` | Client Secret |
| `gigachat_scope` | `GIGACHAT_API_PERS` | OAuth scope (`_PERS` / `_B2B` / `_CORP`) |
| `gigachat_base_url` | `https://gigachat.devices.sberbank.ru/api/v1` | Эндпоинт `/chat/completions` |
| `gigachat_oauth_url` | `https://ngw.devices.sberbank.ru:9443/api/v2/oauth` | Эндпоинт OAuth |
| `gigachat_model` | `GigaChat-2` | Доступны: `GigaChat`, `GigaChat-2`, `GigaChat-2-Pro`, `GigaChat-2-Max`, `GigaChat-Pro`, `GigaChat-Max`, `GigaChat-Plus` (для PERS) |
| `gigachat_verify_ssl` | `false` | Russian Trusted Root CA отсутствуют в `certifi` |
| `telegram_bot_token` | `""` | Placeholder для будущих уведомлений |
| `log_level` | `INFO` | Уровень логирования |

`.env.example` содержит шаблоны для всех `GIGACHAT_*` переменных. Секреты кладутся в локальные `.env` / `.env.local`; оба файла игнорируются git.

Docker Compose читает `.env` и опциональный `.env.local`. Для сервиса `app` DSN переопределяется на `postgresql+asyncpg://postgres:postgres@db:5432/outage_agent`, потому что внутри контейнера PostgreSQL доступен по имени сервиса `db`.

---

## 5. База данных

Для схемы добавлен Alembic baseline (`alembic/versions/20260514_0001_initial.py`). Runtime пока сохраняет `Base.metadata.create_all` при старте для удобства локального demo, но дальнейшие изменения схемы надо оформлять миграциями.

| Таблица | Назначение |
| --- | --- |
| `sources` | Реестр источников: URL, тип, интервал опроса, `parser_profile` |
| `raw_records` | Сырые ответы источников, dedup по `content_hash` |
| `parsed_records` | Структурированные записи до нормализации |
| `normalized_events` | Нормализованные LLM-события |
| `dedup_events` | Факты merge дубликатов для audit/KPI |
| `offices` | Реестр офисов: название, город, адрес, регион, ручные координаты |
| `office_impacts` | Результаты сопоставления офисов и нормализованных событий |
| `notifications` | История dashboard/Telegram/email/webhook уведомлений |
| `tasks` | Lifecycle задач и DLQ |
| `poll_requests` | Заявки Admin API на немедленный poll источника |
| `retry_requests` | Заявки Admin API на ручной retry failed-задачи |

`normalized_events` хранит:

- `event_id`
- `parsed_record_id`
- `event_type`
- `start_time`, `end_time`
- `location_raw`, `location_normalized`, `location_city`, `location_street`, `location_building`
- `reason`
- `sources`
- `confidence`
- `trace_id`, `normalized_at`

`offices` хранит nullable `latitude` / `longitude`. Автоматический геокодинг не
используется: координаты добавляются вручную в seed/БД. Офис без координат
остаётся в API и UI, но не получает маркер на карте.

---

## 6. `parser_profile`

`parser_profile` — JSON в таблице `sources`. Его читают CollectorHandler, ParseHandler и indirectly Normalization flow.

| Ключ | Тип | Назначение |
| --- | --- | --- |
| `parser` | str | Имя парсера: `rosseti_sib`, `rosseti_tomsk`, `eseti` |
| `date_filter_days` | int | Окно `today..today+N`, по умолчанию 4 |
| `verify_ssl` | bool | Проверка SSL, для Томска сейчас `false` |
| `paginate.param` | str | URL-параметр пагинации, например `PAGEN_1` |
| `paginate.max_pages` | int | Сколько страниц фетчить |
| `date_params` | object | Подстановка дат в URL |
| `normalize_enabled` | bool | Если `false`, `ParsedRecord` сохраняются, но LLM-задачи не ставятся |
| `normalize_limit` | int | Максимум LLM-задач на один raw-ответ |

Дефолтная стратегия:

- Россети Сибирь: `normalize_enabled=false`, потому что источник даёт тысячи записей.
- Россети Томск: `normalize_limit=3`, подходит для smoke-теста.
- eseti.ru: `normalize_enabled=false`, потому что источник даёт около 2k записей в окне.

---

## 7. Worker-контракты

### 7.1 Dispatcher

Lifecycle:

1. `submit` пишет `pending`.
2. `_process` пишет `running`.
3. Успех handler-а пишет `done`.
4. Исключение запускает retry с backoff `min(2**attempt, 600)`.
5. После `max_attempts` задача получает `failed`.

Если handler для типа задачи не зарегистрирован, задача дропается с warning. Сейчас зарегистрированы `fetch_source`, `parse_content`, `normalize_event`, `deduplicate_event`, `match_offices`, `emit_event`.

### 7.2 RequestWatcher

Admin API и pipeline живут в разных процессах. `RequestWatcher` в pipeline-процессе
периодически забирает `pending`-заявки:

- `poll_requests` → создаёт `FETCH_SOURCE` для активного источника;
- `retry_requests` → повторно ставит failed-задачу в очередь с `attempt=0`.

Заявки получают `done` после успешного enqueue или `failed`, если источник/задача
не найдены либо статус не подходит.

### 7.3 CollectorHandler

Берёт `source_id`, `url`, `source_type` из task payload. Загружает профиль источника, строит URL-ы с пагинацией и date params, делает HTTP fetch, сохраняет `RawRecord`, затем ставит `PARSE_CONTENT`.

Повторный raw с тем же `content_hash` пропускается и не парсится повторно.

### 7.4 ParseHandler

Загружает raw, выбирает парсер по `parser_profile["parser"]`, получает `list[ParsedRecordSchema]`, сохраняет пачкой в `parsed_records`.

После сохранения:

- если `normalize_enabled=false`, нормализация не ставится;
- если `normalize_limit=N`, в очередь ставятся только первые `N` записей;
- иначе ставится `NORMALIZE_EVENT` для каждой parsed-записи.

### 7.5 NormalizationHandler

Берёт `parsed_record_id`, загружает `ParsedRecord`, преобразует ORM-запись в `ParsedRecordSchema`, вызывает `LLMNormalizer.normalize`, сохраняет результат в `normalized_events`.

Если нормализатор вернул `None` (битый JSON, нарушение схемы, отсутствующий `start_time`), задача считается обработанной без записи события — повторные попытки не помогут, модель отдаст то же. Transport-ошибки (HTTP 5xx, OAuth fail, network) — бросаются наружу, Dispatcher делает экспоненциальный retry (`backoff = 2^attempt`, max 5).

### 7.6 DeduplicationHandler

Берёт `event_id`, загружает `NormalizedEvent`, применяет минимальную dedup-логику
через уникальные ключи БД/store и ставит `MATCH_OFFICES`. Это не отдельная
гео/ML-система, а короткий pipeline step между нормализацией и matcher-ом.

### 7.7 OfficeMatchHandler

Берёт нормализованное событие и активные офисы. Использует `OfficeMatcher`:

- exact address;
- house range;
- street area, когда источник перечисляет улицу без домов.

Сохраняет `OfficeImpact` и ставит `EMIT_EVENT` для затронутых офисов. В demo-режиме
может также emit-ить unmatched события, чтобы UI показывал полный pipeline.

### 7.8 NotificationHandler

Формирует `NotificationSchema` из office impact payload, сохраняет запись в
`notifications` и при настроенном канале может отправить Telegram. Dashboard
видит эти записи через `/api/notifications`.

---

## 8. LLMNormalizer

Файл: `app/normalization/llm.py`.

Использует `GigaChatClient` из `app/normalization/gigachat_client.py` (см. §8a). Контракт `NormalizerProtocol` — `async def normalize(record: ParsedRecordSchema) -> NormalizedEventSchema | None`. Клиент строится лениво из `settings.gigachat_*` при первом вызове, либо может быть инжектирован в конструктор (для тестов).

Вход: `ParsedRecordSchema`.

Выход: `NormalizedEventSchema`:

- классификация `event_type`;
- UTC-время;
- `location.raw`, `location.normalized`, `city`, `street`, `building`;
- `reason`;
- `confidence` (clamped `[0, 1]`).

Промпт (см. `_SYSTEM_PROMPT`) запрещает выдумывать отсутствующие номера домов, требует строгий JSON без markdown-обёртки и фиксирует ожидаемую структуру ответа. На случай, если модель всё-таки добавит ` ```json ``` ` — есть фолбэк `_strip_json_fences`.

Все helper-функции (`_record_payload`, `_build_event`, `_parse_dt`, `_event_type`, `_confidence`, `_clean`, `_raw_location`) — pure functions, протестированы независимо в `tests/test_normalizer.py`.

### 8a. GigaChatClient

Файл: `app/normalization/gigachat_client.py`. Только транспорт, без бизнес-логики.

- **Credentials**: принимает либо готовый `auth_key` (base64 `client_id:client_secret`), либо пару `client_id` + `client_secret` (тогда base64 собирается в `_encode_basic`). Если переданы оба — `auth_key` приоритетнее. Пустые credentials → `GigaChatAuthError` в конструкторе.
- **OAuth**: `_get_token()` шлёт `POST {oauth_url}` с `Authorization: Basic <auth_key>`, `RqUID: <uuid>`, `scope=<scope>` в form-data. Ответ `{access_token, expires_at}` (expires_at — миллисекунды). Кеш в памяти, ре-выпуск за 60 сек до истечения.
- **Чат**: `chat_completion(messages, temperature, max_tokens)` шлёт `POST {base_url}/chat/completions` с `Authorization: Bearer <token>`. Возвращает сырой OpenAI-совместимый JSON-dict.
- **Helper**: статический `extract_message_content(response)` → `choices[0].message.content`.
- **Исключения**: `GigaChatError` → `GigaChatAuthError` / `GigaChatHTTPError` / `GigaChatInvalidResponseError`.
- **SSL**: `verify=False` по умолчанию — Russian Trusted Root CA отсутствуют в стандартном `certifi`-bundle.
- **Безопасность логов**: НЕ логирует `auth_key`, `access_token`, заголовок `Authorization`. RqUID и TTL — на INFO; raw response — только на DEBUG.

---

## 9. Источники

### Россети Сибирь

JSON API: `data.php`. В ответе до 18k записей. Парсер фильтрует окно `today..today+N`, маппит дату/время в UTC и сохраняет регион, район, город, улицу, причину.

### Россети Томск

HTML-страница с таблицей `table.shuthown_table`. Включена пагинация `PAGEN_1`, `max_pages=2`, SSL verification отключён из-за российского корневого сертификата. Это основной маленький источник для smoke-теста LLM.

### eseti.ru

DotNetNuke JSON API `/API/Shutdown`. Возвращает около 2700 записей, даты считаются локальным UTC+7. `commaSeparatedHouses` сохраняется в `extra.houses`.

---

## 10. Admin API и Web UI

FastAPI приложение живёт в `app/api/app.py`. При старте оно вызывает `init_db()` и
seed-ит offices из `app/matching/defaults.py`, если таблица пустая.

Основные admin endpoints:

- `/api/dashboard/*` — KPI, activity, normalization quality, backlog;
- `/api/sources`, `/api/raw`, `/api/parsed`, `/api/normalized`, `/api/tasks`;
- `/api/offices`, `/api/office-impacts`;
- `/api/notifications`;
- `/api/map/offices` — UI-проекция для карты офисов.

`GET /api/map/offices` возвращает:

```json
{
  "offices": [
    {
      "id": "uuid",
      "name": "Office",
      "address": "street, house",
      "latitude": 56.4846,
      "longitude": 84.9476,
      "status": "ok | risk | critical",
      "active_impacts": [
        {
          "id": "uuid",
          "reason": "string",
          "severity": "low | medium | high | critical | unknown",
          "starts_at": "iso8601",
          "ends_at": "iso8601 | null"
        }
      ]
    }
  ]
}
```

Активный impact: `impact_start <= now` и `impact_end IS NULL OR impact_end >= now`.
Статус:

- `ok` — активных impacts нет;
- `risk` — low/medium/unknown active impact;
- `critical` — high/critical active impact или событие явно похоже на outage/closure.

Frontend: `web/`, стек Vite + React + TypeScript + Tailwind + React Query.
Страница `/map` использует Leaflet напрямую и lazy-load-ится только на своём route.
Тайлы — OpenStreetMap/CARTO dark layer, без API-ключей и без геокодинга.

---

## 11. Запуск и smoke-проверка

Подготовка:

```bash
cp .env.example .env
echo "GIGACHAT_AUTH_KEY=..." > .env.local
docker compose up db -d
```

Запуск:

```bash
docker compose up --build app
```

Быстрый статус:

```bash
docker compose exec db psql -U postgres -d outage_agent -c "SELECT COUNT(*) FROM raw_records; SELECT COUNT(*) FROM parsed_records; SELECT COUNT(*) FROM normalized_events; SELECT task_type, status, COUNT(*) FROM tasks GROUP BY task_type, status ORDER BY task_type, status;"
```

Ожидаемый smoke после тестового прогона:

- raw скачиваются;
- parsed записей становится тысячи;
- `normalize_event` handler зарегистрирован;
- при доступном LLM provider появляются строки в `normalized_events`.

GigaChat API доступен из РФ напрямую. SSL отключён (`verify_ssl=false`) — Russian Trusted Root CA не входят в стандартный `certifi`-bundle. Первый запрос после старта процесса может изредка падать с `httpx.ConnectError` (cold-start TCP), повторный пройдёт.

Demo E2E без внешних сайтов и LLM credentials:

```bash
docker compose --profile demo up --build db api web demo-runner
```

Более устойчивый вариант, если терминал можно закрыть:

```bash
docker compose --profile demo up --build -d db api web
docker compose --profile demo run --rm demo-runner
docker compose exec -T api python -m app.tools.smoke_check --expected-offices 50 --min-active-risk-offices 10 --min-impacts 10 --min-notifications 10
```

После demo:

- dashboard: `http://localhost:5173`
- карта офисов: `http://localhost:5173/map`
- API docs: `http://localhost:8000/docs`

---

## 12. Тесты

Тестовый набор покрывает:

- стабильность `Task.input_hash`;
- очередь;
- scheduler;
- dispatcher retry/DLQ;
- collector pagination/date params/SSL;
- три парсера;
- ParseHandler;
- NormalizationHandler;
- сборку `NormalizedEventSchema`;
- `normalize_enabled` и `normalize_limit`;
- GigaChatClient (credentials, base64-encoding, response shape, JSON fence stripping);
- LLMNormalizer happy path / invalid JSON / transport error / no start_time / lazy init без credentials.
- Office Matcher и matcher worker.
- NotificationHandler / Telegram formatting.
- `/api/map/offices`: ok/risk/critical, завершённые impacts, офисы без координат.
- RequestWatcher для DB-backed `poll` / `retry` заявок.

Текущий счётчик: 126 pytest-кейсов, все зелёные.

Команды:

```bash
pytest
ruff check .
```

---

## 13. Известные ограничения

- Alembic baseline есть; нужен процесс обязательного `alembic upgrade head` перед production-start.
- Pending/running задачи не re-enqueue-ятся из БД после перезапуска.
- Для крупных источников LLM надо запускать батчами или с лимитами.
- Queue backlog в dashboard синтетический: persistent time series пока нет.
- Logs tail в web UI использует mock-данные: лог-агрегации в БД пока нет.
- Карта офисов не делает автоматический геокодинг; координаты нужно задавать вручную.
- GigaChat-промпт не идеально нормализует адрес: модель оставляет сокращения (`ул.` вместо `улица`) и иногда даёт `confidence=1.0` на типовых случаях — нужна калибровка. На будущее: few-shot примеры или жёстче формулировка.
- GigaChat `verify_ssl=False` — компромисс по российским корневым CA. Корректный фикс — установить Russian Trusted Root CA в `certifi`-bundle или системный cert store.
- `RossetiTomskParser` всё ещё содержит простую эвристику split locality по запятым.
- `.env.example` не должен содержать реальные ключи или chat id; секреты держатся только в локальных `.env` / `.env.local`.
- Поля `LLM_BASE_URL`/`LLM_API_KEY`/`LLM_MODEL` зарезервированы под будущий мульти-провайдер (DeepSeek/OpenAI-compatible), сейчас не используются `LLMNormalizer` (он хардкодит GigaChat).
