# Power Outage Agent

Прототип агента, который раз в N часов парсит сайты энергокомпаний, узнаёт о плановых отключениях, сопоставляет с адресами офисов и шлёт уведомление в корпоративные каналы.

## Запуск

### Pipeline (как было)

```bash
docker compose up db -d
python -m app.main
```

### Admin API (FastAPI)

```bash
pip install -e .[web]
uvicorn app.api.app:app --reload --port 8000
# http://localhost:8000/docs — OpenAPI
```

### Web admin panel (Vite + React)

```bash
cd web
npm install
npm run dev
# http://localhost:5173
```

Фронт по умолчанию использует **mock-данные** (`VITE_USE_MOCK=1`). Чтобы переключиться на реальный backend, поднимите Admin API (выше) и создайте `web/.env.local`:

```env
VITE_USE_MOCK=0
```

В dev-режиме Vite проксирует `/api/*` на `http://localhost:8000`. Для продакшен-сборки (`npm run build`) выкладывайте `web/dist/` за тем же origin, что и FastAPI, либо настройте reverse proxy.

### Архитектура frontend

```
web/
├── src/
│   ├── lib/api/         # типы + ApiClient + mock + real (переключатель VITE_USE_MOCK)
│   ├── lib/format.ts    # форматтеры дат/чисел/relative time
│   ├── components/
│   │   ├── layout/      # AppShell, Sidebar, Header
│   │   ├── ui/          # Card, Badge, KpiCard, DataTable, PageHeader, EmptyState
│   │   ├── pipeline/    # PipelineFlow (горизонтальный flow со статусами стейджей)
│   │   ├── activity/    # ActivityFeed
│   │   └── charts/      # QueueBacklogChart, ConfidenceBars (recharts)
│   ├── pages/           # Dashboard + 14 разделов
│   └── styles/index.css # Tailwind layer + кастомные классы (.card, .btn, .data-table)
└── ...
```

Стек: **Vite + React 18 + TypeScript + Tailwind + react-router-dom + @tanstack/react-query + recharts + lucide-react**. Сервер-состояние идёт через React Query, доменные типы — в `lib/api/types.ts` и зеркалят `app/api/schemas.py`.

### Что mock vs реальный backend

| Раздел | Источник |
| --- | --- |
| Dashboard summary / KPI | реальный (`/api/dashboard/summary`) |
| Pipeline status | реальный (`/api/pipeline/status`) |
| Sources / Raw / Parsed / Normalized / Tasks / DLQ | реальный (`/api/...`) |
| Activity feed | реальный (синтезируется из RAW+Parsed+Normalized+failed Tasks) |
| Normalization quality | реальный (`/api/dashboard/normalization-quality`) |
| Queue backlog 24h | **mock** (нет персистентности глубины очереди) |
| Office matcher / Office impacts | **mock** (Week 3) |
| Notifications | **mock** (Week 4) |
| Logs tail | **mock** (логи не агрегируются в БД) |
| Action `Run poll now` / `Retry` | **stub** на бекенде (202 + сообщение; нужен IPC между admin API и pipeline-процессом) |

### Желательные следующие backend-эндпоинты

- IPC от admin API к pipeline для `POST /api/sources/{id}/poll` и `POST /api/tasks/{id}/retry` (отдельная таблица `poll_requests` / `retry_requests`, которую слушает scheduler).
- Реальные office registry endpoints (Week 3).
- Таблица notifications + endpoint.
- Persistent queue-depth time series (минимально — periodic снапшот в БД).
- Лог-стрим — например, через journald/loki, либо отдельная таблица `events` со структурированной записью этапов.


## План на 4 недели

### Неделя 1 - скелет

- [x] Репозиторий, архитектура v0, стэк
- [x] Базовая структура (директории, модули)
- [x] Первичная инфраструктура: планировщик, очередь, воркеры (коллектор и парсер)
- [x] Архитектура адаптивного парсера
- [x] Простой нормализатор (затычка)
- [x] Запись в БД, схемы, базовый dedup

### Неделя 2 - реальный парсинг + LLM

- [x] Расширение парсера до 2-3 источников (Россети Сибирь, Россети Томск, eseti.ru)
- [x] Унификация формата RAW-данных (`RawRecord` + `ParsedRecord`)
- [x] Обработка краевых случаев (пустые поля, битые данные, разные кодировки, SSL)
- [x] Подготовка промптов
- [x] Подключение LLM для нормализации
- [ ] Улучшенный dedup (время + адрес + источники)

### Неделя 3 - матчинг 

- [ ] Модель офисов (адреса, регионы, базовая нормализация), офисы в закрытом контуре - придумать как туда ходить
- [ ] Приведение адресов к единому формату 
- [ ] Базовый матчинг: точное совпадение
- [ ] Генерация событий о затронутых офисахлм

### Неделя 4 - расписание и причёсывание

- [ ] Реализация алертов (дашборд, логи, уведомления в тг, шина событий)
- [ ] Настройка триггеров (когда и при каких условиях отправлять уведомления)
- [ ] Retry и обработка ошибок 
- [ ] Базовая идемпотентность
- [ ] Подготовка демо-сценария

![архитектура](pics/arch1.png)

## Текущий статус

**12.05.2026 — LLM-нормализатор: GigaChat**

После `ParsedRecord` добавлен следующий слой pipeline:

```text
PARSE_CONTENT → ParsedStore.save_many → NORMALIZE_EVENT → LLMNormalizer (GigaChat) → normalized_events
```

Что появилось:

- `app/normalization/gigachat_client.py` — async-клиент Sber GigaChat: OAuth-flow (Basic auth → `/api/v2/oauth` → access_token), кеш токена в памяти на 30 минут, chat completion на `/api/v1/chat/completions`. Типизированные исключения (`GigaChatAuthError`, `GigaChatHTTPError`, `GigaChatInvalidResponseError`). Не логирует `auth_key` / `access_token` / `Authorization`-заголовок.
- `app/normalization/llm.py::LLMNormalizer` — использует `GigaChatClient` как транспорт. Промпт требует строгий JSON, без markdown-обёртки; есть фолбэк `_strip_json_fences` на случай ` ```json ``` ` обёртки. Возвращает `NormalizedEventSchema | None`. Transport-ошибки бросаются наружу (Dispatcher делает retry); LLM-ошибки (битый JSON, нарушение схемы) → `None`.
- Таблица `normalized_events` + `NormalizedEventStore` (как было).
- `NormalizationHandler` зарегистрирован на `NORMALIZE_EVENT` в Dispatcher (как было).
- В `parser_profile`: `normalize_enabled=false` и `normalize_limit=N` для troттлинга на крупных источниках.

Конфиг (GigaChat — единственный активный LLM-провайдер):
- `GIGACHAT_AUTH_KEY` (base64 `client_id:client_secret`, готовый Authorization Key из ЛК)  **или**  `GIGACHAT_CLIENT_ID` + `GIGACHAT_CLIENT_SECRET` (клиент сам соберёт base64).
- `GIGACHAT_SCOPE=GIGACHAT_API_PERS`, `GIGACHAT_MODEL=GigaChat-2`, `GIGACHAT_VERIFY_SSL=false` (Russian Trusted Root CA отсутствуют в `certifi`).

Поля `LLM_BASE_URL` / `LLM_API_KEY` / `LLM_MODEL` оставлены как baseline под DeepSeek для будущего переключения провайдеров — сейчас не используются.

End-to-end проверка вживую: OAuth → токен → chat/completions → полный `NormalizedEventSchema` (event_type, location.normalized, confidence). 88 тестов зелёных, ruff чист.

---

**12.05.2026 — реестр источников закрыт**

Реестр источников финализирован — три параллельные ветки парсинга:

| Источник | Транспорт | Особенности | Записей в окне `today+4` |
| --- | --- | --- | --- |
| Россети Сибирь | JSON-API (`data.php`) | Один запрос на 18к записей, фильтр окна на клиенте | ~8100 |
| Россети Томск | HTML + Bitrix-пагинация (`PAGEN_1`) | Сорт DESC по дате, 2 страниц хватает; SSL отключён (российский корневой УЦ) | ~10 |
| eseti.ru | JSON-API (DotNetNuke `/API/Shutdown`) | Один запрос на 2728 записей; ISO-даты, UTC+7 | ~1950 |

Все три источника подключаются через `parser_profile` (JSON в `sources`): `parser` (имя в реестре), `paginate`, `date_params`, `verify_ssl`, `date_filter_days`, `normalize_enabled`, `normalize_limit`. Добавление нового источника = одна строка в seed + новый парсер.

Следующая итерация после LLM-нормализации: дедуп нормализованных событий по композитному ключу адрес+время и Office Matcher. Часть эвристик парсеров (например, сплит локалити Tomsk-парсера) постепенно должна переехать в нормализацию.

Тестов 88 (включая нормализатор и GigaChat-клиент), все зелёные. Linter чист.

---

**12.05.2026 — конец недели 2**

Pipeline вытаскивает реальные данные из двух источников: Россети Сибирь (JSON-API на 18к записей за запрос) и Россети Томск (HTML с Bitrix-пагинацией). Между collector и parser добавлен ParseHandler с реестром парсеров, в `parser_profile` источника описывается всё специфичное — какой парсер, пагинация (`PAGEN_1` со стопом по `max_pages`), подстановка дат в URL (`date_start`/`date_end`), отключение SSL-валидации (для сайтов на российских корневых сертификатах). Новая таблица `parsed_records` хранит структурированные записи перед нормализацией; даты приводятся к UTC, локалити сплитится на регион/район/город.

Исторически на этом этапе было 49 зелёных тестов и чистый linter.

Следующий шаг из этого статуса уже выполнен: LLM-нормализация подключена через GigaChat (Sber, OAuth + chat/completions). Остаются Office Matcher, улучшенный dedup и калибровка промпта для более качественного `location.normalized`.

---

**08.05.2026 — конец недели 1**

Скелет системы готов и локально запущен. Pipeline поднимается, БД инициализируется, scheduler ставит задачи в очередь, воркер забирает их и делает HTTP-запросы с exponential backoff retry . Все core-модули написаны: Pydantic-схемы по spec, async SQLAlchemy модели (Source / RawRecord / TaskRecord), конфиг через .env.

Остаток недели 1: дописать сохранение raw-контента в БД и загрузку источников из таблицы sources вместо хардкода.

На следующей неделе: подключение реальных источников (сайты энергокомпаний), LLM-нормализация, улучшенный dedup.
