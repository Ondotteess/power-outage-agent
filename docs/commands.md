# Commands

## Первый запуск

```bash
# 1. Скопировать конфиг и заполнить ключи
cp .env.example .env

# 1a. Секреты лучше держать в локальном override-файле
# Файл .env.local читается приложением и docker-compose, но игнорируется git.
echo "GIGACHAT_AUTH_KEY=..." > .env.local

# 2. Установить зависимости (включая dev-инструменты)
pip install -e ".[dev]"
```

## Запуск

### Demo E2E одной командой

```bash
docker compose --profile demo up --build db api web demo-runner
```

Что делает команда:

- поднимает Postgres, FastAPI (`http://localhost:8000`) и web (`http://localhost:5173`);
- запускает одноразовый demo-runner;
- берёт по 5 локальных demo-записей на каждый активный источник;
- прогоняет стадии `fetch_source → parse_content → normalize_event → deduplicate_event → match_offices → emit_event`;
- пишет office impacts и dashboard notifications в БД, чтобы процесс был виден в UI.

Demo-режим не ходит во внешние сайты и не требует GigaChat credentials. Обычный smoke
с реальным LLM остаётся отдельной командой ниже.

Если терминал часто закрывается случайно, удобнее поднять долгоживущие сервисы detached,
а demo-runner запустить отдельной одноразовой командой:

```bash
docker compose --profile demo up --build -d db api web
docker compose --profile demo run --rm demo-runner
```

После этого:

- web UI: `http://localhost:5173`
- карта офисов: `http://localhost:5173/map`
- FastAPI docs: `http://localhost:8000/docs`
- map API: `http://localhost:8000/api/map/offices`

```bash
# Поднять только Postgres
docker compose up db -d

# Запустить pipeline (уровень логов из .env или INFO по умолчанию)
python -m app.main

# С детальным DEBUG-логированием
python -m app.main --log-level DEBUG

# Только проблемы
python -m app.main --log-level WARNING

# Справка по аргументам
python -m app.main --help

# Полный стек в Docker (Postgres + app)
docker compose up --build

# Полный стек только для app-логов, если db уже поднята
docker compose up --build app

# Остановить Docker-сервисы
docker compose down

# Остановить и удалить тома (сбросить БД)
docker compose down -v
```

Доступные уровни: `DEBUG | INFO | WARNING | ERROR`.

### Что означают горячие клавиши Docker Compose

Когда `docker compose up` attached к контейнеру, внизу терминала может появиться панель:

```text
v View in Docker Desktop   o View Config   w Enable Watch   d Detach
```

- `d` — отсоединиться от логов, контейнер продолжит работать.
- `Ctrl+C` — остановить attached app.
- `w` — watch mode, для текущего проекта не нужен.
- `v` / `o` — открыть Docker Desktop или посмотреть compose config.

## Тесты

```bash
# Все тесты
pytest

# Тихий вывод (только итог)
pytest -q

# С подробным выводом
pytest -v

# Конкретный файл
pytest tests/test_dispatcher.py

# Конкретный тест
pytest tests/test_dispatcher.py::test_handler_failure_goes_to_dlq_after_max_attempts

# С покрытием (нужен pytest-cov: pip install pytest-cov)
pytest --cov=app --cov-report=term-missing
```

## Линтер и форматтер

```bash
# Проверить (без изменений)
ruff check .

# Применить автофиксы
ruff check --fix .

# Проверить форматирование (без изменений)
ruff format --check .

# Применить форматирование
ruff format .

# Всё сразу: линт + формат
ruff check --fix . && ruff format .
```

## База данных

```bash
# Подключиться к Postgres в контейнере
docker compose exec db psql -U postgres -d outage_agent

# Подключиться через psql (если установлен локально)
psql postgresql://postgres:postgres@localhost:5432/outage_agent

# Полезные psql-команды внутри сессии
\dt                   -- список таблиц
\d tasks              -- схема таблицы tasks
SELECT * FROM sources;
SELECT * FROM tasks WHERE status = 'failed';   -- DLQ
SELECT * FROM raw_records ORDER BY fetched_at DESC LIMIT 10;

-- Распарсенные записи по источникам
SELECT s.name, COUNT(p.id)
FROM parsed_records p JOIN sources s ON p.source_id = s.id
GROUP BY s.name;

-- Примеры извлечённых данных
SELECT location_region_code, location_district, location_city,
       location_street, start_time, reason
FROM parsed_records
ORDER BY start_time
LIMIT 20;

-- Нормализованные события
SELECT event_type, location_raw, location_normalized, confidence, start_time, reason
FROM normalized_events
ORDER BY normalized_at DESC
LIMIT 20;

-- Быстрый smoke-статус
SELECT COUNT(*) FROM raw_records;
SELECT COUNT(*) FROM parsed_records;
SELECT COUNT(*) FROM normalized_events;
SELECT COUNT(*) FROM offices;
SELECT COUNT(*) FROM office_impacts;
SELECT task_type, status, COUNT(*)
FROM tasks
GROUP BY task_type, status
ORDER BY task_type, status;

-- Office matcher output
SELECT o.name, o.city, o.address, i.impact_level, i.match_strategy, i.match_score,
       i.impact_start, i.impact_end
FROM office_impacts i
JOIN offices o ON o.id = i.office_id
ORDER BY i.detected_at DESC
LIMIT 20;

-- Офисы и ручные координаты для карты
SELECT name, city, address, latitude, longitude
FROM offices
ORDER BY name;

-- Активные impacts, которые влияют на /map прямо сейчас
SELECT o.name, i.impact_level, i.impact_start, i.impact_end, e.event_type, e.reason
FROM office_impacts i
JOIN offices o ON o.id = i.office_id
LEFT JOIN normalized_events e ON e.event_id = i.event_id
WHERE i.impact_start <= now()
  AND (i.impact_end IS NULL OR i.impact_end >= now())
ORDER BY i.impact_start DESC;

-- Очистить хвост LLM-задач после неудачного тестового запуска
DELETE FROM tasks WHERE task_type = 'normalize_event';

-- Оставить активным только маленький источник Томск
UPDATE sources SET is_active = false;
UPDATE sources SET is_active = true WHERE name ILIKE '%Томск%';

-- Обновить защитные настройки нормализации в уже созданной БД
UPDATE sources
SET parser_profile = '{"parser":"rosseti_sib","date_filter_days":4,"normalize_enabled":false}'::json
WHERE name ILIKE '%Сибир%';

UPDATE sources
SET parser_profile = '{"parser":"rosseti_tomsk","date_filter_days":4,"normalize_limit":3,"verify_ssl":false,"paginate":{"param":"PAGEN_1","max_pages":2}}'::json
WHERE name ILIKE '%Томск%';

UPDATE sources
SET parser_profile = '{"parser":"eseti","date_filter_days":4,"normalize_enabled":false}'::json
WHERE name ILIKE '%eseti%';
```

## LLM-нормализация (GigaChat)

Текущий провайдер — Sber GigaChat. Credentials кладутся в `.env` или `.env.local` (последний предпочтительнее — он только для секретов и в `.gitignore`).

Два равноправных способа авторизации:

```env
# Способ 1: готовый Authorization Key из ЛК GigaChat (одной строкой)
GIGACHAT_AUTH_KEY=<base64-строка>

# Способ 2: Client ID и Client Secret отдельно — клиент сам соберёт base64
GIGACHAT_CLIENT_ID=<id>
GIGACHAT_CLIENT_SECRET=<secret>
```

Остальные поля имеют разумные defaults (см. `.env.example`):

```env
GIGACHAT_SCOPE=GIGACHAT_API_PERS
GIGACHAT_MODEL=GigaChat-2
GIGACHAT_VERIFY_SSL=false
```

Доступные модели для scope `GIGACHAT_API_PERS`: `GigaChat`, `GigaChat-2`, `GigaChat-2-Pro`, `GigaChat-2-Max`, `GigaChat-Pro`, `GigaChat-Max`, `GigaChat-Plus`. Запросить список с живого API:

```bash
# Требует валидный access_token (получи через первый прогон pipeline и логи)
curl -k https://gigachat.devices.sberbank.ru/api/v1/models -H "Authorization: Bearer <token>"
```

Проверка нормализатора end-to-end (без БД):

```bash
python - <<'PY'
import asyncio
from datetime import UTC, datetime
from uuid import uuid4
from app.models.schemas import ParsedRecordSchema
from app.normalization.llm import LLMNormalizer

async def main():
    record = ParsedRecordSchema(
        id=uuid4(), raw_record_id=uuid4(), source_id=uuid4(),
        start_time=datetime(2026, 5, 12, 3, tzinfo=UTC),
        end_time=datetime(2026, 5, 12, 9, tzinfo=UTC),
        location_city="г Новокузнецк", location_street="ул Ленина",
        location_region_code="42", reason="Плановые работы",
        extra={"houses": "12"}, trace_id=uuid4(), extracted_at=datetime.now(UTC),
    )
    event = await LLMNormalizer().normalize(record)
    print(event)

asyncio.run(main())
PY
```

Cold-start: первый OAuth-запрос после старта процесса может изредка падать с `httpx.ConnectError` — повторный пройдёт. На прод-нагрузку — рассмотри retry на `ConnectError` в `gigachat_client.py`.

## Smoke E2E: по 5 LLM-записей из каждого источника

Сценарий для проверки всего текущего pipeline без ожидания расписания:

- берёт все источники из таблицы `sources`, включая неактивные;
- делает один `FETCH_SOURCE` на каждый источник;
- если raw уже был сохранён раньше, всё равно ставит его на повторный parse;
- включает LLM-нормализацию независимо от глобального флага;
- отправляет в LLM не больше 5 parsed-записей на источник;
- ждёт drain очереди и завершает процесс.

```bash
docker compose run --rm --volume "${PWD}:/src" --workdir /src app \
  python -m app.main --log-level INFO --smoke-e2e --smoke-normalize-limit 5
```

Перед запуском нужны GigaChat credentials в `.env.local`:

```env
LLM_NORMALIZATION_ENABLED=true
GIGACHAT_AUTH_KEY=<authorization-key>
GIGACHAT_VERIFY_SSL=false
```

## Alembic (миграции)

> Пока не настроен — схема создаётся через `Base.metadata.create_all` при старте.
> `alembic` не входит в текущие runtime-зависимости MVP. Команды ниже — ориентир для будущего шага после добавления зависимости и папки миграций.

```bash
# Инициализация (один раз)
alembic init alembic

# Создать миграцию по изменениям моделей
alembic revision --autogenerate -m "описание"

# Применить все миграции
alembic upgrade head

# Откатить одну миграцию
alembic downgrade -1

# Посмотреть историю
alembic history
```
