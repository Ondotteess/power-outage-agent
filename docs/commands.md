# Commands

## Первый запуск

```bash
# 1. Скопировать конфиг и заполнить ключи
cp .env.example .env

# 2. Установить зависимости (включая dev-инструменты)
pip install -e ".[dev]"
```

## Запуск

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

# Остановить Docker-сервисы
docker compose down

# Остановить и удалить тома (сбросить БД)
docker compose down -v
```

Доступные уровни: `DEBUG | INFO | WARNING | ERROR`.

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
```

## Alembic (миграции)

> Пока не настроен — схема создаётся через `Base.metadata.create_all` при старте.
> Команды для будущего использования:

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
