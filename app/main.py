from __future__ import annotations

import argparse
import asyncio
import logging

from app.config import settings
from app.db.engine import async_session_factory, init_db
from app.db.repositories import ParsedStore, RawStore, SourceStore, TaskStore
from app.workers.collector import CollectorHandler
from app.workers.dispatcher import Dispatcher
from app.workers.parser import ParseHandler
from app.workers.queue import TaskQueue, TaskType
from app.workers.scheduler import Scheduler, SourceConfig

logger = logging.getLogger(__name__)

_DEFAULT_SOURCES = [
    {
        "name": "Россети Сибирь — плановые отключения",
        "url": (
            "https://www.rosseti-sib.ru/local/templates/rosseti/components"
            "/is/proxy/shutdown_schedule_table/data.php"
        ),
        "source_type": "json",
        "poll_interval_seconds": 21600,  # 6 h
        "parser_profile": {
            "parser": "rosseti_sib",
            "date_filter_days": 4,
        },
    },
    {
        "name": "Россети Томск — плановые отключения",
        "url": "https://rosseti-tomsk.ru/customers/info_disconections/planovie_otklucheniya.php",
        "source_type": "html",
        "poll_interval_seconds": 21600,  # 6 h
        "parser_profile": {
            "parser": "rosseti_tomsk",
            "date_filter_days": 4,
            "verify_ssl": False,  # site uses Russian state root CA not in certifi bundle
            # Server-side date filter (date_start/date_end) returns empty pages when applied
            # via query string — likely the form uses POST or JS-side filtering. Skip it and
            # rely on parser-side date filtering. Pages are sorted DESC by date, so the
            # first few pages cover today + 4 days.
            "paginate": {"param": "PAGEN_1", "max_pages": 2},
        },
    },
]

_LOG_LEVELS = ("DEBUG", "INFO", "WARNING", "ERROR")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Power Outage Agent pipeline")
    parser.add_argument(
        "--log-level",
        choices=_LOG_LEVELS,
        default=settings.log_level.upper(),
        metavar="LEVEL",
        help=f"Logging level: {' | '.join(_LOG_LEVELS)}. Default: {settings.log_level} (from .env)",
    )
    return parser.parse_args()


def _setup_logging(level: str) -> None:
    # force=True removes any handlers added by libraries during import,
    # so our level and format are guaranteed to apply.
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)-8s %(name)-40s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        force=True,
    )
    # silence noisy third-party loggers even on DEBUG
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("sqlalchemy.engine").setLevel(
        logging.INFO if level == "DEBUG" else logging.WARNING
    )


async def _bootstrap_sources(scheduler: Scheduler) -> None:
    source_store = SourceStore(async_session_factory)
    logger.debug("Seeding sources table if empty")
    await source_store.seed_if_empty(_DEFAULT_SOURCES)

    sources = await source_store.list_active()
    if not sources:
        logger.warning("No active sources found in DB — pipeline will idle")
        return

    for s in sources:
        logger.debug(
            "Registering source id=%s type=%s interval=%ds url=%s",
            s.id,
            s.source_type,
            s.poll_interval_seconds,
            s.url,
        )
        scheduler.add_source(
            SourceConfig(
                source_id=s.id,
                url=s.url,
                source_type=s.source_type,
                poll_interval_seconds=s.poll_interval_seconds,
            )
        )

    logger.info("Loaded %d active source(s) from DB", len(sources))


async def main() -> None:
    args = _parse_args()
    _setup_logging(args.log_level)

    logger.info("=== Power Outage Agent starting (log_level=%s) ===", args.log_level)
    logger.debug(
        "Config: db=%s  llm_base_url=%s  llm_model=%s",
        settings.database_url,
        settings.llm_base_url,
        settings.llm_model,
    )

    logger.info("Initializing database")
    try:
        await init_db()
    except OSError as exc:
        logger.error(
            "Cannot connect to database: %s\n"
            "  → Is Docker running?  Try: docker compose up db -d\n"
            "  → DATABASE_URL in .env: %s",
            exc,
            settings.database_url,
        )
        return
    logger.debug("Database schema ready")

    queue = TaskQueue()
    task_store = TaskStore(async_session_factory)
    raw_store = RawStore(async_session_factory)
    source_store = SourceStore(async_session_factory)
    parsed_store = ParsedStore(async_session_factory)
    logger.debug("Core objects created: TaskQueue, TaskStore, RawStore, ParsedStore")

    dispatcher = Dispatcher(queue, task_store)

    collector_handler = CollectorHandler(dispatcher.submit, raw_store, source_store)
    dispatcher.register(TaskType.FETCH_SOURCE, collector_handler.handle)

    parse_handler = ParseHandler(dispatcher.submit, raw_store, source_store, parsed_store)
    dispatcher.register(TaskType.PARSE_CONTENT, parse_handler.handle)

    logger.debug("Dispatcher created; handlers registered for FETCH_SOURCE, PARSE_CONTENT")

    scheduler = Scheduler(dispatcher.submit)
    await _bootstrap_sources(scheduler)

    logger.info("Pipeline ready — starting scheduler and dispatcher")
    await asyncio.gather(scheduler.run(), dispatcher.run())


if __name__ == "__main__":
    asyncio.run(main())
