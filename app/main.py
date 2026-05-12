from __future__ import annotations

import argparse
import asyncio
import contextlib
import logging
from urllib.parse import urlsplit, urlunsplit
from uuid import uuid4

from app.config import settings
from app.db.engine import async_session_factory, init_db
from app.db.repositories import (
    NormalizedEventStore,
    OfficeImpactStore,
    OfficeStore,
    ParsedStore,
    RawStore,
    SourceStore,
    TaskStore,
)
from app.matching.defaults import DEFAULT_OFFICES
from app.normalization.llm import LLMNormalizer
from app.workers.collector import CollectorHandler
from app.workers.dispatcher import Dispatcher
from app.workers.matcher import OfficeMatchHandler
from app.workers.normalizer import NormalizationHandler
from app.workers.parser import ParseHandler
from app.workers.queue import Task, TaskQueue, TaskType
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
            "normalize_enabled": False,
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
            "normalize_limit": 3,
            "verify_ssl": False,  # site uses Russian state root CA not in certifi bundle
            # Server-side date filter (date_start/date_end) returns empty pages when applied
            # via query string — likely the form uses POST or JS-side filtering. Skip it and
            # rely on parser-side date filtering. Pages are sorted DESC by date, so the
            # first few pages cover today + 4 days.
            "paginate": {"param": "PAGEN_1", "max_pages": 2},
        },
    },
    {
        "name": "eseti.ru — плановые отключения",
        "url": "https://www.eseti.ru/DesktopModules/ResWebApi/API/Shutdown",
        "source_type": "json",
        "poll_interval_seconds": 21600,  # 6 h
        "parser_profile": {
            "parser": "eseti",
            "date_filter_days": 4,
            "normalize_enabled": False,
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
    parser.add_argument(
        "--smoke-e2e",
        action="store_true",
        help=(
            "Run every configured source once, force LLM normalization on, "
            "normalize only N parsed records per source, then exit."
        ),
    )
    parser.add_argument(
        "--smoke-normalize-limit",
        type=int,
        default=5,
        metavar="N",
        help="Number of parsed records per source to normalize in --smoke-e2e mode.",
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


def _redact_dsn(dsn: str) -> str:
    try:
        parts = urlsplit(dsn)
        if not parts.hostname:
            return dsn
        user = parts.username or ""
        port = f":{parts.port}" if parts.port else ""
        auth = f"{user}:***@" if user else ""
        return urlunsplit(parts._replace(netloc=f"{auth}{parts.hostname}{port}"))
    except ValueError:
        return "<invalid database_url>"


async def _load_source_configs(*, include_inactive: bool = False) -> list[SourceConfig]:
    source_store = SourceStore(async_session_factory)
    logger.debug("Seeding sources table if empty")
    await source_store.seed_if_empty(_DEFAULT_SOURCES)

    sources = await source_store.list_all() if include_inactive else await source_store.list_active()
    if not sources:
        logger.warning("No sources found in DB — pipeline will idle")
        return []

    configs: list[SourceConfig] = []
    for s in sources:
        logger.debug(
            "Registering source id=%s type=%s interval=%ds url=%s",
            s.id,
            s.source_type,
            s.poll_interval_seconds,
            s.url,
        )
        configs.append(
            SourceConfig(
                source_id=s.id,
                url=s.url,
                source_type=s.source_type,
                poll_interval_seconds=s.poll_interval_seconds,
            )
        )

    label = "configured" if include_inactive else "active"
    logger.info("Loaded %d %s source(s) from DB", len(configs), label)
    return configs


async def _bootstrap_sources(scheduler: Scheduler) -> None:
    for source in await _load_source_configs():
        scheduler.add_source(source)


async def _submit_sources_once(dispatcher: Dispatcher, sources: list[SourceConfig]) -> None:
    for source in sources:
        task = Task(
            task_type=TaskType.FETCH_SOURCE,
            payload={
                "source_id": str(source.source_id),
                "url": source.url,
                "source_type": source.source_type,
                "reparse_duplicate": True,
            },
            trace_id=uuid4(),
        )
        logger.info(
            "Smoke E2E  submitting FETCH_SOURCE  source_id=%s  task_id=%s",
            source.source_id,
            task.task_id,
        )
        await dispatcher.submit(task)


async def main() -> None:
    args = _parse_args()
    _setup_logging(args.log_level)

    logger.info("=== Power Outage Agent starting (log_level=%s) ===", args.log_level)
    logger.debug(
        "Config: db=%s  llm_base_url=%s  llm_model=%s  llm_enabled=%s  llm_max_per_raw=%d",
        _redact_dsn(settings.database_url),
        settings.llm_base_url,
        settings.llm_model,
        settings.llm_normalization_enabled,
        settings.llm_normalization_max_per_raw,
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
            _redact_dsn(settings.database_url),
        )
        return
    logger.debug("Database schema ready")
    await OfficeStore(async_session_factory).seed_if_empty(DEFAULT_OFFICES)

    smoke_profile_override: dict | None = None
    llm_normalization_enabled = settings.llm_normalization_enabled
    llm_normalization_max_per_raw = settings.llm_normalization_max_per_raw
    if args.smoke_e2e:
        limit = max(0, args.smoke_normalize_limit)
        smoke_profile_override = {"normalize_enabled": True, "normalize_limit": limit}
        llm_normalization_enabled = True
        llm_normalization_max_per_raw = limit
        logger.warning(
            "Smoke E2E mode: all configured sources will be polled once; "
            "LLM normalization forced on; normalize_limit=%d",
            limit,
        )

    queue = TaskQueue()
    task_store = TaskStore(async_session_factory)
    stale_tasks = await task_store.fail_incomplete("abandoned by previous pipeline process")
    if stale_tasks:
        logger.warning("Marked %d stale pending/running task(s) as failed", stale_tasks)

    raw_store = RawStore(async_session_factory)
    source_store = SourceStore(async_session_factory)
    parsed_store = ParsedStore(async_session_factory)
    normalized_store = NormalizedEventStore(async_session_factory)
    office_store = OfficeStore(async_session_factory)
    office_impact_store = OfficeImpactStore(async_session_factory)
    logger.debug(
        "Core objects created: TaskQueue, TaskStore, RawStore, ParsedStore, "
        "NormalizedEventStore, OfficeStore, OfficeImpactStore"
    )

    dispatcher = Dispatcher(queue, task_store)

    collector_handler = CollectorHandler(dispatcher.submit, raw_store, source_store)
    dispatcher.register(TaskType.FETCH_SOURCE, collector_handler.handle)

    parse_handler = ParseHandler(
        dispatcher.submit,
        raw_store,
        source_store,
        parsed_store,
        llm_normalization_enabled=llm_normalization_enabled,
        llm_normalization_max_per_raw=llm_normalization_max_per_raw,
        parser_profile_override=smoke_profile_override,
        llm_normalization_max_per_source=(
            max(0, args.smoke_normalize_limit) if args.smoke_e2e else None
        ),
    )
    dispatcher.register(TaskType.PARSE_CONTENT, parse_handler.handle)

    normalization_handler = NormalizationHandler(
        parsed_store,
        normalized_store,
        LLMNormalizer(),
        dispatcher.submit,
    )
    dispatcher.register(TaskType.NORMALIZE_EVENT, normalization_handler.handle)

    office_match_handler = OfficeMatchHandler(
        normalized_store,
        office_store,
        office_impact_store,
    )
    dispatcher.register(TaskType.MATCH_OFFICES, office_match_handler.handle)

    logger.debug(
        "Dispatcher created; handlers registered for FETCH_SOURCE, PARSE_CONTENT, "
        "NORMALIZE_EVENT, MATCH_OFFICES"
    )

    scheduler = Scheduler(dispatcher.submit)
    if args.smoke_e2e:
        sources = await _load_source_configs(include_inactive=True)
        if not sources:
            return
        runner = asyncio.create_task(dispatcher.run())
        try:
            await _submit_sources_once(dispatcher, sources)
            await dispatcher.join()
        finally:
            runner.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await runner
        logger.info("Smoke E2E complete — queue drained")
        return

    logger.info("Pipeline ready — starting scheduler and dispatcher")
    await _bootstrap_sources(scheduler)
    await asyncio.gather(scheduler.run(), dispatcher.run())


if __name__ == "__main__":
    asyncio.run(main())
