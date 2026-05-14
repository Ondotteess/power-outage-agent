from __future__ import annotations

import argparse
import asyncio
import contextlib
import logging
from collections.abc import Awaitable, Callable
from urllib.parse import urlsplit, urlunsplit
from uuid import uuid4

from app.alerts.telegram import TelegramSender
from app.config import settings
from app.db.engine import async_session_factory, init_db
from app.db.repositories import (
    NormalizedEventStore,
    NotificationStore,
    OfficeImpactStore,
    OfficeStore,
    ParsedStore,
    PollRequestStore,
    RawStore,
    RetryRequestStore,
    SourceStore,
    TaskStore,
)
from app.matching.defaults import DEFAULT_OFFICES
from app.normalization.automaton import AutomatonNormalizer, FallbackNormalizer
from app.normalization.demo import DemoNormalizer
from app.normalization.llm import LLMNormalizer
from app.parsers.demo_collectors import DemoHtmlCollector, DemoJsonCollector
from app.workers.collector import CollectorHandler
from app.workers.deduplicator import DeduplicationHandler
from app.workers.dispatcher import Dispatcher
from app.workers.matcher import OfficeMatchHandler
from app.workers.normalizer import NormalizationHandler
from app.workers.notifier import NotificationHandler
from app.workers.parser import ParseHandler
from app.workers.queue import Task, TaskQueue, TaskType
from app.workers.requests import RequestWatcher
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
    parser.add_argument(
        "--demo-e2e",
        action="store_true",
        help=(
            "Run a deterministic end-to-end demo: 10 local threat records per active source, "
            "no external sites or LLM credentials required, then exit."
        ),
    )
    parser.add_argument(
        "--demo-records-per-source",
        type=int,
        default=10,
        metavar="N",
        help="Number of demo records generated for each active source.",
    )
    parser.add_argument(
        "--demo-step-delay",
        type=float,
        default=0.75,
        metavar="SECONDS",
        help="Small per-task delay in --demo-e2e so the web UI can show running stages.",
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


def _build_telegram_sender() -> TelegramSender | None:
    token_configured = bool(settings.telegram_bot_token.strip())
    chat_configured = bool(settings.telegram_chat_id.strip())
    if token_configured and chat_configured:
        logger.info("Telegram notifications enabled")
        return TelegramSender(settings.telegram_bot_token, settings.telegram_chat_id)
    if token_configured or chat_configured:
        logger.warning(
            "Telegram notifications disabled: TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID are both required"
        )
    return None


async def _load_source_configs(*, include_inactive: bool = False) -> list[SourceConfig]:
    source_store = SourceStore(async_session_factory)
    logger.debug("Seeding sources table if empty")
    await source_store.seed_if_empty(_DEFAULT_SOURCES)

    sources = (
        await source_store.list_all() if include_inactive else await source_store.list_active()
    )
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


def _with_delay(
    handler: Callable[[Task], Awaitable[None]], delay_seconds: float
) -> Callable[[Task], Awaitable[None]]:
    if delay_seconds <= 0:
        return handler

    async def wrapped(task: Task) -> None:
        await asyncio.sleep(delay_seconds)
        await handler(task)

    return wrapped


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

    if args.smoke_e2e and args.demo_e2e:
        logger.error("Choose either --smoke-e2e or --demo-e2e, not both")
        return

    office_registry = OfficeStore(async_session_factory)
    if args.demo_e2e:
        await office_registry.replace_all(DEFAULT_OFFICES)
    else:
        await office_registry.seed_if_empty(DEFAULT_OFFICES)

    smoke_profile_override: dict | None = None
    collector_profile_override: dict | None = None
    llm_normalization_enabled = settings.llm_normalization_enabled
    llm_normalization_max_per_raw = settings.llm_normalization_max_per_raw
    run_once = args.smoke_e2e or args.demo_e2e
    include_inactive_sources = False
    # Two-stage normalizer: deterministic Token-FSA first, GigaChat only on
    # low-confidence parses. Demo mode swaps GigaChat for the offline
    # DemoNormalizer below — same fallback shape, different second stage.
    fallback_normalizer: object = LLMNormalizer()
    demo_step_delay = 0.0
    demo_emit_unmatched = False
    collectors = None
    if args.smoke_e2e:
        limit = max(0, args.smoke_normalize_limit)
        smoke_profile_override = {"normalize_enabled": True, "normalize_limit": limit}
        llm_normalization_enabled = True
        llm_normalization_max_per_raw = limit
        include_inactive_sources = True
        logger.warning(
            "Smoke E2E mode: all configured sources will be polled once; "
            "LLM normalization forced on; normalize_limit=%d",
            limit,
        )
    elif args.demo_e2e:
        limit = max(1, args.demo_records_per_source)
        smoke_profile_override = {"normalize_enabled": True, "normalize_limit": limit}
        collector_profile_override = {"paginate": None}
        llm_normalization_enabled = True
        llm_normalization_max_per_raw = limit
        fallback_normalizer = DemoNormalizer()
        demo_step_delay = max(0.0, args.demo_step_delay)
        demo_emit_unmatched = True
        run_id = str(uuid4())
        collectors = {
            "html": DemoHtmlCollector(limit, run_id=run_id),
            "json": DemoJsonCollector(limit, run_id=run_id),
        }
        logger.warning(
            "Demo E2E mode: active sources will use local fixtures; "
            "records_per_source=%d; step_delay=%.2fs; run_id=%s",
            limit,
            demo_step_delay,
            run_id,
        )

    queue = TaskQueue()
    task_store = TaskStore(async_session_factory)
    stale_tasks = await task_store.fail_incomplete("abandoned by previous pipeline process")
    if stale_tasks:
        logger.warning("Marked %d stale pending/running task(s) as failed", stale_tasks)
    poll_request_store = PollRequestStore(async_session_factory)
    retry_request_store = RetryRequestStore(async_session_factory)
    stale_poll_requests = await poll_request_store.fail_incomplete(
        "abandoned by previous pipeline process"
    )
    stale_retry_requests = await retry_request_store.fail_incomplete(
        "abandoned by previous pipeline process"
    )
    if stale_poll_requests or stale_retry_requests:
        logger.warning(
            "Marked stale admin request(s) as failed  poll=%d retry=%d",
            stale_poll_requests,
            stale_retry_requests,
        )

    raw_store = RawStore(async_session_factory)
    source_store = SourceStore(async_session_factory)
    parsed_store = ParsedStore(async_session_factory)
    normalized_store = NormalizedEventStore(async_session_factory)
    office_store = OfficeStore(async_session_factory)
    office_impact_store = OfficeImpactStore(async_session_factory)
    notification_store = NotificationStore(async_session_factory)
    logger.debug(
        "Core objects created: TaskQueue, TaskStore, RawStore, ParsedStore, "
        "NormalizedEventStore, OfficeStore, OfficeImpactStore"
    )

    dispatcher = Dispatcher(queue, task_store)

    collector_handler = CollectorHandler(
        dispatcher.submit,
        raw_store,
        source_store,
        collectors=collectors,
        parser_profile_override=collector_profile_override,
    )
    dispatcher.register(
        TaskType.FETCH_SOURCE,
        _with_delay(collector_handler.handle, demo_step_delay),
    )

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
    dispatcher.register(
        TaskType.PARSE_CONTENT,
        _with_delay(parse_handler.handle, demo_step_delay),
    )

    normalizer = FallbackNormalizer(
        AutomatonNormalizer(),
        fallback_normalizer,
        threshold=settings.normalizer_fallback_threshold,
    )
    normalization_handler = NormalizationHandler(
        parsed_store,
        normalized_store,
        normalizer,
        dispatcher.submit,
    )
    dispatcher.register(
        TaskType.NORMALIZE_EVENT,
        _with_delay(normalization_handler.handle, demo_step_delay),
    )

    deduplication_handler = DeduplicationHandler(normalized_store, dispatcher.submit)
    dispatcher.register(
        TaskType.DEDUPLICATE_EVENT,
        _with_delay(deduplication_handler.handle, demo_step_delay),
    )

    office_match_handler = OfficeMatchHandler(
        normalized_store,
        office_store,
        office_impact_store,
        dispatcher.submit,
        demo_emit_unmatched=demo_emit_unmatched,
    )
    dispatcher.register(
        TaskType.MATCH_OFFICES,
        _with_delay(office_match_handler.handle, demo_step_delay),
    )

    notification_handler = NotificationHandler(
        notification_store,
        office_store,
        normalized_store,
        telegram_sender=_build_telegram_sender(),
    )
    dispatcher.register(
        TaskType.EMIT_EVENT,
        _with_delay(notification_handler.handle, demo_step_delay),
    )

    logger.debug(
        "Dispatcher created; handlers registered for FETCH_SOURCE, PARSE_CONTENT, "
        "NORMALIZE_EVENT, DEDUPLICATE_EVENT, MATCH_OFFICES, EMIT_EVENT"
    )

    scheduler = Scheduler(dispatcher.submit)
    if run_once:
        sources = await _load_source_configs(include_inactive=include_inactive_sources)
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
        label = "Demo E2E" if args.demo_e2e else "Smoke E2E"
        logger.info("%s complete — queue drained", label)
        return

    logger.info("Pipeline ready — starting scheduler and dispatcher")
    await _bootstrap_sources(scheduler)
    request_watcher = RequestWatcher(
        submit=dispatcher.submit,
        source_store=source_store,
        task_store=task_store,
        poll_requests=poll_request_store,
        retry_requests=retry_request_store,
    )
    await asyncio.gather(scheduler.run(), dispatcher.run(), request_watcher.run())


if __name__ == "__main__":
    asyncio.run(main())
