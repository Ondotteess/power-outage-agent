from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from datetime import date, timedelta
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from uuid import UUID

from app.db.repositories import RawStore, SourceStore
from app.models.schemas import SourceType
from app.parsers.base import BaseCollector
from app.parsers.html_collector import HtmlCollector
from app.parsers.json_collector import JsonCollector
from app.workers.queue import Task, TaskType

logger = logging.getLogger(__name__)

Submit = Callable[[Task], Awaitable[None]]

_DATE_FMT = "%d.%m.%Y"
_DEFAULT_MAX_PAGES = 5
_MAX_PAGES = 10
_DEFAULT_DATE_FILTER_DAYS = 4
_MAX_DATE_FILTER_DAYS = 14


def default_collectors() -> dict[str, BaseCollector]:
    return {
        SourceType.HTML: HtmlCollector(),
        SourceType.JSON: JsonCollector(),
    }


class CollectorHandler:
    """Handles FETCH_SOURCE tasks: fetches raw content, persists it, enqueues parse.

    Reads `parser_profile` from the source to support pagination and date-range
    URL params:

        parser_profile = {
            "paginate": {"param": "PAGEN_1", "max_pages": 5},
            "date_params": {"date_start": "today", "date_end": "today+window"},
            "date_filter_days": 4,
        }
    """

    def __init__(
        self,
        submit: Submit,
        raw_store: RawStore,
        source_store: SourceStore | None = None,
        collectors: dict[str, BaseCollector] | None = None,
        parser_profile_override: dict | None = None,
    ) -> None:
        self._submit = submit
        self._raw_store = raw_store
        self._source_store = source_store
        self._collectors = collectors if collectors is not None else default_collectors()
        self._parser_profile_override = parser_profile_override or {}

    async def handle(self, task: Task) -> None:
        source_type = task.payload.get("source_type", SourceType.HTML)
        url = task.payload["url"]
        source_id_str = task.payload.get("source_id")
        source_id = UUID(source_id_str) if source_id_str else None
        reparse_duplicate = bool(task.payload.get("reparse_duplicate", False))

        parser_profile = await self._load_parser_profile(source_id)
        urls = self._build_urls(url, parser_profile)

        logger.debug(
            "Collector  start  task_id=%s  source_type=%s  pages=%d  source_id=%s  trace=%s",
            task.task_id,
            source_type,
            len(urls),
            source_id,
            task.trace_id,
        )

        collector = self._collectors.get(source_type)
        if collector is None:
            logger.error(
                "Collector  unknown source_type=%s  task_id=%s  trace=%s",
                source_type,
                task.task_id,
                task.trace_id,
            )
            raise ValueError(f"No collector for source type: {source_type!r}")

        verify_ssl = bool(parser_profile.get("verify_ssl", True))
        for page_url in urls:
            await self._fetch_and_persist(
                collector,
                page_url,
                source_id,
                task,
                verify_ssl,
                reparse_duplicate=reparse_duplicate,
            )

    async def _load_parser_profile(self, source_id: UUID | None) -> dict:
        if source_id is None or self._source_store is None:
            return dict(self._parser_profile_override)
        source = await self._source_store.get_by_id(source_id)
        parser_profile = source.parser_profile if source is not None else {}
        if self._parser_profile_override:
            parser_profile = {**parser_profile, **self._parser_profile_override}
        return parser_profile

    def _build_urls(self, base_url: str, parser_profile: dict) -> list[str]:
        url_with_dates = self._apply_date_params(base_url, parser_profile)

        paginate = parser_profile.get("paginate")
        if not paginate:
            return [url_with_dates]

        param = paginate.get("param", "PAGEN_1")
        max_pages = _bounded_int(
            paginate.get("max_pages", _DEFAULT_MAX_PAGES),
            default=_DEFAULT_MAX_PAGES,
            minimum=1,
            maximum=_MAX_PAGES,
        )
        return [_add_query_param(url_with_dates, param, str(p)) for p in range(1, max_pages + 1)]

    def _apply_date_params(self, url: str, parser_profile: dict) -> str:
        date_params: dict[str, str] = parser_profile.get("date_params", {})
        if not date_params:
            return url

        today = date.today()
        window = _bounded_int(
            parser_profile.get("date_filter_days", _DEFAULT_DATE_FILTER_DAYS),
            default=_DEFAULT_DATE_FILTER_DAYS,
            minimum=0,
            maximum=_MAX_DATE_FILTER_DAYS,
        )
        cutoff = today + timedelta(days=window)
        values = {
            "today": today.strftime(_DATE_FMT),
            "today+window": cutoff.strftime(_DATE_FMT),
        }

        result = url
        for param_name, value_template in date_params.items():
            value = values.get(value_template, value_template)
            result = _add_query_param(result, param_name, value)
        return result

    async def _fetch_and_persist(
        self,
        collector: BaseCollector,
        url: str,
        source_id: UUID | None,
        task: Task,
        verify_ssl: bool = True,
        *,
        reparse_duplicate: bool = False,
    ) -> None:
        raw = await collector.fetch(url=url, trace_id=task.trace_id, verify_ssl=verify_ssl)
        logger.info(
            "Collector  fetched  %d bytes  url=%s  content_hash=%s  trace=%s",
            len(raw.raw_content),
            url,
            raw.content_hash,
            task.trace_id,
        )

        existing_raw_id = await self._raw_store.get_id_by_hash(raw.content_hash)
        if existing_raw_id is not None:
            logger.info(
                "Collector  duplicate skipped  content_hash=%s  url=%s  trace=%s",
                raw.content_hash,
                url,
                task.trace_id,
            )
            if reparse_duplicate:
                logger.info(
                    "Collector  duplicate reparse requested  raw_id=%s  trace=%s",
                    existing_raw_id,
                    task.trace_id,
                )
                await self._submit_parse(existing_raw_id, task.trace_id)
            return

        await self._raw_store.save(raw, source_id=source_id)
        logger.info(
            "Collector  raw saved  raw_id=%s  source_id=%s  url=%s  trace=%s",
            raw.id,
            source_id,
            url,
            task.trace_id,
        )

        await self._submit_parse(raw.id, task.trace_id)

    async def _submit_parse(self, raw_id: UUID, trace_id: UUID) -> None:
        parse_task = Task(
            task_type=TaskType.PARSE_CONTENT,
            payload={"raw_record_id": str(raw_id)},
            trace_id=trace_id,
        )
        logger.debug(
            "Collector  enqueue PARSE_CONTENT  parse_task_id=%s  raw_id=%s  trace=%s",
            parse_task.task_id,
            raw_id,
            trace_id,
        )
        await self._submit(parse_task)


def _add_query_param(url: str, name: str, value: str) -> str:
    parts = urlsplit(url)
    query = parse_qsl(parts.query, keep_blank_values=True)
    query.append((name, value))
    return urlunsplit(parts._replace(query=urlencode(query)))


def _bounded_int(value: object, *, default: int, minimum: int, maximum: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = default
    return max(minimum, min(maximum, parsed))
