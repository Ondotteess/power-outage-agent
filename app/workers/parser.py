from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from typing import Protocol
from uuid import UUID

from app.models.schemas import ParsedRecordSchema
from app.parsers.eseti import EsetiParser
from app.parsers.rosseti_sib import RossetiSibParser
from app.parsers.rosseti_tomsk import RossetiTomskParser
from app.workers.queue import Task, TaskType

logger = logging.getLogger(__name__)

Submit = Callable[[Task], Awaitable[None]]


class ParserProtocol(Protocol):
    def parse(
        self,
        raw_content: str,
        raw_record_id: UUID,
        source_id: UUID | None,
        trace_id: UUID,
        parser_profile: dict,
    ) -> list[ParsedRecordSchema]: ...


# Registry: parser_profile["parser"] value → parser instance
_PARSER_REGISTRY: dict[str, ParserProtocol] = {
    "rosseti_sib": RossetiSibParser(),
    "rosseti_tomsk": RossetiTomskParser(),
    "eseti": EsetiParser(),
}


class ParseHandler:
    """Handles PARSE_CONTENT tasks: extracts structured records from raw content."""

    def __init__(
        self,
        submit: Submit,
        raw_store,
        source_store,
        parsed_store,
        *,
        normalization_enabled: bool = True,
        fallback_normalization_enabled: bool | None = None,
        fallback_normalization_max_per_raw: int | None = None,
        fallback_normalization_max_per_source: int | None = None,
        llm_normalization_enabled: bool | None = None,
        llm_normalization_max_per_raw: int | None = None,
        parser_profile_override: dict | None = None,
        llm_normalization_max_per_source: int | None = None,
    ) -> None:
        self._submit = submit
        self._raw_store = raw_store
        self._source_store = source_store
        self._parsed_store = parsed_store
        self._normalization_enabled = normalization_enabled
        if fallback_normalization_enabled is None:
            fallback_normalization_enabled = (
                True if llm_normalization_enabled is None else llm_normalization_enabled
            )
        if fallback_normalization_max_per_raw is None:
            fallback_normalization_max_per_raw = llm_normalization_max_per_raw
        if fallback_normalization_max_per_source is None:
            fallback_normalization_max_per_source = llm_normalization_max_per_source
        self._fallback_normalization_enabled = fallback_normalization_enabled
        self._fallback_normalization_max_per_raw = fallback_normalization_max_per_raw
        self._parser_profile_override = parser_profile_override or {}
        self._fallback_normalization_max_per_source = fallback_normalization_max_per_source
        self._fallback_per_source: dict[UUID | None, int] = {}

    async def handle(self, task: Task) -> None:
        raw_id = UUID(task.payload["raw_record_id"])

        logger.debug(
            "ParseHandler  start  task_id=%s  raw_id=%s  trace=%s",
            task.task_id,
            raw_id,
            task.trace_id,
        )

        raw = await self._raw_store.get_by_id(raw_id)
        if raw is None:
            logger.error(
                "ParseHandler  raw record not found  raw_id=%s  trace=%s", raw_id, task.trace_id
            )
            raise ValueError(f"RawRecord not found: {raw_id}")

        parser_profile: dict = {}
        if raw.source_id is not None:
            source = await self._source_store.get_by_id(raw.source_id)
            if source is not None:
                parser_profile = source.parser_profile or {}
        if self._parser_profile_override:
            parser_profile = {**parser_profile, **self._parser_profile_override}

        parser_name: str = parser_profile.get("parser", "")
        parser = _PARSER_REGISTRY.get(parser_name)
        if parser is None:
            logger.error(
                "ParseHandler  unknown parser=%r  raw_id=%s  trace=%s",
                parser_name,
                raw_id,
                task.trace_id,
            )
            raise ValueError(f"No parser registered for: {parser_name!r}")

        records = parser.parse(
            raw_content=raw.raw_content,
            raw_record_id=raw.id,
            source_id=raw.source_id,
            trace_id=task.trace_id,
            parser_profile=parser_profile,
        )

        logger.info(
            "ParseHandler  parsed  count=%d  raw_id=%s  trace=%s",
            len(records),
            raw_id,
            task.trace_id,
        )

        if records:
            persisted_records = await self._parsed_store.save_many(records)
            if persisted_records is not None:
                records = persisted_records

        if not self._normalization_enabled:
            logger.info(
                "ParseHandler  normalization globally disabled  raw_id=%s  parsed_count=%d  trace=%s",
                raw_id,
                len(records),
                task.trace_id,
            )
            return

        if parser_profile.get("normalization_enabled") is False:
            logger.info(
                "ParseHandler  normalization disabled  raw_id=%s  parsed_count=%d  trace=%s",
                raw_id,
                len(records),
                task.trace_id,
            )
            return

        fallback_enabled = self._fallback_enabled(parser_profile)
        fallback_limit = _effective_normalize_limit(
            parser_profile.get("fallback_limit"),
            self._fallback_normalization_max_per_raw,
        )
        fallback_records = records if fallback_enabled else []
        if fallback_limit is not None and fallback_enabled:
            fallback_records = fallback_records[:fallback_limit]
            logger.info(
                "ParseHandler  regex fallback limited  raw_id=%s  limit=%d/%d  trace=%s",
                raw_id,
                len(fallback_records),
                len(records),
                task.trace_id,
            )

        fallback_records = self._apply_source_budget(raw.source_id, fallback_records)
        fallback_ids = {record.id for record in fallback_records}
        if not fallback_ids and fallback_enabled:
            logger.info(
                "ParseHandler  source regex fallback budget exhausted  raw_id=%s  source_id=%s  trace=%s",
                raw_id,
                raw.source_id,
                task.trace_id,
            )

        for record in records:
            normalize_task = Task(
                task_type=TaskType.NORMALIZE_EVENT,
                payload={
                    "parsed_record_id": str(record.id),
                    "allow_fallback": record.id in fallback_ids,
                    "allow_llm_fallback": record.id in fallback_ids,
                },
                trace_id=task.trace_id,
            )
            logger.debug(
                "ParseHandler  enqueue NORMALIZE_EVENT  normalize_task_id=%s  parsed_id=%s  trace=%s",
                normalize_task.task_id,
                record.id,
                task.trace_id,
            )
            await self._submit(normalize_task)

    def _fallback_enabled(self, parser_profile: dict) -> bool:
        if not self._fallback_normalization_enabled:
            return False
        if "fallback_enabled" in parser_profile:
            return bool(parser_profile["fallback_enabled"])
        if "regex_fallback_enabled" in parser_profile:
            return bool(parser_profile["regex_fallback_enabled"])
        # Legacy `llm_fallback_enabled=false` / `normalize_enabled=false` were
        # paid-call guardrails. Regex fallback is local and cheap, so old source
        # profiles no longer suppress it.
        return True

    def _apply_source_budget(
        self,
        source_id: UUID | None,
        records: list[ParsedRecordSchema],
    ) -> list[ParsedRecordSchema]:
        if self._fallback_normalization_max_per_source is None:
            return records
        limit = max(0, self._fallback_normalization_max_per_source)
        used = self._fallback_per_source.get(source_id, 0)
        remaining = max(0, limit - used)
        selected = records[:remaining]
        self._fallback_per_source[source_id] = used + len(selected)
        return selected


def _effective_normalize_limit(
    source_limit: object,
    global_limit: int | None,
) -> int | None:
    limits: list[int] = []
    if source_limit is not None:
        limits.append(_safe_non_negative_int(source_limit))
    if global_limit is not None:
        limits.append(_safe_non_negative_int(global_limit))
    if not limits:
        return None
    return min(limits)


def _safe_non_negative_int(value: object) -> int:
    try:
        return max(0, int(value))
    except (TypeError, ValueError):
        return 0
