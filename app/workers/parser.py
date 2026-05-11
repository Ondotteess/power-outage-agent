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
        llm_normalization_enabled: bool = True,
        llm_normalization_max_per_raw: int | None = None,
    ) -> None:
        self._submit = submit
        self._raw_store = raw_store
        self._source_store = source_store
        self._parsed_store = parsed_store
        self._llm_normalization_enabled = llm_normalization_enabled
        self._llm_normalization_max_per_raw = llm_normalization_max_per_raw

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
            await self._parsed_store.save_many(records)

        if not self._llm_normalization_enabled:
            logger.info(
                "ParseHandler  LLM normalization globally disabled  raw_id=%s  parsed_count=%d  trace=%s",
                raw_id,
                len(records),
                task.trace_id,
            )
            return

        if not parser_profile.get("normalize_enabled", True):
            logger.info(
                "ParseHandler  normalization disabled  raw_id=%s  parsed_count=%d  trace=%s",
                raw_id,
                len(records),
                task.trace_id,
            )
            return

        normalize_limit = _effective_normalize_limit(
            parser_profile.get("normalize_limit"),
            self._llm_normalization_max_per_raw,
        )
        records_to_normalize = records
        if normalize_limit is not None:
            records_to_normalize = records[:normalize_limit]
            logger.info(
                "ParseHandler  normalization limited  raw_id=%s  limit=%d/%d  trace=%s",
                raw_id,
                len(records_to_normalize),
                len(records),
                task.trace_id,
            )

        for record in records_to_normalize:
            normalize_task = Task(
                task_type=TaskType.NORMALIZE_EVENT,
                payload={"parsed_record_id": str(record.id)},
                trace_id=task.trace_id,
            )
            logger.debug(
                "ParseHandler  enqueue NORMALIZE_EVENT  normalize_task_id=%s  parsed_id=%s  trace=%s",
                normalize_task.task_id,
                record.id,
                task.trace_id,
            )
            await self._submit(normalize_task)


def _effective_normalize_limit(
    source_limit: object,
    global_limit: int | None,
) -> int | None:
    limits: list[int] = []
    if source_limit is not None:
        limits.append(max(0, int(source_limit)))
    if global_limit is not None:
        limits.append(max(0, int(global_limit)))
    if not limits:
        return None
    return min(limits)
