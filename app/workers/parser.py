from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from uuid import UUID

from app.parsers.rosseti_sib import RossetiSibParser
from app.parsers.rosseti_tomsk import RossetiTomskParser
from app.workers.queue import Task, TaskType

logger = logging.getLogger(__name__)

Submit = Callable[[Task], Awaitable[None]]

# Registry: parser_profile["parser"] value → parser instance
_PARSER_REGISTRY: dict[str, object] = {
    "rosseti_sib": RossetiSibParser(),
    "rosseti_tomsk": RossetiTomskParser(),
}


class ParseHandler:
    """Handles PARSE_CONTENT tasks: extracts structured records from raw content."""

    def __init__(
        self,
        submit: Submit,
        raw_store,
        source_store,
        parsed_store,
    ) -> None:
        self._submit = submit
        self._raw_store = raw_store
        self._source_store = source_store
        self._parsed_store = parsed_store

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

        for record in records:
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
