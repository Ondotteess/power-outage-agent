from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from typing import Protocol
from uuid import UUID

from app.models.schemas import NormalizedEventSchema, ParsedRecordSchema
from app.workers.queue import Task, TaskType

logger = logging.getLogger(__name__)


class ParsedStoreProtocol(Protocol):
    async def get_by_id(self, parsed_id: UUID): ...


class NormalizedEventStoreProtocol(Protocol):
    async def save(self, event: NormalizedEventSchema, trace_id: UUID) -> UUID: ...


class NormalizerProtocol(Protocol):
    async def normalize(self, record: ParsedRecordSchema) -> NormalizedEventSchema | None: ...


class NormalizationHandler:
    """Handles NORMALIZE_EVENT tasks."""

    def __init__(
        self,
        parsed_store: ParsedStoreProtocol,
        normalized_store: NormalizedEventStoreProtocol,
        normalizer: NormalizerProtocol,
        submit: Callable[[Task], Awaitable[None]] | None = None,
    ) -> None:
        self._parsed_store = parsed_store
        self._normalized_store = normalized_store
        self._normalizer = normalizer
        self._submit = submit

    async def handle(self, task: Task) -> None:
        parsed_id = UUID(task.payload["parsed_record_id"])
        logger.debug(
            "NormalizationHandler  start  task_id=%s  parsed_id=%s  trace=%s",
            task.task_id,
            parsed_id,
            task.trace_id,
        )

        parsed = await self._parsed_store.get_by_id(parsed_id)
        if parsed is None:
            logger.error(
                "NormalizationHandler  parsed record not found  parsed_id=%s  trace=%s",
                parsed_id,
                task.trace_id,
            )
            raise ValueError(f"ParsedRecord not found: {parsed_id}")

        schema = _to_schema(parsed)
        normalized = await self._normalizer.normalize(schema)
        if normalized is None:
            logger.warning(
                "NormalizationHandler  skipped  parsed_id=%s  trace=%s",
                parsed_id,
                task.trace_id,
            )
            return

        event_id = await self._normalized_store.save(normalized, trace_id=task.trace_id)
        logger.info(
            "NormalizationHandler  normalized  parsed_id=%s  event_id=%s  trace=%s",
            parsed_id,
            event_id,
            task.trace_id,
        )

        if self._submit is not None:
            await self._submit(
                Task(
                    task_type=TaskType.DEDUPLICATE_EVENT,
                    payload={"event_id": str(event_id)},
                    trace_id=task.trace_id,
                )
            )


def _to_schema(record) -> ParsedRecordSchema:
    return ParsedRecordSchema(
        id=record.id,
        raw_record_id=record.raw_record_id,
        source_id=record.source_id,
        external_id=record.external_id,
        start_time=record.start_time,
        end_time=record.end_time,
        location_city=record.location_city,
        location_district=record.location_district,
        location_street=record.location_street,
        location_region_code=record.location_region_code,
        reason=record.reason,
        extra=record.extra or {},
        trace_id=record.trace_id,
        extracted_at=record.extracted_at,
    )
