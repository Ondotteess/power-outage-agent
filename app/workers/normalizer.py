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


class TaskPathStoreProtocol(Protocol):
    async def set_normalizer_path(self, task_id: UUID, path: str) -> None: ...


class NormalizationHandler:
    """Handles NORMALIZE_EVENT tasks."""

    def __init__(
        self,
        parsed_store: ParsedStoreProtocol,
        normalized_store: NormalizedEventStoreProtocol,
        normalizer: NormalizerProtocol,
        submit: Callable[[Task], Awaitable[None]] | None = None,
        task_path_store: TaskPathStoreProtocol | None = None,
    ) -> None:
        self._parsed_store = parsed_store
        self._normalized_store = normalized_store
        self._normalizer = normalizer
        self._submit = submit
        self._task_path_store = task_path_store

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
        cached = await self._cached_event(parsed_id)
        if cached is not None:
            await self._add_cached_sources(cached.event_id, [schema.raw_record_id], task.trace_id)
            logger.info(
                "NormalizationHandler  cache hit  parsed_id=%s  event_id=%s  trace=%s",
                parsed_id,
                cached.event_id,
                task.trace_id,
            )
            if self._submit is not None:
                await self._submit(
                    Task(
                        task_type=TaskType.DEDUPLICATE_EVENT,
                        payload={"event_id": str(cached.event_id)},
                        trace_id=task.trace_id,
                    )
                )
            return

        normalized = await self._normalizer.normalize(schema)
        # Surface which normalizer path produced (or failed) — the Metrics
        # page uses this to show automaton-vs-LLM hit ratio.
        path = getattr(self._normalizer, "last_path", None)
        if self._task_path_store is not None and path is not None:
            try:
                await self._task_path_store.set_normalizer_path(task.task_id, path)
            except Exception:  # noqa: BLE001 — metrics must never break pipeline
                logger.exception("NormalizationHandler  failed to persist normalizer_path")

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

    async def _cached_event(self, parsed_id: UUID):
        getter = getattr(self._normalized_store, "get_by_parsed_record_id", None)
        if getter is None:
            return None
        return await getter(parsed_id)

    async def _add_cached_sources(self, event_id: UUID, sources: list[UUID], trace_id: UUID) -> None:
        updater = getattr(self._normalized_store, "add_sources", None)
        if updater is None:
            return
        await updater(event_id, sources, trace_id)


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
