from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import Protocol
from uuid import UUID, uuid4

from app.matching.office_matcher import MatchableEvent, MatchableOffice, OfficeMatch, OfficeMatcher
from app.models.schemas import ImpactLevel, OfficeImpactSchema
from app.workers.queue import Task, TaskType

logger = logging.getLogger(__name__)


class NormalizedEventStoreProtocol(Protocol):
    async def get_by_id(self, event_id: UUID): ...


class OfficeStoreProtocol(Protocol):
    async def list_active(self): ...


class OfficeImpactStoreProtocol(Protocol):
    async def save_many(self, impacts: list[OfficeImpactSchema], trace_id: UUID) -> int: ...

    async def replace_for_event(
        self,
        event_id: UUID,
        impacts: list[OfficeImpactSchema],
        trace_id: UUID,
    ) -> int: ...


class OfficeMatchHandler:
    """Handles MATCH_OFFICES tasks."""

    def __init__(
        self,
        normalized_store: NormalizedEventStoreProtocol,
        office_store: OfficeStoreProtocol,
        impact_store: OfficeImpactStoreProtocol,
        submit: Callable[[Task], Awaitable[None]] | None = None,
        *,
        demo_emit_unmatched: bool = False,
    ) -> None:
        self._normalized_store = normalized_store
        self._office_store = office_store
        self._impact_store = impact_store
        self._submit = submit
        self._demo_emit_unmatched = demo_emit_unmatched

    async def handle(self, task: Task) -> None:
        event_id = UUID(task.payload["event_id"])
        logger.debug(
            "OfficeMatchHandler  start  task_id=%s  event_id=%s  trace=%s",
            task.task_id,
            event_id,
            task.trace_id,
        )

        event = await self._normalized_store.get_by_id(event_id)
        if event is None:
            logger.error(
                "OfficeMatchHandler  normalized event not found  event_id=%s  trace=%s",
                event_id,
                task.trace_id,
            )
            raise ValueError(f"NormalizedEvent not found: {event_id}")

        offices = await self._office_store.list_active()
        matcher = OfficeMatcher([_to_matchable_office(office) for office in offices])
        matches = matcher.match(_to_matchable_event(event))
        if not matches and self._demo_emit_unmatched and offices:
            matches = [
                OfficeMatch(
                    office=_to_matchable_office(offices[0]),
                    impact_level=ImpactLevel.LOW,
                    match_strategy="demo_unmatched",
                    match_score=0.1,
                    explanation=("demo fallback: no deterministic address match",),
                )
            ]
        detected_at = datetime.now(UTC)
        impacts = [
            OfficeImpactSchema(
                id=uuid4(),
                office_id=match.office.id,
                event_id=event.event_id,
                impact_start=event.start_time,
                impact_end=event.end_time,
                impact_level=match.impact_level,
                match_strategy=match.match_strategy,
                match_score=match.match_score,
                match_explanation=list(match.explanation),
                detected_at=detected_at,
            )
            for match in matches
        ]

        saved = await self._impact_store.replace_for_event(
            event.event_id,
            impacts,
            trace_id=task.trace_id,
        )
        if self._submit is not None:
            for impact in impacts:
                await self._submit(
                    Task(
                        task_type=TaskType.EMIT_EVENT,
                        payload={
                            "office_id": str(impact.office_id),
                            "event_id": str(impact.event_id),
                            "impact_level": str(impact.impact_level),
                            "match_strategy": impact.match_strategy,
                            "match_score": impact.match_score,
                            "match_explanation": impact.match_explanation,
                        },
                        trace_id=task.trace_id,
                    )
                )
        logger.info(
            "OfficeMatchHandler  matched  event_id=%s  matches=%d  saved=%d  trace=%s",
            event_id,
            len(matches),
            saved,
            task.trace_id,
        )


def _to_matchable_office(office) -> MatchableOffice:
    return MatchableOffice(
        id=office.id,
        name=office.name,
        city=office.city,
        address=office.address,
        region=office.region,
    )


def _to_matchable_event(event) -> MatchableEvent:
    return MatchableEvent(
        event_id=event.event_id,
        event_type=event.event_type,
        start_time=event.start_time,
        end_time=event.end_time,
        location_raw=event.location_raw,
        location_normalized=event.location_normalized,
        location_city=event.location_city,
        location_street=event.location_street,
        location_building=event.location_building,
    )
