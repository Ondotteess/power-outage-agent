from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from uuid import UUID, uuid4

from app.db.repositories import _merge_normalized_event, _merge_source_lists
from app.models.schemas import EventType, LocationSchema, NormalizedEventSchema


@dataclass
class ExistingEvent:
    event_id: UUID = field(default_factory=uuid4)
    parsed_record_id: UUID | None = field(default_factory=uuid4)
    event_type: str = "power_outage"
    start_time: datetime = field(default_factory=lambda: datetime(2026, 5, 12, 3, tzinfo=UTC))
    end_time: datetime | None = field(
        default_factory=lambda: datetime(2026, 5, 12, 9, tzinfo=UTC)
    )
    location_raw: str = "old raw"
    location_normalized: str | None = "old normalized"
    location_city: str | None = "Old City"
    location_street: str | None = "Old Street"
    location_building: str | None = None
    reason: str | None = "old reason"
    sources: list[str] = field(default_factory=list)
    confidence: float = 0.4
    trace_id: UUID = field(default_factory=uuid4)
    normalized_at: datetime = field(default_factory=lambda: datetime(2026, 5, 12, tzinfo=UTC))


def _event(*, confidence: float, reason: str | None = "new reason") -> NormalizedEventSchema:
    return NormalizedEventSchema(
        event_id=uuid4(),
        parsed_record_id=uuid4(),
        event_type=EventType.MAINTENANCE,
        start_time=datetime(2026, 5, 12, 4, tzinfo=UTC),
        end_time=datetime(2026, 5, 12, 10, tzinfo=UTC),
        location=LocationSchema(
            raw="new raw",
            normalized="new normalized",
            city="New City",
            street="New Street",
            building="10",
        ),
        reason=reason,
        sources=[uuid4()],
        confidence=confidence,
    )


def test_merge_source_lists_deduplicates_and_preserves_order():
    assert _merge_source_lists(["a", "b"], ["b", "c"]) == ["a", "b", "c"]


def test_merge_normalized_event_prefers_higher_confidence_payload():
    existing = ExistingEvent(sources=["raw-a"], confidence=0.4)
    event = _event(confidence=0.9)
    trace_id = uuid4()

    _merge_normalized_event(existing, event, trace_id, [str(event.sources[0])])

    assert existing.trace_id == trace_id
    assert existing.confidence == 0.9
    assert existing.event_type == "maintenance"
    assert existing.location_normalized == "new normalized"
    assert existing.sources == ["raw-a", str(event.sources[0])]


def test_merge_normalized_event_keeps_higher_confidence_payload():
    existing = ExistingEvent(sources=["raw-a"], confidence=0.9, reason=None)
    event = _event(confidence=0.2, reason="fallback reason")
    trace_id = uuid4()

    _merge_normalized_event(existing, event, trace_id, [str(event.sources[0])])

    assert existing.trace_id == trace_id
    assert existing.confidence == 0.9
    assert existing.location_normalized == "old normalized"
    assert existing.reason == "fallback reason"
    assert existing.sources == ["raw-a", str(event.sources[0])]
