from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import pytest

from app.models.schemas import OfficeImpactSchema
from app.workers.matcher import OfficeMatchHandler
from app.workers.queue import Task, TaskType

NOW = datetime.now(UTC)


@dataclass
class FakeEvent:
    event_id: UUID = field(default_factory=uuid4)
    event_type: str = "power_outage"
    start_time: datetime = field(default_factory=lambda: NOW + timedelta(hours=2))
    end_time: datetime | None = field(default_factory=lambda: NOW + timedelta(hours=5))
    location_raw: str = "село Бичура, улица Кирова, дом 12"
    location_normalized: str | None = "село Бичура, улица Кирова, дом 12"
    location_city: str | None = "село Бичура"
    location_street: str | None = "улица Кирова"
    location_building: str | None = "12"


@dataclass
class FakeOffice:
    id: UUID = field(default_factory=uuid4)
    name: str = "Бичура, офис"
    city: str = "село Бичура"
    address: str = "улица Кирова, 12"
    region: str = "RU-BU"


@dataclass
class FakeNormalizedStore:
    event: FakeEvent | None

    async def get_by_id(self, event_id: UUID):
        return self.event


@dataclass
class FakeOfficeStore:
    offices: list[FakeOffice]

    async def list_active(self):
        return self.offices


@dataclass
class FakeImpactStore:
    saved: list[tuple[list[OfficeImpactSchema], UUID]] = field(default_factory=list)

    async def save_many(self, impacts: list[OfficeImpactSchema], trace_id: UUID) -> int:
        self.saved.append((impacts, trace_id))
        return len(impacts)


def _task(event_id: UUID) -> Task:
    return Task(
        task_type=TaskType.MATCH_OFFICES,
        payload={"event_id": str(event_id)},
        trace_id=uuid4(),
    )


async def test_office_match_handler_saves_impacts():
    event = FakeEvent()
    office = FakeOffice()
    impact_store = FakeImpactStore()
    handler = OfficeMatchHandler(
        FakeNormalizedStore(event),
        FakeOfficeStore([office]),
        impact_store,
    )
    task = _task(event.event_id)

    await handler.handle(task)

    assert len(impact_store.saved) == 1
    impacts, trace_id = impact_store.saved[0]
    assert trace_id == task.trace_id
    assert len(impacts) == 1
    assert impacts[0].office_id == office.id
    assert impacts[0].event_id == event.event_id
    assert impacts[0].match_strategy == "exact_address"


async def test_office_match_handler_raises_when_event_missing():
    handler = OfficeMatchHandler(
        FakeNormalizedStore(None),
        FakeOfficeStore([]),
        FakeImpactStore(),
    )

    with pytest.raises(ValueError, match="NormalizedEvent not found"):
        await handler.handle(_task(uuid4()))
