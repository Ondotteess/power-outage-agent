from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import pytest

from app.alerts.telegram import TelegramDeliveryError
from app.models.schemas import NotificationSchema
from app.workers.notifier import NotificationHandler
from app.workers.queue import Task, TaskType


@dataclass
class FakeOffice:
    id: UUID = field(default_factory=uuid4)
    name: str = "Demo office"


@dataclass
class FakeEvent:
    event_id: UUID = field(default_factory=uuid4)
    event_type: str = "power_outage"
    start_time: datetime = field(default_factory=lambda: datetime.now(UTC) + timedelta(hours=2))
    end_time: datetime | None = field(
        default_factory=lambda: datetime.now(UTC) + timedelta(hours=6)
    )
    location_raw: str = "село Бичура, улица Кирова, дом 12"
    location_normalized: str | None = "село Бичура, улица Кирова, дом 12"
    reason: str | None = "плановые работы"


@dataclass
class FakeNotificationStore:
    saved: list[tuple[NotificationSchema, UUID, str, str]] = field(default_factory=list)

    async def save(
        self,
        notification: NotificationSchema,
        trace_id: UUID,
        *,
        channel: str,
        status: str,
    ) -> UUID:
        self.saved.append((notification, trace_id, channel, status))
        return notification.notification_id


@dataclass
class FakeOfficeStore:
    office: FakeOffice | None

    async def get_by_id(self, office_id: UUID):
        return self.office


@dataclass
class FakeNormalizedStore:
    event: FakeEvent | None

    async def get_by_id(self, event_id: UUID):
        return self.event


@dataclass
class FakeTelegramSender:
    error: Exception | None = None
    sent: list[str] = field(default_factory=list)

    async def send_message(self, text: str) -> None:
        if self.error is not None:
            raise self.error
        self.sent.append(text)


def _task(office_id: UUID, event_id: UUID) -> Task:
    return Task(
        task_type=TaskType.EMIT_EVENT,
        payload={
            "office_id": str(office_id),
            "event_id": str(event_id),
            "impact_level": "high",
            "match_strategy": "exact_address",
        },
        trace_id=uuid4(),
    )


async def test_notification_handler_saves_dashboard_notification():
    office = FakeOffice()
    event = FakeEvent()
    store = FakeNotificationStore()
    handler = NotificationHandler(
        store,
        FakeOfficeStore(office),
        FakeNormalizedStore(event),
    )
    task = _task(office.id, event.event_id)

    await handler.handle(task)

    assert len(store.saved) == 1
    notification, trace_id, channel, status = store.saved[0]
    assert trace_id == task.trace_id
    assert channel == "dashboard"
    assert status == "sent"
    assert notification.channels == ["dashboard"]
    assert notification.office_id == office.id
    assert notification.event_id == event.event_id
    assert office.name in notification.source_summary


async def test_notification_handler_sends_telegram_when_enabled():
    office = FakeOffice()
    event = FakeEvent()
    store = FakeNotificationStore()
    telegram = FakeTelegramSender()
    handler = NotificationHandler(
        store,
        FakeOfficeStore(office),
        FakeNormalizedStore(event),
        telegram_sender=telegram,
    )
    task = _task(office.id, event.event_id)

    await handler.handle(task)

    assert len(telegram.sent) == 1
    assert "Офис: Demo office" in telegram.sent[0]
    assert f"Event ID: {event.event_id}" in telegram.sent[0]
    assert [(channel, status) for _, _, channel, status in store.saved] == [
        ("dashboard", "sent"),
        ("telegram", "sent"),
    ]
    assert store.saved[0][0].channels == ["dashboard", "telegram"]


async def test_notification_handler_keeps_dashboard_when_telegram_fails(caplog):
    office = FakeOffice()
    event = FakeEvent()
    store = FakeNotificationStore()
    telegram = FakeTelegramSender(error=TelegramDeliveryError("bad chat id"))
    handler = NotificationHandler(
        store,
        FakeOfficeStore(office),
        FakeNormalizedStore(event),
        telegram_sender=telegram,
    )
    task = _task(office.id, event.event_id)

    with caplog.at_level(logging.WARNING):
        await handler.handle(task)

    assert [(channel, status) for _, _, channel, status in store.saved] == [
        ("dashboard", "sent"),
        ("telegram", "failed"),
    ]
    assert "telegram failed" in caplog.text
    assert "bad chat id" in caplog.text


async def test_notification_handler_raises_when_office_missing():
    event = FakeEvent()
    handler = NotificationHandler(
        FakeNotificationStore(),
        FakeOfficeStore(None),
        FakeNormalizedStore(event),
    )

    with pytest.raises(ValueError, match="Office not found"):
        await handler.handle(_task(uuid4(), event.event_id))
