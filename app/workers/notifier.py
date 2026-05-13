from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Protocol
from uuid import UUID, uuid4

from app.alerts.telegram import TelegramDeliveryError
from app.models.schemas import EventType, ImpactLevel, NotificationSchema
from app.workers.queue import Task

logger = logging.getLogger(__name__)


class NotificationStoreProtocol(Protocol):
    async def save(
        self,
        notification: NotificationSchema,
        trace_id: UUID,
        *,
        channel: str,
        status: str,
    ) -> UUID: ...


class OfficeStoreProtocol(Protocol):
    async def get_by_id(self, office_id: UUID): ...


class NormalizedEventStoreProtocol(Protocol):
    async def get_by_id(self, event_id: UUID): ...


class TelegramSenderProtocol(Protocol):
    async def send_message(self, text: str) -> None: ...


class NotificationHandler:
    """Handles EMIT_EVENT by recording local and optional Telegram notifications."""

    def __init__(
        self,
        notification_store: NotificationStoreProtocol,
        office_store: OfficeStoreProtocol,
        normalized_store: NormalizedEventStoreProtocol,
        *,
        channel: str = "dashboard",
        telegram_sender: TelegramSenderProtocol | None = None,
    ) -> None:
        self._notification_store = notification_store
        self._office_store = office_store
        self._normalized_store = normalized_store
        self._channel = channel
        self._telegram_sender = telegram_sender

    async def handle(self, task: Task) -> None:
        office_id = UUID(task.payload["office_id"])
        event_id = UUID(task.payload["event_id"])

        office = await self._office_store.get_by_id(office_id)
        if office is None:
            raise ValueError(f"Office not found: {office_id}")
        event = await self._normalized_store.get_by_id(event_id)
        if event is None:
            raise ValueError(f"NormalizedEvent not found: {event_id}")

        severity = _impact_level(task.payload.get("impact_level"))
        channels = [self._channel]
        if self._telegram_sender is not None:
            channels.append("telegram")

        notification = NotificationSchema(
            notification_id=uuid4(),
            office_id=office_id,
            event_id=event_id,
            type=_event_type(event.event_type),
            severity=severity,
            start_time=event.start_time,
            end_time=event.end_time,
            source_summary=_summary(office, event, task.payload),
            channels=channels,
            emitted_at=datetime.now(UTC),
        )
        notification_id = await self._notification_store.save(
            notification,
            task.trace_id,
            channel=self._channel,
            status="sent",
        )
        logger.info(
            "NotificationHandler  emitted  notification_id=%s  office_id=%s  event_id=%s  trace=%s",
            notification_id,
            office_id,
            event_id,
            task.trace_id,
        )

        if self._telegram_sender is not None:
            await self._emit_telegram(notification, office, event, task)

    async def _emit_telegram(self, notification, office, event, task: Task) -> None:
        text = _telegram_message(office, event, task.payload)
        telegram_notification = notification.model_copy(
            update={
                "notification_id": uuid4(),
                "source_summary": text,
                "emitted_at": datetime.now(UTC),
            }
        )

        try:
            await self._telegram_sender.send_message(text)
        except TelegramDeliveryError as exc:
            await self._notification_store.save(
                telegram_notification,
                task.trace_id,
                channel="telegram",
                status="failed",
            )
            logger.warning(
                "NotificationHandler  telegram failed  office_id=%s  event_id=%s  trace=%s  error=%s",
                telegram_notification.office_id,
                telegram_notification.event_id,
                task.trace_id,
                exc,
            )
            return
        except Exception:
            await self._notification_store.save(
                telegram_notification,
                task.trace_id,
                channel="telegram",
                status="failed",
            )
            logger.exception(
                "NotificationHandler  telegram crashed  office_id=%s  event_id=%s  trace=%s",
                telegram_notification.office_id,
                telegram_notification.event_id,
                task.trace_id,
            )
            return

        telegram_id = await self._notification_store.save(
            telegram_notification,
            task.trace_id,
            channel="telegram",
            status="sent",
        )
        logger.info(
            "NotificationHandler  telegram emitted  notification_id=%s  office_id=%s  event_id=%s  trace=%s",
            telegram_id,
            telegram_notification.office_id,
            telegram_notification.event_id,
            task.trace_id,
        )


def _event_type(value: object) -> EventType:
    try:
        return EventType(str(value))
    except ValueError:
        return EventType.POWER_OUTAGE


def _impact_level(value: object) -> ImpactLevel:
    try:
        return ImpactLevel(str(value))
    except ValueError:
        return ImpactLevel.MEDIUM


def _summary(office, event, payload: dict) -> str:
    location = event.location_normalized or event.location_raw
    strategy = payload.get("match_strategy", "match")
    return (
        f"{office.name}: отключение затрагивает {location}. "
        f"Окно: {event.start_time.isoformat()} - "
        f"{event.end_time.isoformat() if event.end_time else 'без окончания'}. "
        f"Матчинг: {strategy}."
    )


def _telegram_message(office, event, payload: dict) -> str:
    location = event.location_normalized or event.location_raw
    reason = getattr(event, "reason", None) or "не указана"
    impact_level = _impact_level(payload.get("impact_level"))
    strategy = payload.get("match_strategy", "match")
    return "\n".join(
        [
            "Отключение электроэнергии",
            "",
            f"Офис: {office.name}",
            f"Локация: {location}",
            f"Причина: {reason}",
            f"Начало: {event.start_time.isoformat()}",
            f"Окончание: {event.end_time.isoformat() if event.end_time else 'не указано'}",
            f"Уровень: {impact_level}",
            f"Матчинг: {strategy}",
            f"Event ID: {event.event_id}",
        ]
    )
