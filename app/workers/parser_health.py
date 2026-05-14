"""Parser health watchdog.

Periodically asks `queries.silent_sources` for any active source whose last
RawRecord is older than `multiplier × poll_interval_seconds`. For each newly
silent source, writes an EventLog row at WARNING and sends a Telegram alert
(if configured). Once a source recovers, emits an INFO recovery event.

Alerts are de-duplicated by source id — a single silence yields one alert,
not one per check tick.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Protocol
from uuid import UUID

logger = logging.getLogger(__name__)


class SilentSourceQueryProtocol(Protocol):
    async def __call__(self, *, multiplier: float) -> list[dict]: ...


class EventLogProtocol(Protocol):
    async def record(
        self,
        *,
        event_type: str,
        severity: str,
        message: str,
        source: str | None = None,
        task_id: UUID | None = None,
        trace_id: UUID | None = None,
        payload: dict | None = None,
    ) -> None: ...


class AlertSenderProtocol(Protocol):
    async def send_message(self, text: str) -> None: ...


class ParserHealthWatchdog:
    def __init__(
        self,
        query: SilentSourceQueryProtocol,
        event_log: EventLogProtocol,
        *,
        alert_sender: AlertSenderProtocol | None = None,
        check_interval_seconds: float = 300.0,
        silent_multiplier: float = 3.0,
    ) -> None:
        self._query = query
        self._event_log = event_log
        self._alert_sender = alert_sender
        self._check_interval = check_interval_seconds
        self._multiplier = silent_multiplier
        # Per-source latch — fire alert on transition, not every tick.
        self._alerted: set[UUID] = set()

    async def run(self) -> None:
        logger.info(
            "ParserHealthWatchdog started  interval=%.0fs  multiplier=%.1f",
            self._check_interval,
            self._multiplier,
        )
        while True:
            try:
                await self.tick()
            except Exception:  # noqa: BLE001 — observability must not crash pipeline
                logger.exception("ParserHealthWatchdog tick failed")
            await asyncio.sleep(self._check_interval)

    async def tick(self) -> None:
        silent = await self._query(multiplier=self._multiplier)
        silent_ids = {item["id"] for item in silent}

        # Recovery: previously silent sources that no longer appear.
        for recovered_id in list(self._alerted - silent_ids):
            await self._emit_recovery(recovered_id)
            self._alerted.discard(recovered_id)

        # New silence: not yet alerted.
        for item in silent:
            source_id: UUID = item["id"]
            if source_id in self._alerted:
                continue
            await self._emit_alert(item)
            self._alerted.add(source_id)

    async def _emit_alert(self, item: dict) -> None:
        silent_for = item.get("silent_for_seconds")
        last = item.get("last_fetched_at")
        details = (
            "never fetched"
            if last is None
            else f"silent for {silent_for}s (last fetch {last.isoformat()})"
        )
        message = f"Parser silent — {item['name']}: {details}"
        logger.warning(
            "ParserHealthWatchdog alert  source_id=%s  name=%s  details=%s",
            item["id"],
            item["name"],
            details,
        )
        try:
            await self._event_log.record(
                event_type="parser.silent",
                severity="WARNING",
                message=message,
                source=item["name"],
                payload={
                    "source_id": str(item["id"]),
                    "url": item.get("url"),
                    "poll_interval_seconds": item.get("poll_interval_seconds"),
                    "silent_for_seconds": silent_for,
                },
            )
        except Exception:  # noqa: BLE001
            logger.exception("ParserHealthWatchdog event_log.record failed")
        if self._alert_sender is not None:
            try:
                await self._alert_sender.send_message(message)
            except Exception:  # noqa: BLE001
                logger.exception("ParserHealthWatchdog telegram send failed")

    async def _emit_recovery(self, source_id: UUID) -> None:
        message = f"Parser recovered — source_id={source_id}"
        logger.info("ParserHealthWatchdog recovery  source_id=%s", source_id)
        try:
            await self._event_log.record(
                event_type="parser.recovered",
                severity="INFO",
                message=message,
                payload={"source_id": str(source_id)},
            )
        except Exception:  # noqa: BLE001
            logger.exception("ParserHealthWatchdog recovery event_log failed")
        if self._alert_sender is not None:
            try:
                await self._alert_sender.send_message(message)
            except Exception:  # noqa: BLE001
                logger.exception("ParserHealthWatchdog telegram recovery send failed")
