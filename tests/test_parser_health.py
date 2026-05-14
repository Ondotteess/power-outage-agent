from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import pytest

from app.workers.parser_health import ParserHealthWatchdog


@dataclass
class FakeSilentQuery:
    """Returns a static silent-source list; lets the test toggle it per tick."""

    silent: list[dict] = field(default_factory=list)
    calls: list[float] = field(default_factory=list)

    async def __call__(self, *, multiplier: float) -> list[dict]:
        self.calls.append(multiplier)
        return list(self.silent)


@dataclass
class FakeEventLog:
    rows: list[dict] = field(default_factory=list)

    async def record(self, **kwargs) -> None:
        self.rows.append(kwargs)


@dataclass
class FakeAlertSender:
    messages: list[str] = field(default_factory=list)

    async def send_message(self, text: str) -> None:
        self.messages.append(text)


def _silent_row(source_id: UUID, name: str, *, silent_for: int | None = 1800) -> dict:
    last = datetime.now(UTC) - timedelta(seconds=silent_for) if silent_for is not None else None
    return {
        "id": source_id,
        "name": name,
        "url": f"https://example.com/{name}",
        "poll_interval_seconds": 600,
        "last_fetched_at": last,
        "silent_for_seconds": silent_for,
    }


async def test_tick_emits_alert_for_each_silent_source_once():
    src_id = uuid4()
    query = FakeSilentQuery(silent=[_silent_row(src_id, "src-a")])
    log = FakeEventLog()
    sender = FakeAlertSender()
    watchdog = ParserHealthWatchdog(query, log, alert_sender=sender, silent_multiplier=3.0)

    await watchdog.tick()
    await watchdog.tick()  # Second tick: same source still silent — no re-alert.

    parser_silent_events = [row for row in log.rows if row["event_type"] == "parser.silent"]
    assert len(parser_silent_events) == 1
    assert parser_silent_events[0]["severity"] == "WARNING"
    assert parser_silent_events[0]["source"] == "src-a"
    assert len(sender.messages) == 1
    assert "src-a" in sender.messages[0]


async def test_tick_emits_recovery_when_source_no_longer_silent():
    src_id = uuid4()
    query = FakeSilentQuery(silent=[_silent_row(src_id, "src-a")])
    log = FakeEventLog()
    sender = FakeAlertSender()
    watchdog = ParserHealthWatchdog(query, log, alert_sender=sender, silent_multiplier=3.0)

    await watchdog.tick()  # initial alert
    query.silent = []  # source recovered
    await watchdog.tick()

    recovery_events = [row for row in log.rows if row["event_type"] == "parser.recovered"]
    assert len(recovery_events) == 1
    assert any("recovered" in msg.lower() for msg in sender.messages)


async def test_alert_re_fires_after_recovery_then_silence_again():
    src_id = uuid4()
    query = FakeSilentQuery(silent=[_silent_row(src_id, "src-a")])
    log = FakeEventLog()
    watchdog = ParserHealthWatchdog(query, log, silent_multiplier=3.0)

    await watchdog.tick()  # alert #1
    query.silent = []
    await watchdog.tick()  # recovery
    query.silent = [_silent_row(src_id, "src-a")]
    await watchdog.tick()  # alert #2 (latch cleared by recovery)

    parser_silent_events = [row for row in log.rows if row["event_type"] == "parser.silent"]
    assert len(parser_silent_events) == 2


async def test_tick_uses_configured_multiplier():
    query = FakeSilentQuery(silent=[])
    log = FakeEventLog()
    watchdog = ParserHealthWatchdog(query, log, silent_multiplier=4.5)
    await watchdog.tick()
    assert query.calls == [4.5]


async def test_tick_handles_never_fetched_source_gracefully():
    src_id = uuid4()
    query = FakeSilentQuery(silent=[_silent_row(src_id, "never", silent_for=None)])
    log = FakeEventLog()
    sender = FakeAlertSender()
    watchdog = ParserHealthWatchdog(query, log, alert_sender=sender)

    await watchdog.tick()
    assert "never fetched" in log.rows[0]["message"]
    assert "never fetched" in sender.messages[0]


async def test_event_log_failure_does_not_break_alert():
    @dataclass
    class FlakyEventLog:
        rows: list[dict] = field(default_factory=list)

        async def record(self, **kwargs):
            raise RuntimeError("DB down")

    sender = FakeAlertSender()
    src_id = uuid4()
    query = FakeSilentQuery(silent=[_silent_row(src_id, "src-a")])
    watchdog = ParserHealthWatchdog(query, FlakyEventLog(), alert_sender=sender)

    # Should not raise; should still send the Telegram message.
    await watchdog.tick()
    assert len(sender.messages) == 1


pytest.importorskip("pytest_asyncio")  # belt-and-braces; suite is auto-mode
