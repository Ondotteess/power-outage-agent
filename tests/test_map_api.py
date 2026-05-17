from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

from app.api.routers import map as map_router

NOW = datetime(2026, 5, 13, 12, tzinfo=UTC)


@dataclass
class FakeOffice:
    id: UUID = field(default_factory=uuid4)
    name: str = "Office"
    city: str = "Tomsk"
    address: str = "Lenina, 1"
    region: str = "RU-TOM"
    latitude: float | None = 56.4846
    longitude: float | None = 84.9476


@dataclass
class FakeImpact:
    id: UUID = field(default_factory=uuid4)
    impact_level: str = "medium"
    impact_start: datetime = field(default_factory=lambda: NOW - timedelta(hours=1))
    impact_end: datetime | None = field(default_factory=lambda: NOW + timedelta(hours=1))
    match_strategy: str = "exact_address"


@dataclass
class FakeEvent:
    event_type: str = "maintenance"
    reason: str | None = "Planned feeder maintenance"
    location_raw: str = "Tomsk, Lenina, 1"
    location_normalized: str | None = "Tomsk, Lenina, 1"


def test_map_office_without_impact_is_ok():
    office = FakeOffice()

    response = map_router.build_map_offices_response([(office, None, None)], now=NOW)

    assert len(response.offices) == 1
    assert response.offices[0].status == "ok"
    assert response.offices[0].active_impacts == []


def test_map_office_with_active_medium_impact_is_risk():
    office = FakeOffice()
    impact = FakeImpact(impact_level="medium")
    event = FakeEvent(event_type="maintenance")

    response = map_router.build_map_offices_response([(office, impact, event)], now=NOW)

    mapped = response.offices[0]
    assert mapped.status == "risk"
    assert mapped.active_impacts[0].severity == "medium"
    assert (
        mapped.active_impacts[0].reason
        == "Плановое отключение электроэнергии: Planned feeder maintenance"
    )


def test_map_office_grid_unit_reason_is_explained_as_outage():
    office = FakeOffice()
    impact = FakeImpact(impact_level="high")
    event = FakeEvent(event_type="power_outage", reason="Краснотуранский РЭС")

    response = map_router.build_map_offices_response([(office, impact, event)], now=NOW)

    assert (
        response.offices[0].active_impacts[0].reason
        == "Плановое отключение электроэнергии. Участок: Краснотуранский РЭС"
    )


def test_map_office_with_active_high_impact_is_critical():
    office = FakeOffice()
    impact = FakeImpact(impact_level="high")
    event = FakeEvent(event_type="maintenance")

    response = map_router.build_map_offices_response([(office, impact, event)], now=NOW)

    assert response.offices[0].status == "critical"


def test_map_completed_impact_does_not_affect_status():
    office = FakeOffice()
    impact = FakeImpact(
        impact_level="high",
        impact_start=NOW - timedelta(hours=3),
        impact_end=NOW - timedelta(minutes=1),
    )

    response = map_router.build_map_offices_response([(office, impact, FakeEvent())], now=NOW)

    mapped = response.offices[0]
    assert mapped.status == "ok"
    assert mapped.active_impacts == []


def test_map_future_impact_within_horizon_affects_status():
    office = FakeOffice()
    impact = FakeImpact(
        impact_level="medium",
        impact_start=NOW + timedelta(days=2),
        impact_end=NOW + timedelta(days=2, hours=4),
    )

    response = map_router.build_map_offices_response([(office, impact, FakeEvent())], now=NOW)

    mapped = response.offices[0]
    assert mapped.status == "risk"
    assert mapped.active_impacts[0].starts_at == impact.impact_start


def test_map_office_without_coordinates_stays_in_response():
    office = FakeOffice(latitude=None, longitude=None)

    response = map_router.build_map_offices_response([(office, None, None)], now=NOW)

    mapped = response.offices[0]
    assert mapped.latitude is None
    assert mapped.longitude is None
    assert mapped.status == "ok"


async def test_map_endpoint_returns_offices(monkeypatch):
    office = FakeOffice()

    async def fake_rows(_session, *, now, horizon_until):
        assert now.tzinfo is not None
        assert horizon_until > now
        return [(office, None, None)]

    monkeypatch.setattr(map_router.queries, "list_map_office_rows", fake_rows)

    response = await map_router.list_map_offices(object())  # type: ignore[arg-type]

    assert response.offices[0].id == office.id
    assert response.offices[0].status == "ok"
