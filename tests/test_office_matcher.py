from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import uuid4

from app.matching.office_matcher import MatchableEvent, MatchableOffice, OfficeMatcher
from app.models.schemas import ImpactLevel

NOW = datetime(2026, 5, 12, 12, tzinfo=UTC)


def _office(city: str = "село Бичура", address: str = "улица Кирова, 12") -> MatchableOffice:
    return MatchableOffice(
        id=uuid4(),
        name="Office",
        city=city,
        address=address,
        region="RU-BU",
    )


def _event(
    *,
    city: str | None = "село Бичура",
    street: str | None = "улица Кирова",
    building: str | None = "12",
    normalized: str | None = "село Бичура, улица Кирова, дом 12",
    end_time: datetime | None = None,
) -> MatchableEvent:
    return MatchableEvent(
        event_id=uuid4(),
        event_type="power_outage",
        start_time=NOW + timedelta(hours=2),
        end_time=end_time or NOW + timedelta(hours=6),
        location_raw=normalized or "",
        location_normalized=normalized,
        location_city=city,
        location_street=street,
        location_building=building,
    )


def test_exact_address_match_wins_with_full_building():
    office = _office()
    matches = OfficeMatcher([office]).match(_event(), now=NOW)

    assert len(matches) == 1
    assert matches[0].office.id == office.id
    assert matches[0].match_strategy == "exact_address"
    assert matches[0].match_score == 1.0
    assert matches[0].impact_level == ImpactLevel.HIGH
    assert any("building=12 exact" in item for item in matches[0].explanation)


def test_house_range_match_uses_numbers_from_matching_street_segment():
    office = _office(city="Богашево", address="улица Киевская, 24")
    event = _event(
        city="Богашево",
        street="улица Киевская, улица Ключевская",
        building=None,
        normalized="Богашево, улица Киевская 22-26, улица Ключевская 2",
    )

    matches = OfficeMatcher([office]).match(event, now=NOW)

    assert len(matches) == 1
    assert matches[0].match_strategy == "house_range"
    assert matches[0].match_score == 0.92
    assert any("covered_by" in item for item in matches[0].explanation)


def test_house_data_blocks_office_on_same_street_when_number_is_not_covered():
    office = _office(city="Богашево", address="улица Киевская, 40")
    event = _event(
        city="Богашево",
        street="улица Киевская",
        building=None,
        normalized="Богашево, улица Киевская 22-26",
    )

    assert OfficeMatcher([office]).match(event, now=NOW) == []


def test_street_area_match_when_source_lists_whole_street_without_houses():
    office = _office(address="улица Гагарина, 8")
    event = _event(
        street="улица Гагарина",
        building=None,
        normalized="село Бичура, улица Гагарина",
    )

    matches = OfficeMatcher([office]).match(event, now=NOW)

    assert len(matches) == 1
    assert matches[0].match_strategy == "street_area"


def test_expired_event_is_ignored():
    office = _office()
    event = _event(end_time=NOW - timedelta(minutes=1))

    assert OfficeMatcher([office]).match(event, now=NOW) == []


def test_fuzzy_city_and_street_match_house_range():
    office = _office(city="Новосибирск", address="улица Станиславского, 24")
    event = _event(
        city="г. Новосибирск",
        street="ул Станиславскго",
        building=None,
        normalized="г. Новосибирск, ул Станиславскго 22-26",
    )

    matches = OfficeMatcher([office]).match(event, now=NOW)

    assert len(matches) == 1
    assert matches[0].match_strategy == "fuzzy_house_range"
    assert matches[0].match_score >= 0.84
    assert any("street_score" in item for item in matches[0].explanation)
