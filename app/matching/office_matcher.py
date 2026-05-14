from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import UUID

from app.models.schemas import ImpactLevel
from app.normalization.address import (
    HOUSE_RANGE_RE,
    HOUSE_RE,
    HOUSE_WORDS,
    STREET_WORDS,
    is_house_token,
    normalize_building,
    normalize_city,
    normalize_street,
    normalize_text,
    split_address,
)


@dataclass(frozen=True)
class MatchableOffice:
    id: UUID
    name: str
    city: str
    address: str
    region: str


@dataclass(frozen=True)
class MatchableEvent:
    event_id: UUID
    event_type: str
    start_time: datetime
    end_time: datetime | None
    location_raw: str
    location_normalized: str | None
    location_city: str | None
    location_street: str | None
    location_building: str | None


@dataclass(frozen=True)
class OfficeMatch:
    office: MatchableOffice
    impact_level: ImpactLevel
    match_strategy: str
    match_score: float


@dataclass(frozen=True)
class _OfficeProfile:
    office: MatchableOffice
    city_key: str
    street_key: str
    building_key: str | None
    house_number: int | None


@dataclass(frozen=True)
class _EventProfile:
    city_key: str | None
    street_keys: tuple[str, ...]
    building_key: str | None
    street_house_numbers: dict[str, set[int]]
    street_has_house_data: set[str]


class OfficeMatcher:
    """Address matcher optimized for the current MVP office registry shape.

    The matcher builds O(office_count) exact/street indexes once and then resolves
    each event by key lookups. It deliberately prefers precision over recall:
    exact building and house-range matches are strong, street-wide matches are
    allowed only when the outage text has no contradicting house coverage.
    """

    def __init__(self, offices: list[MatchableOffice]) -> None:
        self._by_exact: dict[tuple[str, str, str], list[_OfficeProfile]] = {}
        self._by_street: dict[tuple[str, str], list[_OfficeProfile]] = {}
        self._by_street_only: dict[str, list[_OfficeProfile]] = {}

        for office in offices:
            profile = _office_profile(office)
            if profile is None:
                continue
            self._by_street.setdefault((profile.city_key, profile.street_key), []).append(profile)
            self._by_street_only.setdefault(profile.street_key, []).append(profile)
            if profile.building_key:
                self._by_exact.setdefault(
                    (profile.city_key, profile.street_key, profile.building_key), []
                ).append(profile)

    def match(self, event: MatchableEvent, *, now: datetime | None = None) -> list[OfficeMatch]:
        now = now or datetime.now(UTC)
        if _is_expired(event, now) or event.event_type == "other":
            return []

        profile = _event_profile(event)
        if not profile.street_keys:
            return []

        matched: dict[UUID, OfficeMatch] = {}
        impact_level = _impact_level(event, now)

        for street_key in profile.street_keys:
            candidates = self._candidates(profile.city_key, street_key)
            if not candidates:
                continue

            for candidate in candidates:
                match = _score_candidate(candidate, street_key, profile, impact_level)
                if match is None:
                    continue
                existing = matched.get(candidate.office.id)
                if existing is None or match.match_score > existing.match_score:
                    matched[candidate.office.id] = match

        return sorted(matched.values(), key=lambda m: (-m.match_score, m.office.name))

    def _candidates(self, city_key: str | None, street_key: str) -> list[_OfficeProfile]:
        if city_key:
            return self._by_street.get((city_key, street_key), [])
        offices = self._by_street_only.get(street_key, [])
        return offices if len({p.city_key for p in offices}) == 1 else []


def _score_candidate(
    candidate: _OfficeProfile,
    street_key: str,
    event: _EventProfile,
    impact_level: ImpactLevel,
) -> OfficeMatch | None:
    if event.building_key and candidate.building_key == event.building_key:
        return OfficeMatch(candidate.office, impact_level, "exact_address", 1.0)

    house_numbers = event.street_house_numbers.get(street_key, set())
    has_house_data = street_key in event.street_has_house_data
    if candidate.house_number is not None and candidate.house_number in house_numbers:
        return OfficeMatch(candidate.office, impact_level, "house_range", 0.92)

    if has_house_data:
        return None

    return OfficeMatch(candidate.office, impact_level, "street_area", 0.68)


def _office_profile(office: MatchableOffice) -> _OfficeProfile | None:
    city_key = normalize_city(office.city)
    street, building = split_address(office.address)
    street_key = normalize_street(street)
    building_key = normalize_building(building)
    if not city_key or not street_key:
        return None
    return _OfficeProfile(
        office=office,
        city_key=city_key,
        street_key=street_key,
        building_key=building_key,
        house_number=_first_house_number(building_key),
    )


def _event_profile(event: MatchableEvent) -> _EventProfile:
    city_key = normalize_city(event.location_city)
    building_key = normalize_building(event.location_building)
    street_house_numbers: dict[str, set[int]] = {}
    street_has_house_data: set[str] = set()
    street_keys: list[str] = []

    # NOTE: `location_normalized` is now a canonical pipe-delimited key
    # (see app.normalization.address.canonical_key), not free text. The
    # matcher draws richer multi-street/house-range signal from
    # `location_raw` and `location_street` instead.
    values = [event.location_street, event.location_raw]
    for segment in _segments(values):
        street_key = normalize_street(segment)
        if not street_key or street_key == city_key:
            continue
        if street_key not in street_keys:
            street_keys.append(street_key)

        if _has_house_data(segment):
            street_has_house_data.add(street_key)
            street_house_numbers.setdefault(street_key, set()).update(_house_numbers(segment))

    if building_key and street_keys:
        street_has_house_data.add(street_keys[0])
        street_house_numbers.setdefault(street_keys[0], set()).update(_house_numbers(building_key))

    return _EventProfile(
        city_key=city_key,
        street_keys=tuple(street_keys),
        building_key=building_key,
        street_house_numbers=street_house_numbers,
        street_has_house_data=street_has_house_data,
    )


def _segments(values: list[str | None]) -> list[str]:
    segments: list[str] = []
    for value in values:
        if not value:
            continue
        for segment in re.split(r"[,;]", value):
            segment = segment.strip()
            if segment:
                segments.append(segment)
    return segments


def _has_house_data(value: str) -> bool:
    normalized = normalize_text(value)
    tokens = normalized.split()
    if HOUSE_RANGE_RE.search(normalized):
        return True
    if any(token in HOUSE_WORDS for token in tokens):
        return True
    return bool(tokens and is_house_token(tokens[-1]) and any(t in STREET_WORDS for t in tokens))


def _house_numbers(value: str | None) -> set[int]:
    numbers: set[int] = set()
    normalized = normalize_text(value)
    for match in HOUSE_RE.finditer(normalized):
        start = int(match.group(1))
        end = int(match.group(2) or start)
        if end < start:
            start, end = end, start
        if end - start > 300:
            numbers.add(start)
            numbers.add(end)
            continue
        numbers.update(range(start, end + 1))
    return numbers


def _first_house_number(value: str | None) -> int | None:
    numbers = _house_numbers(value)
    return min(numbers) if numbers else None


def _is_expired(event: MatchableEvent, now: datetime) -> bool:
    return event.end_time is not None and event.end_time < now


def _impact_level(event: MatchableEvent, now: datetime) -> ImpactLevel:
    if event.start_time <= now and (event.end_time is None or event.end_time >= now):
        return ImpactLevel.HIGH
    lead_hours = (event.start_time - now).total_seconds() / 3600
    if lead_hours <= 24:
        return ImpactLevel.HIGH
    if lead_hours <= 72:
        return ImpactLevel.MEDIUM
    return ImpactLevel.LOW
