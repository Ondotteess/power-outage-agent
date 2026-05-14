from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, datetime
from difflib import SequenceMatcher
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
    explanation: tuple[str, ...] = ()


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
    street_house_coverage: dict[str, HouseCoverage]
    street_has_house_data: set[str]


@dataclass(frozen=True)
class HouseCoverage:
    ranges: tuple[tuple[int, int], ...] = ()

    @classmethod
    def parse(cls, value: str | None) -> HouseCoverage:
        ranges: list[tuple[int, int]] = []
        normalized = normalize_text(value)
        for match in HOUSE_RE.finditer(normalized):
            start = int(match.group(1))
            end = int(match.group(2) or start)
            if end < start:
                start, end = end, start
            ranges.append((start, end))
        return cls(tuple(ranges))

    def covers(self, house_number: int | None) -> bool:
        if house_number is None:
            return False
        return any(start <= house_number <= end for start, end in self.ranges)

    def first(self) -> int | None:
        return min((start for start, _end in self.ranges), default=None)

    @property
    def has_data(self) -> bool:
        return bool(self.ranges)


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
        self._profiles: list[_OfficeProfile] = []

        for office in offices:
            profile = _office_profile(office)
            if profile is None:
                continue
            self._profiles.append(profile)
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

        for candidate in self._profiles:
            match = _score_candidate(candidate, profile, impact_level)
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
    event: _EventProfile,
    impact_level: ImpactLevel,
) -> OfficeMatch | None:
    city_score = _city_score(event.city_key, candidate.city_key)
    if city_score < 0.80:
        return None

    best: OfficeMatch | None = None
    for street_key in event.street_keys:
        street_score = _similarity(street_key, candidate.street_key)
        if street_score < 0.84:
            continue

        exact_street = street_key == candidate.street_key
        exact_city = event.city_key is not None and event.city_key == candidate.city_key
        explanation = [
            f"city_score={city_score:.2f} ({event.city_key or 'unknown'} -> {candidate.city_key})",
            f"street_score={street_score:.2f} ({street_key} -> {candidate.street_key})",
        ]

        coverage = event.street_house_coverage.get(street_key, HouseCoverage())
        has_house_data = street_key in event.street_has_house_data
        if event.building_key and candidate.building_key == event.building_key and exact_street:
            score = 1.0 if exact_city else 0.96
            match = OfficeMatch(
                candidate.office,
                impact_level,
                "exact_address" if exact_city else "fuzzy_city_exact_address",
                score,
                tuple([*explanation, f"building={candidate.building_key} exact"]),
            )
        elif coverage.covers(candidate.house_number):
            score = 0.92 if exact_street and exact_city else 0.84 + 0.06 * street_score
            match = OfficeMatch(
                candidate.office,
                impact_level,
                "house_range" if exact_street else "fuzzy_house_range",
                round(min(score, 0.92), 3),
                tuple([*explanation, f"house={candidate.house_number} covered_by={coverage.ranges}"]),
            )
        elif has_house_data:
            continue
        else:
            score = 0.68 if exact_street and exact_city else 0.55 + 0.08 * street_score + 0.05 * city_score
            if score < 0.62:
                continue
            match = OfficeMatch(
                candidate.office,
                impact_level,
                "street_area" if exact_street else "fuzzy_street_area",
                round(min(score, 0.74), 3),
                tuple([*explanation, "no house data in event; street-wide impact"]),
            )

        if best is None or match.match_score > best.match_score:
            best = match

    return best


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
        house_number=HouseCoverage.parse(building_key).first(),
    )


def _event_profile(event: MatchableEvent) -> _EventProfile:
    city_key = normalize_city(event.location_city)
    building_key = normalize_building(event.location_building)
    street_house_coverage: dict[str, HouseCoverage] = {}
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
            street_house_coverage[street_key] = _merge_coverage(
                street_house_coverage.get(street_key),
                HouseCoverage.parse(segment),
            )

    if building_key and street_keys:
        street_has_house_data.add(street_keys[0])
        street_house_coverage[street_keys[0]] = _merge_coverage(
            street_house_coverage.get(street_keys[0]),
            HouseCoverage.parse(building_key),
        )

    return _EventProfile(
        city_key=city_key,
        street_keys=tuple(street_keys),
        building_key=building_key,
        street_house_coverage=street_house_coverage,
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


def _merge_coverage(left: HouseCoverage | None, right: HouseCoverage) -> HouseCoverage:
    if left is None:
        return right
    return HouseCoverage(tuple([*left.ranges, *right.ranges]))


def _city_score(event_city: str | None, office_city: str) -> float:
    if event_city is None:
        return 0.88
    return _similarity(event_city, office_city)


def _similarity(left: str | None, right: str | None) -> float:
    if not left or not right:
        return 0.0
    if left == right:
        return 1.0
    if left in right or right in left:
        return 0.93
    return SequenceMatcher(a=left, b=right).ratio()


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
