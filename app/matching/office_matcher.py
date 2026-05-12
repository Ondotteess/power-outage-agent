from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import UUID

from app.models.schemas import ImpactLevel

_TEXT_CLEAN_RE = re.compile(r"[^0-9a-zа-яё/\-–—]+", re.IGNORECASE)
_HOUSE_RE = re.compile(r"(?<!\d)(\d{1,4})(?:\s*[-–—]\s*(\d{1,4}))?", re.IGNORECASE)
_HOUSE_KEY_RE = re.compile(r"(?<!\d)(\d{1,4}(?:/\d{1,4})?[a-zа-я]?)", re.IGNORECASE)
_HOUSE_RANGE_RE = re.compile(r"\d{1,4}\s*[-–—]\s*\d{1,4}")

_REPLACEMENTS = (
    (re.compile(r"\bул\.?\b", re.IGNORECASE), " улица "),
    (re.compile(r"\bпр-?кт\.?\b", re.IGNORECASE), " проспект "),
    (re.compile(r"\bпр\.?\b", re.IGNORECASE), " проспект "),
    (re.compile(r"\bпер\.?\b", re.IGNORECASE), " переулок "),
    (re.compile(r"\bб-р\.?\b", re.IGNORECASE), " бульвар "),
    (re.compile(r"\bмкр\.?\b", re.IGNORECASE), " микрорайон "),
    (re.compile(r"\bд\.?\b", re.IGNORECASE), " дом "),
    (re.compile(r"\bк\.?\b", re.IGNORECASE), " корпус "),
    (re.compile(r"\bстр\.?\b", re.IGNORECASE), " строение "),
)

_LOCALITY_WORDS = {
    "город",
    "г",
    "село",
    "с",
    "деревня",
    "д",
    "поселок",
    "пос",
    "пгт",
    "аул",
}
_STREET_WORDS = {
    "улица",
    "проспект",
    "переулок",
    "проезд",
    "бульвар",
    "площадь",
    "шоссе",
    "тракт",
    "микрорайон",
    "набережная",
    "аллея",
}
_HOUSE_WORDS = {"дом", "корпус", "строение", "владение", "литера", "офис"}
_NO_BUILDING = {"", "бн", "б/н", "без номера", "без номер", "нет номера"}


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
    city_key = _normalize_city(office.city)
    street, building = _split_office_address(office.address)
    street_key = _normalize_street(street)
    building_key = _normalize_building(building)
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
    city_key = _normalize_city(event.location_city)
    building_key = _normalize_building(event.location_building)
    street_house_numbers: dict[str, set[int]] = {}
    street_has_house_data: set[str] = set()
    street_keys: list[str] = []

    values = [event.location_street, event.location_normalized, event.location_raw]
    for segment in _segments(values):
        street_key = _normalize_street(segment)
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


def _split_office_address(address: str) -> tuple[str, str | None]:
    parts = [p.strip() for p in re.split(r"[,;]", address) if p.strip()]
    if len(parts) > 1 and _normalize_building(parts[-1]):
        return ", ".join(parts[:-1]), parts[-1]

    matches = list(_HOUSE_KEY_RE.finditer(address))
    if not matches:
        return address, None

    last = matches[-1]
    street = address[: last.start()].strip(" ,;")
    return street or address, last.group(1)


def _normalize_text(value: str | None) -> str:
    if not value:
        return ""
    text = value.casefold().replace("ё", "е").replace("№", " ")
    for pattern, replacement in _REPLACEMENTS:
        text = pattern.sub(replacement, text)
    text = _TEXT_CLEAN_RE.sub(" ", text)
    return re.sub(r"\s+", " ", text).strip()


def _normalize_city(value: str | None) -> str | None:
    tokens = [t for t in _normalize_text(value).split() if t not in _LOCALITY_WORDS]
    return " ".join(tokens) or None


def _normalize_street(value: str | None) -> str:
    tokens = [
        t
        for t in _normalize_text(value).split()
        if t not in _STREET_WORDS and t not in _HOUSE_WORDS and t not in _LOCALITY_WORDS
    ]
    while len(tokens) > 1 and _is_house_token(tokens[-1]):
        tokens.pop()
    return " ".join(tokens)


def _normalize_building(value: str | None) -> str | None:
    normalized = _normalize_text(value)
    if normalized in _NO_BUILDING:
        return None
    match = _HOUSE_KEY_RE.search(normalized)
    return match.group(1) if match else None


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
    normalized = _normalize_text(value)
    tokens = normalized.split()
    if _HOUSE_RANGE_RE.search(normalized):
        return True
    if any(token in _HOUSE_WORDS for token in tokens):
        return True
    return bool(tokens and _is_house_token(tokens[-1]) and any(t in _STREET_WORDS for t in tokens))


def _house_numbers(value: str | None) -> set[int]:
    numbers: set[int] = set()
    normalized = _normalize_text(value)
    for match in _HOUSE_RE.finditer(normalized):
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


def _is_house_token(value: str) -> bool:
    return bool(_HOUSE_KEY_RE.fullmatch(value) or _HOUSE_RANGE_RE.fullmatch(value))


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
