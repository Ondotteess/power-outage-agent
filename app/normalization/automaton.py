"""Token-FSA address normalizer with deterministic regex fallback.

The automaton turns a `ParsedRecordSchema` into a `NormalizedEventSchema`
without any external calls. It classifies each token of the city / street /
building fields as `LOCALITY_PREFIX | STREET_PREFIX | HOUSE_PREFIX | NUMBER |
RANGE | PROPER` and runs a small state machine per field:

    city   :  [LOCALITY_PREFIX]? PROPER+
    street :  [STREET_PREFIX]?  PROPER+ ((HOUSE_PREFIX | NUMBER | RANGE)+)?
    house  :  [HOUSE_PREFIX]?   (NUMBER | RANGE)+

Each field parse emits a confidence in [0, 1]. The overall record confidence
is the minimum across required slots (city + street). If that score falls
below a threshold — or the FSA can't recover a street at all — the
`FallbackNormalizer` escalates to the regex normalizer.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from enum import Enum, auto
from typing import Protocol
from uuid import uuid4

from app.models.schemas import (
    EventType,
    LocationSchema,
    NormalizedEventSchema,
    ParsedRecordSchema,
)
from app.normalization.address import (
    HOUSE_KEY_RE,
    HOUSE_RANGE_RE,
    HOUSE_WORDS,
    LOCALITY_WORDS,
    STREET_WORDS,
    canonical_key,
    normalize_building,
    normalize_city,
    normalize_street,
    normalize_text,
    split_address,
)

logger = logging.getLogger(__name__)

DEFAULT_FALLBACK_THRESHOLD = 0.6

_STREET_TYPE_RE = re.compile(
    r"\b(?P<type>"
    r"ул(?:ица)?|пр(?:оспект|-?кт|-?т)?|пер(?:еулок)?|проезд|пр-?д|"
    r"бульвар|б-?р|пл(?:ощадь)?|ш(?:оссе)?|тракт|мкр|микрорайон|"
    r"наб(?:ережная)?|аллея|тупик"
    r")\.?\s+(?P<name>[^,;:\n]+)",
    re.IGNORECASE,
)
_STREET_TYPE_TAIL_RE = re.compile(
    r"(?P<name>[^,;:\n]+?)\s+\b(?P<type>"
    r"улица|проспект|переулок|проезд|бульвар|площадь|шоссе|тракт|"
    r"микрорайон|набережная|аллея|тупик"
    r")\b",
    re.IGNORECASE,
)
_HOUSE_WITH_PREFIX_RE = re.compile(
    r"\b(?:д(?:ом)?|зд(?:ание)?|строение|стр|корп(?:ус)?|к|влад(?:ение)?|"
    r"лит(?:ера)?)\.?\s*(?P<house>\d{1,4}(?:/\d{1,4})?[a-zа-я]?"
    r"(?:\s*[-–—]\s*\d{1,4})?)\b",
    re.IGNORECASE,
)
_TRAILING_HOUSE_RE = re.compile(
    r"(?P<prefix>.*?\D)\s+(?P<house>\d{1,4}(?:/\d{1,4})?[a-zа-я]?"
    r"(?:\s*[-–—]\s*\d{1,4})?)\s*$",
    re.IGNORECASE,
)
_ALPHA_RE = re.compile(r"[a-zа-яё]", re.IGNORECASE)
_ROAD_DISTANCE_RE = re.compile(
    r"\b(?:км|а/?д|автодор|кадастр|к\.?\s*н|нас(?:еленный)?\s*пункт|"
    r"разъезд|свертк|тер)\b",
    re.IGNORECASE,
)
_CADASTRAL_RE = re.compile(r"\b\d{2}:\d{2}:\d{3,}", re.IGNORECASE)
_LOCALITY_RE = re.compile(
    r"\b(?:г(?:ород)?|с(?:ело)?|п(?:ос(?:елок|ёлок)?)?|пос(?:елок|ёлок)?|"
    r"пгт|рп|аал|аул|деревня)\.?\s+(?P<name>[^,;:\n]+)",
    re.IGNORECASE,
)
_CITY_PREFIX_RE = re.compile(
    r"^(?:г(?:ород)?|с(?:ело)?|д(?:еревня)?|п(?:ос(?:елок|ёлок)?)?|"
    r"пос(?:елок|ёлок)?|пгт|рп|аал|аул)\.?\s+",
    re.IGNORECASE,
)
_ADMIN_MARKERS = (
    "область",
    "обл",
    "край",
    "республика",
    "респ",
    "район",
    "р-н",
    "ао",
)


class TokenType(Enum):
    LOCALITY_PREFIX = auto()
    STREET_PREFIX = auto()
    HOUSE_PREFIX = auto()
    NUMBER = auto()
    RANGE = auto()
    PROPER = auto()


@dataclass(frozen=True)
class _Token:
    text: str
    type: TokenType


@dataclass(frozen=True)
class _FieldParse:
    value: str | None  # cleaned token sequence (or None when unparsable)
    extra: str | None  # spillover (e.g. building captured inside street field)
    confidence: float


@dataclass(frozen=True)
class AutomatonResult:
    event: NormalizedEventSchema | None
    confidence: float


class NormalizerProtocol(Protocol):
    async def normalize(self, record: ParsedRecordSchema) -> NormalizedEventSchema | None: ...


class AutomatonNormalizer:
    """Deterministic ParsedRecord → NormalizedEvent translator.

    Runs without network calls. `parse()` exposes confidence so callers can
    decide whether to escalate to a deterministic fallback; `normalize()` keeps the
    NormalizerProtocol contract for direct use.
    """

    async def normalize(self, record: ParsedRecordSchema) -> NormalizedEventSchema | None:
        return self.parse(record).event

    def parse(self, record: ParsedRecordSchema) -> AutomatonResult:
        if record.start_time is None:
            return AutomatonResult(event=None, confidence=0.0)

        city_parse = _parse_city(record.location_city)
        street_parse = _parse_street(record.location_street)

        if street_parse.value is None:
            # No street → no event the matcher can use. Escalate.
            return AutomatonResult(event=None, confidence=0.0)

        houses_value = None
        if isinstance(record.extra, dict):
            houses_value = record.extra.get("houses")
        building_source = str(houses_value) if houses_value else street_parse.extra
        building_parse = _parse_building(building_source)

        slot_confs = [city_parse.confidence, street_parse.confidence]
        if building_source:
            slot_confs.append(building_parse.confidence)
        confidence = min(slot_confs) if slot_confs else 0.0
        if city_parse.value is None:
            # Missing city is recoverable (matcher can still try) but trust drops.
            confidence *= 0.5

        # Display values: keep the user-visible originals so the UI still shows
        # "Томск" / "улица Кирова", not the lowercase token soup.
        city = (record.location_city or "").strip() or None
        street = (record.location_street or "").strip() or None
        building = building_parse.value

        raw_location = _raw_location(record)
        event = NormalizedEventSchema(
            event_id=uuid4(),
            parsed_record_id=record.id,
            event_type=EventType.POWER_OUTAGE,
            start_time=record.start_time,
            end_time=record.end_time,
            location=LocationSchema(
                raw=raw_location,
                normalized=canonical_key(city, street, building),
                city=city,
                street=street,
                building=building,
            ),
            reason=record.reason,
            sources=[record.raw_record_id],
            confidence=confidence,
        )
        return AutomatonResult(event=event, confidence=confidence)


class RegexNormalizer:
    """Regex-heavy fallback for messy parser output.

    This is intentionally still deterministic: no network, no model, no token
    budget. It is more permissive than the Token-FSA and tries to recover
    address slots from glued fields such as
    "Красноярский край, Краснотуранский р-н, с Лебяжье, ул Ленина, д 13".
    """

    async def normalize(self, record: ParsedRecordSchema) -> NormalizedEventSchema | None:
        return self.parse(record).event

    def parse(self, record: ParsedRecordSchema) -> AutomatonResult:
        if record.start_time is None:
            return AutomatonResult(event=None, confidence=0.0)

        raw_location = _raw_location(record)
        city, city_confidence = _regex_city(record, raw_location)
        street, street_confidence = _regex_street(record, raw_location)
        if street is None:
            return AutomatonResult(event=None, confidence=0.0)

        building, building_confidence = _regex_building(record, raw_location)
        confidence = min(
            0.95,
            max(0.0, street_confidence + city_confidence + building_confidence),
        )
        if city is None:
            confidence *= 0.75

        event = NormalizedEventSchema(
            event_id=uuid4(),
            parsed_record_id=record.id,
            event_type=EventType.POWER_OUTAGE,
            start_time=record.start_time,
            end_time=record.end_time,
            location=LocationSchema(
                raw=raw_location,
                normalized=canonical_key(city, street, building),
                city=city,
                street=street,
                building=building,
            ),
            reason=record.reason,
            sources=[record.raw_record_id],
            confidence=round(confidence, 3),
        )
        return AutomatonResult(event=event, confidence=event.confidence)


class FallbackNormalizer:
    """Two-stage normalizer: automaton first, regex on low-confidence cases.

    The threshold is the confidence below which the automaton's output is
    considered unsafe to trust on its own; callers pass it explicitly so the
    pipeline can wire it from settings.

    After each `normalize()` call, `last_path` reflects which stage produced
    the result. Safe to read from the same coroutine that awaited normalize()
    because the dispatcher processes tasks sequentially.
    """

    PATH_AUTOMATON = "automaton"
    PATH_REGEX_FALLBACK = "regex_fallback"
    PATH_LLM_FALLBACK = PATH_REGEX_FALLBACK  # backward-compatible constant alias
    PATH_NONE = "none"

    def __init__(
        self,
        primary: AutomatonNormalizer,
        fallback: NormalizerProtocol,
        threshold: float = DEFAULT_FALLBACK_THRESHOLD,
    ) -> None:
        self._primary = primary
        self._fallback = fallback
        self._threshold = threshold
        self.last_path: str | None = None

    async def normalize(self, record: ParsedRecordSchema) -> NormalizedEventSchema | None:
        result = self._primary.parse(record)
        if result.event is not None and result.confidence >= self._threshold:
            logger.debug(
                "FallbackNormalizer  automaton hit  parsed_id=%s  conf=%.2f",
                record.id,
                result.confidence,
            )
            self.last_path = self.PATH_AUTOMATON
            return result.event

        logger.info(
            "FallbackNormalizer  escalating to fallback  parsed_id=%s  conf=%.2f",
            record.id,
            result.confidence,
        )
        fallback_event = await self._fallback.normalize(record)
        if fallback_event is not None:
            if result.event is not None and _address_slot_count(fallback_event) < _address_slot_count(result.event):
                self.last_path = self.PATH_AUTOMATON
                return result.event
            self.last_path = self.PATH_REGEX_FALLBACK
            return fallback_event

        # Fallback also failed — return the automaton's best effort if any.
        if result.event is not None:
            self.last_path = self.PATH_AUTOMATON
        else:
            self.last_path = self.PATH_NONE
        return result.event


def _tokenize(value: str) -> list[_Token]:
    return [_Token(text=raw, type=_classify(raw)) for raw in normalize_text(value).split()]


def _address_slot_count(event: NormalizedEventSchema) -> int:
    location = event.location
    return int(bool(location.city)) + int(bool(location.street)) + int(bool(location.building))


def _classify(token: str) -> TokenType:
    if HOUSE_RANGE_RE.fullmatch(token):
        return TokenType.RANGE
    if HOUSE_KEY_RE.fullmatch(token) and token[0].isdigit():
        return TokenType.NUMBER
    if token in LOCALITY_WORDS:
        return TokenType.LOCALITY_PREFIX
    if token in STREET_WORDS:
        return TokenType.STREET_PREFIX
    if token in HOUSE_WORDS:
        return TokenType.HOUSE_PREFIX
    return TokenType.PROPER


def _parse_city(value: str | None) -> _FieldParse:
    """FSA: [LOCALITY_PREFIX]? PROPER+"""
    if not value:
        return _FieldParse(value=None, extra=None, confidence=0.0)

    tokens = _tokenize(value)
    if not tokens:
        return _FieldParse(value=None, extra=None, confidence=0.0)

    parts: list[str] = []
    saw_prefix = False
    unexpected = 0
    for tok in tokens:
        if tok.type == TokenType.LOCALITY_PREFIX and not parts:
            saw_prefix = True
        elif tok.type == TokenType.PROPER:
            parts.append(tok.text)
        else:
            unexpected += 1

    if not parts:
        return _FieldParse(value=None, extra=None, confidence=0.0)

    confidence = 1.0 - 0.2 * unexpected
    # 3+ tokens without a locality prefix is suspicious ("Кемеровская область
    # Кузбасс Кемерово" — likely the parser glued region into city).
    if not saw_prefix and len(parts) > 2:
        confidence -= 0.1
    confidence = max(0.0, min(1.0, confidence))
    return _FieldParse(value=" ".join(parts), extra=None, confidence=confidence)


def _parse_street(value: str | None) -> _FieldParse:
    """FSA: [STREET_PREFIX]? PROPER+ ((HOUSE_PREFIX | NUMBER | RANGE)+)?

    A trailing house number is exposed as `extra` so the caller can hand it
    to the building parser without re-tokenizing.
    """
    if not value:
        return _FieldParse(value=None, extra=None, confidence=0.0)

    tokens = _tokenize(value)
    if not tokens:
        return _FieldParse(value=None, extra=None, confidence=0.0)

    parts: list[str] = []
    house_parts: list[str] = []
    unexpected = 0
    state = "start"  # start → name → house

    for tok in tokens:
        if tok.type == TokenType.STREET_PREFIX:
            if state == "start":
                state = "name"
            elif state == "name" and not parts:
                # "ул Набережная": the second street-word is the name, not
                # another prefix.
                parts.append(tok.text)
            elif state in ("name", "house"):
                # Embedded second street type — rare, treat as separator hint.
                state = "name" if state == "house" else state
            else:
                unexpected += 1
        elif tok.type == TokenType.PROPER:
            if state in ("start", "name"):
                parts.append(tok.text)
                state = "name"
            else:
                unexpected += 1
        elif tok.type in (TokenType.NUMBER, TokenType.RANGE):
            if state in ("name", "house") and parts:
                house_parts.append(tok.text)
                state = "house"
            else:
                unexpected += 1
        elif tok.type == TokenType.HOUSE_PREFIX:
            if state in ("name", "house") and parts:
                state = "house"
            else:
                unexpected += 1

    if not parts:
        return _FieldParse(value=None, extra=None, confidence=0.0)

    confidence = 1.0 - 0.15 * unexpected
    confidence = max(0.0, min(1.0, confidence))
    return _FieldParse(
        value=" ".join(parts),
        extra=" ".join(house_parts) if house_parts else None,
        confidence=confidence,
    )


def _parse_building(value: str | None) -> _FieldParse:
    """FSA: [HOUSE_PREFIX]? ((NUMBER | RANGE) HOUSE_PREFIX?)+

    Also recognises "no number" markers (`б/н`, `без номера`) — those return
    a None value but with full confidence (the automaton handled them).
    """
    if value is None:
        return _FieldParse(value=None, extra=None, confidence=1.0)

    stripped = value.strip()
    if not stripped:
        return _FieldParse(value=None, extra=None, confidence=1.0)
    if stripped.casefold() in {"б/н", "бн", "без номера", "без номер", "нет номера"}:
        return _FieldParse(value=None, extra=None, confidence=1.0)

    tokens = _tokenize(stripped)
    if not tokens:
        return _FieldParse(value=None, extra=None, confidence=0.0)

    parts: list[str] = []
    unexpected = 0
    for tok in tokens:
        if tok.type in (TokenType.NUMBER, TokenType.RANGE):
            parts.append(tok.text)
        elif tok.type == TokenType.HOUSE_PREFIX:
            pass  # prefix is informational only
        else:
            unexpected += 1

    if not parts:
        return _FieldParse(value=None, extra=None, confidence=0.0)

    confidence = 1.0 - 0.2 * unexpected
    confidence = max(0.0, min(1.0, confidence))
    return _FieldParse(value=" ".join(parts), extra=None, confidence=confidence)


def _regex_city(record: ParsedRecordSchema, raw_location: str) -> tuple[str | None, float]:
    for candidate in (record.location_city, record.location_district):
        city = _clean_city_candidate(candidate)
        if city and _has_alpha_signal(city) and normalize_city(city):
            return city, 0.08

    for candidate in _candidate_parts_before_street(raw_location):
        city = _clean_city_candidate(candidate)
        if city and _has_alpha_signal(city) and normalize_city(city):
            return city, 0.05

    matches = list(_LOCALITY_RE.finditer(raw_location))
    for match in reversed(matches):
        city = _clean_city_candidate(match.group("name"))
        if city and _has_alpha_signal(city) and normalize_city(city):
            return city, 0.04

    return None, 0.0


def _regex_street(record: ParsedRecordSchema, raw_location: str) -> tuple[str | None, float]:
    extra_address = _extra_address(record)
    typed_candidates = [record.location_street, extra_address, raw_location]
    for candidate in typed_candidates:
        street = _street_from_typed_regex(candidate)
        if street and normalize_street(street):
            return street, 0.78

    # Untyped recovery is deliberately narrower: using the full raw location
    # here turns city-only rows ("03, заимка Саганур") into fake streets.
    for candidate in (record.location_street, extra_address):
        street = _street_from_untyped_text(candidate)
        if street and normalize_street(street):
            return street, 0.64

    return None, 0.0


def _regex_building(record: ParsedRecordSchema, raw_location: str) -> tuple[str | None, float]:
    houses_value = record.extra.get("houses") if isinstance(record.extra, dict) else None
    extra_address = _extra_address(record)
    for candidate in (houses_value,):
        building = _building_from_text(str(candidate) if candidate else None)
        if building:
            return building, 0.09
    for candidate in (record.location_street, extra_address, raw_location):
        building = _building_after_typed_street(candidate)
        if building:
            return building, 0.09
    for candidate in (record.location_street, extra_address):
        building = _building_from_text(str(candidate) if candidate else None)
        if building:
            return building, 0.09
    return None, 0.0


def _clean_city_candidate(value: str | None) -> str | None:
    if not value:
        return None
    if (
        _CADASTRAL_RE.search(value)
        or _ROAD_DISTANCE_RE.search(value)
        or _ROAD_DISTANCE_RE.search(normalize_text(value))
    ):
        return None

    parts = [p.strip() for p in re.split(r"[,;]", value) if p.strip()]
    if not parts:
        return None

    for part in reversed(parts):
        normalized = normalize_text(part)
        if not normalized:
            continue
        if any(marker in normalized.split() for marker in _ADMIN_MARKERS):
            continue
        match = _LOCALITY_RE.search(part)
        city = match.group("name") if match else part
        city = _CITY_PREFIX_RE.sub("", city)
        city = _strip_street_and_house_tail(city).strip(" .,;:-")
        if city and _has_alpha_signal(city) and normalize_city(city):
            return city
    return None


def _candidate_parts_before_street(value: str) -> list[str]:
    if not value:
        return []
    match = _STREET_TYPE_RE.search(value)
    prefix = value[: match.start()] if match else value
    return [p.strip() for p in re.split(r"[,;]", prefix) if p.strip()]


def _street_from_typed_regex(value: str | None) -> str | None:
    if not value:
        return None

    for pattern in (_STREET_TYPE_RE, _STREET_TYPE_TAIL_RE):
        match = pattern.search(value)
        if not match:
            continue
        name = _strip_street_and_house_tail(match.group("name")).strip(" .,;:-")
        street_type = match.group("type").strip(" .")
        street = f"{street_type} {name}" if pattern is _STREET_TYPE_RE else f"{name} {street_type}"
        if _has_street_name_signal(street):
            return street
    return None


def _street_from_untyped_text(value: str | None) -> str | None:
    if not value:
        return None

    if _ROAD_DISTANCE_RE.search(normalize_text(value)):
        return None

    street, building = split_address(value)
    if building and _has_street_name_signal(street):
        return street.strip(" .,;:-")

    parts = [p.strip() for p in re.split(r"[,;]", value) if p.strip()]
    for part in reversed(parts):
        normalized = normalize_text(part)
        if not normalized:
            continue
        if normalize_building(normalized) == normalize_text(normalized):
            continue
        if any(marker in normalized.split() for marker in (*LOCALITY_WORDS, *_ADMIN_MARKERS)):
            continue
        candidate = _strip_street_and_house_tail(part).strip(" .,;:-")
        if candidate and _has_street_name_signal(candidate):
            return candidate
    return None


def _building_from_text(value: str | None) -> str | None:
    if not value:
        return None
    if _CADASTRAL_RE.search(value):
        return None

    parsed = _parse_building(value)
    if parsed.value:
        return parsed.value

    prefix_match = _HOUSE_WITH_PREFIX_RE.search(value)
    if prefix_match:
        parsed = _parse_building(prefix_match.group("house"))
        if parsed.value:
            return parsed.value

    trailing_match = _TRAILING_HOUSE_RE.search(value)
    if trailing_match:
        parsed = _parse_building(trailing_match.group("house"))
        if parsed.value:
            return parsed.value

    return None


def _building_after_typed_street(value: str | None) -> str | None:
    if not value:
        return None

    match = _STREET_TYPE_RE.search(value)
    if match:
        parsed = _parse_building(match.group("name"))
        if parsed.value:
            return parsed.value
        trailing_match = _TRAILING_HOUSE_RE.search(match.group("name"))
        if trailing_match:
            parsed = _parse_building(trailing_match.group("house"))
            if parsed.value:
                return parsed.value

    prefix_match = _HOUSE_WITH_PREFIX_RE.search(value)
    if prefix_match:
        parsed = _parse_building(prefix_match.group("house"))
        if parsed.value:
            return parsed.value
    return None


def _strip_street_and_house_tail(value: str) -> str:
    prefix_match = _HOUSE_WITH_PREFIX_RE.search(value)
    if prefix_match:
        value = value[: prefix_match.start()]
    trailing_match = _TRAILING_HOUSE_RE.match(value)
    if trailing_match:
        value = trailing_match.group("prefix")
    return value


def _has_alpha_signal(value: str | None, *, min_letters: int = 2) -> bool:
    if not value:
        return False
    return len(_ALPHA_RE.findall(value)) >= min_letters


def _has_street_name_signal(value: str | None) -> bool:
    normalized = normalize_text(value)
    street = normalize_street(value)
    if not street or not _has_alpha_signal(street, min_letters=3):
        return False
    if normalized in STREET_WORDS:
        return False
    if any(marker in normalized.split() for marker in _ADMIN_MARKERS):
        return False
    # Kilometre/cadastral/road-distance descriptions are outage locations, but
    # not office-like street addresses. Let the automaton keep its best effort
    # instead of replacing it with a fake "км|1" key.
    return _ROAD_DISTANCE_RE.search(normalized) is None


def _raw_location(record: ParsedRecordSchema) -> str:
    parts = [
        record.location_region_code,
        record.location_district,
        record.location_city,
        record.location_street,
    ]
    houses = record.extra.get("houses") if isinstance(record.extra, dict) else None
    if houses:
        parts.append(str(houses))
    extra_address = _extra_address(record)
    if extra_address:
        parts.append(extra_address)
    return ", ".join(p.strip() for p in parts if p and p.strip())


def _extra_address(record: ParsedRecordSchema) -> str | None:
    if not isinstance(record.extra, dict):
        return None
    for key in ("address", "addr", "location", "raw_location"):
        value = record.extra.get(key)
        if value:
            return str(value)
    return None
