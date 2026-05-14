"""Token-FSA address normalizer with optional LLM fallback.

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
`FallbackNormalizer` escalates to the LLM normalizer.
"""

from __future__ import annotations

import logging
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
    normalize_text,
)

logger = logging.getLogger(__name__)

DEFAULT_FALLBACK_THRESHOLD = 0.6


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
    decide whether to escalate to an LLM; `normalize()` keeps the
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


class FallbackNormalizer:
    """Two-stage normalizer: automaton first, LLM only on low-confidence cases.

    The threshold is the confidence below which the automaton's output is
    considered unsafe to trust on its own; callers pass it explicitly so the
    pipeline can wire it from settings.
    """

    def __init__(
        self,
        primary: AutomatonNormalizer,
        fallback: NormalizerProtocol,
        threshold: float = DEFAULT_FALLBACK_THRESHOLD,
    ) -> None:
        self._primary = primary
        self._fallback = fallback
        self._threshold = threshold

    async def normalize(self, record: ParsedRecordSchema) -> NormalizedEventSchema | None:
        result = self._primary.parse(record)
        if result.event is not None and result.confidence >= self._threshold:
            logger.debug(
                "FallbackNormalizer  automaton hit  parsed_id=%s  conf=%.2f",
                record.id,
                result.confidence,
            )
            return result.event

        logger.info(
            "FallbackNormalizer  escalating to fallback  parsed_id=%s  conf=%.2f",
            record.id,
            result.confidence,
        )
        fallback_event = await self._fallback.normalize(record)
        if fallback_event is not None:
            return fallback_event

        # Fallback also failed — return the automaton's best effort if there
        # was any, otherwise None.
        return result.event


def _tokenize(value: str) -> list[_Token]:
    return [_Token(text=raw, type=_classify(raw)) for raw in normalize_text(value).split()]


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
    return ", ".join(p.strip() for p in parts if p and p.strip())
