from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from uuid import UUID, uuid4

from app.models.schemas import (
    EventType,
    LocationSchema,
    NormalizedEventSchema,
    ParsedRecordSchema,
)
from app.normalization.automaton import (
    AutomatonNormalizer,
    FallbackNormalizer,
    _parse_building,
    _parse_city,
    _parse_street,
)


def _parsed(**overrides) -> ParsedRecordSchema:
    base = {
        "id": uuid4(),
        "raw_record_id": uuid4(),
        "source_id": uuid4(),
        "external_id": "ext-1",
        "start_time": datetime(2026, 5, 12, 3, 0, tzinfo=UTC),
        "end_time": datetime(2026, 5, 12, 9, 0, tzinfo=UTC),
        "location_city": "Томск",
        "location_district": None,
        "location_street": "улица Кирова",
        "location_region_code": "RU-TOM",
        "reason": "Ремонт",
        "extra": {"houses": "12"},
        "trace_id": uuid4(),
        "extracted_at": datetime(2026, 5, 12, 0, 0, tzinfo=UTC),
    }
    base.update(overrides)
    return ParsedRecordSchema(**base)


# ---------------------------------------------------------------------------
# Field-level FSAs
# ---------------------------------------------------------------------------


def test_parse_city_clean_input_yields_full_confidence():
    result = _parse_city("Томск")
    assert result.value == "томск"
    assert result.confidence == 1.0


def test_parse_city_with_locality_prefix_is_clean():
    result = _parse_city("г. Томск")
    assert result.value == "томск"
    assert result.confidence == 1.0


def test_parse_city_long_glued_input_loses_confidence():
    result = _parse_city("Томская область Томский район Томск")
    assert result.value is not None
    assert result.confidence < 1.0


def test_parse_street_extracts_house_into_extra():
    result = _parse_street("улица Кирова, 12")
    assert result.value == "кирова"
    assert result.extra == "12"
    assert result.confidence == 1.0


def test_parse_street_pure_name_no_extra():
    result = _parse_street("Красный проспект")
    assert result.value == "красный"
    assert result.extra is None
    assert result.confidence == 1.0


def test_parse_street_empty_returns_zero_confidence():
    result = _parse_street(None)
    assert result.value is None
    assert result.confidence == 0.0


def test_parse_building_recognises_no_number_marker():
    result = _parse_building("б/н")
    assert result.value is None
    assert result.confidence == 1.0


def test_parse_building_with_house_prefix():
    result = _parse_building("дом 12")
    assert result.value == "12"
    assert result.confidence == 1.0


def test_parse_building_range_kept_intact():
    result = _parse_building("22-26")
    assert result.value == "22-26"
    assert result.confidence == 1.0


# ---------------------------------------------------------------------------
# AutomatonNormalizer end-to-end
# ---------------------------------------------------------------------------


async def test_automaton_clean_record_high_confidence():
    normalizer = AutomatonNormalizer()
    result = normalizer.parse(_parsed())

    assert result.event is not None
    assert result.confidence == 1.0
    assert result.event.location.normalized == "томск|кирова|12"
    assert result.event.location.building == "12"


async def test_automaton_dirty_abbreviation_still_high_confidence():
    # All the dirty-demo variants must hit the automaton path.
    normalizer = AutomatonNormalizer()
    record = _parsed(
        location_city="г. Томск",
        location_street="пр. Ленина 120",
        extra={"houses": "120"},
    )
    result = normalizer.parse(record)

    assert result.event is not None
    assert result.confidence >= 0.6
    assert result.event.location.normalized == "томск|ленина|120"


async def test_automaton_extracts_building_from_street_when_extra_empty():
    normalizer = AutomatonNormalizer()
    record = _parsed(
        location_street="улица Кирова, 55",
        extra={},  # no houses field
    )
    result = normalizer.parse(record)

    assert result.event is not None
    assert result.event.location.building == "55"
    assert result.event.location.normalized == "томск|кирова|55"


async def test_automaton_drops_record_with_no_street():
    normalizer = AutomatonNormalizer()
    record = _parsed(location_street=None, extra={})
    result = normalizer.parse(record)

    assert result.event is None
    assert result.confidence == 0.0


async def test_automaton_drops_record_with_no_start_time():
    normalizer = AutomatonNormalizer()
    record = _parsed(start_time=None)
    result = normalizer.parse(record)

    assert result.event is None


async def test_automaton_low_confidence_when_city_glued_with_region():
    normalizer = AutomatonNormalizer()
    record = _parsed(location_city="Томская область Томский район Томск")
    result = normalizer.parse(record)

    assert result.event is not None
    # Below 1.0 — should still trip the fallback gate when threshold is 1.0.
    assert result.confidence < 1.0


# ---------------------------------------------------------------------------
# FallbackNormalizer wiring
# ---------------------------------------------------------------------------


@dataclass
class FakeLLMNormalizer:
    return_value: NormalizedEventSchema | None
    calls: list[ParsedRecordSchema] = field(default_factory=list)

    async def normalize(self, record: ParsedRecordSchema) -> NormalizedEventSchema | None:
        self.calls.append(record)
        return self.return_value


def _llm_event(parsed_id: UUID, raw_id: UUID) -> NormalizedEventSchema:
    return NormalizedEventSchema(
        event_id=uuid4(),
        parsed_record_id=parsed_id,
        event_type=EventType.POWER_OUTAGE,
        start_time=datetime(2026, 5, 12, 3, 0, tzinfo=UTC),
        end_time=None,
        location=LocationSchema(
            raw="LLM raw",
            normalized="llm|key|0",
            city="LLM City",
            street="LLM Street",
            building="0",
        ),
        reason=None,
        sources=[raw_id],
        confidence=0.9,
    )


async def test_fallback_uses_automaton_when_confidence_above_threshold():
    record = _parsed()
    llm = FakeLLMNormalizer(return_value=None)
    normalizer = FallbackNormalizer(AutomatonNormalizer(), llm, threshold=0.6)

    event = await normalizer.normalize(record)

    assert event is not None
    assert event.location.normalized == "томск|кирова|12"
    assert llm.calls == []  # LLM never called


async def test_fallback_calls_llm_when_confidence_below_threshold():
    # Force low confidence by setting threshold = 1.0 (only perfect parses pass).
    record = _parsed(location_city="Томская область Томский район Томск")
    llm_event = _llm_event(record.id, record.raw_record_id)
    llm = FakeLLMNormalizer(return_value=llm_event)
    normalizer = FallbackNormalizer(AutomatonNormalizer(), llm, threshold=1.0)

    event = await normalizer.normalize(record)

    assert event is llm_event
    assert llm.calls == [record]


async def test_fallback_calls_llm_when_automaton_produced_nothing():
    record = _parsed(location_street=None)
    llm_event = _llm_event(record.id, record.raw_record_id)
    llm = FakeLLMNormalizer(return_value=llm_event)
    normalizer = FallbackNormalizer(AutomatonNormalizer(), llm, threshold=0.6)

    event = await normalizer.normalize(record)

    assert event is llm_event
    assert llm.calls == [record]


async def test_fallback_returns_automaton_when_llm_also_fails():
    record = _parsed(location_city="Томская область Томский район Томск")
    llm = FakeLLMNormalizer(return_value=None)
    normalizer = FallbackNormalizer(AutomatonNormalizer(), llm, threshold=1.0)

    event = await normalizer.normalize(record)

    assert event is not None  # automaton's best effort
    assert llm.calls == [record]


async def test_fallback_returns_none_when_both_paths_fail():
    record = _parsed(location_street=None)
    llm = FakeLLMNormalizer(return_value=None)
    normalizer = FallbackNormalizer(AutomatonNormalizer(), llm, threshold=0.6)

    event = await normalizer.normalize(record)

    assert event is None
