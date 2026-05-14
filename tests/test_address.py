from __future__ import annotations

from app.normalization.address import (
    canonical_key,
    normalize_building,
    normalize_city,
    normalize_street,
    split_address,
)


def test_canonical_key_collapses_abbreviations():
    assert (
        canonical_key("Новокузнецк", "ул. Кирова", "55")
        == canonical_key("Новокузнецк", "улица Кирова", "55")
        == canonical_key("Новокузнецк", "ул Кирова", "д. 55")
        == "новокузнецк|кирова|55"
    )


def test_canonical_key_handles_prospect_variants():
    assert (
        canonical_key("Кемерово", "пр-т Ленина", "90")
        == canonical_key("Кемерово", "проспект Ленина", "90")
        == canonical_key("Кемерово", "пр. Ленина", "90")
        == "кемерово|ленина|90"
    )


def test_canonical_key_handles_locality_prefixes():
    assert (
        canonical_key("г. Кемерово", "проспект Ленина", "90")
        == canonical_key("Кемерово", "проспект Ленина", "90")
        == canonical_key("город Кемерово", "проспект Ленина", "90")
        == "кемерово|ленина|90"
    )


def test_canonical_key_extracts_building_from_street_when_missing():
    assert canonical_key("Томск", "проспект Ленина, 120", None) == "томск|ленина|120"
    assert canonical_key("Томск", "пр. Ленина 120", "") == "томск|ленина|120"


def test_canonical_key_returns_none_for_empty_input():
    assert canonical_key(None, None, None) is None
    assert canonical_key("", "", "") is None


def test_canonical_key_treats_yo_as_ye():
    assert canonical_key("Берёзовский", "улица Школьная", "17") == canonical_key(
        "Березовский", "улица Школьная", "17"
    )


def test_canonical_key_no_building_markers_collapse_to_empty():
    assert canonical_key("Томск", "Весенняя", "б/н") == "томск|весенняя|"
    assert canonical_key("Томск", "Весенняя", "без номера") == "томск|весенняя|"
    assert canonical_key("Томск", "Весенняя", None) == "томск|весенняя|"


def test_canonical_key_dirty_demo_matches_clean_office():
    # The scenarios introduced into the demo pipeline must collapse onto the
    # same key as the corresponding office address.
    assert (
        canonical_key("Новокузнецк", "ул. Кирова, д. 55", None)
        == canonical_key("Новокузнецк", "улица Кирова", "55")
        == "новокузнецк|кирова|55"
    )
    assert (
        canonical_key("Кемерово", "пр-т Ленина", "90")
        == canonical_key("Кемерово", "проспект Ленина", "90")
        == "кемерово|ленина|90"
    )
    assert (
        canonical_key("г. Томск", "пр. Ленина 120", None)
        == canonical_key("Томск", "проспект Ленина", "120")
        == "томск|ленина|120"
    )


def test_split_address_extracts_building():
    assert split_address("улица Кирова, 55") == ("улица Кирова", "55")
    assert split_address("Красный проспект, 77") == ("Красный проспект", "77")


def test_split_address_returns_none_when_no_number():
    street, building = split_address("улица без номера")
    assert street == "улица без номера"
    assert building is None


def test_normalize_building_recognises_no_number_markers():
    assert normalize_building("б/н") is None
    assert normalize_building("без номера") is None
    assert normalize_building("12") == "12"


def test_normalize_city_strips_locality_prefix():
    assert normalize_city("город Кемерово") == "кемерово"
    assert normalize_city("г. Томск") == "томск"
    assert normalize_city("село Бичура") == "бичура"


def test_normalize_street_strips_house_tail():
    assert normalize_street("улица Кирова, 12") == "кирова"
    assert normalize_street("проспект Ленина 90") == "ленина"
    assert normalize_street("Красный проспект") == "красный"
