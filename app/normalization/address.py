"""Deterministic address normalization shared by normalization, dedup and matcher.

Single source of truth: every code path that needs to compare addresses goes
through `canonical_key`. Anything that needs to inspect individual parts
(matcher's house-range logic) uses the lower-level `normalize_*` helpers.

The intent is that two phrasings of the same address — "ул. Кирова, 12",
"улица Кирова, дом 12", "Кирова 12" — collapse to the same key so dedup
and matcher work without depending on LLM output stability.
"""

from __future__ import annotations

import re

_TEXT_CLEAN_RE = re.compile(r"[^0-9a-zа-яё/\-–—]+", re.IGNORECASE)
HOUSE_KEY_RE = re.compile(r"(?<!\d)(\d{1,4}(?:/\d{1,4})?[a-zа-я]?)", re.IGNORECASE)
HOUSE_RANGE_RE = re.compile(r"\d{1,4}\s*[-–—]\s*\d{1,4}")
HOUSE_RE = re.compile(r"(?<!\d)(\d{1,4})(?:\s*[-–—]\s*(\d{1,4}))?", re.IGNORECASE)

# Order matters: longer / multi-character patterns must run before short ones
# whose match would be a prefix (e.g. `пр-кт` before `пр`).
REPLACEMENTS: tuple[tuple[re.Pattern[str], str], ...] = (
    # Multi-token abbreviations with dashes. The first one accepts both
    # `пр-кт`/`пркт` (full short form) and `пр-т`/`прт` (medium short form).
    (re.compile(r"\bпр-?(?:кт|т)\.?\b", re.IGNORECASE), " проспект "),
    (re.compile(r"\bпр-д\.?\b", re.IGNORECASE), " проезд "),
    (re.compile(r"\bр-н\.?\b", re.IGNORECASE), " район "),
    (re.compile(r"\bб-р\.?\b", re.IGNORECASE), " бульвар "),
    # Multi-letter street / locality types
    (re.compile(r"\bбульв\.?\b", re.IGNORECASE), " бульвар "),
    (re.compile(r"\bпер\.?\b", re.IGNORECASE), " переулок "),
    (re.compile(r"\bмкр\.?\b", re.IGNORECASE), " микрорайон "),
    (re.compile(r"\bнаб\.?\b", re.IGNORECASE), " набережная "),
    (re.compile(r"\bтуп\.?\b", re.IGNORECASE), " тупик "),
    (re.compile(r"\bпос\.?\b", re.IGNORECASE), " поселок "),
    (re.compile(r"\bпгт\.?\b", re.IGNORECASE), " пгт "),
    (re.compile(r"\bобл\.?\b", re.IGNORECASE), " область "),
    (re.compile(r"\bстр\.?\b", re.IGNORECASE), " строение "),
    (re.compile(r"\bул\.?\b", re.IGNORECASE), " улица "),
    (re.compile(r"\bпл\.?\b", re.IGNORECASE), " площадь "),
    # Single-letter abbreviations (period optional, word-bounded)
    (re.compile(r"\bпр\.?\b", re.IGNORECASE), " проспект "),
    (re.compile(r"\bш\.?\b", re.IGNORECASE), " шоссе "),
    (re.compile(r"\bг\.?\b", re.IGNORECASE), " город "),
    (re.compile(r"\bс\.?\b", re.IGNORECASE), " село "),
    (re.compile(r"\bп\.?\b", re.IGNORECASE), " поселок "),
    (re.compile(r"\bд\.?\b", re.IGNORECASE), " дом "),
    (re.compile(r"\bк\.?\b", re.IGNORECASE), " корпус "),
)

LOCALITY_WORDS: frozenset[str] = frozenset(
    {"город", "г", "село", "с", "деревня", "д", "поселок", "пос", "пгт", "аул"}
)
STREET_WORDS: frozenset[str] = frozenset(
    {
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
        "тупик",
    }
)
HOUSE_WORDS: frozenset[str] = frozenset({"дом", "корпус", "строение", "владение", "литера", "офис"})
NO_BUILDING: frozenset[str] = frozenset({"", "бн", "б/н", "без номера", "без номер", "нет номера"})


def normalize_text(value: str | None) -> str:
    if not value:
        return ""
    text = value.casefold().replace("ё", "е").replace("№", " ")
    for pattern, replacement in REPLACEMENTS:
        text = pattern.sub(replacement, text)
    text = _TEXT_CLEAN_RE.sub(" ", text)
    return re.sub(r"\s+", " ", text).strip()


def normalize_city(value: str | None) -> str | None:
    tokens = [t for t in normalize_text(value).split() if t not in LOCALITY_WORDS]
    return " ".join(tokens) or None


def normalize_street(value: str | None) -> str:
    tokens = [
        t
        for t in normalize_text(value).split()
        if t not in STREET_WORDS and t not in HOUSE_WORDS and t not in LOCALITY_WORDS
    ]
    while len(tokens) > 1 and is_house_token(tokens[-1]):
        tokens.pop()
    return " ".join(tokens)


def normalize_building(value: str | None) -> str | None:
    normalized = normalize_text(value)
    if normalized in NO_BUILDING:
        return None
    match = HOUSE_KEY_RE.search(normalized)
    return match.group(1) if match else None


def is_house_token(value: str) -> bool:
    return bool(HOUSE_KEY_RE.fullmatch(value) or HOUSE_RANGE_RE.fullmatch(value))


def split_address(address: str) -> tuple[str, str | None]:
    """Split free-text address into (street, building).

    Trailing ", 12" / ", д.12" becomes the building part. If nothing looks
    like a house number, returns (address, None).
    """
    parts = [p.strip() for p in re.split(r"[,;]", address) if p.strip()]
    if len(parts) > 1 and normalize_building(parts[-1]):
        return ", ".join(parts[:-1]), parts[-1]

    matches = list(HOUSE_KEY_RE.finditer(address))
    if not matches:
        return address, None

    last = matches[-1]
    street = address[: last.start()].strip(" ,;")
    return street or address, last.group(1)


def canonical_key(
    city: str | None,
    street: str | None,
    building: str | None,
) -> str | None:
    """Build a stable comparison key for an address.

    Same address in different phrasings (`ул. Кирова, 12` / `улица Кирова,
    дом 12` / `Кирова 12`) collapses to the same string. Returns None when
    there is no addressable signal at all — callers should treat that as
    'do not compare by key'.
    """
    # Recover building from street if LLM/parser left it empty: "Кирова, 12"
    # should not lose the 12.
    if not building and street:
        _, recovered = split_address(street)
        if recovered:
            building = recovered

    city_part = normalize_city(city) or ""
    street_part = normalize_street(street)
    building_part = normalize_building(building) or ""

    if not (city_part or street_part or building_part):
        return None

    return f"{city_part}|{street_part}|{building_part}"
