from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from uuid import UUID

from fastapi import APIRouter

from app.api import queries
from app.api.deps import SessionDep
from app.api.schemas import MapOfficeImpactOut, MapOfficeOut, MapOfficesResponse
from app.db.models import NormalizedEvent, Office, OfficeImpact

router = APIRouter(prefix="/api/map", tags=["map"])

_STATUS_RANK = {"ok": 0, "risk": 1, "critical": 2}
_SEVERITY_RANK = {"unknown": 0, "low": 1, "medium": 2, "high": 3, "critical": 4}
_KNOWN_SEVERITIES = {"low", "medium", "high", "critical"}
_OUTAGE_TERMS = (
    "power_outage",
    "outage",
    "shutdown",
    "closure",
    "blackout",
    "отключ",
    "закрыт",
)
_GRID_UNIT_TERMS = ("рэс", "уэс", "сет", "участ")
_RISK_HORIZON = timedelta(days=7)


@dataclass
class _OfficeBucket:
    office: Office
    impacts: list[tuple[MapOfficeImpactOut, str]] = field(default_factory=list)


@router.get("/offices", response_model=MapOfficesResponse)
async def list_map_offices(session: SessionDep) -> MapOfficesResponse:
    now = datetime.now(UTC)
    rows = await queries.list_map_office_rows(session, now=now, horizon_until=now + _RISK_HORIZON)
    return build_map_offices_response(rows, now=now)


def build_map_offices_response(
    rows: list[tuple[Office, OfficeImpact | None, NormalizedEvent | None]],
    *,
    now: datetime | None = None,
) -> MapOfficesResponse:
    active_at = _as_aware_utc(now or datetime.now(UTC))
    buckets: dict[UUID, _OfficeBucket] = {}
    for office, impact, event in rows:
        bucket = buckets.setdefault(office.id, _OfficeBucket(office=office))
        if impact is None or not _is_relevant_impact(impact, active_at):
            continue

        severity = _severity(impact.impact_level)
        status = _impact_status(severity, event)
        bucket.impacts.append(
            (
                MapOfficeImpactOut(
                    id=impact.id,
                    reason=_reason(impact, event),
                    severity=severity,
                    starts_at=impact.impact_start,
                    ends_at=impact.impact_end,
                    event_type=event.event_type if event is not None else None,
                    match_strategy=impact.match_strategy,
                    match_score=getattr(impact, "match_score", None),
                    match_explanation=getattr(impact, "match_explanation", None) or [],
                ),
                status,
            )
        )

    offices = [_map_office(bucket) for bucket in buckets.values()]
    return MapOfficesResponse(offices=offices)


def _map_office(bucket: _OfficeBucket) -> MapOfficeOut:
    impacts = sorted(
        bucket.impacts,
        key=lambda item: (
            -_STATUS_RANK[item[1]],
            -_SEVERITY_RANK.get(item[0].severity, 0),
            item[0].starts_at,
        ),
    )
    active_impacts = [impact for impact, _status in impacts]
    status = "ok"
    if impacts:
        status = max((impact_status for _impact, impact_status in impacts), key=_STATUS_RANK.get)

    office = bucket.office
    return MapOfficeOut(
        id=office.id,
        name=office.name,
        address=office.address,
        city=office.city,
        region=office.region,
        latitude=office.latitude,
        longitude=office.longitude,
        status=status,
        active_impacts=active_impacts,
    )


def _severity(value: str | None) -> str:
    severity = (value or "").strip().lower()
    return severity if severity in _KNOWN_SEVERITIES else "unknown"


def _impact_status(severity: str, event: NormalizedEvent | None) -> str:
    if severity in {"high", "critical"} or _event_means_outage(event):
        return "critical"
    return "risk"


def _event_means_outage(event: NormalizedEvent | None) -> bool:
    if event is None:
        return False
    haystack = " ".join(
        str(value or "").lower()
        for value in (
            event.event_type,
            event.reason,
            event.location_raw,
            event.location_normalized,
        )
    )
    return any(term in haystack for term in _OUTAGE_TERMS)


def _reason(impact: OfficeImpact, event: NormalizedEvent | None) -> str:
    label = _event_label(event)
    detail = (event.reason or "").strip() if event is not None and event.reason else ""
    if not detail:
        return label
    if _looks_like_grid_unit(detail):
        return f"{label}. Участок: {detail}"
    if _already_describes_outage(detail):
        return f"{label}: {detail}"
    return f"{label}. Детали источника: {detail}"


def _event_label(event: NormalizedEvent | None) -> str:
    event_type = (event.event_type if event is not None else "").strip().lower()
    if event_type == "infrastructure_failure":
        return "Аварийное отключение электроэнергии"
    if event_type in {"power_outage", "maintenance"}:
        return "Плановое отключение электроэнергии"
    if event is not None and _event_means_outage(event):
        return "Отключение электроэнергии"
    return "Возможное влияние на работу офиса"


def _looks_like_grid_unit(value: str) -> bool:
    text = value.casefold()
    return any(term in text for term in _GRID_UNIT_TERMS)


def _already_describes_outage(value: str) -> bool:
    text = value.casefold()
    return any(
        term in text
        for term in (
            "отключ",
            "электро",
            "ремонт",
            "авар",
            "обесточ",
            "вл-",
            "тп-",
            "outage",
            "maintenance",
            "shutdown",
            "blackout",
            "repair",
        )
    )


def _is_relevant_impact(impact: OfficeImpact, now: datetime) -> bool:
    starts_at = _as_aware_utc(impact.impact_start)
    ends_at = _as_aware_utc(impact.impact_end) if impact.impact_end is not None else None
    return starts_at <= now + _RISK_HORIZON and (ends_at is None or ends_at >= now)


def _as_aware_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)
