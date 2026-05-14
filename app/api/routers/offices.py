from __future__ import annotations

import csv
import io
import logging
from typing import Annotated

from fastapi import APIRouter, File, Header, HTTPException, Query, UploadFile, status

from app.api import queries
from app.api.deps import SessionDep
from app.api.schemas import (
    OfficeImpactOut,
    OfficeImportRequest,
    OfficeImportResult,
    OfficeImportRow,
    OfficeOut,
)
from app.config import settings
from app.db.engine import async_session_factory
from app.db.repositories import OfficeStore

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api", tags=["offices"])

# Columns CSV imports MUST carry. `latitude` / `longitude` / `is_active` /
# `extra` are optional — missing values default per OfficeImportRow.
_CSV_REQUIRED_COLUMNS = {"name", "city", "address", "region"}


def _check_import_token(provided: str | None) -> None:
    expected = settings.office_import_token
    if not expected:
        # Open mode — pilot/dev. Log once-per-call so it shows up in audit.
        logger.info("office import: token guard disabled (settings.office_import_token empty)")
        return
    if provided != expected:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="invalid X-Import-Token",
        )


@router.get("/offices", response_model=list[OfficeOut])
async def list_offices(session: SessionDep) -> list[OfficeOut]:
    rows = await queries.list_offices(session)
    return [
        OfficeOut(
            id=r.id,
            name=r.name,
            city=r.city,
            address=r.address,
            region=r.region,
            is_active=r.is_active,
            latitude=r.latitude,
            longitude=r.longitude,
        )
        for r in rows
    ]


@router.post(
    "/offices/import",
    response_model=OfficeImportResult,
    summary="Idempotent office registry import",
)
async def import_offices(
    body: OfficeImportRequest,
    x_import_token: str | None = Header(default=None, alias="X-Import-Token"),
) -> OfficeImportResult:
    """Upsert offices by (name, city, address). JSON body shape:
    `{ "offices": [{...}, {...}] }`. Returns counts of inserted / updated /
    skipped rows (skipped = malformed).
    """
    _check_import_token(x_import_token)
    return await _do_upsert(body.offices)


@router.post(
    "/offices/import/csv",
    response_model=OfficeImportResult,
    summary="Idempotent office registry import via CSV upload",
)
async def import_offices_csv(
    file: Annotated[UploadFile, File(description="CSV with header row")],
    x_import_token: str | None = Header(default=None, alias="X-Import-Token"),
) -> OfficeImportResult:
    """Same semantics as `/offices/import` but reads `multipart/form-data`.
    Required columns: name, city, address, region. Optional: is_active,
    latitude, longitude.
    """
    _check_import_token(x_import_token)
    raw = await file.read()
    try:
        text = raw.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"file must be UTF-8: {exc}",
        ) from exc
    reader = csv.DictReader(io.StringIO(text))
    missing = _CSV_REQUIRED_COLUMNS - set(reader.fieldnames or [])
    if missing:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"missing required columns: {sorted(missing)}",
        )
    rows = [_row_from_csv(record) for record in reader]
    # Filter out None entries (rows that failed to parse) — they're counted as
    # skipped by _do_upsert.
    return await _do_upsert([row for row in rows if row is not None], received=len(rows))


def _row_from_csv(record: dict) -> OfficeImportRow | None:
    try:
        return OfficeImportRow(
            name=(record.get("name") or "").strip(),
            city=(record.get("city") or "").strip(),
            address=(record.get("address") or "").strip(),
            region=(record.get("region") or "").strip(),
            is_active=_csv_bool(record.get("is_active"), default=True),
            latitude=_csv_float(record.get("latitude")),
            longitude=_csv_float(record.get("longitude")),
        )
    except Exception:  # noqa: BLE001 — malformed row → skipped
        logger.warning("office import: skipping malformed CSV row=%r", record)
        return None


def _csv_bool(value: str | None, *, default: bool) -> bool:
    if value is None or value == "":
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "t"}


def _csv_float(value: str | None) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except ValueError:
        return None


async def _do_upsert(
    rows: list[OfficeImportRow],
    *,
    received: int | None = None,
) -> OfficeImportResult:
    payloads = [
        {
            "name": row.name,
            "city": row.city,
            "address": row.address,
            "region": row.region,
            "is_active": row.is_active,
            "latitude": row.latitude,
            "longitude": row.longitude,
            "extra": row.extra,
        }
        for row in rows
        if row.name and row.city and row.address and row.region
    ]
    store = OfficeStore(async_session_factory)
    inserted, updated = await store.upsert_many(payloads)
    received_count = received if received is not None else len(rows)
    return OfficeImportResult(
        received=received_count,
        inserted=inserted,
        updated=updated,
        skipped=received_count - inserted - updated,
    )


@router.get("/office-impacts", response_model=list[OfficeImpactOut])
async def list_office_impacts(
    session: SessionDep,
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
) -> list[OfficeImpactOut]:
    rows = await queries.list_office_impacts(session, limit=limit, offset=offset)
    return [
        OfficeImpactOut(
            id=impact.id,
            office_id=impact.office_id,
            office_name=office.name,
            event_id=impact.event_id,
            impact_start=impact.impact_start,
            impact_end=impact.impact_end,
            impact_level=impact.impact_level,
            match_strategy=impact.match_strategy,
            match_score=impact.match_score,
            match_explanation=impact.match_explanation or [],
            detected_at=impact.detected_at,
        )
        for impact, office in rows
    ]
