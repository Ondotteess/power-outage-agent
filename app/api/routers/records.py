from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Query

from app.api import queries
from app.api.deps import SessionDep
from app.api.schemas import NormalizedEventOut, ParsedRecordOut, RawRecordOut

router = APIRouter(prefix="/api", tags=["records"])


@router.get("/raw", response_model=list[RawRecordOut])
async def list_raw(
    session: SessionDep,
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    source_id: UUID | None = None,
) -> list[RawRecordOut]:
    rows = await queries.list_raw(session, limit=limit, offset=offset, source_id=source_id)
    return [
        RawRecordOut(
            id=r.id,
            source_id=r.source_id,
            source_url=r.source_url,
            source_type=r.source_type,
            content_hash=r.content_hash,
            fetched_at=r.fetched_at,
            trace_id=r.trace_id,
            size_bytes=len(r.raw_content or ""),
        )
        for r in rows
    ]


@router.get("/parsed", response_model=list[ParsedRecordOut])
async def list_parsed(
    session: SessionDep,
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    source_id: UUID | None = None,
    city: str | None = None,
) -> list[ParsedRecordOut]:
    rows = await queries.list_parsed(
        session, limit=limit, offset=offset, source_id=source_id, city=city
    )
    return [
        ParsedRecordOut(
            id=r.id,
            raw_record_id=r.raw_record_id,
            source_id=r.source_id,
            external_id=r.external_id,
            start_time=r.start_time,
            end_time=r.end_time,
            city=r.location_city,
            district=r.location_district,
            street=r.location_street,
            region_code=r.location_region_code,
            reason=r.reason,
            extracted_at=r.extracted_at,
        )
        for r in rows
    ]


@router.get("/normalized", response_model=list[NormalizedEventOut])
async def list_normalized(
    session: SessionDep,
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
) -> list[NormalizedEventOut]:
    rows = await queries.list_normalized(session, limit=limit, offset=offset)
    return [
        NormalizedEventOut(
            event_id=r.event_id,
            parsed_record_id=r.parsed_record_id,
            event_type=r.event_type,
            start_time=r.start_time,
            end_time=r.end_time,
            location_raw=r.location_raw,
            location_normalized=r.location_normalized,
            city=r.location_city,
            street=r.location_street,
            building=r.location_building,
            reason=r.reason,
            confidence=r.confidence,
            normalized_at=r.normalized_at,
        )
        for r in rows
    ]
