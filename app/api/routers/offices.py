from __future__ import annotations

from fastapi import APIRouter, Query

from app.api import queries
from app.api.deps import SessionDep
from app.api.schemas import OfficeImpactOut, OfficeOut

router = APIRouter(prefix="/api", tags=["offices"])


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
            detected_at=impact.detected_at,
        )
        for impact, office in rows
    ]
