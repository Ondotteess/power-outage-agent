from __future__ import annotations

from uuid import UUID, uuid4

from fastapi import APIRouter, HTTPException

from app.api import queries
from app.api.deps import SessionDep
from app.api.schemas import ActionResponse, SourceOut

router = APIRouter(prefix="/api/sources", tags=["sources"])


def _build_source_out(
    src,
    last_fetch_map: dict,
    counts_map: dict,
) -> SourceOut:
    profile = src.parser_profile or {}
    last_fetch = last_fetch_map.get(src.id)
    records_in_window = counts_map.get(src.id, 0)

    if not src.is_active:
        status = "inactive"
    elif last_fetch is None:
        status = "pending"
    elif records_in_window == 0:
        status = "warning"
    else:
        status = "healthy"

    return SourceOut(
        id=src.id,
        name=src.name,
        url=src.url,
        source_type=src.source_type,
        poll_interval_seconds=src.poll_interval_seconds,
        is_active=src.is_active,
        parser_profile=profile,
        last_fetch=last_fetch,
        records_in_window=records_in_window,
        success_rate=None,
        status=status,
        region=profile.get("region", "RU-KEM"),
        parser=profile.get("parser"),
    )


@router.get("", response_model=list[SourceOut])
async def list_sources(session: SessionDep) -> list[SourceOut]:
    sources = await queries.list_sources(session)
    last_fetch_map = await queries.last_fetch_per_source(session)
    counts_map = await queries.count_raw_per_source_since(session, queries.utc_window(24))
    return [_build_source_out(s, last_fetch_map, counts_map) for s in sources]


@router.get("/{source_id}", response_model=SourceOut)
async def get_source(source_id: UUID, session: SessionDep) -> SourceOut:
    src = await queries.get_source(session, source_id)
    if not src:
        raise HTTPException(status_code=404, detail="source not found")
    last_fetch_map = await queries.last_fetch_per_source(session)
    counts_map = await queries.count_raw_per_source_since(session, queries.utc_window(24))
    return _build_source_out(src, last_fetch_map, counts_map)


@router.post("/{source_id}/poll", response_model=ActionResponse, status_code=202)
async def poll_source(source_id: UUID, session: SessionDep) -> ActionResponse:
    """Trigger an immediate poll for a source.

    Stub for now — admin API runs in a separate process from the pipeline worker,
    so we don't have direct access to the in-memory TaskQueue. Returning 202 with
    a synthetic task id keeps the UI flow intact until we wire an IPC mechanism
    (e.g. a `polls` table the scheduler watches, or a small Redis pub/sub).
    """
    src = await queries.get_source(session, source_id)
    if not src:
        raise HTTPException(status_code=404, detail="source not found")
    return ActionResponse(
        ok=True,
        message=f"Poll scheduled for {src.name}",
        task_id=uuid4(),
    )
