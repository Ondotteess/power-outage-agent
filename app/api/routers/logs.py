from __future__ import annotations

from fastapi import APIRouter, Query

from app.api import queries
from app.api.deps import SessionDep
from app.api.schemas import EventLogOut

router = APIRouter(prefix="/api/logs", tags=["logs"])


@router.get("", response_model=list[EventLogOut])
async def list_logs(
    session: SessionDep,
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
    severity: str | None = Query(None, description="DEBUG | INFO | WARNING | ERROR"),
) -> list[EventLogOut]:
    rows = await queries.list_event_logs(
        session,
        limit=limit,
        offset=offset,
        severity=severity,
    )
    return [
        EventLogOut(
            id=row.id,
            event_type=row.event_type,
            severity=row.severity,
            message=row.message,
            source=row.source,
            task_id=row.task_id,
            trace_id=row.trace_id,
            payload=row.payload or {},
            created_at=row.created_at,
        )
        for row in rows
    ]
