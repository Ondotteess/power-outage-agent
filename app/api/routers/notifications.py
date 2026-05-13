from __future__ import annotations

from fastapi import APIRouter, Query

from app.api import queries
from app.api.deps import SessionDep
from app.api.schemas import NotificationOut

router = APIRouter(prefix="/api/notifications", tags=["notifications"])


@router.get("", response_model=list[NotificationOut])
async def list_notifications(
    session: SessionDep,
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
) -> list[NotificationOut]:
    rows = await queries.list_notifications(session, limit=limit, offset=offset)
    return [
        NotificationOut(
            id=notification.id,
            office_id=notification.office_id,
            office_name=office.name,
            event_id=notification.event_id,
            channel=notification.channel,
            status=notification.status,
            severity=notification.severity,
            emitted_at=notification.emitted_at,
            summary=notification.summary,
        )
        for notification, office in rows
    ]
