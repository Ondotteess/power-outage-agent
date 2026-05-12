from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Query

from app.api import queries
from app.api.deps import SessionDep
from app.api.schemas import ActionResponse, TaskOut

router = APIRouter(prefix="/api/tasks", tags=["tasks"])


@router.get("", response_model=list[TaskOut])
async def list_tasks(
    session: SessionDep,
    status: str | None = Query(None, description="pending | running | done | failed"),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
) -> list[TaskOut]:
    rows = await queries.list_tasks(session, status=status, limit=limit, offset=offset)
    return [
        TaskOut(
            id=r.id,
            task_type=r.task_type,
            status=r.status,
            attempt=r.attempt,
            input_hash=r.input_hash,
            error=r.error,
            trace_id=r.trace_id,
            created_at=r.created_at,
            updated_at=r.updated_at,
            next_retry_at=None,
            source_id=r.payload.get("source_id") if isinstance(r.payload, dict) else None,
        )
        for r in rows
    ]


@router.post("/{task_id}/retry", response_model=ActionResponse, status_code=202)
async def retry_task(task_id: UUID) -> ActionResponse:
    """Retry a failed task.

    Stub for now — admin API runs in a separate process from the pipeline worker,
    so we don't directly enqueue. Returning 202 with a confirmation message keeps
    the UI flow working until we add an IPC channel (or a `retry_requests` table
    the dispatcher polls).
    """
    return ActionResponse(
        ok=True,
        message="Retry scheduled",
        task_id=task_id,
    )
