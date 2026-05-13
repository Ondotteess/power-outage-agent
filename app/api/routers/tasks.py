from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, HTTPException, Query

from app.api import queries
from app.api.deps import SessionDep
from app.api.schemas import ActionResponse, TaskOut
from app.db.engine import async_session_factory
from app.db.repositories import RetryRequestStore

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
async def retry_task(task_id: UUID, session: SessionDep) -> ActionResponse:
    """Retry a failed task.

    The API writes a DB-backed retry request. The pipeline's RequestWatcher
    claims it and re-enqueues the failed task in the worker process.
    """
    task = await queries.get_task(session, task_id)
    if task is None:
        raise HTTPException(status_code=404, detail="task not found")
    if task.status != "failed":
        raise HTTPException(status_code=409, detail="task is not failed")
    request_id = await RetryRequestStore(async_session_factory).create(task_id)
    return ActionResponse(
        ok=True,
        message="Retry request queued",
        task_id=None,
        request_id=request_id,
    )
