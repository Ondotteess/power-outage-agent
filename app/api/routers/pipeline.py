from __future__ import annotations

from datetime import UTC, datetime

from fastapi import APIRouter

from app.api import queries
from app.api.deps import SessionDep
from app.api.schemas import PipelineStage, PipelineStatus

router = APIRouter(prefix="/api/pipeline", tags=["pipeline"])


_STAGES = [
    ("scheduler", "Scheduler", "fetch_source", None),
    ("collector", "Collector", "fetch_source", None),
    ("parser", "Parser", "parse_content", None),
    ("normalizer", "Regex Normalizer", "normalize_event", None),
    ("dedup", "Dedup Engine", "deduplicate_event", None),
    ("matcher", "Office Matcher", "match_offices", None),
    ("notifier", "Notifier", "emit_event", None),
]


def _stage_status(failed: int, running: int, pending: int, done: int) -> str:
    if failed > 0 and failed >= (done + running) / 4:
        return "failed"
    if running > 0:
        return "running"
    if pending > 0:
        return "pending"
    if done > 0:
        return "healthy"
    return "pending"


@router.get("/status", response_model=PipelineStatus)
async def pipeline_status(session: SessionDep) -> PipelineStatus:
    counts = await queries.count_tasks_by_type_status(session)
    timings = {
        row["task_type"]: row
        for row in await queries.stage_timings(session, since=queries.utc_window(24))
    }

    stages: list[PipelineStage] = []
    for key, label, task_type, _ in _STAGES:
        failed = counts.get((task_type, "failed"), 0)
        running = counts.get((task_type, "running"), 0)
        pending = counts.get((task_type, "pending"), 0)
        done = counts.get((task_type, "done"), 0)
        timing = timings.get(task_type)

        status = _stage_status(failed, running, pending, done)
        # Future stages with no tasks yet — show as "pending" so the UI shows them grey.
        if (done + running + pending + failed) == 0 and key in {"dedup", "matcher", "notifier"}:
            status = "pending"

        stages.append(
            PipelineStage(
                key=key,
                label=label,
                status=status,
                throughput=round(timing["count"] / (24 * 60), 3) if timing else None,
                queue_size=pending + running,
                latency_ms=float(timing["avg_ms"]) if timing else None,
                retry_count=failed,
                metric_label="processed",
                metric_value=str(done),
            )
        )

    has_failed = any(s.status == "failed" for s in stages)
    overall = "failed" if has_failed else "healthy"

    return PipelineStatus(
        overall=overall,
        last_heartbeat=datetime.now(UTC),
        stages=stages,
    )
