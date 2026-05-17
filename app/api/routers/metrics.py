"""Pipeline performance & cost metrics.

Aggregates per-stage timings (from TaskRecord.duration_ms), GigaChat call
volumes / token spend (from LLMCall), and the FSA-vs-regex mix recorded by
NormalizationHandler.

The cost number is an SDK-side estimate: tokens times the tariff configured
in `settings.gigachat_price_per_1k_*`. The real bill comes from Sber.
"""

from __future__ import annotations

import logging
import os

from fastapi import APIRouter, Query

from app.api import queries
from app.api.deps import SessionDep
from app.api.schemas import (
    LLMCallOut,
    LLMCostSummary,
    NormalizerPathMix,
    PipelineMetrics,
    RuntimeMemoryOut,
    StageTimingOut,
)
from app.config import settings

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/metrics", tags=["metrics"])

_BYTES_PER_MB = 1024 * 1024


def _llm_cost(prompt: int, completion: int) -> tuple[float, float, float]:
    """Return (prompt_rub, completion_rub, total_rub) using configured tariff."""
    prompt_rub = prompt / 1000.0 * settings.gigachat_price_per_1k_prompt_rub
    completion_rub = completion / 1000.0 * settings.gigachat_price_per_1k_completion_rub
    return round(prompt_rub, 4), round(completion_rub, 4), round(prompt_rub + completion_rub, 4)


def _runtime_snapshot() -> RuntimeMemoryOut | None:
    """Memory of the FastAPI process serving this request.

    The pipeline worker runs in a different process — this snapshot is the
    admin API's footprint, useful as a sanity baseline but not the real
    workload number. Returns None if psutil isn't available.
    """
    try:
        import psutil  # type: ignore[import-not-found]
    except ImportError:
        logger.debug("psutil not installed — runtime memory unavailable")
        return None

    proc = psutil.Process(os.getpid())
    with proc.oneshot():
        mem = proc.memory_info()
        try:
            cpu = proc.cpu_percent(interval=0.0)
        except psutil.AccessDenied:
            cpu = None
    return RuntimeMemoryOut(
        process="api",
        rss_mb=round(mem.rss / _BYTES_PER_MB, 1),
        vms_mb=round(mem.vms / _BYTES_PER_MB, 1) if hasattr(mem, "vms") else None,
        cpu_percent=cpu,
    )


@router.get("/pipeline", response_model=PipelineMetrics)
async def pipeline_metrics(
    session: SessionDep,
    hours: int = Query(default=24, ge=1, le=24 * 30),
) -> PipelineMetrics:
    since = queries.utc_window(hours)

    timings = await queries.stage_timings(session, since=since)
    llm = await queries.llm_totals(session, since=since)
    path_counts = await queries.normalizer_path_counts(session, since=since)
    recent = await queries.llm_recent_calls(session, limit=20)

    prompt_rub, completion_rub, total_rub = _llm_cost(
        llm["prompt_tokens"], llm["completion_tokens"]
    )

    automaton = path_counts.get("automaton", 0)
    regex_path = path_counts.get("regex_fallback", 0)
    llm_path = path_counts.get("llm_fallback", 0)
    none_path = path_counts.get("none", 0)
    total_paths = automaton + regex_path + llm_path + none_path
    automaton_pct = automaton / total_paths if total_paths else 0.0

    return PipelineMetrics(
        stage_timings=[StageTimingOut(**row) for row in timings],
        llm_cost=LLMCostSummary(
            calls_ok=llm["calls_ok"],
            calls_error=llm["calls_error"],
            prompt_tokens=llm["prompt_tokens"],
            completion_tokens=llm["completion_tokens"],
            total_tokens=llm["total_tokens"],
            avg_duration_ms=llm["avg_duration_ms"],
            max_duration_ms=llm["max_duration_ms"],
            prompt_cost_rub=prompt_rub,
            completion_cost_rub=completion_rub,
            total_cost_rub=total_rub,
            prompt_price_per_1k_rub=settings.gigachat_price_per_1k_prompt_rub,
            completion_price_per_1k_rub=settings.gigachat_price_per_1k_completion_rub,
        ),
        normalizer_path=NormalizerPathMix(
            automaton=automaton,
            regex_fallback=regex_path,
            llm_fallback=llm_path,
            none=none_path,
            automaton_pct=round(automaton_pct, 3),
        ),
        recent_llm_calls=[
            LLMCallOut(
                id=call.id,
                model=call.model,
                prompt_tokens=call.prompt_tokens,
                completion_tokens=call.completion_tokens,
                total_tokens=call.total_tokens,
                duration_ms=call.duration_ms,
                status=call.status,
                cost_rub=_llm_cost(call.prompt_tokens, call.completion_tokens)[2],
                created_at=call.created_at,
            )
            for call in recent
        ],
        runtime=_runtime_snapshot(),
        window_hours=hours,
    )
