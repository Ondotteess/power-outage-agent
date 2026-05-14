"""Response schemas for the admin API.

Kept distinct from `app.models.schemas` (which is the domain/pipeline contract)
because the API surface is a UI projection — flat, denormalised, friendly to
the dashboard rather than to the worker pipeline.
"""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, Field

# ── Sources ────────────────────────────────────────────────────────────────


class SourceOut(BaseModel):
    id: UUID
    name: str
    url: str
    source_type: str
    poll_interval_seconds: int
    is_active: bool
    parser_profile: dict = Field(default_factory=dict)
    last_fetch: datetime | None = None
    records_in_window: int = 0
    success_rate: float | None = None  # 0..1
    status: str = "unknown"  # healthy | warning | failed | inactive
    region: str | None = None
    parser: str | None = None


# ── Raw / Parsed / Normalized ──────────────────────────────────────────────


class RawRecordOut(BaseModel):
    id: UUID
    source_id: UUID | None
    source_url: str
    source_type: str
    content_hash: str
    fetched_at: datetime
    trace_id: UUID
    size_bytes: int


class ParsedRecordOut(BaseModel):
    id: UUID
    raw_record_id: UUID
    source_id: UUID | None
    external_id: str | None
    start_time: datetime | None
    end_time: datetime | None
    city: str | None
    district: str | None
    street: str | None
    region_code: str | None
    reason: str | None
    extracted_at: datetime


class NormalizedEventOut(BaseModel):
    event_id: UUID
    parsed_record_id: UUID | None
    event_type: str
    start_time: datetime
    end_time: datetime | None
    location_raw: str
    location_normalized: str | None
    city: str | None
    street: str | None
    building: str | None
    reason: str | None
    confidence: float
    normalized_at: datetime


class OfficeOut(BaseModel):
    id: UUID
    name: str
    city: str
    address: str
    region: str
    is_active: bool
    latitude: float | None = None
    longitude: float | None = None


class OfficeImportRow(BaseModel):
    """One row in an /api/offices/import payload.

    `name + city + address` form the natural key — upsert matches on this
    tuple. Other fields are overwritten when present.
    """

    name: str
    city: str
    address: str
    region: str
    is_active: bool = True
    latitude: float | None = None
    longitude: float | None = None
    extra: dict = Field(default_factory=dict)


class OfficeImportRequest(BaseModel):
    offices: list[OfficeImportRow] = Field(default_factory=list)


class OfficeImportResult(BaseModel):
    received: int
    inserted: int
    updated: int
    skipped: int


class OfficeImpactOut(BaseModel):
    id: UUID
    office_id: UUID
    office_name: str
    event_id: UUID
    impact_start: datetime
    impact_end: datetime | None
    impact_level: str
    match_strategy: str
    match_score: float
    match_explanation: list[str] = Field(default_factory=list)
    detected_at: datetime


class MapOfficeImpactOut(BaseModel):
    id: UUID
    reason: str | None
    severity: str
    starts_at: datetime
    ends_at: datetime | None
    event_type: str | None = None
    match_strategy: str | None = None
    match_score: float | None = None
    match_explanation: list[str] = Field(default_factory=list)


class MapOfficeOut(BaseModel):
    id: UUID
    name: str
    address: str
    city: str
    region: str
    latitude: float | None = None
    longitude: float | None = None
    status: str
    active_impacts: list[MapOfficeImpactOut] = Field(default_factory=list)


class MapOfficesResponse(BaseModel):
    offices: list[MapOfficeOut] = Field(default_factory=list)


class NotificationOut(BaseModel):
    id: UUID
    office_id: UUID
    office_name: str
    event_id: UUID
    channel: str
    status: str
    severity: str
    emitted_at: datetime
    summary: str


# ── Tasks / DLQ ────────────────────────────────────────────────────────────


class TaskOut(BaseModel):
    id: UUID
    task_type: str
    status: str
    attempt: int
    input_hash: str
    error: str | None
    trace_id: UUID
    created_at: datetime
    updated_at: datetime
    next_retry_at: datetime | None = None
    source_id: UUID | None = None


# ── Pipeline / dashboard ───────────────────────────────────────────────────


class PipelineStage(BaseModel):
    key: str
    label: str
    status: str  # healthy | running | pending | failed
    throughput: float | None = None  # items / minute (rolling)
    queue_size: int = 0
    latency_ms: float | None = None
    retry_count: int = 0
    metric_label: str | None = None
    metric_value: str | None = None


class PipelineStatus(BaseModel):
    overall: str  # healthy | degraded | failed
    last_heartbeat: datetime
    stages: list[PipelineStage]


class KpiDelta(BaseModel):
    value: int | float
    delta_pct: float | None = None
    delta_label: str | None = None
    status: str = "neutral"  # success | warning | error | neutral


class DashboardSummary(BaseModel):
    active_sources: KpiDelta
    raw_records_today: KpiDelta
    parsed_outages: KpiDelta
    duplicates_skipped: KpiDelta
    failed_tasks: KpiDelta
    offices_at_risk: KpiDelta


class ActivityEvent(BaseModel):
    id: str
    type: str  # RawFetched | RawParsed | DuplicateSkipped | TaskFailed | OfficeImpactDetected | PipelineHeartbeat
    severity: str  # info | success | warning | error
    source: str | None = None
    message: str
    at: datetime


class EventLogOut(BaseModel):
    id: UUID
    event_type: str
    severity: str
    message: str
    source: str | None = None
    task_id: UUID | None = None
    trace_id: UUID | None = None
    payload: dict = Field(default_factory=dict)
    created_at: datetime


class NormalizationQuality(BaseModel):
    average_confidence: float
    normalized_count: int
    parsed_total: int
    high: int
    medium: int
    low: int
    estimated_tokens: int | None = None
    estimated_cost_usd: float | None = None


class QueueBacklogPoint(BaseModel):
    at: datetime
    pending: int
    running: int
    failed: int


class ActionResponse(BaseModel):
    ok: bool
    message: str
    task_id: UUID | None = None
    request_id: UUID | None = None


# ── Metrics ────────────────────────────────────────────────────────────────


class StageTimingOut(BaseModel):
    task_type: str
    count: int
    avg_ms: int
    p50_ms: int
    p95_ms: int
    max_ms: int


class LLMCallOut(BaseModel):
    id: UUID
    model: str
    prompt_tokens: int
    completion_tokens: int
    total_tokens: int
    duration_ms: int
    status: str
    cost_rub: float
    created_at: datetime


class LLMCostSummary(BaseModel):
    calls_ok: int
    calls_error: int
    prompt_tokens: int
    completion_tokens: int
    total_tokens: int
    avg_duration_ms: int
    max_duration_ms: int
    # Cost is the SDK estimate using the settings tariff. The real bill comes
    # from Sber — `prompt_price` / `completion_price` are exposed so the UI
    # can show the assumption alongside the number.
    prompt_cost_rub: float
    completion_cost_rub: float
    total_cost_rub: float
    prompt_price_per_1k_rub: float
    completion_price_per_1k_rub: float


class NormalizerPathMix(BaseModel):
    automaton: int
    llm_fallback: int
    none: int
    automaton_pct: float  # 0..1


class RuntimeMemoryOut(BaseModel):
    process: str  # "api" — the FastAPI process answering this request
    rss_mb: float
    vms_mb: float | None = None
    cpu_percent: float | None = None


class PipelineMetrics(BaseModel):
    stage_timings: list[StageTimingOut]
    llm_cost: LLMCostSummary
    normalizer_path: NormalizerPathMix
    recent_llm_calls: list[LLMCallOut]
    runtime: RuntimeMemoryOut | None = None
    window_hours: int
