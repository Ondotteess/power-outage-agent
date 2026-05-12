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
