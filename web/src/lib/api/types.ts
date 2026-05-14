// API types mirror app/api/schemas.py. Keep these in sync.

export type StatusTone = "healthy" | "running" | "pending" | "failed" | "degraded";
export type SourceStatus = "healthy" | "warning" | "failed" | "inactive" | "pending" | "unknown";
export type Severity = "info" | "success" | "warning" | "error";
export type TaskStatus = "pending" | "running" | "done" | "failed";

export interface Source {
  id: string;
  name: string;
  url: string;
  source_type: string;
  poll_interval_seconds: number;
  is_active: boolean;
  parser_profile: Record<string, unknown>;
  last_fetch: string | null;
  records_in_window: number;
  success_rate: number | null;
  status: SourceStatus;
  region: string | null;
  parser: string | null;
}

export interface RawRecord {
  id: string;
  source_id: string | null;
  source_url: string;
  source_type: string;
  content_hash: string;
  fetched_at: string;
  trace_id: string;
  size_bytes: number;
}

export interface ParsedRecord {
  id: string;
  raw_record_id: string;
  source_id: string | null;
  external_id: string | null;
  start_time: string | null;
  end_time: string | null;
  city: string | null;
  district: string | null;
  street: string | null;
  region_code: string | null;
  reason: string | null;
  extracted_at: string;
}

export interface NormalizedEvent {
  event_id: string;
  parsed_record_id: string | null;
  event_type: string;
  start_time: string;
  end_time: string | null;
  location_raw: string;
  location_normalized: string | null;
  city: string | null;
  street: string | null;
  building: string | null;
  reason: string | null;
  confidence: number;
  normalized_at: string;
}

export interface Task {
  id: string;
  task_type: string;
  status: TaskStatus;
  attempt: number;
  input_hash: string;
  error: string | null;
  trace_id: string;
  created_at: string;
  updated_at: string;
  next_retry_at: string | null;
  source_id: string | null;
}

export interface PipelineStage {
  key: string;
  label: string;
  status: StatusTone;
  throughput: number | null;
  queue_size: number;
  latency_ms: number | null;
  retry_count: number;
  metric_label: string | null;
  metric_value: string | null;
}

export interface PipelineStatus {
  overall: "healthy" | "degraded" | "failed";
  last_heartbeat: string;
  stages: PipelineStage[];
}

export interface KpiDelta {
  value: number;
  delta_pct: number | null;
  delta_label: string | null;
  status: "success" | "warning" | "error" | "neutral";
}

export interface DashboardSummary {
  active_sources: KpiDelta;
  raw_records_today: KpiDelta;
  parsed_outages: KpiDelta;
  duplicates_skipped: KpiDelta;
  failed_tasks: KpiDelta;
  offices_at_risk: KpiDelta;
}

export interface ActivityEvent {
  id: string;
  type:
    | "RawFetched"
    | "RawParsed"
    | "DuplicateSkipped"
    | "TaskFailed"
    | "OfficeImpactDetected"
    | "NotificationEmitted"
    | "PipelineHeartbeat";
  severity: Severity;
  source: string | null;
  message: string;
  at: string;
}

export interface NormalizationQuality {
  average_confidence: number;
  normalized_count: number;
  parsed_total: number;
  high: number;
  medium: number;
  low: number;
  estimated_tokens: number | null;
  estimated_cost_usd: number | null;
}

export interface QueueBacklogPoint {
  at: string;
  pending: number;
  running: number;
  failed: number;
}

export interface ActionResponse {
  ok: boolean;
  message: string;
  task_id: string | null;
  request_id?: string | null;
}

// ── Metrics ───────────────────────────────────────────────────────────────

export interface StageTiming {
  task_type: string;
  count: number;
  avg_ms: number;
  p50_ms: number;
  p95_ms: number;
  max_ms: number;
}

export interface LLMCall {
  id: string;
  model: string;
  prompt_tokens: number;
  completion_tokens: number;
  total_tokens: number;
  duration_ms: number;
  status: string;
  cost_rub: number;
  created_at: string;
}

export interface LLMCostSummary {
  calls_ok: number;
  calls_error: number;
  prompt_tokens: number;
  completion_tokens: number;
  total_tokens: number;
  avg_duration_ms: number;
  max_duration_ms: number;
  prompt_cost_rub: number;
  completion_cost_rub: number;
  total_cost_rub: number;
  prompt_price_per_1k_rub: number;
  completion_price_per_1k_rub: number;
}

export interface NormalizerPathMix {
  automaton: number;
  llm_fallback: number;
  none: number;
  automaton_pct: number;
}

export interface RuntimeMemory {
  process: string;
  rss_mb: number;
  vms_mb: number | null;
  cpu_percent: number | null;
}

export interface PipelineMetrics {
  stage_timings: StageTiming[];
  llm_cost: LLMCostSummary;
  normalizer_path: NormalizerPathMix;
  recent_llm_calls: LLMCall[];
  runtime: RuntimeMemory | null;
  window_hours: number;
}

// Domain/UI types shared by the real and mock API clients.
export interface Office {
  id: string;
  name: string;
  city: string;
  address: string;
  region: string;
  is_active?: boolean;
  latitude?: number | null;
  longitude?: number | null;
}

export interface OfficeImpact {
  id: string;
  office_id: string;
  office_name: string;
  event_id: string;
  impact_start: string;
  impact_end: string | null;
  impact_level: "low" | "medium" | "high";
  match_strategy: string;
  match_score?: number;
  detected_at: string;
}

export type MapOfficeStatus = "ok" | "risk" | "critical";
export type MapImpactSeverity = "low" | "medium" | "high" | "critical" | "unknown";

export interface MapOfficeImpact {
  id: string;
  reason: string | null;
  severity: MapImpactSeverity;
  starts_at: string;
  ends_at: string | null;
  event_type: string | null;
}

export interface MapOffice {
  id: string;
  name: string;
  address: string;
  city: string;
  region: string;
  latitude: number | null;
  longitude: number | null;
  status: MapOfficeStatus;
  active_impacts: MapOfficeImpact[];
}

export interface MapOfficesResponse {
  offices: MapOffice[];
}

export interface Notification {
  id: string;
  office_id: string;
  office_name: string;
  event_id?: string;
  channel: "dashboard" | "telegram" | "email" | "webhook";
  status: "sent" | "queued" | "failed";
  severity?: "low" | "medium" | "high";
  emitted_at: string;
  summary: string;
}

export interface LogLine {
  ts: string;
  level: "DEBUG" | "INFO" | "WARNING" | "ERROR";
  logger: string;
  message: string;
}

export interface ListParams {
  limit?: number;
  offset?: number;
  source_id?: string;
  city?: string;
  status?: TaskStatus | "";
}
