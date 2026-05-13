/**
 * In-memory mock provider. Generates realistic, deterministic-ish data so the
 * dashboard looks alive without a backend. Replace by setting VITE_USE_MOCK=0.
 */
import type { ApiClient } from "./client";
import type {
  ActivityEvent,
  LogLine,
  ListParams,
  MapOffice,
  NormalizedEvent,
  Notification,
  Office,
  OfficeImpact,
  ParsedRecord,
  RawRecord,
  Source,
  Task,
} from "./types";

const wait = (ms = 120) => new Promise((r) => setTimeout(r, ms));
const uuid = () =>
  "xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx".replace(/[xy]/g, (c) => {
    const r = (Math.random() * 16) | 0;
    const v = c === "x" ? r : (r & 0x3) | 0x8;
    return v.toString(16);
  });

const now = () => new Date();
const minutesAgo = (m: number) => new Date(Date.now() - m * 60_000).toISOString();
const hoursAgo = (h: number) => new Date(Date.now() - h * 3_600_000).toISOString();

// ── Static-ish seed data ───────────────────────────────────────────────────

const SOURCES: Source[] = [
  {
    id: "11111111-1111-4111-a111-111111111111",
    name: "Россети Сибирь — плановые отключения",
    url: "https://www.rosseti-sib.ru/.../data.php",
    source_type: "json",
    poll_interval_seconds: 21_600,
    is_active: true,
    parser_profile: { parser: "rosseti_sib", date_filter_days: 4 },
    last_fetch: minutesAgo(18),
    records_in_window: 8124,
    success_rate: 0.98,
    status: "healthy",
    region: "RU-KEM",
    parser: "rosseti_sib",
  },
  {
    id: "22222222-2222-4222-a222-222222222222",
    name: "Россети Томск — плановые отключения",
    url: "https://rosseti-tomsk.ru/.../planovie_otklucheniya.php",
    source_type: "html",
    poll_interval_seconds: 21_600,
    is_active: true,
    parser_profile: {
      parser: "rosseti_tomsk",
      paginate: { param: "PAGEN_1", max_pages: 2 },
      verify_ssl: false,
    },
    last_fetch: minutesAgo(42),
    records_in_window: 12,
    success_rate: 0.92,
    status: "warning",
    region: "RU-TOM",
    parser: "rosseti_tomsk",
  },
  {
    id: "33333333-3333-4333-a333-333333333333",
    name: "eseti.ru — плановые отключения",
    url: "https://www.eseti.ru/.../API/Shutdown",
    source_type: "json",
    poll_interval_seconds: 21_600,
    is_active: true,
    parser_profile: { parser: "eseti", date_filter_days: 4 },
    last_fetch: minutesAgo(8),
    records_in_window: 1948,
    success_rate: 0.99,
    status: "healthy",
    region: "RU-KEM",
    parser: "eseti",
  },
  {
    id: "44444444-4444-4444-a444-444444444444",
    name: "Telegram: kemerovo_admin (черновик)",
    url: "tg://kemerovo_admin",
    source_type: "telegram",
    poll_interval_seconds: 3_600,
    is_active: false,
    parser_profile: { parser: "telegram_llm" },
    last_fetch: null,
    records_in_window: 0,
    success_rate: null,
    status: "inactive",
    region: "RU-KEM",
    parser: "telegram_llm",
  },
];

const CITIES = ["Кемерово", "Новокузнецк", "Ленинск-Кузнецкий", "Прокопьевск", "Белово"];
const STREETS = [
  "ул. Ленина",
  "пр. Советский",
  "ул. Кирова",
  "ул. Тухачевского",
  "ул. Весенняя",
  "ул. 50 лет Октября",
];
const REASONS = [
  "Плановые ремонтные работы",
  "Замена оборудования на ТП",
  "Профилактика линии 10 кВ",
  "Подключение нового потребителя",
];

function buildParsedRecords(n: number): ParsedRecord[] {
  return Array.from({ length: n }, (_, i) => {
    const src = SOURCES[i % 3];
    const startsIn = (i % 12) - 2;
    return {
      id: uuid(),
      raw_record_id: uuid(),
      source_id: src.id,
      external_id: `EXT-${1000 + i}`,
      start_time: new Date(Date.now() + startsIn * 3_600_000).toISOString(),
      end_time: new Date(Date.now() + (startsIn + 4) * 3_600_000).toISOString(),
      city: CITIES[i % CITIES.length],
      district: i % 3 === 0 ? "Центральный р-н" : null,
      street: `${STREETS[i % STREETS.length]}, ${1 + (i % 80)}`,
      region_code: "RU-KEM",
      reason: REASONS[i % REASONS.length],
      extracted_at: minutesAgo(2 + i * 3),
    };
  });
}

const PARSED = buildParsedRecords(60);

const RAW: RawRecord[] = Array.from({ length: 40 }, (_, i) => {
  const src = SOURCES[i % 3];
  return {
    id: uuid(),
    source_id: src.id,
    source_url: src.url,
    source_type: src.source_type,
    content_hash: Math.random().toString(36).slice(2, 10).padEnd(64, "0"),
    fetched_at: minutesAgo(5 + i * 12),
    trace_id: uuid(),
    size_bytes: 4096 + Math.floor(Math.random() * 280_000),
  };
});

const NORMALIZED: NormalizedEvent[] = PARSED.slice(0, 32).map((p, i) => ({
  event_id: uuid(),
  parsed_record_id: p.id,
  event_type: "power_outage",
  start_time: p.start_time ?? new Date().toISOString(),
  end_time: p.end_time,
  location_raw: `${p.city}, ${p.street}`,
  location_normalized: `${p.city}, ${p.street}`,
  city: p.city,
  street: p.street,
  building: null,
  reason: p.reason,
  confidence: [0.92, 0.88, 0.74, 0.61, 0.45][i % 5],
  normalized_at: minutesAgo(4 + i * 5),
}));

const TASKS: Task[] = [
  ...Array.from({ length: 6 }, (_, i) => ({
    id: uuid(),
    task_type: ["fetch_source", "parse_content", "normalize_event"][i % 3],
    status: "failed" as const,
    attempt: 2 + (i % 3),
    input_hash: Math.random().toString(36).slice(2, 12).padEnd(64, "0"),
    error:
      i % 2
        ? "httpx.ConnectTimeout: timeout=10s connecting to host"
        : "GigaChatHTTPError 503 — service temporarily unavailable",
    trace_id: uuid(),
    created_at: hoursAgo(i + 1),
    updated_at: minutesAgo(15 + i * 7),
    next_retry_at: minutesAgo(-(5 + i)),
    source_id: SOURCES[i % 3].id,
  })),
  ...Array.from({ length: 10 }, (_, i) => ({
    id: uuid(),
    task_type: ["fetch_source", "parse_content", "normalize_event"][i % 3],
    status: (["done", "running", "pending", "done"] as const)[i % 4],
    attempt: 1,
    input_hash: Math.random().toString(36).slice(2, 12).padEnd(64, "0"),
    error: null,
    trace_id: uuid(),
    created_at: hoursAgo(i),
    updated_at: minutesAgo(3 + i * 4),
    next_retry_at: null,
    source_id: SOURCES[i % 3].id,
  })),
];

const OFFICES: Office[] = [
  {
    id: "of-1",
    name: "Кемерово, головной офис",
    city: "Кемерово",
    address: "ул. Кирова, 12",
    region: "RU-KEM",
    latitude: 55.3549,
    longitude: 86.0873,
  },
  {
    id: "of-2",
    name: "Кемерово, склад №1",
    city: "Кемерово",
    address: "пр. Советский, 47",
    region: "RU-KEM",
    latitude: 55.3552,
    longitude: 86.0918,
  },
  {
    id: "of-3",
    name: "Новокузнецк, операционный офис",
    city: "Новокузнецк",
    address: "ул. Тухачевского, 5",
    region: "RU-KEM",
    latitude: 53.7557,
    longitude: 87.1099,
  },
];

const OFFICE_IMPACTS: OfficeImpact[] = [
  {
    id: uuid(),
    office_id: "of-1",
    office_name: OFFICES[0].name,
    event_id: NORMALIZED[0].event_id,
    impact_start: NORMALIZED[0].start_time,
    impact_end: NORMALIZED[0].end_time,
    impact_level: "high",
    match_strategy: "exact_address",
    detected_at: minutesAgo(6),
  },
  {
    id: uuid(),
    office_id: "of-3",
    office_name: OFFICES[2].name,
    event_id: NORMALIZED[2].event_id,
    impact_start: NORMALIZED[2].start_time,
    impact_end: NORMALIZED[2].end_time,
    impact_level: "medium",
    match_strategy: "geo_radius",
    detected_at: minutesAgo(22),
  },
];

const MAP_OFFICES: MapOffice[] = [
  {
    id: OFFICES[0].id,
    name: OFFICES[0].name,
    city: OFFICES[0].city,
    address: OFFICES[0].address,
    region: OFFICES[0].region,
    latitude: OFFICES[0].latitude ?? null,
    longitude: OFFICES[0].longitude ?? null,
    status: "critical",
    active_impacts: [
      {
        id: OFFICE_IMPACTS[0].id,
        reason: "Planned power outage on the office feeder",
        severity: "high",
        starts_at: OFFICE_IMPACTS[0].impact_start,
        ends_at: OFFICE_IMPACTS[0].impact_end,
        event_type: "power_outage",
      },
    ],
  },
  {
    id: OFFICES[1].id,
    name: OFFICES[1].name,
    city: OFFICES[1].city,
    address: OFFICES[1].address,
    region: OFFICES[1].region,
    latitude: OFFICES[1].latitude ?? null,
    longitude: OFFICES[1].longitude ?? null,
    status: "ok",
    active_impacts: [],
  },
  {
    id: OFFICES[2].id,
    name: OFFICES[2].name,
    city: OFFICES[2].city,
    address: OFFICES[2].address,
    region: OFFICES[2].region,
    latitude: OFFICES[2].latitude ?? null,
    longitude: OFFICES[2].longitude ?? null,
    status: "risk",
    active_impacts: [
      {
        id: OFFICE_IMPACTS[1].id,
        reason: "Maintenance work near the office address",
        severity: "medium",
        starts_at: OFFICE_IMPACTS[1].impact_start,
        ends_at: OFFICE_IMPACTS[1].impact_end,
        event_type: "maintenance",
      },
    ],
  },
  {
    id: "of-4",
    name: "Address-only office",
    city: "Томск",
    address: "ул. Учебная, 3",
    region: "RU-TOM",
    latitude: null,
    longitude: null,
    status: "ok",
    active_impacts: [],
  },
];

const NOTIFICATIONS: Notification[] = [
  {
    id: uuid(),
    office_id: "of-1",
    office_name: OFFICES[0].name,
    channel: "telegram",
    status: "sent",
    emitted_at: minutesAgo(4),
    summary: "Плановое отключение завтра 09:00–13:00 на ул. Кирова",
  },
  {
    id: uuid(),
    office_id: "of-3",
    office_name: OFFICES[2].name,
    channel: "telegram",
    status: "queued",
    emitted_at: minutesAgo(22),
    summary: "Затронут офис: Новокузнецк",
  },
  {
    id: uuid(),
    office_id: "of-2",
    office_name: OFFICES[1].name,
    channel: "email",
    status: "failed",
    emitted_at: hoursAgo(2),
    summary: "SMTP 421 — повторная попытка через 5 мин",
  },
];

const LOGS: LogLine[] = Array.from({ length: 40 }, (_, i) => {
  const levels: LogLine["level"][] = ["INFO", "INFO", "DEBUG", "WARNING", "ERROR"];
  const logger = [
    "app.workers.collector",
    "app.workers.parser",
    "app.normalization.llm",
    "app.workers.dispatcher",
    "app.workers.scheduler",
  ][i % 5];
  const level = levels[i % levels.length];
  const message =
    level === "ERROR"
      ? "GigaChatHTTPError 503 — service temporarily unavailable"
      : level === "WARNING"
        ? "Source returned 0 records in window — check parser_profile.date_filter_days"
        : `Processed task ${uuid().slice(0, 8)} attempt=1 status=done`;
  return { ts: minutesAgo(i * 2 + 1), level, logger, message };
});

function paginate<T>(rows: T[], { limit, offset }: ListParams = {}): T[] {
  const o = offset ?? 0;
  const l = limit ?? 50;
  return rows.slice(o, o + l);
}

export const mockClient: ApiClient = {
  async getDashboardSummary() {
    await wait();
    return {
      active_sources: { value: SOURCES.filter((s) => s.is_active).length, delta_pct: null, delta_label: null, status: "success" },
      raw_records_today: {
        value: RAW.length * 6,
        delta_pct: 12.4,
        delta_label: "+12.4% vs prev 24h",
        status: "success",
      },
      parsed_outages: {
        value: PARSED.length,
        delta_pct: 3.1,
        delta_label: "+3.1% vs prev 24h",
        status: "success",
      },
      duplicates_skipped: { value: 17, delta_pct: -8.0, delta_label: "-8% vs prev 24h", status: "neutral" },
      failed_tasks: { value: TASKS.filter((t) => t.status === "failed").length, delta_pct: null, delta_label: "DLQ", status: "error" },
      offices_at_risk: {
        value: MAP_OFFICES.filter((office) => office.status !== "ok").length,
        delta_pct: null,
        delta_label: "active now",
        status: "warning",
      },
    };
  },
  async getPipelineStatus() {
    await wait();
    return {
      overall: "healthy",
      last_heartbeat: now().toISOString(),
      stages: [
        { key: "scheduler", label: "Scheduler", status: "healthy", throughput: 0.5, queue_size: 0, latency_ms: null, retry_count: 0, metric_label: "tick", metric_value: "every 6h" },
        { key: "collector", label: "Collector", status: "healthy", throughput: 1.2, queue_size: 1, latency_ms: 320, retry_count: 0, metric_label: "fetched", metric_value: "40" },
        { key: "parser", label: "Parser", status: "running", throughput: 4.4, queue_size: 3, latency_ms: 90, retry_count: 0, metric_label: "parsed", metric_value: String(PARSED.length) },
        { key: "normalizer", label: "LLM Normalizer", status: "running", throughput: 0.9, queue_size: 2, latency_ms: 1240, retry_count: 1, metric_label: "normalized", metric_value: String(NORMALIZED.length) },
        { key: "dedup", label: "Dedup Engine", status: "pending", throughput: null, queue_size: 0, latency_ms: null, retry_count: 0, metric_label: "skipped", metric_value: "17" },
        { key: "matcher", label: "Office Matcher", status: "pending", throughput: null, queue_size: 0, latency_ms: null, retry_count: 0, metric_label: "matched", metric_value: String(OFFICE_IMPACTS.length) },
        { key: "notifier", label: "Notifier", status: "pending", throughput: null, queue_size: 0, latency_ms: null, retry_count: 1, metric_label: "sent", metric_value: String(NOTIFICATIONS.filter((n) => n.status === "sent").length) },
      ],
    };
  },
  async getActivity(limit = 30) {
    await wait();
    const events: ActivityEvent[] = [
      ...RAW.slice(0, 10).map((r) => ({
        id: `raw-${r.id}`,
        type: "RawFetched" as const,
        severity: "info" as const,
        source: SOURCES.find((s) => s.id === r.source_id)?.name ?? null,
        message: `Fetched ${r.size_bytes.toLocaleString()} bytes from ${r.source_type}`,
        at: r.fetched_at,
      })),
      ...PARSED.slice(0, 12).map((p) => ({
        id: `parsed-${p.id}`,
        type: "RawParsed" as const,
        severity: "success" as const,
        source: SOURCES.find((s) => s.id === p.source_id)?.name ?? null,
        message: `Parsed outage @ ${p.city}, ${p.street}`,
        at: p.extracted_at,
      })),
      ...NORMALIZED.slice(0, 6).map((n) => ({
        id: `norm-${n.event_id}`,
        type: "OfficeImpactDetected" as const,
        severity: n.confidence < 0.6 ? ("warning" as const) : ("success" as const),
        source: null,
        message: `Normalized: ${n.location_normalized} (conf ${(n.confidence * 100).toFixed(0)}%)`,
        at: n.normalized_at,
      })),
      ...TASKS.filter((t) => t.status === "failed")
        .slice(0, 4)
        .map((t) => ({
          id: `task-${t.id}`,
          type: "TaskFailed" as const,
          severity: "error" as const,
          source: t.task_type,
          message: t.error ?? "task failed",
          at: t.updated_at,
        })),
      {
        id: `hb-${uuid()}`,
        type: "DuplicateSkipped",
        severity: "info",
        source: "dedup",
        message: "Skipped 3 duplicate events (same address + window)",
        at: minutesAgo(7),
      },
    ];
    events.sort((a, b) => +new Date(b.at) - +new Date(a.at));
    return events.slice(0, limit);
  },
  async getNormalizationQuality() {
    await wait();
    const high = NORMALIZED.filter((n) => n.confidence >= 0.8).length;
    const medium = NORMALIZED.filter((n) => n.confidence >= 0.5 && n.confidence < 0.8).length;
    const low = NORMALIZED.filter((n) => n.confidence < 0.5).length;
    const avg = NORMALIZED.reduce((s, n) => s + n.confidence, 0) / NORMALIZED.length;
    return {
      average_confidence: Number(avg.toFixed(3)),
      normalized_count: NORMALIZED.length,
      parsed_total: PARSED.length,
      high,
      medium,
      low,
      estimated_tokens: 184_320,
      estimated_cost_usd: 0.42,
    };
  },
  async getQueueBacklog() {
    await wait();
    const base = [4, 6, 9, 7, 5, 4, 3, 3, 5, 8, 11, 14, 12, 9, 7, 6, 5, 4, 4, 5, 6, 5, 4, 3];
    return base.map((v, i) => ({
      at: new Date(Date.now() - (23 - i) * 3_600_000).toISOString(),
      pending: v,
      running: Math.max(0, Math.floor(v / 3)),
      failed: Math.max(0, Math.floor(v / 7)),
    }));
  },
  async listSources() {
    await wait();
    return SOURCES;
  },
  async pollSource(id: string) {
    await wait(400);
    const s = SOURCES.find((x) => x.id === id);
    if (!s) throw new Error("source not found");
    return { ok: true, message: `Poll scheduled for ${s.name}`, task_id: uuid() };
  },
  async listRaw(params) {
    await wait();
    let rows = RAW;
    if (params?.source_id) rows = rows.filter((r) => r.source_id === params.source_id);
    return paginate(rows, params);
  },
  async listParsed(params) {
    await wait();
    let rows = PARSED;
    if (params?.source_id) rows = rows.filter((r) => r.source_id === params.source_id);
    if (params?.city) rows = rows.filter((r) => (r.city || "").toLowerCase().includes(params.city!.toLowerCase()));
    return paginate(rows, params);
  },
  async listNormalized(params) {
    await wait();
    return paginate(NORMALIZED, params);
  },
  async listTasks(params) {
    await wait();
    let rows = TASKS;
    if (params?.status) rows = rows.filter((t) => t.status === params.status);
    return paginate(rows, params);
  },
  async retryTask(id: string) {
    await wait(300);
    return { ok: true, message: "Retry scheduled", task_id: id };
  },
  async listOffices() {
    await wait();
    return OFFICES;
  },
  async listOfficeImpacts() {
    await wait();
    return OFFICE_IMPACTS;
  },
  async getMapOffices() {
    await wait();
    return { offices: MAP_OFFICES };
  },
  async listNotifications() {
    await wait();
    return NOTIFICATIONS;
  },
  async listLogs() {
    await wait();
    return LOGS;
  },
};

export const mockLogs = LOGS;
