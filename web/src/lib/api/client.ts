/**
 * Provider interface — both `mock.ts` and `real.ts` implement it.
 * Switch via VITE_USE_MOCK.
 */
import type {
  ActionResponse,
  ActivityEvent,
  DashboardSummary,
  LogLine,
  ListParams,
  MapOfficesResponse,
  NormalizationQuality,
  NormalizedEvent,
  Notification,
  Office,
  OfficeImpact,
  ParsedRecord,
  PipelineStatus,
  QueueBacklogPoint,
  RawRecord,
  Source,
  Task,
} from "./types";

export interface ApiClient {
  // Dashboard / pipeline
  getDashboardSummary(): Promise<DashboardSummary>;
  getPipelineStatus(): Promise<PipelineStatus>;
  getActivity(limit?: number): Promise<ActivityEvent[]>;
  getNormalizationQuality(): Promise<NormalizationQuality>;
  getQueueBacklog(): Promise<QueueBacklogPoint[]>;

  // Sources
  listSources(): Promise<Source[]>;
  pollSource(id: string): Promise<ActionResponse>;

  // Records
  listRaw(params?: ListParams): Promise<RawRecord[]>;
  listParsed(params?: ListParams): Promise<ParsedRecord[]>;
  listNormalized(params?: ListParams): Promise<NormalizedEvent[]>;

  // Tasks / DLQ
  listTasks(params?: ListParams): Promise<Task[]>;
  retryTask(id: string): Promise<ActionResponse>;

  // Office matcher
  listOffices(): Promise<Office[]>;
  listOfficeImpacts(): Promise<OfficeImpact[]>;
  getMapOffices(): Promise<MapOfficesResponse>;

  // Logs still use mock data in the real client.
  listNotifications(): Promise<Notification[]>;
  listLogs(): Promise<LogLine[]>;
}
