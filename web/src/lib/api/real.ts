/**
 * Real backend client — talks to FastAPI at /api/*.
 * Logs fall back to mock data until the backend exposes a log stream.
 */
import type { ApiClient } from "./client";
import { mockLogs } from "./mock";
import type { ListParams, Office, OfficeImpact } from "./types";

const BASE = "/api";

async function get<T>(path: string, params?: Record<string, unknown>): Promise<T> {
  const qs = params
    ? "?" +
      Object.entries(params)
        .filter(([, v]) => v !== undefined && v !== null && v !== "")
        .map(([k, v]) => `${encodeURIComponent(k)}=${encodeURIComponent(String(v))}`)
        .join("&")
    : "";
  const res = await fetch(`${BASE}${path}${qs}`);
  if (!res.ok) throw new Error(`${res.status} ${res.statusText} — ${path}`);
  return res.json() as Promise<T>;
}

async function post<T>(path: string): Promise<T> {
  const res = await fetch(`${BASE}${path}`, { method: "POST" });
  if (!res.ok) throw new Error(`${res.status} ${res.statusText} — ${path}`);
  return res.json() as Promise<T>;
}

function listP(p?: ListParams) {
  return p
    ? {
        limit: p.limit,
        offset: p.offset,
        source_id: p.source_id,
        city: p.city,
        status: p.status,
      }
    : undefined;
}

export const realClient: ApiClient = {
  getDashboardSummary: () => get("/dashboard/summary"),
  getPipelineStatus: () => get("/pipeline/status"),
  getActivity: (limit) => get("/dashboard/activity", { limit }),
  getNormalizationQuality: () => get("/dashboard/normalization-quality"),
  getQueueBacklog: () => get("/dashboard/queue-backlog"),

  listSources: () => get("/sources"),
  pollSource: (id) => post(`/sources/${id}/poll`),

  listRaw: (p) => get("/raw", listP(p)),
  listParsed: (p) => get("/parsed", listP(p)),
  listNormalized: (p) => get("/normalized", listP(p)),

  listTasks: (p) => get("/tasks", listP(p)),
  retryTask: (id) => post(`/tasks/${id}/retry`),

  listOffices: () => get<Office[]>("/offices"),
  listOfficeImpacts: () => get<OfficeImpact[]>("/office-impacts"),
  listNotifications: () => get("/notifications"),
  async listLogs() {
    return mockLogs;
  },
};
