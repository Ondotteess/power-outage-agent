export type Tone = "teal" | "green" | "amber" | "red" | "gray" | "blue";

const STATUS_TONE: Record<string, Tone> = {
  healthy: "green",
  running: "teal",
  pending: "gray",
  failed: "red",
  warning: "amber",
  degraded: "amber",
  inactive: "gray",
  done: "green",
  success: "green",
  info: "blue",
  error: "red",
  neutral: "gray",
  sent: "green",
  queued: "amber",
  unknown: "gray",
};

export function statusTone(status: string): Tone {
  return STATUS_TONE[status] ?? "gray";
}
