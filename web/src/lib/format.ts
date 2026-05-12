export function fmtNumber(n: number | null | undefined): string {
  if (n == null) return "—";
  return new Intl.NumberFormat("en-US").format(n);
}

export function fmtBytes(n: number | null | undefined): string {
  if (n == null) return "—";
  if (n < 1024) return `${n} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`;
  return `${(n / 1024 / 1024).toFixed(2)} MB`;
}

export function fmtDate(iso: string | null | undefined): string {
  if (!iso) return "—";
  const d = new Date(iso);
  return d.toLocaleString("en-GB", {
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
  });
}

export function fmtTime(iso: string | null | undefined): string {
  if (!iso) return "—";
  return new Date(iso).toLocaleTimeString("en-GB", { hour: "2-digit", minute: "2-digit" });
}

export function fmtRelative(iso: string | null | undefined): string {
  if (!iso) return "—";
  const diffMs = Date.now() - new Date(iso).getTime();
  const abs = Math.abs(diffMs);
  const fmt = (n: number, unit: string) => `${diffMs >= 0 ? "" : "in "}${Math.round(n)}${unit}${diffMs >= 0 ? " ago" : ""}`;
  if (abs < 60_000) return "just now";
  if (abs < 3_600_000) return fmt(abs / 60_000, "m");
  if (abs < 86_400_000) return fmt(abs / 3_600_000, "h");
  return fmt(abs / 86_400_000, "d");
}

export function fmtInterval(seconds: number): string {
  if (seconds < 60) return `${seconds}s`;
  if (seconds < 3600) return `${Math.round(seconds / 60)}m`;
  return `${Math.round(seconds / 3600)}h`;
}

export function fmtConfidence(c: number): string {
  return `${(c * 100).toFixed(0)}%`;
}

export function truncate(s: string | null | undefined, n: number): string {
  if (!s) return "—";
  return s.length <= n ? s : s.slice(0, n - 1) + "…";
}
