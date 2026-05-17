import type { QueueBacklogPoint } from "@/lib/api/types";

const WIDTH = 640;
const HEIGHT = 180;
const PADDING = 16;

type SeriesKey = "pending" | "running" | "failed";

const SERIES: { key: SeriesKey; label: string; color: string }[] = [
  { key: "pending", label: "pending", color: "#111111" },
  { key: "running", label: "running", color: "#16A34A" },
  { key: "failed", label: "failed", color: "#DC2626" },
];

export function QueueBacklogChart({ data }: { data: QueueBacklogPoint[] }) {
  const rows = data.slice(-24);
  const maxValue = Math.max(
    1,
    ...rows.flatMap((point) => [point.pending, point.running, point.failed]),
  );

  if (rows.length === 0) {
    return <div className="grid h-48 place-items-center text-sm text-ink-muted">No backlog data</div>;
  }

  return (
    <div className="h-48 w-full">
      <svg
        viewBox={`0 0 ${WIDTH} ${HEIGHT}`}
        className="h-full w-full overflow-visible"
        role="img"
        aria-label="Queue backlog"
      >
        {[0.25, 0.5, 0.75].map((ratio) => (
          <line
            key={ratio}
            x1={PADDING}
            x2={WIDTH - PADDING}
            y1={PADDING + (HEIGHT - PADDING * 2) * ratio}
            y2={PADDING + (HEIGHT - PADDING * 2) * ratio}
            stroke="#E5E7EB"
            strokeWidth="1"
          />
        ))}
        {SERIES.map((series) => (
          <polyline
            key={series.key}
            points={points(rows, series.key, maxValue)}
            fill="none"
            stroke={series.color}
            strokeLinecap="round"
            strokeLinejoin="round"
            strokeWidth="2"
          />
        ))}
      </svg>
      <div className="mt-2 flex flex-wrap gap-4 text-xs text-ink-muted">
        {SERIES.map((series) => (
          <span key={series.key} className="inline-flex items-center gap-2">
            <span className="h-2 w-2 rounded-full" style={{ background: series.color }} />
            {series.label}
          </span>
        ))}
      </div>
    </div>
  );
}

function points(rows: QueueBacklogPoint[], key: SeriesKey, maxValue: number): string {
  const innerWidth = WIDTH - PADDING * 2;
  const innerHeight = HEIGHT - PADDING * 2;
  return rows
    .map((row, index) => {
      const x = PADDING + (rows.length === 1 ? 0 : (index / (rows.length - 1)) * innerWidth);
      const y = PADDING + innerHeight - (row[key] / maxValue) * innerHeight;
      return `${x.toFixed(1)},${y.toFixed(1)}`;
    })
    .join(" ");
}
