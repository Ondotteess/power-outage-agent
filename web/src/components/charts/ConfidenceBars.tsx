export function ConfidenceBars({
  high,
  medium,
  low,
}: {
  high: number;
  medium: number;
  low: number;
}) {
  const rows = [
    { name: "low <50%", value: low, className: "bg-accent-red" },
    { name: "medium 50-80%", value: medium, className: "bg-accent-amber" },
    { name: "high >=80%", value: high, className: "bg-accent-green" },
  ];
  const maxValue = Math.max(1, ...rows.map((row) => row.value));

  return (
    <div className="space-y-3">
      {rows.map((row) => (
        <div key={row.name} className="grid grid-cols-[110px_minmax(0,1fr)_44px] items-center gap-3">
          <div className="truncate text-xs text-ink-muted">{row.name}</div>
          <div className="h-2 overflow-hidden rounded-sm bg-bg-muted">
            <div
              className={`h-full rounded-sm ${row.className}`}
              style={{ width: `${Math.max(4, (row.value / maxValue) * 100)}%` }}
            />
          </div>
          <div className="text-right font-mono text-xs text-ink">{row.value}</div>
        </div>
      ))}
    </div>
  );
}
