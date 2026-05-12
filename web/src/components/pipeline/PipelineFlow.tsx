import { ChevronRight, Loader2 } from "lucide-react";
import type { PipelineStage } from "@/lib/api/types";
import { StatusDot } from "@/components/ui/Badge";
import { statusTone } from "@/components/ui/statusTone";
import { fmtNumber } from "@/lib/format";

function StageCard({ s }: { s: PipelineStage }) {
  const tone = statusTone(s.status);
  return (
    <div
      className={`flex min-w-[170px] flex-col gap-2 rounded-lg border p-3 transition-colors ${
        s.status === "running"
          ? "border-accent-teal/40 bg-accent-teal/5"
          : s.status === "failed"
            ? "border-accent-red/40 bg-accent-red/5"
            : "border-line bg-bg-elevated"
      }`}
    >
      <div className="flex items-center gap-2">
        <StatusDot tone={tone} pulse={s.status === "running"} />
        <span className="text-sm font-medium text-ink">{s.label}</span>
        {s.status === "running" && <Loader2 size={12} className="ml-auto animate-spin text-accent-teal" />}
      </div>
      <div className="grid grid-cols-2 gap-x-2 gap-y-1 text-2xs text-ink-muted">
        <span>queue</span>
        <span className="text-right font-mono text-ink">{fmtNumber(s.queue_size)}</span>
        {s.latency_ms != null && (
          <>
            <span>latency</span>
            <span className="text-right font-mono text-ink">{s.latency_ms}ms</span>
          </>
        )}
        {s.retry_count > 0 && (
          <>
            <span>retries</span>
            <span className="text-right font-mono text-accent-amber">{s.retry_count}</span>
          </>
        )}
        {s.metric_label && s.metric_value && (
          <>
            <span>{s.metric_label}</span>
            <span className="text-right font-mono text-ink">{s.metric_value}</span>
          </>
        )}
      </div>
    </div>
  );
}

export function PipelineFlow({ stages }: { stages: PipelineStage[] }) {
  return (
    <div className="flex items-stretch gap-2 overflow-x-auto pb-1">
      {stages.map((s, i) => (
        <div key={s.key} className="flex items-center gap-2">
          <StageCard s={s} />
          {i < stages.length - 1 && (
            <ChevronRight size={16} className="shrink-0 text-ink-dim" strokeWidth={1.5} />
          )}
        </div>
      ))}
    </div>
  );
}
