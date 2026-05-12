import type { ActivityEvent } from "@/lib/api/types";
import { StatusDot, statusTone } from "@/components/ui/Badge";
import { fmtRelative } from "@/lib/format";

const TYPE_LABEL: Record<ActivityEvent["type"], string> = {
  RawFetched: "Raw fetched",
  RawParsed: "Parsed",
  DuplicateSkipped: "Dedup skip",
  TaskFailed: "Task failed",
  OfficeImpactDetected: "Office impact",
  PipelineHeartbeat: "Heartbeat",
};

export function ActivityFeed({ events }: { events: ActivityEvent[] }) {
  if (!events.length) {
    return <div className="p-6 text-center text-sm text-ink-muted">No recent activity</div>;
  }
  return (
    <ul className="divide-y divide-line/60">
      {events.map((e) => (
        <li key={e.id} className="flex items-start gap-3 px-4 py-3">
          <div className="mt-1.5">
            <StatusDot tone={statusTone(e.severity)} />
          </div>
          <div className="min-w-0 flex-1">
            <div className="flex items-baseline gap-2">
              <span className="text-xs font-medium text-ink">{TYPE_LABEL[e.type]}</span>
              {e.source && <span className="truncate text-2xs text-ink-muted">· {e.source}</span>}
              <span className="ml-auto shrink-0 text-2xs text-ink-dim" title={e.at}>
                {fmtRelative(e.at)}
              </span>
            </div>
            <div className="mt-0.5 truncate text-xs text-ink-muted" title={e.message}>
              {e.message}
            </div>
          </div>
        </li>
      ))}
    </ul>
  );
}
