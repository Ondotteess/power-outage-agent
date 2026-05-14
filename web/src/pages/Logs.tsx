import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { Card, CardHeader } from "@/components/ui/Card";
import { PageHeader } from "@/components/ui/PageHeader";
import { Badge } from "@/components/ui/Badge";
import { fmtDate } from "@/lib/format";
import type { LogLine } from "@/lib/api/types";

const LEVEL_TONE: Record<LogLine["level"], "gray" | "blue" | "amber" | "red"> = {
  DEBUG: "gray",
  INFO: "blue",
  WARNING: "amber",
  ERROR: "red",
};

export function Logs() {
  const [level, setLevel] = useState<string>("");
  const { data, isLoading, error } = useQuery({ queryKey: ["logs"], queryFn: () => api.listLogs() });

  const rows = (data ?? []).filter((l) => !level || l.level === level);

  return (
    <div className="space-y-6">
      <PageHeader
        title="Logs"
        description="Structured events persisted by the pipeline worker."
        actions={
          <select className="input !h-8 !text-xs" value={level} onChange={(e) => setLevel(e.target.value)}>
            <option value="">All levels</option>
            <option>DEBUG</option>
            <option>INFO</option>
            <option>WARNING</option>
            <option>ERROR</option>
          </select>
        }
      />
      <Card>
        <CardHeader title="Tail" subtitle={`${rows.length} lines`} />
        {isLoading ? (
          <div className="p-4 text-sm text-ink-muted">Loading…</div>
        ) : error ? (
          <div className="p-4 text-sm text-accent-red">{(error as Error).message}</div>
        ) : (
          <pre className="max-h-[640px] overflow-y-auto bg-bg-subtle px-4 py-3 font-mono text-2xs leading-5 text-ink">
            {rows.map((l, i) => (
              <div key={i} className="flex gap-3">
                <span className="text-ink-dim">{fmtDate(l.ts)}</span>
                <Badge tone={LEVEL_TONE[l.level]} className="!py-0">{l.level}</Badge>
                <span className="text-accent-teal/80">{l.logger}</span>
                <span className="text-ink">{l.message}</span>
              </div>
            ))}
          </pre>
        )}
      </Card>
    </div>
  );
}
