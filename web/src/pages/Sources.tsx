import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Loader2, RefreshCw } from "lucide-react";
import { api } from "@/lib/api";
import { Card, CardHeader } from "@/components/ui/Card";
import { DataTable, Column } from "@/components/ui/DataTable";
import { Badge, StatusBadge } from "@/components/ui/Badge";
import { PageHeader } from "@/components/ui/PageHeader";
import { fmtInterval, fmtNumber, fmtRelative, truncate } from "@/lib/format";
import type { Source } from "@/lib/api/types";

function PollButton({ source }: { source: Source }) {
  const qc = useQueryClient();
  const m = useMutation({
    mutationFn: () => api.pollSource(source.id),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["sources"] }),
  });
  return (
    <button
      className="btn btn-primary !py-1 !text-xs"
      onClick={() => m.mutate()}
      disabled={m.isPending || !source.is_active}
    >
      {m.isPending ? <Loader2 size={12} className="animate-spin" /> : <RefreshCw size={12} />}
      Poll
    </button>
  );
}

export function Sources() {
  const { data, isLoading, error } = useQuery({ queryKey: ["sources"], queryFn: () => api.listSources() });

  const cols: Column<Source>[] = [
    { key: "name", header: "Source", cell: (r) => <span className="text-ink">{r.name}</span> },
    { key: "type", header: "Type", cell: (r) => <Badge tone="blue">{r.source_type}</Badge> },
    { key: "region", header: "Region", cell: (r) => r.region ?? "—" },
    { key: "interval", header: "Poll", cell: (r) => <span className="font-mono text-xs">{fmtInterval(r.poll_interval_seconds)}</span> },
    { key: "parser", header: "Parser", cell: (r) => <span className="font-mono text-xs text-ink-muted">{r.parser ?? "—"}</span> },
    { key: "url", header: "URL", cell: (r) => <span className="font-mono text-xs text-ink-muted">{truncate(r.url, 40)}</span> },
    { key: "last", header: "Last fetch", cell: (r) => <span className="text-xs text-ink-muted" title={r.last_fetch ?? ""}>{fmtRelative(r.last_fetch)}</span> },
    { key: "win", header: "Records 24h", cell: (r) => <span className="font-mono">{fmtNumber(r.records_in_window)}</span> },
    { key: "ok", header: "Success", cell: (r) => (r.success_rate == null ? "—" : `${(r.success_rate * 100).toFixed(0)}%`) },
    { key: "status", header: "Status", cell: (r) => <StatusBadge status={r.status} /> },
    { key: "actions", header: "", cell: (r) => <PollButton source={r} /> },
  ];

  return (
    <div className="space-y-6">
      <PageHeader title="Sources" description="Configured polling sources and their recent performance." />
      <Card>
        <CardHeader title="All sources" subtitle={data ? `${data.length} configured` : "—"} />
        <DataTable columns={cols} rows={data} isLoading={isLoading} error={error} rowKey={(r) => r.id} />
      </Card>
    </div>
  );
}
