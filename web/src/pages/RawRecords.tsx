import { useQuery } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { Card, CardHeader } from "@/components/ui/Card";
import { DataTable, Column } from "@/components/ui/DataTable";
import { Badge } from "@/components/ui/Badge";
import { PageHeader } from "@/components/ui/PageHeader";
import { fmtBytes, fmtDate, truncate } from "@/lib/format";
import type { RawRecord } from "@/lib/api/types";

export function RawRecords() {
  const { data, isLoading, error } = useQuery({
    queryKey: ["raw", { limit: 200 }],
    queryFn: () => api.listRaw({ limit: 200 }),
  });

  const cols: Column<RawRecord>[] = [
    { key: "fetched", header: "Fetched at", cell: (r) => <span className="font-mono text-xs">{fmtDate(r.fetched_at)}</span> },
    { key: "type", header: "Type", cell: (r) => <Badge tone="blue">{r.source_type}</Badge> },
    { key: "url", header: "Source URL", cell: (r) => <span className="font-mono text-xs text-ink-muted">{truncate(r.source_url, 64)}</span> },
    { key: "size", header: "Size", cell: (r) => <span className="font-mono text-xs">{fmtBytes(r.size_bytes)}</span> },
    { key: "hash", header: "Hash", cell: (r) => <span className="font-mono text-2xs text-ink-muted">{r.content_hash.slice(0, 12)}…</span> },
    { key: "trace", header: "Trace", cell: (r) => <span className="font-mono text-2xs text-ink-muted">{r.trace_id.slice(0, 8)}</span> },
  ];

  return (
    <div className="space-y-6">
      <PageHeader title="Raw records" description="Unprocessed responses from sources, content-hashed for dedup." />
      <Card>
        <CardHeader title="All raw records" subtitle={data ? `${data.length} loaded` : "—"} />
        <DataTable columns={cols} rows={data} isLoading={isLoading} error={error} rowKey={(r) => r.id} />
      </Card>
    </div>
  );
}
