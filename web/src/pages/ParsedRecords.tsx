import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { Card, CardHeader } from "@/components/ui/Card";
import { DataTable, Column } from "@/components/ui/DataTable";
import { Badge } from "@/components/ui/Badge";
import { PageHeader } from "@/components/ui/PageHeader";
import { fmtDate, truncate } from "@/lib/format";
import type { ParsedRecord } from "@/lib/api/types";

export function ParsedRecords() {
  const [city, setCity] = useState("");
  const { data, isLoading, error } = useQuery({
    queryKey: ["parsed", { limit: 200, city }],
    queryFn: () => api.listParsed({ limit: 200, city }),
  });

  const cols: Column<ParsedRecord>[] = [
    { key: "date", header: "Date", cell: (r) => <span className="font-mono text-xs">{fmtDate(r.start_time).split(",")[0]}</span> },
    { key: "time", header: "Time", cell: (r) => <span className="font-mono text-xs">{fmtDate(r.start_time).split(",")[1]?.trim()}</span> },
    { key: "city", header: "City", cell: (r) => r.city ?? "—" },
    { key: "addr", header: "Address", cell: (r) => <span className="text-ink-muted">{truncate(r.street, 40)}</span> },
    { key: "reason", header: "Reason", cell: (r) => <span className="text-ink-muted">{truncate(r.reason, 40)}</span> },
    { key: "src", header: "Source", cell: (r) => <span className="font-mono text-2xs text-ink-muted">{r.source_id?.slice(0, 8) ?? "—"}</span> },
    { key: "ext", header: "External ID", cell: (r) => <Badge tone="gray">{r.external_id ?? "—"}</Badge> },
  ];

  return (
    <div className="space-y-6">
      <PageHeader
        title="Parsed records"
        description="Structured outage entries extracted from raw payloads, prior to LLM normalization."
        actions={
          <input
            className="input !h-8 !text-xs"
            placeholder="Filter by city…"
            value={city}
            onChange={(e) => setCity(e.target.value)}
          />
        }
      />
      <Card>
        <CardHeader title="All parsed records" subtitle={data ? `${data.length} shown` : "—"} />
        <DataTable columns={cols} rows={data} isLoading={isLoading} error={error} rowKey={(r) => r.id} />
      </Card>
    </div>
  );
}
