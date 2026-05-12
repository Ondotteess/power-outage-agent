import { useQuery } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { Card, CardBody, CardHeader } from "@/components/ui/Card";
import { DataTable, Column } from "@/components/ui/DataTable";
import { Badge } from "@/components/ui/Badge";
import { PageHeader } from "@/components/ui/PageHeader";
import { ConfidenceBars } from "@/components/charts/ConfidenceBars";
import { fmtConfidence, fmtDate, fmtNumber, truncate } from "@/lib/format";
import type { NormalizedEvent } from "@/lib/api/types";

function confTone(c: number) {
  if (c >= 0.8) return "green" as const;
  if (c >= 0.5) return "amber" as const;
  return "red" as const;
}

export function Normalization() {
  const events = useQuery({ queryKey: ["normalized", { limit: 200 }], queryFn: () => api.listNormalized({ limit: 200 }) });
  const quality = useQuery({ queryKey: ["quality"], queryFn: () => api.getNormalizationQuality() });

  const cols: Column<NormalizedEvent>[] = [
    { key: "date", header: "Start", cell: (r) => <span className="font-mono text-xs">{fmtDate(r.start_time)}</span> },
    { key: "loc", header: "Location (normalized)", cell: (r) => r.location_normalized ?? "—" },
    { key: "raw", header: "Raw", cell: (r) => <span className="text-ink-muted">{truncate(r.location_raw, 40)}</span> },
    { key: "reason", header: "Reason", cell: (r) => <span className="text-ink-muted">{truncate(r.reason, 32)}</span> },
    { key: "conf", header: "Confidence", cell: (r) => <Badge tone={confTone(r.confidence)}>{fmtConfidence(r.confidence)}</Badge> },
  ];

  return (
    <div className="space-y-6">
      <PageHeader title="Normalization" description="LLM (GigaChat) output: normalized event schema and quality metrics." />

      <div className="grid grid-cols-1 gap-6 xl:grid-cols-3">
        <Card>
          <CardHeader title="Avg confidence" />
          <CardBody>
            <div className="font-mono text-3xl text-accent-teal">
              {quality.data ? fmtConfidence(quality.data.average_confidence) : "—"}
            </div>
            <div className="mt-1 text-xs text-ink-muted">across {fmtNumber(quality.data?.normalized_count ?? 0)} events</div>
          </CardBody>
        </Card>
        <Card>
          <CardHeader title="Coverage" />
          <CardBody>
            <div className="font-mono text-3xl text-ink">
              {fmtNumber(quality.data?.normalized_count ?? 0)} / {fmtNumber(quality.data?.parsed_total ?? 0)}
            </div>
            <div className="mt-1 text-xs text-ink-muted">normalized / parsed (30d)</div>
          </CardBody>
        </Card>
        <Card>
          <CardHeader title="Token usage" />
          <CardBody>
            <div className="font-mono text-3xl text-ink">{fmtNumber(quality.data?.estimated_tokens ?? 0)}</div>
            <div className="mt-1 text-xs text-ink-muted">
              {quality.data?.estimated_cost_usd != null ? `est. $${quality.data.estimated_cost_usd.toFixed(2)}` : "cost unavailable"}
            </div>
          </CardBody>
        </Card>
      </div>

      <Card>
        <CardHeader title="Confidence distribution" />
        <CardBody>
          {quality.data && <ConfidenceBars high={quality.data.high} medium={quality.data.medium} low={quality.data.low} />}
        </CardBody>
      </Card>

      <Card>
        <CardHeader title="Recent normalized events" />
        <DataTable columns={cols} rows={events.data} isLoading={events.isLoading} error={events.error} rowKey={(r) => r.event_id} />
      </Card>
    </div>
  );
}
