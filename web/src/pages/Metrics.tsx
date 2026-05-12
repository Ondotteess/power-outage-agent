import { useQuery } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { Card, CardBody, CardHeader } from "@/components/ui/Card";
import { PageHeader } from "@/components/ui/PageHeader";
import { QueueBacklogChart } from "@/components/charts/QueueBacklogChart";
import { ConfidenceBars } from "@/components/charts/ConfidenceBars";
import { fmtNumber } from "@/lib/format";

export function Metrics() {
  const backlog = useQuery({ queryKey: ["backlog"], queryFn: () => api.getQueueBacklog() });
  const quality = useQuery({ queryKey: ["quality"], queryFn: () => api.getNormalizationQuality() });
  const summary = useQuery({ queryKey: ["dashboard-summary"], queryFn: () => api.getDashboardSummary() });

  return (
    <div className="space-y-6">
      <PageHeader title="Metrics" description="Throughput, latency, queue depth and normalization quality." />

      <div className="grid grid-cols-2 gap-3 md:grid-cols-4">
        <KPI label="Active sources" value={summary.data?.active_sources.value} />
        <KPI label="Raw / 24h" value={summary.data?.raw_records_today.value} />
        <KPI label="Parsed" value={summary.data?.parsed_outages.value} />
        <KPI label="Failed" value={summary.data?.failed_tasks.value} tone="red" />
      </div>

      <Card>
        <CardHeader title="Queue backlog (24h)" />
        <CardBody>{backlog.data && <QueueBacklogChart data={backlog.data} />}</CardBody>
      </Card>

      <Card>
        <CardHeader title="Normalization confidence distribution" />
        <CardBody>
          {quality.data && <ConfidenceBars high={quality.data.high} medium={quality.data.medium} low={quality.data.low} />}
        </CardBody>
      </Card>
    </div>
  );
}

function KPI({ label, value, tone }: { label: string; value: number | undefined; tone?: "red" }) {
  return (
    <Card>
      <CardBody>
        <div className="text-2xs uppercase tracking-wider text-ink-muted">{label}</div>
        <div className={`mt-1 font-mono text-2xl ${tone === "red" ? "text-accent-red" : "text-ink"}`}>
          {fmtNumber(value ?? 0)}
        </div>
      </CardBody>
    </Card>
  );
}
