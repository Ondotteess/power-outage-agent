import { useQuery } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { Card, CardBody, CardHeader } from "@/components/ui/Card";
import { PipelineFlow } from "@/components/pipeline/PipelineFlow";
import { PageHeader } from "@/components/ui/PageHeader";
import { Badge, StatusBadge, statusTone } from "@/components/ui/Badge";
import { fmtNumber, fmtRelative } from "@/lib/format";

export function Pipeline() {
  const { data } = useQuery({ queryKey: ["pipeline-status"], queryFn: () => api.getPipelineStatus(), refetchInterval: 15_000 });

  return (
    <div className="space-y-6">
      <PageHeader
        title="Pipeline"
        description="Per-stage health, throughput and queue depth across the outage pipeline."
        actions={
          data && (
            <Badge tone={statusTone(data.overall)} className="!text-xs">
              {data.overall} · heartbeat {fmtRelative(data.last_heartbeat)}
            </Badge>
          )
        }
      />
      <Card>
        <CardHeader title="Flow" subtitle="Scheduler → Notifier" />
        <CardBody>{data && <PipelineFlow stages={data.stages} />}</CardBody>
      </Card>

      <div className="grid grid-cols-1 gap-4 md:grid-cols-2 xl:grid-cols-3 2xl:grid-cols-4">
        {(data?.stages ?? []).map((s) => (
          <Card key={s.key}>
            <CardHeader title={s.label} right={<StatusBadge status={s.status} />} />
            <CardBody className="space-y-1.5 text-sm">
              <Row label="Queue" value={fmtNumber(s.queue_size)} />
              {s.throughput != null && <Row label="Throughput" value={`${s.throughput.toFixed(1)} / min`} />}
              {s.latency_ms != null && <Row label="Latency" value={`${s.latency_ms} ms`} />}
              <Row label="Retries" value={fmtNumber(s.retry_count)} tone={s.retry_count > 0 ? "amber" : undefined} />
              {s.metric_label && s.metric_value && <Row label={s.metric_label} value={s.metric_value} />}
            </CardBody>
          </Card>
        ))}
      </div>
    </div>
  );
}

function Row({ label, value, tone }: { label: string; value: string; tone?: "amber" }) {
  return (
    <div className="flex items-center justify-between">
      <span className="text-ink-muted">{label}</span>
      <span className={`font-mono ${tone === "amber" ? "text-accent-amber" : "text-ink"}`}>{value}</span>
    </div>
  );
}
