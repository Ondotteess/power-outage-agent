import { useQuery } from "@tanstack/react-query";
import {
  AlertOctagon,
  Building2,
  Copy,
  Database,
  FileSearch,
  Radio,
} from "lucide-react";
import { api } from "@/lib/api";
import { Card, CardBody, CardHeader } from "@/components/ui/Card";
import { KpiCard } from "@/components/ui/KpiCard";
import { DataTable, Column } from "@/components/ui/DataTable";
import { Badge, StatusBadge } from "@/components/ui/Badge";
import { statusTone } from "@/components/ui/statusTone";
import { PipelineFlow } from "@/components/pipeline/PipelineFlow";
import { ActivityFeed } from "@/components/activity/ActivityFeed";
import { QueueBacklogChart } from "@/components/charts/QueueBacklogChart";
import { ConfidenceBars } from "@/components/charts/ConfidenceBars";
import { PageHeader } from "@/components/ui/PageHeader";
import { fmtConfidence, fmtDate, fmtInterval, fmtNumber, fmtRelative, truncate } from "@/lib/format";
import type { ParsedRecord, Source, Task } from "@/lib/api/types";

export function Dashboard() {
  const summary = useQuery({ queryKey: ["dashboard-summary"], queryFn: () => api.getDashboardSummary() });
  const pipeline = useQuery({ queryKey: ["pipeline-status"], queryFn: () => api.getPipelineStatus(), refetchInterval: 30_000 });
  const activity = useQuery({ queryKey: ["activity"], queryFn: () => api.getActivity(30), refetchInterval: 30_000 });
  const sources = useQuery({ queryKey: ["sources"], queryFn: () => api.listSources() });
  const parsed = useQuery({ queryKey: ["parsed", { limit: 10 }], queryFn: () => api.listParsed({ limit: 10 }) });
  const dlq = useQuery({ queryKey: ["tasks", "failed", { limit: 5 }], queryFn: () => api.listTasks({ status: "failed", limit: 5 }) });
  const quality = useQuery({ queryKey: ["quality"], queryFn: () => api.getNormalizationQuality() });
  const backlog = useQuery({ queryKey: ["backlog"], queryFn: () => api.getQueueBacklog() });

  const sourceCols: Column<Source>[] = [
    { key: "name", header: "Source", cell: (r) => <span className="text-ink">{r.name}</span> },
    { key: "type", header: "Type", cell: (r) => <Badge tone="blue">{r.source_type}</Badge> },
    { key: "region", header: "Region", cell: (r) => <span className="text-ink-muted">{r.region ?? "—"}</span> },
    { key: "interval", header: "Poll", cell: (r) => <span className="font-mono text-xs text-ink-muted">{fmtInterval(r.poll_interval_seconds)}</span> },
    { key: "last", header: "Last fetch", cell: (r) => <span className="text-xs text-ink-muted" title={r.last_fetch ?? ""}>{fmtRelative(r.last_fetch)}</span> },
    { key: "win", header: "Records 24h", cell: (r) => <span className="font-mono">{fmtNumber(r.records_in_window)}</span> },
    { key: "ok", header: "Success", cell: (r) => (r.success_rate == null ? "—" : `${(r.success_rate * 100).toFixed(0)}%`) },
    { key: "status", header: "Status", cell: (r) => <StatusBadge status={r.status} /> },
  ];

  const parsedCols: Column<ParsedRecord>[] = [
    { key: "date", header: "Date", cell: (r) => <span className="font-mono text-xs">{fmtDate(r.start_time).split(",")[0]}</span> },
    { key: "time", header: "Time", cell: (r) => <span className="font-mono text-xs">{fmtDate(r.start_time).split(",")[1]?.trim()}</span> },
    { key: "city", header: "City", cell: (r) => r.city ?? "—" },
    { key: "addr", header: "Address", cell: (r) => <span className="text-ink-muted">{truncate(r.street, 38)}</span> },
    { key: "reason", header: "Reason", cell: (r) => <span className="text-ink-muted">{truncate(r.reason, 32)}</span> },
  ];

  const dlqCols: Column<Task>[] = [
    { key: "time", header: "Time", cell: (r) => <span className="text-xs">{fmtRelative(r.updated_at)}</span> },
    { key: "type", header: "Task", cell: (r) => <Badge tone="gray">{r.task_type}</Badge> },
    { key: "err", header: "Error", cell: (r) => <span className="text-xs text-accent-red/90">{truncate(r.error, 60)}</span> },
    { key: "att", header: "Att.", cell: (r) => <span className="font-mono text-xs">{r.attempt}</span> },
  ];

  return (
    <div className="space-y-6">
      <PageHeader
        title="Operations dashboard"
        description="Live view of pipeline health, sources, normalization quality and incidents."
      />

      {/* KPI row */}
      <div className="grid grid-cols-2 gap-3 md:grid-cols-3 xl:grid-cols-6">
        {summary.data && (
          <>
            <KpiCard label="Active sources" data={summary.data.active_sources} icon={<Database size={16} />} />
            <KpiCard label="Raw records (24h)" data={summary.data.raw_records_today} icon={<Radio size={16} />} />
            <KpiCard label="Parsed outages" data={summary.data.parsed_outages} icon={<FileSearch size={16} />} />
            <KpiCard label="Duplicates skipped" data={summary.data.duplicates_skipped} icon={<Copy size={16} />} />
            <KpiCard label="Failed tasks / DLQ" data={summary.data.failed_tasks} icon={<AlertOctagon size={16} />} />
            <KpiCard label="Offices at risk" data={summary.data.offices_at_risk} icon={<Building2 size={16} />} />
          </>
        )}
      </div>

      <div className="grid grid-cols-1 gap-6 xl:grid-cols-[1fr_360px]">
        <div className="space-y-6">
          {/* Pipeline */}
          <Card>
            <CardHeader
              title="Pipeline overview"
              subtitle="Scheduler → Collector → Parser → Normalizer → Dedup → Office Matcher → Notifier"
              right={
                pipeline.data && (
                  <Badge tone={statusTone(pipeline.data.overall)}>
                    {pipeline.data.overall}
                  </Badge>
                )
              }
            />
            <CardBody>
              {pipeline.data ? (
                <PipelineFlow stages={pipeline.data.stages} />
              ) : (
                <div className="h-24 animate-pulse rounded bg-bg-elevated/60" />
              )}
            </CardBody>
          </Card>

          {/* Sources */}
          <Card>
            <CardHeader title="Sources" subtitle="Polling status and recent yield" />
            <DataTable
              columns={sourceCols}
              rows={sources.data}
              isLoading={sources.isLoading}
              error={sources.error}
              rowKey={(r) => r.id}
              empty="No sources configured"
            />
          </Card>

          {/* Parsed events + DLQ side-by-side */}
          <div className="grid grid-cols-1 gap-6 2xl:grid-cols-2">
            <Card>
              <CardHeader title="Recent outage events" subtitle="Parsed records, newest first" />
              <DataTable
                columns={parsedCols}
                rows={parsed.data}
                isLoading={parsed.isLoading}
                error={parsed.error}
                rowKey={(r) => r.id}
              />
            </Card>
            <Card>
              <CardHeader
                title="Recent DLQ"
                subtitle="Failed tasks"
                right={dlq.data && <Badge tone="red">{dlq.data.length}</Badge>}
              />
              <DataTable
                columns={dlqCols}
                rows={dlq.data}
                isLoading={dlq.isLoading}
                error={dlq.error}
                rowKey={(r) => r.id}
                empty="No failed tasks 🎉"
              />
            </Card>
          </div>

          {/* Normalization quality + queue backlog */}
          <div className="grid grid-cols-1 gap-6 2xl:grid-cols-2">
            <Card>
              <CardHeader
                title="LLM normalization quality"
                subtitle="GigaChat output confidence distribution"
                right={quality.data && <Badge tone="teal">avg {fmtConfidence(quality.data.average_confidence)}</Badge>}
              />
              <CardBody className="space-y-3">
                {quality.data ? (
                  <>
                    <ConfidenceBars high={quality.data.high} medium={quality.data.medium} low={quality.data.low} />
                    <div className="grid grid-cols-3 gap-3 border-t border-line/60 pt-3 text-xs">
                      <div>
                        <div className="text-ink-muted">Normalized</div>
                        <div className="font-mono text-ink">
                          {fmtNumber(quality.data.normalized_count)} / {fmtNumber(quality.data.parsed_total)}
                        </div>
                      </div>
                      <div>
                        <div className="text-ink-muted">Tokens used</div>
                        <div className="font-mono text-ink">{fmtNumber(quality.data.estimated_tokens ?? 0)}</div>
                      </div>
                      <div>
                        <div className="text-ink-muted">Est. cost</div>
                        <div className="font-mono text-ink">
                          {quality.data.estimated_cost_usd != null ? `$${quality.data.estimated_cost_usd.toFixed(2)}` : "—"}
                        </div>
                      </div>
                    </div>
                  </>
                ) : (
                  <div className="h-40 animate-pulse rounded bg-bg-elevated/60" />
                )}
              </CardBody>
            </Card>
            <Card>
              <CardHeader title="Queue backlog (24h)" subtitle="Pending / running / failed" />
              <CardBody>
                {backlog.data ? <QueueBacklogChart data={backlog.data} /> : <div className="h-40 animate-pulse rounded bg-bg-elevated/60" />}
              </CardBody>
            </Card>
          </div>
        </div>

        {/* Activity column */}
        <Card className="h-fit xl:sticky xl:top-20">
          <CardHeader title="Activity" subtitle="Recent pipeline events" />
          {activity.isLoading ? (
            <div className="space-y-2 p-4">
              {Array.from({ length: 8 }).map((_, i) => (
                <div key={i} className="h-10 animate-pulse rounded bg-bg-elevated/60" />
              ))}
            </div>
          ) : (
            <div className="max-h-[640px] overflow-y-auto">
              <ActivityFeed events={activity.data ?? []} />
            </div>
          )}
        </Card>
      </div>

      <div className="pt-2 text-2xs text-ink-dim">
        Data source: <span className="font-mono">{import.meta.env.VITE_USE_MOCK !== "0" ? "mock" : "FastAPI /api"}</span>
        {summary.dataUpdatedAt > 0 && <> · updated {fmtRelative(new Date(summary.dataUpdatedAt).toISOString())}</>}
      </div>
    </div>
  );
}
