import { useMemo } from "react";
import { useQuery } from "@tanstack/react-query";
import { AlertTriangle, Cpu, Database, Radio, TimerReset } from "lucide-react";
import { api } from "@/lib/api";
import { Badge, StatusBadge, StatusDot } from "@/components/ui/Badge";
import { Card, CardBody, CardHeader } from "@/components/ui/Card";
import { DataTable, type Column } from "@/components/ui/DataTable";
import { EmptyState } from "@/components/ui/EmptyState";
import { PipelineFlow } from "@/components/pipeline/PipelineFlow";
import { PageHeader } from "@/components/ui/PageHeader";
import { statusTone } from "@/components/ui/statusTone";
import { fmtDate, fmtInterval, fmtNumber, fmtRelative, truncate } from "@/lib/format";
import type { LogLine, PipelineStage, Source, Task } from "@/lib/api/types";

function MetricCell({
  label,
  value,
  hint,
  tone = "gray",
}: {
  label: string;
  value: string;
  hint?: string;
  tone?: "gray" | "green" | "red" | "amber";
}) {
  return (
    <div className="rounded-lg border border-line bg-bg-surface p-3">
      <div className="flex items-center justify-between gap-2">
        <div className="text-2xs uppercase tracking-wider text-ink-muted">{label}</div>
        <StatusDot tone={tone} pulse={tone === "red" || tone === "amber"} />
      </div>
      <div className="mt-2 font-mono text-2xl font-semibold text-ink">{value}</div>
      {hint && <div className="mt-1 truncate text-xs text-ink-muted">{hint}</div>}
    </div>
  );
}

function StageTable({ stages }: { stages: PipelineStage[] }) {
  return (
    <div className="table-wrap">
      <table className="data-table">
        <thead>
          <tr>
            <th>Stage</th>
            <th>Status</th>
            <th className="text-right">Queue</th>
            <th className="text-right">Throughput</th>
            <th className="text-right">Latency</th>
            <th className="text-right">Retries</th>
            <th>Metric</th>
          </tr>
        </thead>
        <tbody>
          {stages.map((stage) => (
            <tr key={stage.key}>
              <td>
                <div className="font-medium text-ink">{stage.label}</div>
                <div className="font-mono text-2xs text-ink-muted">{stage.key}</div>
              </td>
              <td>
                <StatusBadge status={stage.status} />
              </td>
              <td className="text-right font-mono">{fmtNumber(stage.queue_size)}</td>
              <td className="text-right font-mono">
                {stage.throughput == null ? "n/a" : `${stage.throughput.toFixed(1)}/m`}
              </td>
              <td className="text-right font-mono">
                {stage.latency_ms == null ? "n/a" : `${fmtNumber(stage.latency_ms)} ms`}
              </td>
              <td
                className={`text-right font-mono ${
                  stage.retry_count > 0 ? "text-accent-red" : "text-ink-muted"
                }`}
              >
                {fmtNumber(stage.retry_count)}
              </td>
              <td className="text-xs text-ink-muted">
                {stage.metric_label && stage.metric_value
                  ? `${stage.metric_label}: ${stage.metric_value}`
                  : "—"}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function SourceMatrix({ sources }: { sources: Source[] | undefined }) {
  const cols: Column<Source>[] = [
    { key: "name", header: "Source", cell: (r) => <span className="text-ink">{r.name}</span> },
    { key: "status", header: "Status", cell: (r) => <StatusBadge status={r.status} /> },
    { key: "parser", header: "Parser", cell: (r) => <span className="font-mono text-xs">{r.parser ?? "—"}</span> },
    { key: "poll", header: "Poll", cell: (r) => <span className="font-mono text-xs">{fmtInterval(r.poll_interval_seconds)}</span> },
    { key: "records", header: "24h", cell: (r) => <span className="font-mono">{fmtNumber(r.records_in_window)}</span> },
    { key: "success", header: "Success", cell: (r) => (r.success_rate == null ? "—" : `${(r.success_rate * 100).toFixed(0)}%`) },
    { key: "last", header: "Last", cell: (r) => <span title={r.last_fetch ?? ""}>{fmtRelative(r.last_fetch)}</span> },
    { key: "url", header: "URL", cell: (r) => <span className="font-mono text-2xs text-ink-muted">{truncate(r.url, 56)}</span> },
  ];
  return <DataTable columns={cols} rows={sources} rowKey={(r) => r.id} />;
}

function TaskMatrix({ tasks }: { tasks: Task[] | undefined }) {
  const cols: Column<Task>[] = [
    { key: "type", header: "Type", cell: (r) => <Badge tone="blue">{r.task_type}</Badge> },
    { key: "status", header: "Status", cell: (r) => <StatusBadge status={r.status} /> },
    { key: "attempt", header: "Attempt", cell: (r) => <span className="font-mono">{r.attempt}</span> },
    { key: "source", header: "Source", cell: (r) => <span className="font-mono text-2xs text-ink-muted">{r.source_id?.slice(0, 8) ?? "—"}</span> },
    { key: "trace", header: "Trace", cell: (r) => <span className="font-mono text-2xs text-ink-muted">{r.trace_id.slice(0, 8)}</span> },
    { key: "updated", header: "Updated", cell: (r) => <span title={r.updated_at}>{fmtRelative(r.updated_at)}</span> },
    { key: "retry", header: "Next retry", cell: (r) => <span title={r.next_retry_at ?? ""}>{fmtRelative(r.next_retry_at)}</span> },
    { key: "err", header: "Error", cell: (r) => <span className="text-xs text-accent-red">{truncate(r.error, 86)}</span> },
  ];
  return <DataTable columns={cols} rows={tasks} rowKey={(r) => r.id} />;
}

function LogRows({ logs }: { logs: LogLine[] }) {
  if (logs.length === 0) {
    return <EmptyState title="No warnings" hint="Recent log tail has no WARNING or ERROR rows." />;
  }
  return (
    <div className="space-y-2">
      {logs.map((log, index) => (
        <div key={`${log.ts}-${index}`} className="rounded-md border border-line bg-bg-surface px-3 py-2">
          <div className="flex items-center justify-between gap-2">
            <Badge tone={log.level === "ERROR" ? "red" : "amber"}>{log.level}</Badge>
            <span className="font-mono text-2xs text-ink-muted">{fmtDate(log.ts)}</span>
          </div>
          <div className="mt-1 font-mono text-2xs text-ink-muted">{log.logger}</div>
          <div className="mt-1 text-sm text-ink">{truncate(log.message, 140)}</div>
        </div>
      ))}
    </div>
  );
}

export function Pipeline() {
  const pipeline = useQuery({
    queryKey: ["pipeline-status"],
    queryFn: () => api.getPipelineStatus(),
    refetchInterval: 15_000,
  });
  const sources = useQuery({ queryKey: ["sources"], queryFn: () => api.listSources(), refetchInterval: 30_000 });
  const tasks = useQuery({
    queryKey: ["tasks", { limit: 200 }],
    queryFn: () => api.listTasks({ limit: 200 }),
    refetchInterval: 30_000,
  });
  const backlog = useQuery({ queryKey: ["backlog"], queryFn: () => api.getQueueBacklog(), refetchInterval: 30_000 });
  const metrics = useQuery({
    queryKey: ["pipeline-metrics", 24],
    queryFn: () => api.getPipelineMetrics(24),
    refetchInterval: 60_000,
  });
  const logs = useQuery({ queryKey: ["logs"], queryFn: () => api.listLogs(), refetchInterval: 30_000 });

  const taskRows = tasks.data ?? [];
  const stages = pipeline.data?.stages ?? [];
  const currentBacklog = backlog.data?.[backlog.data.length - 1];
  const failedTasks = taskRows.filter((task) => task.status === "failed");
  const runningTasks = taskRows.filter((task) => task.status === "running");
  const pendingTasks = taskRows.filter((task) => task.status === "pending");
  const warningLogs = (logs.data ?? []).filter((log) => log.level === "WARNING" || log.level === "ERROR").slice(0, 8);

  const queueDepth = (currentBacklog?.pending ?? 0) + (currentBacklog?.running ?? 0);
  const sourceProblems = useMemo(
    () => (sources.data ?? []).filter((source) => source.status !== "healthy" && source.status !== "inactive"),
    [sources.data],
  );
  const overallTone = statusTone(pipeline.data?.overall ?? "pending");

  return (
    <div className="space-y-5">
      <PageHeader
        title="Pipeline Debug"
        description="Stages, sources, queue, runtime, LLM usage and recent failures."
        actions={
          pipeline.data && (
            <Badge tone={overallTone} className="!text-xs">
              <StatusDot tone={overallTone} pulse={pipeline.data.overall === "healthy"} />
              {pipeline.data.overall} · {fmtRelative(pipeline.data.last_heartbeat)}
            </Badge>
          )
        }
      />

      <div className="grid grid-cols-2 gap-3 lg:grid-cols-5">
        <MetricCell
          label="Stages"
          value={`${stages.filter((stage) => stage.status === "healthy" || stage.status === "running").length}/${stages.length}`}
          hint="healthy or running"
          tone={pipeline.data?.overall === "failed" ? "red" : pipeline.data?.overall === "degraded" ? "amber" : "green"}
        />
        <MetricCell label="Queue" value={fmtNumber(queueDepth)} hint={`${fmtNumber(pendingTasks.length)} pending tasks`} tone={queueDepth > 0 ? "amber" : "green"} />
        <MetricCell label="Running" value={fmtNumber(runningTasks.length)} hint="tasks in progress" tone={runningTasks.length > 0 ? "amber" : "gray"} />
        <MetricCell label="Failed" value={fmtNumber(failedTasks.length)} hint="DLQ candidates" tone={failedTasks.length > 0 ? "red" : "green"} />
        <MetricCell label="Source issues" value={fmtNumber(sourceProblems.length)} hint={`${sources.data?.length ?? 0} configured`} tone={sourceProblems.length > 0 ? "amber" : "green"} />
      </div>

      <div className="grid grid-cols-1 gap-4 xl:grid-cols-[minmax(0,1fr)_420px]">
        <div className="space-y-4">
          <Card>
            <CardHeader title="Stage flow" subtitle="Scheduler → collector → parser → normalizer → matcher → notifier" />
            <CardBody>
              {pipeline.data ? <PipelineFlow stages={pipeline.data.stages} /> : <EmptyState title="No pipeline heartbeat" />}
            </CardBody>
          </Card>

          <Card>
            <CardHeader title="Stage matrix" subtitle={`${stages.length} stages`} />
            {stages.length > 0 ? <StageTable stages={stages} /> : <CardBody><EmptyState title="No stages" /></CardBody>}
          </Card>
        </div>

        <div className="space-y-4">
          <Card>
            <CardHeader title="Runtime" right={<Cpu size={16} />} />
            <CardBody className="space-y-3 text-sm">
              <DebugRow label="API RSS" value={metrics.data?.runtime ? `${metrics.data.runtime.rss_mb.toFixed(1)} MB` : "n/a"} />
              <DebugRow label="API CPU" value={metrics.data?.runtime?.cpu_percent == null ? "n/a" : `${metrics.data.runtime.cpu_percent.toFixed(1)}%`} />
              <DebugRow label="LLM calls ok" value={fmtNumber(metrics.data?.llm_cost.calls_ok ?? 0)} />
              <DebugRow label="LLM calls error" value={fmtNumber(metrics.data?.llm_cost.calls_error ?? 0)} tone={(metrics.data?.llm_cost.calls_error ?? 0) > 0 ? "red" : undefined} />
              <DebugRow label="LLM cost" value={`${(metrics.data?.llm_cost.total_cost_rub ?? 0).toFixed(2)} ₽`} />
              <DebugRow label="Automaton path" value={`${((metrics.data?.normalizer_path.automaton_pct ?? 0) * 100).toFixed(0)}%`} />
            </CardBody>
          </Card>

          <Card>
            <CardHeader title="Queue snapshot" right={<Database size={16} />} />
            <CardBody className="space-y-3 text-sm">
              <DebugRow label="Pending" value={fmtNumber(currentBacklog?.pending ?? 0)} />
              <DebugRow label="Running" value={fmtNumber(currentBacklog?.running ?? 0)} />
              <DebugRow label="Failed" value={fmtNumber(currentBacklog?.failed ?? failedTasks.length)} tone={(currentBacklog?.failed ?? failedTasks.length) > 0 ? "red" : undefined} />
              <DebugRow label="Sample size" value={fmtNumber(taskRows.length)} />
            </CardBody>
          </Card>

          <Card>
            <CardHeader title="Recent warnings" right={<AlertTriangle size={16} />} />
            <CardBody>
              <LogRows logs={warningLogs} />
            </CardBody>
          </Card>
        </div>
      </div>

      <Card>
        <CardHeader title="Sources" subtitle="Parser profile, timing and source health in one table" right={<Radio size={16} />} />
        <SourceMatrix sources={sources.data} />
      </Card>

      <Card>
        <CardHeader title="Recent tasks" subtitle="Last 200 task rows with ids, traces, retry timing and errors" right={<TimerReset size={16} />} />
        <TaskMatrix tasks={taskRows} />
      </Card>
    </div>
  );
}

function DebugRow({ label, value, tone }: { label: string; value: string; tone?: "red" }) {
  return (
    <div className="flex items-center justify-between gap-3 border-b border-line/60 pb-2 last:border-0 last:pb-0">
      <span className="text-ink-muted">{label}</span>
      <span className={`font-mono ${tone === "red" ? "text-accent-red" : "text-ink"}`}>{value}</span>
    </div>
  );
}
