import { lazy, Suspense, useEffect, useMemo, useState, type ReactNode } from "react";
import { useQuery } from "@tanstack/react-query";
import {
  Activity,
  AlertTriangle,
  BadgeDollarSign,
  Building2,
  Database,
  Loader2,
  RefreshCw,
  ServerCog,
  ShieldAlert,
  TimerReset,
} from "lucide-react";
import { api } from "@/lib/api";
import type { MapImpactSeverity, MapOffice, MapOfficeStatus, Source } from "@/lib/api/types";
import { Badge, StatusBadge, StatusDot } from "@/components/ui/Badge";
import { Card, CardBody, CardHeader } from "@/components/ui/Card";
import { EmptyState } from "@/components/ui/EmptyState";
import { PageHeader } from "@/components/ui/PageHeader";
import { statusTone, type Tone } from "@/components/ui/statusTone";
import { fmtDate, fmtNumber, fmtRelative, truncate } from "@/lib/format";

const OfficeLeafletMap = lazy(async () => {
  const module = await import("@/pages/OfficeMap");
  return { default: module.OfficeLeafletMap };
});

const EMPTY_OFFICES: MapOffice[] = [];
const EMPTY_SOURCES: Source[] = [];

const STATUS_TONE: Record<MapOfficeStatus, Tone> = {
  ok: "green",
  risk: "amber",
  critical: "red",
};

const SEVERITY_TONE: Record<MapImpactSeverity, Tone> = {
  unknown: "gray",
  low: "amber",
  medium: "amber",
  high: "red",
  critical: "red",
};

function threatRank(office: MapOffice): number {
  if (office.status === "critical") return 3;
  if (office.status === "risk") return 2;
  return office.active_impacts.length > 0 ? 1 : 0;
}

function primarySeverity(office: MapOffice): MapImpactSeverity {
  return office.active_impacts[0]?.severity ?? "unknown";
}

function kpiStatusTone(status: "success" | "warning" | "error" | "neutral" | undefined): Tone {
  return statusTone(status ?? "neutral");
}

function MetricTile({
  label,
  value,
  hint,
  icon,
  tone = "gray",
}: {
  label: string;
  value: ReactNode;
  hint?: ReactNode;
  icon: ReactNode;
  tone?: Tone;
}) {
  return (
    <Card>
      <div className="flex items-start justify-between gap-3 p-4">
        <div className="min-w-0">
          <div className="text-2xs uppercase tracking-wider text-ink-muted">{label}</div>
          <div className="mt-2 font-mono text-2xl font-semibold text-ink">{value}</div>
          {hint && <div className="mt-1 truncate text-xs text-ink-muted">{hint}</div>}
        </div>
        <Badge tone={tone} className="shrink-0">
          {icon}
        </Badge>
      </div>
    </Card>
  );
}

function SourceRow({ source }: { source: Source }) {
  const success =
    source.success_rate == null ? "no data" : `${(source.success_rate * 100).toFixed(0)}%`;
  return (
    <div className="grid grid-cols-[minmax(0,1fr)_92px_96px] items-center gap-3 rounded-md border border-line/70 bg-bg-elevated/40 px-3 py-2">
      <div className="min-w-0">
        <div className="truncate text-sm font-medium text-ink">{source.name}</div>
        <div className="mt-0.5 flex flex-wrap items-center gap-2 text-2xs text-ink-muted">
          <span>{source.region ?? "region n/a"}</span>
          <span className="font-mono">{source.parser ?? source.source_type}</span>
          <span>{fmtRelative(source.last_fetch)}</span>
        </div>
      </div>
      <div className="text-right font-mono text-xs text-ink">{success}</div>
      <div className="flex justify-end">
        <StatusBadge status={source.status} />
      </div>
    </div>
  );
}

function RiskOfficeRow({
  office,
  selected,
  onSelect,
}: {
  office: MapOffice;
  selected: boolean;
  onSelect: (id: string) => void;
}) {
  const primary = office.active_impacts[0];
  const severity = primarySeverity(office);
  return (
    <button
      className={`w-full rounded-md border px-3 py-2 text-left transition-colors ${
        selected
          ? "border-accent-teal/60 bg-accent-teal/10"
          : "border-line/70 bg-bg-elevated/40 hover:border-line hover:bg-bg-elevated"
      }`}
      onClick={() => onSelect(office.id)}
    >
      <div className="flex items-start justify-between gap-3">
        <div className="min-w-0">
          <div className="truncate text-sm font-medium text-ink">{office.name}</div>
          <div className="mt-0.5 truncate text-xs text-ink-muted">{office.address}</div>
        </div>
        <Badge tone={STATUS_TONE[office.status]} className="shrink-0">
          <StatusDot tone={STATUS_TONE[office.status]} pulse={office.status !== "ok"} />
          <span className="uppercase tracking-wider">{office.status}</span>
        </Badge>
      </div>
      <div className="mt-2 flex flex-wrap items-center gap-2">
        <Badge tone={SEVERITY_TONE[severity]}>{severity}</Badge>
        <Badge tone="gray">{office.city}</Badge>
        {primary?.match_strategy && <Badge tone="blue">{primary.match_strategy}</Badge>}
        {primary?.match_score != null && (
          <span className="font-mono text-2xs text-ink-muted">
            score {(primary.match_score * 100).toFixed(0)}%
          </span>
        )}
      </div>
      {primary && (
        <div className="mt-2 line-clamp-2 text-xs text-ink-muted">
          {primary.reason ?? "No reason provided"}
        </div>
      )}
    </button>
  );
}

function StageList({ stages }: { stages: { key: string; label: string; status: string; queue_size: number; latency_ms: number | null; retry_count: number }[] }) {
  return (
    <div className="space-y-2">
      {stages.map((stage) => (
        <div
          key={stage.key}
          className="grid grid-cols-[minmax(0,1fr)_64px_84px_64px] items-center gap-3 rounded-md border border-line/70 bg-bg-elevated/40 px-3 py-2 text-xs"
        >
          <div className="flex min-w-0 items-center gap-2">
            <StatusDot tone={statusTone(stage.status)} pulse={stage.status === "running"} />
            <span className="truncate text-sm text-ink">{stage.label}</span>
          </div>
          <div className="text-right font-mono text-ink">{fmtNumber(stage.queue_size)}</div>
          <div className="text-right font-mono text-ink-muted">
            {stage.latency_ms == null ? "n/a" : `${fmtNumber(stage.latency_ms)}ms`}
          </div>
          <div className={stage.retry_count > 0 ? "text-right font-mono text-accent-red" : "text-right font-mono text-ink-muted"}>
            {fmtNumber(stage.retry_count)}
          </div>
        </div>
      ))}
    </div>
  );
}

export function Dashboard() {
  const [selectedOfficeId, setSelectedOfficeId] = useState<string | null>(null);

  const summary = useQuery({
    queryKey: ["dashboard-summary"],
    queryFn: () => api.getDashboardSummary(),
    refetchInterval: 15_000,
  });
  const sources = useQuery({
    queryKey: ["sources"],
    queryFn: () => api.listSources(),
    refetchInterval: 30_000,
  });
  const pipeline = useQuery({
    queryKey: ["pipeline-status"],
    queryFn: () => api.getPipelineStatus(),
    refetchInterval: 15_000,
  });
  const backlog = useQuery({
    queryKey: ["backlog"],
    queryFn: () => api.getQueueBacklog(),
    refetchInterval: 30_000,
  });
  const metrics = useQuery({
    queryKey: ["pipeline-metrics", 24],
    queryFn: () => api.getPipelineMetrics(24),
    refetchInterval: 60_000,
  });
  const failedTasks = useQuery({
    queryKey: ["tasks", "failed", { limit: 6 }],
    queryFn: () => api.listTasks({ status: "failed", limit: 6 }),
    refetchInterval: 30_000,
  });
  const logs = useQuery({
    queryKey: ["logs"],
    queryFn: () => api.listLogs(),
    refetchInterval: 30_000,
  });
  const map = useQuery({
    queryKey: ["map-offices"],
    queryFn: () => api.getMapOffices(),
    refetchInterval: 30_000,
  });

  const offices = map.data?.offices ?? EMPTY_OFFICES;
  const sourceRows = sources.data ?? EMPTY_SOURCES;
  const queueNow = backlog.data?.[backlog.data.length - 1];
  const queueDepth = (queueNow?.pending ?? 0) + (queueNow?.running ?? 0);
  const stages = pipeline.data?.stages ?? [];
  const logProblems = (logs.data ?? [])
    .filter((row) => row.level === "ERROR" || row.level === "WARNING")
    .slice(0, 6);

  const riskOffices = useMemo(
    () =>
      offices
        .filter((office) => office.status !== "ok" || office.active_impacts.length > 0)
        .sort((a, b) => {
          const rankDelta = threatRank(b) - threatRank(a);
          if (rankDelta !== 0) return rankDelta;
          return a.city.localeCompare(b.city) || a.name.localeCompare(b.name);
        }),
    [offices],
  );

  const sourceHealth = useMemo(() => {
    const healthy = sourceRows.filter((source) => source.status === "healthy").length;
    const failing = sourceRows.filter((source) => ["failed", "warning"].includes(source.status));
    return { healthy, failing };
  }, [sourceRows]);

  useEffect(() => {
    if (selectedOfficeId && offices.some((office) => office.id === selectedOfficeId)) return;
    setSelectedOfficeId(riskOffices[0]?.id ?? offices[0]?.id ?? null);
  }, [offices, riskOffices, selectedOfficeId]);

  const refetchAll = () => {
    summary.refetch();
    sources.refetch();
    pipeline.refetch();
    backlog.refetch();
    metrics.refetch();
    failedTasks.refetch();
    logs.refetch();
    map.refetch();
  };
  const isRefreshing =
    summary.isFetching ||
    sources.isFetching ||
    pipeline.isFetching ||
    backlog.isFetching ||
    metrics.isFetching ||
    failedTasks.isFetching ||
    logs.isFetching ||
    map.isFetching;

  return (
    <div className="space-y-5">
      <PageHeader
        title="Operations Console"
        description="Source health, queue, incidents, LLM spend and offices at risk."
        actions={
          <button className="btn btn-primary !py-1.5 !text-xs" onClick={refetchAll} disabled={isRefreshing}>
            {isRefreshing ? <Loader2 size={14} className="animate-spin" /> : <RefreshCw size={14} />}
            Refresh
          </button>
        }
      />

      <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 xl:grid-cols-5">
        <MetricTile
          label="Sources healthy"
          value={`${fmtNumber(sourceHealth.healthy)}/${fmtNumber(sourceRows.length)}`}
          hint={sourceHealth.failing.length > 0 ? `${sourceHealth.failing.length} need attention` : "all reporting cleanly"}
          icon={<ServerCog size={14} />}
          tone={sourceHealth.failing.length > 0 ? "amber" : "green"}
        />
        <MetricTile
          label="Queue depth"
          value={fmtNumber(queueDepth)}
          hint={`pending ${fmtNumber(queueNow?.pending ?? 0)} / running ${fmtNumber(queueNow?.running ?? 0)}`}
          icon={<Database size={14} />}
          tone={queueDepth > 0 ? "amber" : "green"}
        />
        <MetricTile
          label="Failed tasks"
          value={fmtNumber(summary.data?.failed_tasks.value ?? failedTasks.data?.length ?? 0)}
          hint={queueNow?.failed ? `${fmtNumber(queueNow.failed)} in DLQ snapshot` : "DLQ clear"}
          icon={<AlertTriangle size={14} />}
          tone={kpiStatusTone(summary.data?.failed_tasks.status)}
        />
        <MetricTile
          label="LLM cost 24h"
          value={`${(metrics.data?.llm_cost.total_cost_rub ?? 0).toFixed(2)} RUB`}
          hint={`${fmtNumber(metrics.data?.llm_cost.calls_ok ?? 0)} ok / ${fmtNumber(metrics.data?.llm_cost.calls_error ?? 0)} errors`}
          icon={<BadgeDollarSign size={14} />}
          tone={(metrics.data?.llm_cost.calls_error ?? 0) > 0 ? "amber" : "blue"}
        />
        <MetricTile
          label="Offices at risk"
          value={fmtNumber(summary.data?.offices_at_risk.value ?? riskOffices.length)}
          hint={`${riskOffices.filter((office) => office.status === "critical").length} critical`}
          icon={<ShieldAlert size={14} />}
          tone={riskOffices.length > 0 ? "red" : "green"}
        />
      </div>

      <div className="grid grid-cols-1 gap-4 xl:grid-cols-[minmax(0,1fr)_420px]">
        <div className="space-y-4">
          <Card>
            <CardHeader
              title="Source Health"
              subtitle={`${sourceRows.length} configured sources`}
              right={<Badge tone={sourceHealth.failing.length > 0 ? "amber" : "green"}>{sourceHealth.failing.length} issues</Badge>}
            />
            <CardBody className="space-y-2">
              {sources.isLoading ? (
                <div className="h-28 animate-pulse rounded-md bg-bg-elevated" />
              ) : sourceRows.length === 0 ? (
                <EmptyState title="No sources" hint="Configure at least one polling source." icon={<ServerCog size={22} />} />
              ) : (
                [...sourceRows]
                  .sort((a, b) => Number(a.status === "healthy") - Number(b.status === "healthy"))
                  .slice(0, 5)
                  .map((source) => <SourceRow key={source.id} source={source} />)
              )}
            </CardBody>
          </Card>

          <Card>
            <CardHeader
              title="Pipeline Queue"
              subtitle={pipeline.data ? `Heartbeat ${fmtRelative(pipeline.data.last_heartbeat)}` : "waiting for heartbeat"}
              right={pipeline.data && <StatusBadge status={pipeline.data.overall} />}
            />
            <CardBody>
              {stages.length > 0 ? (
                <>
                  <div className="mb-2 grid grid-cols-[minmax(0,1fr)_64px_84px_64px] gap-3 px-3 text-2xs uppercase tracking-wider text-ink-dim">
                    <span>Stage</span>
                    <span className="text-right">Queue</span>
                    <span className="text-right">Latency</span>
                    <span className="text-right">Retry</span>
                  </div>
                  <StageList stages={stages} />
                </>
              ) : (
                <EmptyState title="No pipeline data" hint="No task lifecycle rows yet." icon={<Activity size={22} />} />
              )}
            </CardBody>
          </Card>
        </div>

        <div className="space-y-4">
          <Card>
            <CardHeader
              title="Offices Under Risk"
              subtitle={`${riskOffices.length} active exposure rows`}
              right={<Badge tone={riskOffices.length > 0 ? "red" : "green"}>{riskOffices.length}</Badge>}
            />
            <CardBody className="max-h-[396px] space-y-2 overflow-y-auto">
              {riskOffices.length === 0 ? (
                <EmptyState title="No active office risk" hint="Matcher has no currently active impacts." icon={<Building2 size={22} />} />
              ) : (
                riskOffices
                  .slice(0, 8)
                  .map((office) => (
                    <RiskOfficeRow
                      key={office.id}
                      office={office}
                      selected={office.id === selectedOfficeId}
                      onSelect={setSelectedOfficeId}
                    />
                  ))
              )}
            </CardBody>
          </Card>

          <Card>
            <CardHeader
              title="Errors"
              subtitle={logProblems.length > 0 ? `${logProblems.length} recent warnings/errors` : "no recent warnings"}
              right={<Badge tone={logProblems.some((row) => row.level === "ERROR") ? "red" : logProblems.length > 0 ? "amber" : "green"}>{logProblems.length}</Badge>}
            />
            <CardBody className="space-y-2">
              {logProblems.length === 0 ? (
                <EmptyState title="No recent errors" hint="Structured event log is quiet." icon={<AlertTriangle size={22} />} />
              ) : (
                logProblems.map((row, index) => (
                  <div key={`${row.ts}-${index}`} className="rounded-md border border-line/70 bg-bg-elevated/40 px-3 py-2">
                    <div className="flex items-center justify-between gap-2">
                      <Badge tone={row.level === "ERROR" ? "red" : "amber"}>{row.level}</Badge>
                      <span className="font-mono text-2xs text-ink-muted">{fmtRelative(row.ts)}</span>
                    </div>
                    <div className="mt-1 truncate text-xs text-ink-muted">{row.logger}</div>
                    <div className="mt-1 line-clamp-2 text-sm text-ink">{truncate(row.message, 110)}</div>
                  </div>
                ))
              )}
            </CardBody>
          </Card>
        </div>
      </div>

      <div className="grid grid-cols-1 gap-4 xl:grid-cols-[minmax(0,1fr)_420px]">
        <Card>
          <CardHeader
            title="Risk Map"
            subtitle={map.dataUpdatedAt > 0 ? `Updated ${fmtRelative(new Date(map.dataUpdatedAt).toISOString())}` : "waiting for map data"}
          />
          <CardBody>
            {map.error ? (
              <EmptyState title="Map API failed" hint={(map.error as Error).message} icon={<AlertTriangle size={22} />} />
            ) : (
              <Suspense fallback={<div className="h-[360px] animate-pulse rounded-md bg-bg-elevated" />}>
                <OfficeLeafletMap
                  offices={offices}
                  selectedOfficeId={selectedOfficeId}
                  onSelect={setSelectedOfficeId}
                  className="h-[360px]"
                />
              </Suspense>
            )}
          </CardBody>
        </Card>

        <Card>
          <CardHeader title="Recent DLQ" subtitle={`${failedTasks.data?.length ?? 0} failed tasks`} />
          <CardBody className="space-y-2">
            {(failedTasks.data ?? []).length === 0 ? (
              <EmptyState title="DLQ clear" hint="No failed tasks returned by the API." icon={<TimerReset size={22} />} />
            ) : (
              (failedTasks.data ?? []).map((task) => (
                <div key={task.id} className="rounded-md border border-line/70 bg-bg-elevated/40 px-3 py-2">
                  <div className="flex items-center justify-between gap-2">
                    <Badge tone="blue">{task.task_type}</Badge>
                    <span className="font-mono text-2xs text-ink-muted">{fmtDate(task.updated_at)}</span>
                  </div>
                  <div className="mt-2 line-clamp-2 text-sm text-accent-red/90">
                    {task.error ?? "failed without error message"}
                  </div>
                  <div className="mt-1 font-mono text-2xs text-ink-dim">{task.id.slice(0, 8)}</div>
                </div>
              ))
            )}
          </CardBody>
        </Card>
      </div>

      <div className="text-2xs text-ink-dim">
        Data source: <span className="font-mono">{import.meta.env.VITE_USE_MOCK !== "0" ? "mock" : "FastAPI /api"}</span>
      </div>
    </div>
  );
}
