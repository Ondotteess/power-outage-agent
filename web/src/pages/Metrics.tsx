import { useQuery } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { Card, CardBody, CardHeader } from "@/components/ui/Card";
import { PageHeader } from "@/components/ui/PageHeader";
import { QueueBacklogChart } from "@/components/charts/QueueBacklogChart";
import { ConfidenceBars } from "@/components/charts/ConfidenceBars";
import { fmtNumber } from "@/lib/format";
import type { LLMCall, StageTiming } from "@/lib/api/types";

export function Metrics() {
  const backlog = useQuery({ queryKey: ["backlog"], queryFn: () => api.getQueueBacklog() });
  const quality = useQuery({ queryKey: ["quality"], queryFn: () => api.getNormalizationQuality() });
  const summary = useQuery({ queryKey: ["dashboard-summary"], queryFn: () => api.getDashboardSummary() });
  const pipelineMetrics = useQuery({
    queryKey: ["pipeline-metrics", 24],
    queryFn: () => api.getPipelineMetrics(24),
  });
  const m = pipelineMetrics.data;

  return (
    <div className="space-y-6">
      <PageHeader
        title="Metrics"
        description="Stage timings, LLM cost and runtime — last 24 hours."
      />

      <div className="grid grid-cols-2 gap-3 md:grid-cols-4">
        <KPI label="Active sources" value={summary.data?.active_sources.value} />
        <KPI label="Raw / 24h" value={summary.data?.raw_records_today.value} />
        <KPI label="Parsed" value={summary.data?.parsed_outages.value} />
        <KPI label="Failed" value={summary.data?.failed_tasks.value} tone="red" />
      </div>

      {/* LLM cost panel — primary focus */}
      <Card>
        <CardHeader
          title="LLM cost (GigaChat, 24h)"
          subtitle={
            m
              ? `Tariff: ${m.llm_cost.prompt_price_per_1k_rub} ₽/1k prompt · ${m.llm_cost.completion_price_per_1k_rub} ₽/1k completion`
              : undefined
          }
        />
        <CardBody>
          {m ? (
            <div className="grid grid-cols-2 gap-3 md:grid-cols-5">
              <KPI label="Total cost ₽" value={m.llm_cost.total_cost_rub} format="rub" />
              <KPI label="Calls (ok)" value={m.llm_cost.calls_ok} />
              <KPI
                label="Calls (error)"
                value={m.llm_cost.calls_error}
                tone={m.llm_cost.calls_error > 0 ? "red" : undefined}
              />
              <KPI label="Total tokens" value={m.llm_cost.total_tokens} />
              <KPI label="Avg latency ms" value={m.llm_cost.avg_duration_ms} />
            </div>
          ) : (
            <Empty>No LLM calls in window</Empty>
          )}
        </CardBody>
      </Card>

      {/* Normalizer mix (automaton vs LLM fallback) */}
      <Card>
        <CardHeader
          title="Normalizer path mix"
          subtitle="How often the FSA handled records on its own vs. fell back to GigaChat."
        />
        <CardBody>
          {m ? <PathMix mix={m.normalizer_path} /> : <Empty>No data</Empty>}
        </CardBody>
      </Card>

      {/* Stage timings table */}
      <Card>
        <CardHeader title="Per-stage execution time (24h)" />
        <CardBody>
          {m && m.stage_timings.length > 0 ? (
            <StageTable rows={m.stage_timings} />
          ) : (
            <Empty>No completed tasks in window</Empty>
          )}
        </CardBody>
      </Card>

      {/* Runtime memory */}
      <Card>
        <CardHeader
          title="Runtime"
          subtitle="Memory of the admin API process. The pipeline worker is separate and not sampled yet."
        />
        <CardBody>
          {m?.runtime ? (
            <div className="grid grid-cols-2 gap-3 md:grid-cols-4">
              <KPI label="API RSS MB" value={m.runtime.rss_mb} />
              {m.runtime.vms_mb !== null && (
                <KPI label="API VMS MB" value={m.runtime.vms_mb} />
              )}
              {m.runtime.cpu_percent !== null && (
                <KPI label="API CPU %" value={m.runtime.cpu_percent} />
              )}
            </div>
          ) : (
            <Empty>psutil not installed in API container</Empty>
          )}
        </CardBody>
      </Card>

      {/* Recent LLM calls — useful for spotting outliers */}
      <Card>
        <CardHeader title="Recent LLM calls" subtitle="Latest 20 chat-completion requests." />
        <CardBody>
          {m && m.recent_llm_calls.length > 0 ? (
            <CallsTable rows={m.recent_llm_calls} />
          ) : (
            <Empty>No LLM calls yet</Empty>
          )}
        </CardBody>
      </Card>

      <Card>
        <CardHeader title="Queue backlog (24h)" />
        <CardBody>{backlog.data && <QueueBacklogChart data={backlog.data} />}</CardBody>
      </Card>

      <Card>
        <CardHeader title="Normalization confidence distribution" />
        <CardBody>
          {quality.data && (
            <ConfidenceBars
              high={quality.data.high}
              medium={quality.data.medium}
              low={quality.data.low}
            />
          )}
        </CardBody>
      </Card>
    </div>
  );
}

function KPI({
  label,
  value,
  tone,
  format,
}: {
  label: string;
  value: number | undefined;
  tone?: "red";
  format?: "rub";
}) {
  const shown = value === undefined ? "—" : format === "rub" ? `${value.toFixed(2)} ₽` : fmtNumber(value);
  return (
    <Card>
      <CardBody>
        <div className="text-2xs uppercase tracking-wider text-ink-muted">{label}</div>
        <div
          className={`mt-1 font-mono text-2xl ${tone === "red" ? "text-accent-red" : "text-ink"}`}
        >
          {shown}
        </div>
      </CardBody>
    </Card>
  );
}

function Empty({ children }: { children: React.ReactNode }) {
  return <div className="py-6 text-center text-sm text-ink-muted">{children}</div>;
}

function PathMix({ mix }: { mix: { automaton: number; llm_fallback: number; none: number; automaton_pct: number } }) {
  const total = mix.automaton + mix.llm_fallback + mix.none;
  if (total === 0) return <Empty>No NORMALIZE_EVENT tasks yet</Empty>;
  const autoPct = (mix.automaton / total) * 100;
  const llmPct = (mix.llm_fallback / total) * 100;
  const nonePct = (mix.none / total) * 100;
  return (
    <div className="space-y-3">
      <div className="flex h-3 overflow-hidden rounded-full bg-surface-muted">
        <div className="bg-accent-green" style={{ width: `${autoPct}%` }} title={`Automaton: ${mix.automaton}`} />
        <div className="bg-accent-amber" style={{ width: `${llmPct}%` }} title={`LLM fallback: ${mix.llm_fallback}`} />
        <div className="bg-accent-red" style={{ width: `${nonePct}%` }} title={`None: ${mix.none}`} />
      </div>
      <div className="grid grid-cols-3 gap-3 text-sm">
        <Legend color="bg-accent-green" label="Automaton" count={mix.automaton} pct={autoPct} />
        <Legend color="bg-accent-amber" label="LLM fallback" count={mix.llm_fallback} pct={llmPct} />
        <Legend color="bg-accent-red" label="None" count={mix.none} pct={nonePct} />
      </div>
    </div>
  );
}

function Legend({ color, label, count, pct }: { color: string; label: string; count: number; pct: number }) {
  return (
    <div className="flex items-center gap-2">
      <span className={`inline-block h-2 w-2 rounded-full ${color}`} />
      <span className="text-ink">{label}</span>
      <span className="ml-auto font-mono text-ink-muted">
        {fmtNumber(count)} · {pct.toFixed(0)}%
      </span>
    </div>
  );
}

function StageTable({ rows }: { rows: StageTiming[] }) {
  return (
    <div className="overflow-x-auto">
      <table className="data-table">
        <thead>
          <tr>
            <th>Stage</th>
            <th className="text-right">Count</th>
            <th className="text-right">Avg ms</th>
            <th className="text-right">p50 ms</th>
            <th className="text-right">p95 ms</th>
            <th className="text-right">Max ms</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((r) => (
            <tr key={r.task_type}>
              <td className="font-mono text-xs">{r.task_type}</td>
              <td className="text-right font-mono">{fmtNumber(r.count)}</td>
              <td className="text-right font-mono">{fmtNumber(r.avg_ms)}</td>
              <td className="text-right font-mono">{fmtNumber(r.p50_ms)}</td>
              <td className="text-right font-mono">{fmtNumber(r.p95_ms)}</td>
              <td className="text-right font-mono">{fmtNumber(r.max_ms)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function CallsTable({ rows }: { rows: LLMCall[] }) {
  return (
    <div className="overflow-x-auto">
      <table className="data-table">
        <thead>
          <tr>
            <th>Time</th>
            <th>Model</th>
            <th>Status</th>
            <th className="text-right">Prompt</th>
            <th className="text-right">Completion</th>
            <th className="text-right">Total</th>
            <th className="text-right">Latency ms</th>
            <th className="text-right">Cost ₽</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((r) => (
            <tr key={r.id}>
              <td className="font-mono text-xs">{new Date(r.created_at).toLocaleTimeString()}</td>
              <td className="font-mono text-xs">{r.model}</td>
              <td>
                <span
                  className={
                    r.status === "ok" ? "text-accent-green" : "text-accent-red"
                  }
                >
                  {r.status}
                </span>
              </td>
              <td className="text-right font-mono">{fmtNumber(r.prompt_tokens)}</td>
              <td className="text-right font-mono">{fmtNumber(r.completion_tokens)}</td>
              <td className="text-right font-mono">{fmtNumber(r.total_tokens)}</td>
              <td className="text-right font-mono">{fmtNumber(r.duration_ms)}</td>
              <td className="text-right font-mono">{r.cost_rub.toFixed(3)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
