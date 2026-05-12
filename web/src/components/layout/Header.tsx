import { useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Bell, Check, Loader2, RefreshCw, X } from "lucide-react";
import { api } from "@/lib/api";
import { fmtRelative } from "@/lib/format";
import { StatusDot } from "@/components/ui/Badge";

const REGIONS = ["All regions", "RU-KEM", "RU-TOM"];
const SOURCE_TYPES = ["All types", "json", "html", "telegram"];
const WINDOWS = ["24h", "7d", "30d"];

export function Header() {
  const queryClient = useQueryClient();
  const { data: status } = useQuery({
    queryKey: ["pipeline-status"],
    queryFn: () => api.getPipelineStatus(),
    refetchInterval: 30_000,
  });
  const { data: sources } = useQuery({
    queryKey: ["sources"],
    queryFn: () => api.listSources(),
  });

  const [region, setRegion] = useState(REGIONS[0]);
  const [type, setType] = useState(SOURCE_TYPES[0]);
  const [win, setWin] = useState(WINDOWS[0]);
  const [toast, setToast] = useState<{ kind: "ok" | "err"; msg: string } | null>(null);

  const pollAll = useMutation({
    mutationFn: async () => {
      const list = sources ?? [];
      return Promise.all(list.filter((s) => s.is_active).map((s) => api.pollSource(s.id)));
    },
    onSuccess: (results) => {
      setToast({ kind: "ok", msg: `Polled ${results.length} source(s)` });
      queryClient.invalidateQueries({ queryKey: ["dashboard-summary"] });
      setTimeout(() => setToast(null), 2500);
    },
    onError: (e: Error) => {
      setToast({ kind: "err", msg: e.message });
      setTimeout(() => setToast(null), 4000);
    },
  });

  const overall = status?.overall ?? "healthy";
  const overallTone = overall === "healthy" ? "green" : overall === "degraded" ? "amber" : "red";

  return (
    <header className="sticky top-0 z-30 flex h-14 items-center gap-3 border-b border-line bg-bg-base/85 px-4 backdrop-blur">
      <div className="flex min-w-0 items-center gap-3">
        <span className="truncate text-sm font-medium text-ink">Power Outage Agent</span>
        <span className="hidden h-5 w-px bg-line md:block" />
        <div className="hidden items-center gap-2 md:flex">
          <StatusDot tone={overallTone} pulse={overall === "healthy"} />
          <span className="text-xs text-ink-muted">
            Pipeline {overall === "healthy" ? "running" : overall}
            {" · "}
            <span title={status?.last_heartbeat}>{fmtRelative(status?.last_heartbeat)}</span>
          </span>
        </div>
      </div>

      <div className="ml-auto flex flex-wrap items-center gap-2">
        <select className="input !h-8 !text-xs" value={region} onChange={(e) => setRegion(e.target.value)}>
          {REGIONS.map((r) => (
            <option key={r}>{r}</option>
          ))}
        </select>
        <select className="input !h-8 !text-xs" value={type} onChange={(e) => setType(e.target.value)}>
          {SOURCE_TYPES.map((t) => (
            <option key={t}>{t}</option>
          ))}
        </select>
        <select className="input !h-8 !text-xs" value={win} onChange={(e) => setWin(e.target.value)}>
          {WINDOWS.map((w) => (
            <option key={w}>{w}</option>
          ))}
        </select>

        <button
          className="btn btn-primary !py-1 !text-xs"
          onClick={() => pollAll.mutate()}
          disabled={pollAll.isPending}
        >
          {pollAll.isPending ? <Loader2 size={14} className="animate-spin" /> : <RefreshCw size={14} />}
          Run poll now
        </button>

        <button className="btn-ghost btn !p-1.5" aria-label="Notifications">
          <Bell size={16} />
        </button>
      </div>

      {toast && (
        <div
          className={`fixed bottom-6 right-6 z-50 flex items-center gap-2 rounded-md border px-3 py-2 text-sm shadow-card ${
            toast.kind === "ok"
              ? "border-accent-green/40 bg-accent-green/10 text-accent-green"
              : "border-accent-red/40 bg-accent-red/10 text-accent-red"
          }`}
        >
          {toast.kind === "ok" ? <Check size={14} /> : <X size={14} />}
          {toast.msg}
        </div>
      )}
    </header>
  );
}
