import { lazy, Suspense, useEffect, useMemo, useState, type ReactNode } from "react";
import { useQuery } from "@tanstack/react-query";
import {
  AlertTriangle,
  Building2,
  Clock3,
  Loader2,
  MapPinned,
  RefreshCw,
  ShieldAlert,
  Zap,
} from "lucide-react";
import { api } from "@/lib/api";
import type { MapImpactSeverity, MapOffice, MapOfficeStatus } from "@/lib/api/types";
import { Badge, StatusDot } from "@/components/ui/Badge";
import { Card, CardBody, CardHeader } from "@/components/ui/Card";
import { EmptyState } from "@/components/ui/EmptyState";
import { PageHeader } from "@/components/ui/PageHeader";
import { fmtDate, fmtNumber, fmtRelative } from "@/lib/format";

const EMPTY_OFFICES: MapOffice[] = [];
const OfficeLeafletMap = lazy(async () => {
  const module = await import("@/pages/OfficeMap");
  return { default: module.OfficeLeafletMap };
});

const STATUS_TONE: Record<MapOfficeStatus, "green" | "amber" | "red"> = {
  ok: "green",
  risk: "amber",
  critical: "red",
};

const SEVERITY_TONE: Record<MapImpactSeverity, "gray" | "amber" | "red"> = {
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

function ThreatRow({
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
      className={`w-full rounded-md border p-3 text-left transition-colors ${
        selected
          ? "border-accent-teal/60 bg-accent-teal/10"
          : "border-line bg-bg-elevated/40 hover:border-line/80 hover:bg-bg-elevated"
      }`}
      onClick={() => onSelect(office.id)}
    >
      <div className="flex items-start justify-between gap-2">
        <div className="min-w-0">
          <div className="truncate text-sm font-semibold text-ink">{office.name}</div>
          <div className="mt-1 truncate text-xs text-ink-muted">{office.address}</div>
        </div>
        <Badge tone={STATUS_TONE[office.status]}>
          <StatusDot tone={STATUS_TONE[office.status]} pulse />
          <span className="uppercase tracking-wider">{office.status}</span>
        </Badge>
      </div>

      <div className="mt-3 flex flex-wrap items-center gap-2 text-2xs text-ink-muted">
        <Badge tone="gray">{office.city}</Badge>
        <Badge tone="gray">{office.region}</Badge>
        <Badge tone={SEVERITY_TONE[severity]}>{severity}</Badge>
        {office.active_impacts.length > 1 && (
          <Badge tone="amber">+{office.active_impacts.length - 1}</Badge>
        )}
      </div>

      {primary && (
        <div className="mt-3 space-y-2 border-t border-line/60 pt-3">
          <div className="line-clamp-2 text-sm text-ink">{primary.reason ?? "Причина не указана"}</div>
          <div className="grid grid-cols-1 gap-1 text-xs text-ink-muted">
            <div className="flex items-center justify-between gap-2">
              <span>Начало</span>
              <span className="font-mono text-ink">{fmtDate(primary.starts_at)}</span>
            </div>
            <div className="flex items-center justify-between gap-2">
              <span>Окончание</span>
              <span className="font-mono text-ink">
                {primary.ends_at ? fmtDate(primary.ends_at) : "не задано"}
              </span>
            </div>
          </div>
        </div>
      )}
    </button>
  );
}

function StatTile({
  label,
  value,
  icon,
  tone = "gray",
}: {
  label: string;
  value: string;
  icon: ReactNode;
  tone?: "gray" | "amber" | "red" | "green" | "blue";
}) {
  return (
    <div className="rounded-md border border-line bg-bg-elevated/50 p-3">
      <div className="flex items-center justify-between gap-3">
        <div className="text-xs text-ink-muted">{label}</div>
        <Badge tone={tone}>{icon}</Badge>
      </div>
      <div className="mt-2 font-mono text-xl font-semibold text-ink">{value}</div>
    </div>
  );
}

export function Dashboard() {
  const [selectedOfficeId, setSelectedOfficeId] = useState<string | null>(null);
  const mapQuery = useQuery({
    queryKey: ["map-offices"],
    queryFn: () => api.getMapOffices(),
    refetchInterval: 30_000,
  });

  const offices = mapQuery.data?.offices ?? EMPTY_OFFICES;
  const threatenedOffices = useMemo(
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

  useEffect(() => {
    if (selectedOfficeId && offices.some((office) => office.id === selectedOfficeId)) return;
    setSelectedOfficeId(threatenedOffices[0]?.id ?? null);
  }, [offices, selectedOfficeId, threatenedOffices]);

  const stats = useMemo(() => {
    const critical = threatenedOffices.filter((office) => office.status === "critical").length;
    const activeImpacts = threatenedOffices.reduce(
      (sum, office) => sum + office.active_impacts.length,
      0,
    );
    return { critical, activeImpacts };
  }, [threatenedOffices]);

  const updatedAt =
    mapQuery.dataUpdatedAt > 0
      ? fmtRelative(new Date(mapQuery.dataUpdatedAt).toISOString())
      : "ожидание данных";

  return (
    <div className="space-y-5">
      <PageHeader
        title="Карта угроз"
        description="Офисы компании в Кемеровской, Новосибирской и Томской областях."
        actions={
          <button
            className="btn btn-primary !py-1.5 !text-xs"
            onClick={() => mapQuery.refetch()}
            disabled={mapQuery.isFetching}
          >
            {mapQuery.isFetching ? <Loader2 size={14} className="animate-spin" /> : <RefreshCw size={14} />}
            Обновить
          </button>
        }
      />

      <div className="grid grid-cols-2 gap-3 md:grid-cols-4">
        <StatTile label="Всего офисов" value={fmtNumber(offices.length)} icon={<Building2 size={14} />} tone="blue" />
        <StatTile
          label="Под угрозой"
          value={fmtNumber(threatenedOffices.length)}
          icon={<ShieldAlert size={14} />}
          tone={threatenedOffices.length > 0 ? "amber" : "green"}
        />
        <StatTile label="Критичных" value={fmtNumber(stats.critical)} icon={<Zap size={14} />} tone="red" />
        <StatTile label="Активных событий" value={fmtNumber(stats.activeImpacts)} icon={<Clock3 size={14} />} tone="gray" />
      </div>

      {mapQuery.error ? (
        <Card>
          <CardBody>
            <EmptyState
              title="Карта недоступна"
              hint={(mapQuery.error as Error).message}
              icon={<AlertTriangle size={22} />}
            />
          </CardBody>
        </Card>
      ) : mapQuery.isLoading ? (
        <div className="grid grid-cols-1 gap-4 xl:grid-cols-[minmax(0,1fr)_390px]">
          <div className="h-[calc(100vh-210px)] min-h-[560px] animate-pulse rounded-lg border border-line bg-bg-elevated/60" />
          <div className="h-[calc(100vh-210px)] min-h-[560px] animate-pulse rounded-lg border border-line bg-bg-elevated/60" />
        </div>
      ) : (
        <div className="grid grid-cols-1 gap-4 xl:grid-cols-[minmax(0,1fr)_390px]">
          <Suspense
            fallback={
              <div className="h-[calc(100vh-210px)] min-h-[560px] animate-pulse rounded-lg border border-line bg-bg-elevated/60" />
            }
          >
            <OfficeLeafletMap
              offices={offices}
              selectedOfficeId={selectedOfficeId}
              onSelect={setSelectedOfficeId}
              className="h-[calc(100vh-210px)] min-h-[560px]"
            />
          </Suspense>

          <Card className="h-[calc(100vh-210px)] min-h-[560px] overflow-hidden">
            <CardHeader
              title="Офисы под угрозой"
              subtitle={`Обновлено ${updatedAt}`}
              right={<Badge tone={threatenedOffices.length > 0 ? "amber" : "green"}>{threatenedOffices.length}</Badge>}
            />
            <CardBody className="h-[calc(100%-64px)] space-y-2 overflow-y-auto">
              {threatenedOffices.length === 0 ? (
                <EmptyState
                  title="Активных угроз нет"
                  hint="Боковая панель показывает только офисы с текущими событиями."
                  icon={<MapPinned size={22} />}
                />
              ) : (
                threatenedOffices.map((office) => (
                  <ThreatRow
                    key={office.id}
                    office={office}
                    selected={office.id === selectedOfficeId}
                    onSelect={setSelectedOfficeId}
                  />
                ))
              )}
            </CardBody>
          </Card>
        </div>
      )}

      <div className="text-2xs text-ink-dim">
        Data source: <span className="font-mono">{import.meta.env.VITE_USE_MOCK !== "0" ? "mock" : "FastAPI /api"}</span>
      </div>
    </div>
  );
}
