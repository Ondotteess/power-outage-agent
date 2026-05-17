import { lazy, Suspense, useEffect, useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { AlertTriangle, Loader2, RefreshCw } from "lucide-react";
import { api } from "@/lib/api";
import type { MapOffice } from "@/lib/api/types";
import { Badge, StatusDot } from "@/components/ui/Badge";
import { EmptyState } from "@/components/ui/EmptyState";
import { fmtDate, fmtRelative } from "@/lib/format";

const OfficeLeafletMap = lazy(async () => {
  const module = await import("@/pages/OfficeMap");
  return { default: module.OfficeLeafletMap };
});

const EMPTY_OFFICES: MapOffice[] = [];

function threatReason(office: MapOffice): string {
  return office.active_impacts[0]?.reason || "Причина не указана";
}

function isThreatened(office: MapOffice): boolean {
  return office.status !== "ok" || office.active_impacts.length > 0;
}

function threatRank(office: MapOffice): number {
  if (office.status === "critical") return 3;
  if (office.status === "risk") return 2;
  return office.active_impacts.length > 0 ? 1 : 0;
}

function ThreatOfficeRow({
  office,
  selected,
  onSelect,
}: {
  office: MapOffice;
  selected: boolean;
  onSelect: (id: string) => void;
}) {
  const impact = office.active_impacts[0];
  return (
    <button
      className={`w-full rounded-md border px-3 py-3 text-left transition-colors ${
        selected
          ? "border-ink bg-ink text-bg-surface"
          : "border-line bg-bg-surface text-ink hover:border-ink"
      }`}
      onClick={() => onSelect(office.id)}
    >
      <div className="flex items-start justify-between gap-3">
        <div className="min-w-0">
          <div className="truncate text-sm font-medium">{office.name}</div>
          <div className={`mt-1 text-xs ${selected ? "text-bg-subtle" : "text-ink-muted"}`}>
            {office.address}
          </div>
        </div>
        <StatusDot tone="red" pulse />
      </div>
      <div className={`mt-3 text-sm leading-5 ${selected ? "text-bg-surface" : "text-ink"}`}>
        {threatReason(office)}
      </div>
      <div
        className={`mt-3 grid grid-cols-2 gap-2 font-mono text-2xs ${
          selected ? "text-bg-subtle" : "text-ink-muted"
        }`}
      >
        <span>{office.city}</span>
        <span className="text-right">{impact?.starts_at ? fmtDate(impact.starts_at) : "сейчас"}</span>
      </div>
    </button>
  );
}

function SelectedOfficePanel({ office }: { office: MapOffice | undefined }) {
  const impact = office?.active_impacts[0];

  if (!office) {
    return <EmptyState title="Офис не выбран" hint="Нажмите на точку на карте." />;
  }

  return (
    <div className="border-b border-line px-4 py-4">
      <div className="flex items-start justify-between gap-3">
        <div className="min-w-0">
          <div className="truncate text-sm font-semibold text-ink">{office.name}</div>
          <div className="mt-1 text-xs leading-5 text-ink-muted">{office.address}</div>
        </div>
        <StatusDot tone={impact ? "red" : "green"} pulse={Boolean(impact)} />
      </div>

      {impact ? (
        <div className="mt-4 space-y-3">
          <div>
            <div className="text-2xs uppercase tracking-wider text-ink-dim">Причина</div>
            <div className="mt-1 text-sm leading-5 text-ink">{impact.reason || "Причина не указана"}</div>
          </div>
          <div className="grid grid-cols-2 gap-2 font-mono text-2xs text-ink-muted">
            <div className="rounded-md border border-line bg-bg-elevated px-2 py-2">
              <div className="mb-1 uppercase tracking-wider text-ink-dim">Начало</div>
              <div className="text-ink">{fmtDate(impact.starts_at)}</div>
            </div>
            <div className="rounded-md border border-line bg-bg-elevated px-2 py-2">
              <div className="mb-1 uppercase tracking-wider text-ink-dim">Окончание</div>
              <div className="text-ink">{impact.ends_at ? fmtDate(impact.ends_at) : "не указано"}</div>
            </div>
          </div>
        </div>
      ) : (
        <div className="mt-4 rounded-md border border-line bg-bg-elevated px-3 py-3 text-sm text-ink-muted">
          Для этого офиса ближайших угроз не найдено.
        </div>
      )}
    </div>
  );
}

export function Dashboard() {
  const [selectedOfficeId, setSelectedOfficeId] = useState<string | null>(null);
  const map = useQuery({
    queryKey: ["map-offices"],
    queryFn: () => api.getMapOffices(),
    refetchInterval: 30_000,
  });

  const offices = map.data?.offices ?? EMPTY_OFFICES;
  const threatOffices = useMemo(
    () =>
      offices
        .filter(isThreatened)
        .sort((a, b) => {
          const rank = threatRank(b) - threatRank(a);
          return rank || a.city.localeCompare(b.city) || a.address.localeCompare(b.address);
        }),
    [offices],
  );
  const selectedOffice = useMemo(
    () => offices.find((office) => office.id === selectedOfficeId) ?? threatOffices[0],
    [offices, selectedOfficeId, threatOffices],
  );

  useEffect(() => {
    if (selectedOfficeId && offices.some((office) => office.id === selectedOfficeId)) return;
    setSelectedOfficeId(threatOffices[0]?.id ?? null);
  }, [offices, selectedOfficeId, threatOffices]);

  const totalOffices = offices.length;
  const threatPercent = totalOffices > 0 ? (threatOffices.length / totalOffices) * 100 : 0;
  const threatPercentLabel = `${threatPercent.toLocaleString("ru-RU", {
    maximumFractionDigits: 1,
  })}%`;

  return (
    <div className="grid h-[calc(100vh-88px)] min-h-[620px] grid-cols-1 gap-4 xl:grid-cols-[minmax(0,1fr)_380px]">
      <section className="relative overflow-hidden rounded-lg border border-line bg-bg-surface">
        {map.error ? (
          <div className="grid h-full place-items-center p-6">
            <EmptyState
              title="API карты недоступен"
              hint={(map.error as Error).message}
              icon={<AlertTriangle size={22} />}
            />
          </div>
        ) : map.isLoading ? (
          <div className="grid h-full place-items-center text-ink-muted">
            <Loader2 className="animate-spin" size={22} />
          </div>
        ) : (
          <Suspense
            fallback={
              <div className="grid h-full place-items-center text-ink-muted">
                <Loader2 className="animate-spin" size={22} />
              </div>
            }
          >
            <OfficeLeafletMap
              offices={offices}
              selectedOfficeId={selectedOfficeId}
              onSelect={setSelectedOfficeId}
              className="h-full rounded-none border-0"
            />
          </Suspense>
        )}

        <div className="absolute left-4 top-4 flex items-center gap-2 rounded-md border border-line bg-bg-surface/95 px-3 py-2 text-xs shadow-card backdrop-blur">
          <span className="inline-flex items-center gap-1.5">
            <StatusDot tone="green" />
            без угроз
          </span>
          <span className="h-4 w-px bg-line" />
          <span className="inline-flex items-center gap-1.5">
            <StatusDot tone="red" pulse={threatOffices.length > 0} />
            с угрозами
          </span>
        </div>
      </section>

      <aside className="flex min-h-0 flex-col rounded-lg border border-line bg-bg-surface">
        <div className="flex items-start justify-between gap-3 border-b border-line px-4 py-4">
          <div>
            <div className="text-sm font-semibold text-ink">Красные офисы</div>
            <div className="mt-1 text-xs text-ink-muted">
              {map.dataUpdatedAt ? fmtRelative(new Date(map.dataUpdatedAt).toISOString()) : "нет данных"}
            </div>
          </div>
          <div className="flex flex-wrap items-center justify-end gap-2">
            <Badge tone={threatOffices.length > 0 ? "red" : "green"}>{threatOffices.length}</Badge>
            <span className="font-mono text-xs text-ink-muted">из {totalOffices}</span>
            <Badge tone="gray">{threatPercentLabel}</Badge>
            <button
              className="btn-ghost btn !p-1.5"
              onClick={() => map.refetch()}
              disabled={map.isFetching}
              aria-label="Обновить карту"
            >
              {map.isFetching ? <Loader2 size={14} className="animate-spin" /> : <RefreshCw size={14} />}
            </button>
          </div>
        </div>

        <SelectedOfficePanel office={selectedOffice} />

        <div className="min-h-0 flex-1 space-y-2 overflow-y-auto p-3">
          {threatOffices.length === 0 ? (
            <div className="grid h-full place-items-center">
              <EmptyState title="Красных офисов нет" hint="Активных и ближайших плановых угроз не найдено." />
            </div>
          ) : (
            threatOffices.map((office) => (
              <ThreatOfficeRow
                key={office.id}
                office={office}
                selected={office.id === selectedOfficeId}
                onSelect={setSelectedOfficeId}
              />
            ))
          )}
        </div>
      </aside>
    </div>
  );
}
