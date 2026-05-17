import { useEffect, useMemo, useRef, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import * as L from "leaflet";
import "leaflet/dist/leaflet.css";
import { AlertTriangle, ExternalLink, Loader2, MapPinned, RefreshCw, Search } from "lucide-react";
import { api } from "@/lib/api";
import type { MapImpactSeverity, MapOffice, MapOfficeImpact, MapOfficeStatus } from "@/lib/api/types";
import { Badge, StatusDot } from "@/components/ui/Badge";
import { Card, CardBody, CardHeader } from "@/components/ui/Card";
import { EmptyState } from "@/components/ui/EmptyState";
import { PageHeader } from "@/components/ui/PageHeader";
import { fmtDate } from "@/lib/format";

type StatusFilter = "all" | "problem" | MapOfficeStatus;
type SeverityFilter = "all" | MapImpactSeverity;

const STATUS_TONE: Record<MapOfficeStatus, "green" | "amber" | "red"> = {
  ok: "green",
  risk: "red",
  critical: "red",
};

const STATUS_COLOR: Record<MapOfficeStatus, string> = {
  ok: "#16A34A",
  risk: "#DC2626",
  critical: "#DC2626",
};

const STATUS_LABEL: Record<MapOfficeStatus, string> = {
  ok: "без угроз",
  risk: "риск",
  critical: "критично",
};

const SEVERITY_LABEL: Record<MapImpactSeverity, string> = {
  low: "низкий",
  medium: "средний",
  high: "высокий",
  critical: "критический",
  unknown: "неизвестно",
};

const MARKER_PRIORITY: Record<MapOfficeStatus, number> = {
  ok: 0,
  risk: 1,
  critical: 2,
};

const EMPTY_OFFICES: MapOffice[] = [];
const SEVERITY_ORDER: MapImpactSeverity[] = ["critical", "high", "medium", "low", "unknown"];

function hasCoordinates(office: MapOffice): office is MapOffice & { latitude: number; longitude: number } {
  return typeof office.latitude === "number" && typeof office.longitude === "number";
}

function escapeHtml(value: string | null | undefined): string {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#039;");
}

function severityLabel(severity: MapImpactSeverity): string {
  return SEVERITY_LABEL[severity] ?? severity;
}

function sourceHref(impact: MapOfficeImpact): string | null {
  return impact.source_record_url ?? impact.source_url ?? null;
}

function sourceLabel(impact: MapOfficeImpact): string {
  return impact.source_name ?? "Источник отключения";
}

function popupSourceHtml(impact: MapOfficeImpact): string {
  const href = sourceHref(impact);
  if (!href) return "";
  const record = impact.source_record_id ? ` · запись ${escapeHtml(impact.source_record_id)}` : "";
  return `
    <a class="office-map-popup-source" href="${escapeHtml(href)}" target="_blank" rel="noreferrer">
      ${escapeHtml(sourceLabel(impact))}${record}
    </a>
  `;
}

function popupHtml(office: MapOffice): string {
  const primary = office.active_impacts[0];
  const other = office.active_impacts.slice(1);
  const impactHtml = primary
    ? `
      <div class="office-map-popup-section">
        <div class="office-map-popup-label">Угроза</div>
        <div>${escapeHtml(primary.reason ?? "Причина не указана")}</div>
        <dl>
          <dt>Уровень</dt><dd>${escapeHtml(severityLabel(primary.severity))}</dd>
          <dt>Начало</dt><dd>${escapeHtml(fmtDate(primary.starts_at))}</dd>
          <dt>Окончание</dt><dd>${escapeHtml(primary.ends_at ? fmtDate(primary.ends_at) : "не указано")}</dd>
        </dl>
        ${popupSourceHtml(primary)}
      </div>
      ${
        other.length
          ? `<div class="office-map-popup-section">
              <div class="office-map-popup-label">Другие угрозы</div>
              <ul>${other
                .map(
                  (impact) =>
                    `<li>${escapeHtml(severityLabel(impact.severity))}: ${escapeHtml(
                      impact.reason ?? "Причина не указана",
                    )}</li>`,
                )
                .join("")}</ul>
            </div>`
          : ""
      }
    `
    : `<div class="office-map-popup-section">Ближайших угроз нет.</div>`;

  return `
    <div class="office-map-popup-content">
      <div class="office-map-popup-title">${escapeHtml(office.name)}</div>
      <div class="office-map-popup-muted">${escapeHtml(office.address)}</div>
      <div class="office-map-popup-status">${escapeHtml(STATUS_LABEL[office.status])}</div>
      ${impactHtml}
    </div>
  `;
}

export function OfficeLeafletMap({
  offices,
  selectedOfficeId,
  onSelect,
  className = "h-[calc(100vh-220px)] min-h-[520px]",
}: {
  offices: MapOffice[];
  selectedOfficeId: string | null;
  onSelect: (id: string) => void;
  className?: string;
}) {
  const elementRef = useRef<HTMLDivElement | null>(null);
  const mapRef = useRef<L.Map | null>(null);
  const layerRef = useRef<L.LayerGroup | null>(null);
  const fittedBoundsKeyRef = useRef<string | null>(null);
  const userAdjustedViewRef = useRef(false);
  const autoFittingRef = useRef(false);
  const coordinateOffices = useMemo(() => offices.filter(hasCoordinates), [offices]);

  useEffect(() => {
    if (!elementRef.current || mapRef.current) return;

    const map = L.map(elementRef.current, {
      zoomControl: false,
      attributionControl: false,
    }).setView([55.3, 84.5], 5);

    L.control.zoom({ position: "bottomright" }).addTo(map);
    L.control.attribution({ position: "bottomright", prefix: false }).addTo(map);
    L.tileLayer("https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png", {
      maxZoom: 19,
      subdomains: "abcd",
      attribution:
        '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> &copy; <a href="https://carto.com/attributions">CARTO</a>',
    }).addTo(map);

    layerRef.current = L.layerGroup().addTo(map);
    mapRef.current = map;
    const markUserAdjustedView = () => {
      if (!autoFittingRef.current) userAdjustedViewRef.current = true;
    };
    map.on("zoomstart", markUserAdjustedView);
    map.on("dragstart", markUserAdjustedView);
    const timer = window.setTimeout(() => map.invalidateSize(), 0);

    return () => {
      window.clearTimeout(timer);
      map.off("zoomstart", markUserAdjustedView);
      map.off("dragstart", markUserAdjustedView);
      map.remove();
      mapRef.current = null;
      layerRef.current = null;
    };
  }, []);

  useEffect(() => {
    const map = mapRef.current;
    const layer = layerRef.current;
    if (!map || !layer) return;

    layer.clearLayers();
    const bounds = L.latLngBounds([]);
    const boundsKey = coordinateOffices
      .map((office) => `${office.id}:${office.latitude}:${office.longitude}`)
      .join("|");

    const markerOffices = [...coordinateOffices].sort((a, b) => {
      const selectedDelta = Number(a.id === selectedOfficeId) - Number(b.id === selectedOfficeId);
      return MARKER_PRIORITY[a.status] - MARKER_PRIORITY[b.status] + selectedDelta * 10;
    });

    markerOffices.forEach((office) => {
      const color = STATUS_COLOR[office.status];
      const selected = office.id === selectedOfficeId;
      const marker = L.circleMarker([office.latitude, office.longitude], {
        radius: selected ? 9 : 7,
        color,
        fillColor: color,
        fillOpacity: selected ? 0.95 : 0.82,
        opacity: 1,
        weight: selected ? 3 : 2,
      });

      marker.bindTooltip(
        `<strong>${escapeHtml(office.name)}</strong><br>${escapeHtml(
          office.address,
        )}<br>Статус: ${escapeHtml(STATUS_LABEL[office.status])}`,
        { direction: "top", offset: [0, -8], opacity: 1 },
      );
      marker.bindPopup(popupHtml(office), {
        minWidth: 260,
        maxWidth: 360,
        className: "office-map-popup",
      });
      marker.on("click", () => onSelect(office.id));
      marker.on("mouseover", () => marker.openTooltip());
      marker.addTo(layer);
      bounds.extend([office.latitude, office.longitude]);
    });

    if (bounds.isValid() && boundsKey !== fittedBoundsKeyRef.current) {
      if (!userAdjustedViewRef.current) {
        autoFittingRef.current = true;
        map.fitBounds(bounds.pad(0.25), { maxZoom: 12, animate: false });
        window.setTimeout(() => {
          autoFittingRef.current = false;
        }, 0);
      }
      fittedBoundsKeyRef.current = boundsKey;
    }
  }, [coordinateOffices, onSelect, selectedOfficeId]);

  return (
    <div className={`office-map relative overflow-hidden rounded-lg border border-line bg-bg-surface ${className}`}>
      <div ref={elementRef} className="h-full w-full" />
      {offices.length === 0 && (
        <div className="absolute inset-0 grid place-items-center bg-bg-surface/90">
          <EmptyState title="Офисов нет" hint="API карты вернул пустой список офисов." icon={<MapPinned size={22} />} />
        </div>
      )}
      {offices.length > 0 && coordinateOffices.length === 0 && (
        <div className="absolute inset-0 grid place-items-center bg-bg-surface/90">
          <EmptyState
            title="Нет координат"
            hint="Офисы есть, но ни у одного нет широты и долготы."
            icon={<AlertTriangle size={22} />}
          />
        </div>
      )}
    </div>
  );
}

function StatusPill({ status }: { status: MapOfficeStatus }) {
  return (
    <Badge tone={STATUS_TONE[status]}>
      <StatusDot tone={STATUS_TONE[status]} pulse={status !== "ok"} />
      <span className="uppercase tracking-wider">{STATUS_LABEL[status]}</span>
    </Badge>
  );
}

function OfficeListItem({
  office,
  selected,
  onSelect,
}: {
  office: MapOffice;
  selected: boolean;
  onSelect: (id: string) => void;
}) {
  const primary = office.active_impacts[0];
  return (
    <button
      className={`w-full rounded-md border p-3 text-left transition-colors ${
        selected
          ? "border-accent-teal/50 bg-accent-teal/10"
          : "border-line bg-bg-elevated/40 hover:border-line/80 hover:bg-bg-elevated"
      }`}
      onClick={() => onSelect(office.id)}
    >
      <div className="flex items-start justify-between gap-2">
        <div className="min-w-0">
          <div className="truncate text-sm font-medium text-ink">{office.name}</div>
          <div className="mt-0.5 truncate text-xs text-ink-muted">{office.address}</div>
        </div>
        <StatusPill status={office.status} />
      </div>
      <div className="mt-2 flex flex-wrap items-center gap-2 text-2xs text-ink-muted">
        <Badge tone="gray">{office.region}</Badge>
        {hasCoordinates(office) ? (
          <span className="font-mono">
            {office.latitude.toFixed(3)}, {office.longitude.toFixed(3)}
          </span>
        ) : (
          <Badge tone="amber">нет координат</Badge>
        )}
      </div>
      {primary && (
        <div className="mt-2 rounded border border-line/60 bg-bg-subtle px-2 py-1.5 text-xs text-ink-muted">
          <span className="text-ink">{severityLabel(primary.severity)}</span>
          {" · "}
          {primary.reason ?? "Причина не указана"}
        </div>
      )}
    </button>
  );
}

function SelectedOffice({ office }: { office: MapOffice | undefined }) {
  if (!office) {
    return <EmptyState title="Офис не выбран" hint="Выберите точку или строку офиса." icon={<MapPinned size={20} />} />;
  }

  const primary = office.active_impacts[0];
  return (
    <div className="space-y-3">
      <div className="flex items-start justify-between gap-3">
        <div className="min-w-0">
          <div className="truncate text-sm font-semibold text-ink">{office.name}</div>
          <div className="mt-1 text-xs text-ink-muted">{office.address}</div>
        </div>
        <StatusPill status={office.status} />
      </div>
      <div className="grid grid-cols-2 gap-2 text-xs">
        <div className="rounded-md border border-line/60 bg-bg-elevated/40 p-2">
          <div className="text-ink-dim">Город</div>
          <div className="mt-0.5 truncate text-ink">{office.city}</div>
        </div>
        <div className="rounded-md border border-line/60 bg-bg-elevated/40 p-2">
          <div className="text-ink-dim">Регион</div>
          <div className="mt-0.5 font-mono text-ink">{office.region}</div>
        </div>
      </div>
      {primary ? (
        <div className="space-y-2 border-t border-line/60 pt-3">
          <div className="text-xs font-medium text-ink">Угроза</div>
          <div className="text-sm text-ink">{primary.reason ?? "Причина не указана"}</div>
          <div className="grid grid-cols-1 gap-2 text-xs">
            <div className="flex items-center justify-between gap-2">
              <span className="text-ink-muted">Уровень</span>
              <Badge tone={primary.severity === "high" || primary.severity === "critical" ? "red" : "amber"}>
                  {severityLabel(primary.severity)}
              </Badge>
            </div>
            <div className="flex items-center justify-between gap-2">
              <span className="text-ink-muted">Начало</span>
              <span className="font-mono text-ink">{fmtDate(primary.starts_at)}</span>
            </div>
            <div className="flex items-center justify-between gap-2">
              <span className="text-ink-muted">Окончание</span>
              <span className="font-mono text-ink">{primary.ends_at ? fmtDate(primary.ends_at) : "не указано"}</span>
            </div>
          </div>
          {sourceHref(primary) && (
            <a
              href={sourceHref(primary) ?? undefined}
              target="_blank"
              rel="noreferrer"
              className="flex items-start gap-2 rounded-md border border-line/60 bg-bg-subtle px-2 py-2 text-xs text-ink transition-colors hover:border-ink/30 hover:bg-bg-elevated"
            >
              <ExternalLink size={14} className="mt-0.5 shrink-0 text-ink-muted" />
              <span className="min-w-0">
                <span className="block truncate">{sourceLabel(primary)}</span>
                {primary.source_record_id && (
                  <span className="mt-0.5 block truncate font-mono text-2xs text-ink-muted">
                    запись {primary.source_record_id}
                  </span>
                )}
              </span>
            </a>
          )}
          {office.active_impacts.length > 1 && (
            <div className="space-y-1 pt-2">
              <div className="text-xs text-ink-muted">Другие угрозы</div>
              {office.active_impacts.slice(1).map((impact) => (
                <div key={impact.id} className="rounded border border-line/60 bg-bg-subtle px-2 py-1 text-xs text-ink-muted">
                  {severityLabel(impact.severity)}: {impact.reason ?? "Причина не указана"}
                </div>
              ))}
            </div>
          )}
        </div>
      ) : (
        <div className="rounded-md border border-accent-green/20 bg-accent-green/10 p-3 text-sm text-accent-green">
          Ближайших угроз нет.
        </div>
      )}
    </div>
  );
}

export function OfficeMap() {
  const [statusFilter, setStatusFilter] = useState<StatusFilter>("all");
  const [severityFilter, setSeverityFilter] = useState<SeverityFilter>("all");
  const [search, setSearch] = useState("");
  const [selectedOfficeId, setSelectedOfficeId] = useState<string | null>(null);

  const { data, error, isLoading, isFetching, refetch } = useQuery({
    queryKey: ["map-offices"],
    queryFn: () => api.getMapOffices(),
  });

  const offices = data?.offices ?? EMPTY_OFFICES;
  const filteredOffices = useMemo(() => {
    const q = search.trim().toLowerCase();
    return offices.filter((office) => {
      const statusMatches =
        statusFilter === "all" ||
        (statusFilter === "problem" ? office.status !== "ok" : office.status === statusFilter);
      const severityMatches =
        severityFilter === "all" ||
        office.active_impacts.some((impact) => impact.severity === severityFilter);
      const searchMatches =
        !q ||
        [office.name, office.address, office.city, office.region].some((value) =>
          value.toLowerCase().includes(q),
        );
      return statusMatches && severityMatches && searchMatches;
    });
  }, [offices, search, severityFilter, statusFilter]);

  const severityOptions = useMemo(() => {
    const values = new Set<MapImpactSeverity>();
    offices.forEach((office) => office.active_impacts.forEach((impact) => values.add(impact.severity)));
    return SEVERITY_ORDER.filter((severity) => values.has(severity));
  }, [offices]);

  useEffect(() => {
    if (selectedOfficeId && !filteredOffices.some((office) => office.id === selectedOfficeId)) {
      setSelectedOfficeId(null);
    }
  }, [filteredOffices, selectedOfficeId]);

  useEffect(() => {
    if (!selectedOfficeId && filteredOffices.length > 0) {
      setSelectedOfficeId(
        filteredOffices.find((office) => office.status !== "ok")?.id ?? filteredOffices[0].id,
      );
    }
  }, [filteredOffices, selectedOfficeId]);

  const selectedOffice = filteredOffices.find((office) => office.id === selectedOfficeId);
  const totals = useMemo(
    () => ({
      all: offices.length,
      ok: offices.filter((office) => office.status === "ok").length,
      risk: offices.filter((office) => office.status === "risk").length,
      critical: offices.filter((office) => office.status === "critical").length,
      missingCoordinates: offices.filter((office) => !hasCoordinates(office)).length,
      threats: offices.filter((office) => office.active_impacts.length > 0).length,
    }),
    [offices],
  );

  return (
    <div className="space-y-6">
      <PageHeader
        title="Карта угроз для офисов"
        description="Текущие и ближайшие отключения электроэнергии, влияющие на офисы."
        actions={
          <button className="btn btn-primary !py-1.5 !text-xs" onClick={() => refetch()} disabled={isFetching}>
            {isFetching ? <Loader2 size={14} className="animate-spin" /> : <RefreshCw size={14} />}
            Обновить
          </button>
        }
      />

      {error ? (
        <Card>
          <CardBody>
            <EmptyState
              title="API карты недоступен"
              hint={(error as Error).message}
              icon={<AlertTriangle size={22} />}
            />
          </CardBody>
        </Card>
      ) : (
        <>
          <Card>
            <CardBody className="space-y-3">
              <div className="flex flex-wrap items-center gap-2">
                <Badge tone="gray">Всего {totals.all}</Badge>
                <Badge tone="green">Без угроз {totals.ok}</Badge>
                <Badge tone="amber">Риск {totals.risk}</Badge>
                <Badge tone="red">Критично {totals.critical}</Badge>
                {totals.missingCoordinates > 0 && <Badge tone="amber">Нет координат {totals.missingCoordinates}</Badge>}
                {totals.all > 0 && totals.threats === 0 && <Badge tone="green">Угроз нет</Badge>}
              </div>
              <div className="grid grid-cols-1 gap-2 lg:grid-cols-[minmax(220px,1fr)_180px_180px]">
                <label className="relative">
                  <Search size={14} className="pointer-events-none absolute left-3 top-1/2 -translate-y-1/2 text-ink-dim" />
                  <input
                    className="input w-full pl-8"
                    value={search}
                    onChange={(event) => setSearch(event.target.value)}
                    placeholder="Поиск офиса или адреса"
                  />
                </label>
                <select className="input" value={statusFilter} onChange={(event) => setStatusFilter(event.target.value as StatusFilter)}>
                  <option value="all">Все статусы</option>
                  <option value="problem">Только с угрозами</option>
                  <option value="ok">Без угроз</option>
                  <option value="risk">Риск</option>
                  <option value="critical">Критично</option>
                </select>
                <select
                  className="input"
                  value={severityFilter}
                  onChange={(event) => setSeverityFilter(event.target.value as SeverityFilter)}
                  disabled={severityOptions.length === 0}
                >
                  <option value="all">Любой уровень</option>
                  {severityOptions.map((severity) => (
                    <option key={severity} value={severity}>
                      {severityLabel(severity)}
                    </option>
                  ))}
                </select>
              </div>
            </CardBody>
          </Card>

          {isLoading ? (
            <div className="h-[calc(100vh-220px)] min-h-[520px] animate-pulse rounded-lg border border-line bg-bg-elevated/60" />
          ) : (
            <div className="grid grid-cols-1 gap-4 xl:grid-cols-[minmax(0,1fr)_380px]">
              <OfficeLeafletMap offices={filteredOffices} selectedOfficeId={selectedOfficeId} onSelect={setSelectedOfficeId} />

              <div className="space-y-4">
                <Card>
                  <CardHeader title="Выбранный офис" subtitle={selectedOffice ? selectedOffice.region : "Нет выбора"} />
                  <CardBody>
                    <SelectedOffice office={selectedOffice} />
                  </CardBody>
                </Card>

                <Card>
                  <CardHeader title="Офисы" subtitle={`${filteredOffices.length} показано`} />
                  <CardBody className="max-h-[420px] space-y-2 overflow-y-auto">
                    {filteredOffices.length === 0 ? (
                      <EmptyState title="Офисы не найдены" hint="Измените фильтры или поисковый запрос." />
                    ) : (
                      filteredOffices.map((office) => (
                        <OfficeListItem
                          key={office.id}
                          office={office}
                          selected={office.id === selectedOfficeId}
                          onSelect={setSelectedOfficeId}
                        />
                      ))
                    )}
                  </CardBody>
                </Card>

                {offices.some((office) => !hasCoordinates(office)) && (
                  <Card>
                    <CardHeader title="Нет координат" subtitle="Маркеры пропущены" />
                    <CardBody className="space-y-2">
                      {offices.filter((office) => !hasCoordinates(office)).map((office) => (
                        <div key={office.id} className="rounded-md border border-line/60 bg-bg-elevated/40 p-2">
                          <div className="truncate text-sm text-ink">{office.name}</div>
                          <div className="mt-0.5 text-xs text-ink-muted">{office.address}</div>
                        </div>
                      ))}
                    </CardBody>
                  </Card>
                )}
              </div>
            </div>
          )}
        </>
      )}
    </div>
  );
}
