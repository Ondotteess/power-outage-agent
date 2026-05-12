import { useQuery } from "@tanstack/react-query";
import { Building2 } from "lucide-react";
import { api } from "@/lib/api";
import { Card, CardHeader } from "@/components/ui/Card";
import { DataTable, Column } from "@/components/ui/DataTable";
import { Badge, StatusBadge } from "@/components/ui/Badge";
import { PageHeader } from "@/components/ui/PageHeader";
import { fmtDate, fmtRelative } from "@/lib/format";
import type { Office, OfficeImpact } from "@/lib/api/types";

export function OfficeMatcher() {
  const offices = useQuery({ queryKey: ["offices"], queryFn: () => api.listOffices() });
  const impacts = useQuery({ queryKey: ["office-impacts"], queryFn: () => api.listOfficeImpacts() });

  const officeCols: Column<Office>[] = [
    { key: "name", header: "Office", cell: (r) => <span className="text-ink">{r.name}</span> },
    { key: "city", header: "City", cell: (r) => r.city },
    { key: "addr", header: "Address", cell: (r) => <span className="text-ink-muted">{r.address}</span> },
    { key: "region", header: "Region", cell: (r) => <Badge tone="gray">{r.region}</Badge> },
  ];

  const impactCols: Column<OfficeImpact>[] = [
    { key: "office", header: "Office", cell: (r) => r.office_name },
    { key: "start", header: "Impact start", cell: (r) => <span className="font-mono text-xs">{fmtDate(r.impact_start)}</span> },
    { key: "lvl", header: "Level", cell: (r) => <StatusBadge status={r.impact_level === "high" ? "failed" : r.impact_level === "medium" ? "warning" : "info"} /> },
    { key: "strat", header: "Strategy", cell: (r) => <Badge tone="blue">{r.match_strategy}</Badge> },
    { key: "det", header: "Detected", cell: (r) => <span className="text-xs">{fmtRelative(r.detected_at)}</span> },
  ];

  return (
    <div className="space-y-6">
      <PageHeader
        title="Office matcher"
        description="Maps normalized outage events to known office addresses. Registry is mock — replaced on Week 3."
        actions={<Badge tone="amber"><Building2 size={12} /> mock registry</Badge>}
      />
      <Card>
        <CardHeader title="Impacts" subtitle="Detected next-24h overlaps" />
        <DataTable columns={impactCols} rows={impacts.data} isLoading={impacts.isLoading} error={impacts.error} rowKey={(r) => r.id} />
      </Card>
      <Card>
        <CardHeader title="Office registry" subtitle="Mock data — Кемеровская обл." />
        <DataTable columns={officeCols} rows={offices.data} isLoading={offices.isLoading} error={offices.error} rowKey={(r) => r.id} />
      </Card>
    </div>
  );
}
