import { useQuery } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { Card, CardHeader } from "@/components/ui/Card";
import { DataTable, Column } from "@/components/ui/DataTable";
import { Badge, StatusBadge } from "@/components/ui/Badge";
import { PageHeader } from "@/components/ui/PageHeader";
import { fmtRelative, truncate } from "@/lib/format";
import type { Notification } from "@/lib/api/types";

export function Notifications() {
  const { data, isLoading, error } = useQuery({ queryKey: ["notifications"], queryFn: () => api.listNotifications() });

  const cols: Column<Notification>[] = [
    { key: "time", header: "Emitted", cell: (r) => <span className="text-xs">{fmtRelative(r.emitted_at)}</span> },
    { key: "office", header: "Office", cell: (r) => r.office_name },
    { key: "channel", header: "Channel", cell: (r) => <Badge tone="blue">{r.channel}</Badge> },
    { key: "status", header: "Status", cell: (r) => <StatusBadge status={r.status} /> },
    { key: "summary", header: "Summary", cell: (r) => <span className="text-ink-muted">{truncate(r.summary, 80)}</span> },
  ];

  return (
    <div className="space-y-6">
      <PageHeader title="Notifications" description="Outbound alerts dispatched to office contacts and shared channels." />
      <Card>
        <CardHeader title="Recent notifications" subtitle="Last 24h" />
        <DataTable columns={cols} rows={data} isLoading={isLoading} error={error} rowKey={(r) => r.id} />
      </Card>
    </div>
  );
}
