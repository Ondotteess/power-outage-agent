import { useQuery } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { Card, CardBody, CardHeader } from "@/components/ui/Card";
import { DataTable, Column } from "@/components/ui/DataTable";
import { Badge, StatusBadge } from "@/components/ui/Badge";
import { PageHeader } from "@/components/ui/PageHeader";
import { QueueBacklogChart } from "@/components/charts/QueueBacklogChart";
import { fmtRelative, truncate } from "@/lib/format";
import type { Task } from "@/lib/api/types";

export function Queue() {
  const tasks = useQuery({ queryKey: ["tasks", { limit: 100 }], queryFn: () => api.listTasks({ limit: 100 }) });
  const backlog = useQuery({ queryKey: ["backlog"], queryFn: () => api.getQueueBacklog() });

  const cols: Column<Task>[] = [
    { key: "type", header: "Type", cell: (r) => <Badge tone="blue">{r.task_type}</Badge> },
    { key: "status", header: "Status", cell: (r) => <StatusBadge status={r.status} /> },
    { key: "attempt", header: "Attempt", cell: (r) => <span className="font-mono text-xs">{r.attempt}</span> },
    { key: "hash", header: "Input hash", cell: (r) => <span className="font-mono text-2xs text-ink-muted">{r.input_hash.slice(0, 10)}…</span> },
    { key: "err", header: "Error", cell: (r) => <span className="text-xs text-accent-red/90">{truncate(r.error, 60)}</span> },
    { key: "upd", header: "Updated", cell: (r) => <span className="text-xs" title={r.updated_at}>{fmtRelative(r.updated_at)}</span> },
  ];

  return (
    <div className="space-y-6">
      <PageHeader title="Queue" description="Live task queue across all stages." />
      <Card>
        <CardHeader title="Backlog (24h)" />
        <CardBody>{backlog.data ? <QueueBacklogChart data={backlog.data} /> : null}</CardBody>
      </Card>
      <Card>
        <CardHeader title="Recent tasks" />
        <DataTable columns={cols} rows={tasks.data} isLoading={tasks.isLoading} error={tasks.error} rowKey={(r) => r.id} />
      </Card>
    </div>
  );
}
