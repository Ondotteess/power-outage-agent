import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Loader2, RotateCw } from "lucide-react";
import { api } from "@/lib/api";
import { Card, CardHeader } from "@/components/ui/Card";
import { DataTable, Column } from "@/components/ui/DataTable";
import { Badge, StatusBadge } from "@/components/ui/Badge";
import { PageHeader } from "@/components/ui/PageHeader";
import { fmtRelative, truncate } from "@/lib/format";
import type { Task } from "@/lib/api/types";

function RetryButton({ task }: { task: Task }) {
  const qc = useQueryClient();
  const m = useMutation({
    mutationFn: () => api.retryTask(task.id),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["tasks"] }),
  });
  return (
    <button className="btn btn-primary !py-1 !text-xs" onClick={() => m.mutate()} disabled={m.isPending}>
      {m.isPending ? <Loader2 size={12} className="animate-spin" /> : <RotateCw size={12} />}
      Retry
    </button>
  );
}

export function DLQ() {
  const { data, isLoading, error } = useQuery({
    queryKey: ["tasks", "failed", { limit: 200 }],
    queryFn: () => api.listTasks({ status: "failed", limit: 200 }),
  });

  const cols: Column<Task>[] = [
    { key: "time", header: "Last attempt", cell: (r) => <span className="text-xs" title={r.updated_at}>{fmtRelative(r.updated_at)}</span> },
    { key: "type", header: "Task", cell: (r) => <Badge tone="blue">{r.task_type}</Badge> },
    { key: "source", header: "Source", cell: (r) => <span className="font-mono text-2xs text-ink-muted">{r.source_id?.slice(0, 8) ?? "—"}</span> },
    { key: "err", header: "Error", cell: (r) => <span className="text-xs text-accent-red/90">{truncate(r.error, 80)}</span> },
    { key: "att", header: "Retries", cell: (r) => <span className="font-mono text-xs">{r.attempt}</span> },
    { key: "next", header: "Next retry", cell: (r) => <span className="text-xs text-ink-muted">{fmtRelative(r.next_retry_at)}</span> },
    { key: "status", header: "Status", cell: (r) => <StatusBadge status={r.status} /> },
    { key: "actions", header: "", cell: (r) => <RetryButton task={r} /> },
  ];

  return (
    <div className="space-y-6">
      <PageHeader
        title="Tasks / DLQ"
        description="Failed tasks awaiting retry or manual review. DLQ is persisted in the `tasks` table (status=failed)."
      />
      <Card>
        <CardHeader
          title="Dead letter queue"
          subtitle={data ? `${data.length} failed task(s)` : "—"}
          right={data && data.length > 0 && <Badge tone="red">{data.length}</Badge>}
        />
        <DataTable columns={cols} rows={data} isLoading={isLoading} error={error} rowKey={(r) => r.id} empty="No failed tasks 🎉" />
      </Card>
    </div>
  );
}
