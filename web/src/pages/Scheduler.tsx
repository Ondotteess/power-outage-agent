import { useQuery } from "@tanstack/react-query";
import { Clock } from "lucide-react";
import { api } from "@/lib/api";
import { Card, CardBody, CardHeader } from "@/components/ui/Card";
import { PageHeader } from "@/components/ui/PageHeader";
import { Badge, StatusBadge } from "@/components/ui/Badge";
import { fmtInterval, fmtRelative } from "@/lib/format";

export function Scheduler() {
  const { data } = useQuery({ queryKey: ["sources"], queryFn: () => api.listSources() });

  return (
    <div className="space-y-6">
      <PageHeader
        title="Scheduler"
        description="Per-source polling tick configuration. Tick fires task FETCH_SOURCE into the dispatcher."
      />
      <div className="grid grid-cols-1 gap-4 md:grid-cols-2 xl:grid-cols-3">
        {(data ?? []).map((s) => (
          <Card key={s.id}>
            <CardHeader
              title={s.name}
              subtitle={`every ${fmtInterval(s.poll_interval_seconds)}`}
              right={<StatusBadge status={s.status} />}
            />
            <CardBody className="space-y-2 text-sm">
              <div className="flex items-center gap-2 text-ink-muted">
                <Clock size={14} />
                Last fetch <span className="ml-auto text-ink" title={s.last_fetch ?? ""}>{fmtRelative(s.last_fetch)}</span>
              </div>
              <div className="flex items-center gap-2 text-ink-muted">
                Type
                <Badge tone="blue" className="ml-auto">{s.source_type}</Badge>
              </div>
              <div className="flex items-center gap-2 text-ink-muted">
                Active <span className="ml-auto">{s.is_active ? "yes" : "no"}</span>
              </div>
            </CardBody>
          </Card>
        ))}
      </div>
    </div>
  );
}
