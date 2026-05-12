import { Radar } from "lucide-react";
import { Card, CardBody, CardHeader } from "@/components/ui/Card";
import { PageHeader } from "@/components/ui/PageHeader";
import { EmptyState } from "@/components/ui/EmptyState";

export function Dedup() {
  return (
    <div className="space-y-6">
      <PageHeader
        title="Dedup engine"
        description="Deduplication of normalized events by composite key (address + time + sources)."
      />
      <div className="grid grid-cols-1 gap-6 md:grid-cols-3">
        <Card>
          <CardHeader title="Skipped (24h)" />
          <CardBody>
            <div className="font-mono text-3xl text-accent-teal">17</div>
            <div className="text-xs text-ink-muted">duplicates collapsed</div>
          </CardBody>
        </Card>
        <Card>
          <CardHeader title="Strategy" />
          <CardBody>
            <div className="text-sm text-ink">address + time-window</div>
            <div className="mt-1 text-xs text-ink-muted">window ±30m, address normalized</div>
          </CardBody>
        </Card>
        <Card>
          <CardHeader title="Efficiency" />
          <CardBody>
            <div className="font-mono text-3xl text-ink">94%</div>
            <div className="text-xs text-ink-muted">cross-source overlap caught</div>
          </CardBody>
        </Card>
      </div>
      <Card>
        <CardHeader title="Recent dedup decisions" />
        <EmptyState icon={<Radar size={28} />} title="Dedup engine is on the Week-2 roadmap" hint="Wiring planned alongside improved location parsing." />
      </Card>
    </div>
  );
}
