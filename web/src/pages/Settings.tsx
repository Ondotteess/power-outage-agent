import type { ReactNode } from "react";
import { Card, CardBody, CardHeader } from "@/components/ui/Card";
import { PageHeader } from "@/components/ui/PageHeader";
import { Badge } from "@/components/ui/Badge";
import { usingMock } from "@/lib/api";

function Row({ label, value }: { label: string; value: ReactNode }) {
  return (
    <div className="flex items-center justify-between border-t border-line/60 px-4 py-2.5 text-sm first:border-t-0">
      <span className="text-ink-muted">{label}</span>
      <span className="font-mono text-ink">{value}</span>
    </div>
  );
}

export function Settings() {
  return (
    <div className="space-y-6">
      <PageHeader title="Settings" description="Read-only view of system configuration (sourced from .env on the backend)." />

      <Card>
        <CardHeader title="Data source" />
        <CardBody className="!p-0">
          <Row
            label="API mode"
            value={
              usingMock ? <Badge tone="amber">mock</Badge> : <Badge tone="green">FastAPI /api</Badge>
            }
          />
          <Row label="Web origin" value={typeof window !== "undefined" ? window.location.origin : "—"} />
          <Row label="Backend base" value="/api (proxied to :8000 in dev)" />
        </CardBody>
      </Card>

      <Card>
        <CardHeader title="Pipeline" />
        <CardBody className="!p-0">
          <Row label="LLM provider" value="GigaChat (Sber)" />
          <Row label="Default model" value="GigaChat-2" />
          <Row label="Region" value="RU-KEM (Кемеровская обл.)" />
          <Row label="Task queue" value="asyncio.Queue (in-process)" />
          <Row label="DLQ storage" value="Postgres · tasks (status=failed)" />
        </CardBody>
      </Card>

      <Card>
        <CardHeader title="Notifications" />
        <CardBody className="!p-0">
          <Row label="Telegram" value={<Badge tone="gray">configured via TELEGRAM_BOT_TOKEN</Badge>} />
          <Row label="Email" value={<Badge tone="gray">not configured</Badge>} />
        </CardBody>
      </Card>
    </div>
  );
}
