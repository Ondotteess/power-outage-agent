import { ReactNode } from "react";
import { Card } from "./Card";
import { fmtNumber } from "@/lib/format";
import type { KpiDelta } from "@/lib/api/types";
import { ArrowDownRight, ArrowUpRight, Minus } from "lucide-react";

const STATUS_BAR: Record<KpiDelta["status"], string> = {
  success: "before:bg-accent-green",
  warning: "before:bg-accent-amber",
  error: "before:bg-accent-red",
  neutral: "before:bg-line",
};

const STATUS_TEXT: Record<KpiDelta["status"], string> = {
  success: "text-accent-green",
  warning: "text-accent-amber",
  error: "text-accent-red",
  neutral: "text-ink-muted",
};

export function KpiCard({
  label,
  data,
  icon,
}: {
  label: string;
  data: KpiDelta;
  icon?: ReactNode;
}) {
  const Arrow = data.delta_pct == null ? Minus : data.delta_pct >= 0 ? ArrowUpRight : ArrowDownRight;
  return (
    <Card
      className={`relative overflow-hidden before:absolute before:left-0 before:top-0 before:h-full before:w-0.5 ${STATUS_BAR[data.status]}`}
    >
      <div className="flex items-start justify-between p-4">
        <div className="min-w-0">
          <div className="text-xs uppercase tracking-wider text-ink-muted">{label}</div>
          <div className="mt-2 font-mono text-2xl font-semibold text-ink">
            {fmtNumber(data.value)}
          </div>
          {data.delta_label && (
            <div className={`mt-1 flex items-center gap-1 text-2xs ${STATUS_TEXT[data.status]}`}>
              <Arrow size={12} strokeWidth={2} />
              <span>{data.delta_label}</span>
            </div>
          )}
        </div>
        {icon && <div className="rounded-md border border-line bg-bg-elevated p-2 text-ink-muted">{icon}</div>}
      </div>
    </Card>
  );
}
