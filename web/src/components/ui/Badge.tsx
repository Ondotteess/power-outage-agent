import { ReactNode } from "react";
import { statusTone, type Tone } from "./statusTone";

const TONES: Record<Tone, string> = {
  teal: "bg-accent-teal/10 text-accent-teal border-accent-teal/30",
  green: "bg-accent-green/10 text-accent-green border-accent-green/30",
  amber: "bg-accent-amber/10 text-accent-amber border-accent-amber/30",
  red: "bg-accent-red/10 text-accent-red border-accent-red/30",
  gray: "bg-ink-dim/10 text-ink-muted border-ink-dim/30",
  blue: "bg-accent-blue/10 text-accent-blue border-accent-blue/30",
};

export function Badge({
  tone = "gray",
  children,
  className = "",
}: {
  tone?: Tone;
  children: ReactNode;
  className?: string;
}) {
  return (
    <span
      className={`inline-flex items-center gap-1.5 rounded-md border px-2 py-0.5 text-2xs font-medium ${TONES[tone]} ${className}`}
    >
      {children}
    </span>
  );
}

export function StatusDot({ tone = "gray", pulse = false }: { tone?: Tone; pulse?: boolean }) {
  const colors: Record<Tone, string> = {
    teal: "bg-accent-teal",
    green: "bg-accent-green",
    amber: "bg-accent-amber",
    red: "bg-accent-red",
    gray: "bg-ink-dim",
    blue: "bg-accent-blue",
  };
  return (
    <span className="relative inline-flex h-2 w-2 shrink-0">
      {pulse && (
        <span
          className={`absolute inline-flex h-full w-full animate-ping rounded-full opacity-60 ${colors[tone]}`}
        />
      )}
      <span className={`relative inline-flex h-2 w-2 rounded-full ${colors[tone]}`} />
    </span>
  );
}

export function StatusBadge({ status }: { status: string }) {
  const tone = statusTone(status);
  return (
    <Badge tone={tone}>
      <StatusDot tone={tone} pulse={status === "running"} />
      <span className="uppercase tracking-wider">{status}</span>
    </Badge>
  );
}
