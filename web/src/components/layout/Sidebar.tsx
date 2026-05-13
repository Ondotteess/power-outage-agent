import { NavLink } from "react-router-dom";
import {
  Activity,
  AlertTriangle,
  Bell,
  Building2,
  ChevronsLeft,
  ChevronsRight,
  Database,
  FileText,
  Gauge,
  GitBranch,
  Inbox,
  LayoutDashboard,
  ListChecks,
  MapPinned,
  Radar,
  Server,
  Settings,
  Sparkles,
  Zap,
} from "lucide-react";

interface Item {
  to: string;
  label: string;
  icon: typeof LayoutDashboard;
}

const NAV: Item[] = [
  { to: "/", label: "Dashboard", icon: LayoutDashboard },
  { to: "/sources", label: "Sources", icon: Database },
  { to: "/scheduler", label: "Scheduler", icon: Activity },
  { to: "/queue", label: "Queue", icon: Inbox },
  { to: "/pipeline", label: "Pipeline", icon: GitBranch },
  { to: "/raw", label: "Raw Records", icon: FileText },
  { to: "/parsed", label: "Parsed Records", icon: ListChecks },
  { to: "/normalization", label: "Normalization", icon: Sparkles },
  { to: "/dedup", label: "Dedup Engine", icon: Radar },
  { to: "/offices", label: "Office Matcher", icon: Building2 },
  { to: "/map", label: "Threat Map", icon: MapPinned },
  { to: "/notifications", label: "Notifications", icon: Bell },
  { to: "/dlq", label: "Tasks / DLQ", icon: AlertTriangle },
  { to: "/metrics", label: "Metrics", icon: Gauge },
  { to: "/logs", label: "Logs", icon: Server },
  { to: "/settings", label: "Settings", icon: Settings },
];

export function Sidebar({
  collapsed,
  onToggle,
}: {
  collapsed: boolean;
  onToggle: () => void;
}) {
  return (
    <aside
      className={`flex h-screen shrink-0 flex-col border-r border-line bg-bg-surface transition-[width] duration-200 ${
        collapsed ? "w-16" : "w-60"
      }`}
    >
      <div className={`flex h-14 items-center gap-2 border-b border-line px-3 ${collapsed ? "justify-center" : ""}`}>
        <div className="grid h-8 w-8 shrink-0 place-items-center rounded-md border border-accent-teal/40 bg-accent-teal/10 text-accent-teal">
          <Zap size={16} />
        </div>
        {!collapsed && (
          <div className="min-w-0">
            <div className="truncate text-sm font-semibold text-ink">Outage Agent</div>
            <div className="truncate text-2xs text-ink-muted">Admin console</div>
          </div>
        )}
      </div>

      <nav className="flex-1 overflow-y-auto py-3">
        <ul className="space-y-0.5 px-2">
          {NAV.map(({ to, label, icon: Icon }) => (
            <li key={to}>
              <NavLink
                to={to}
                end={to === "/"}
                className={({ isActive }) =>
                  `group flex items-center gap-3 rounded-md px-2.5 py-2 text-sm transition-colors ${
                    isActive
                      ? "bg-accent-teal/10 text-accent-teal"
                      : "text-ink-muted hover:bg-bg-elevated hover:text-ink"
                  }`
                }
                title={collapsed ? label : undefined}
              >
                <Icon size={16} strokeWidth={1.75} className="shrink-0" />
                {!collapsed && <span className="truncate">{label}</span>}
              </NavLink>
            </li>
          ))}
        </ul>
      </nav>

      <div className={`border-t border-line p-3 ${collapsed ? "flex justify-center" : ""}`}>
        {!collapsed ? (
          <div className="flex items-center gap-2">
            <div className="grid h-8 w-8 shrink-0 place-items-center rounded-full bg-accent-blue/15 text-xs font-semibold text-accent-blue">
              A
            </div>
            <div className="min-w-0 flex-1">
              <div className="truncate text-sm text-ink">admin</div>
              <div className="truncate text-2xs text-ink-muted">noc@power-outage</div>
            </div>
            <button className="btn-ghost btn !p-1" onClick={onToggle} aria-label="Collapse sidebar">
              <ChevronsLeft size={16} />
            </button>
          </div>
        ) : (
          <button className="btn-ghost btn !p-1" onClick={onToggle} aria-label="Expand sidebar">
            <ChevronsRight size={16} />
          </button>
        )}
      </div>
    </aside>
  );
}
