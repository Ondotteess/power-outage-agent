import {
  Area,
  AreaChart,
  CartesianGrid,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import type { QueueBacklogPoint } from "@/lib/api/types";

export function QueueBacklogChart({ data }: { data: QueueBacklogPoint[] }) {
  const rows = data.map((p) => ({
    time: new Date(p.at).getHours() + ":00",
    pending: p.pending,
    running: p.running,
    failed: p.failed,
  }));
  return (
    <div className="h-48 w-full">
      <ResponsiveContainer>
        <AreaChart data={rows} margin={{ top: 4, right: 12, bottom: 4, left: -16 }}>
          <defs>
            <linearGradient id="gp" x1="0" y1="0" x2="0" y2="1">
              <stop offset="0%" stopColor="#22D3EE" stopOpacity={0.4} />
              <stop offset="100%" stopColor="#22D3EE" stopOpacity={0} />
            </linearGradient>
            <linearGradient id="gr" x1="0" y1="0" x2="0" y2="1">
              <stop offset="0%" stopColor="#10B981" stopOpacity={0.4} />
              <stop offset="100%" stopColor="#10B981" stopOpacity={0} />
            </linearGradient>
            <linearGradient id="gf" x1="0" y1="0" x2="0" y2="1">
              <stop offset="0%" stopColor="#EF4444" stopOpacity={0.4} />
              <stop offset="100%" stopColor="#EF4444" stopOpacity={0} />
            </linearGradient>
          </defs>
          <CartesianGrid strokeDasharray="3 3" stroke="#1F2937" vertical={false} />
          <XAxis dataKey="time" stroke="#6B7280" fontSize={10} tickLine={false} axisLine={false} />
          <YAxis stroke="#6B7280" fontSize={10} tickLine={false} axisLine={false} width={28} />
          <Tooltip
            contentStyle={{
              background: "#111827",
              border: "1px solid #1F2937",
              borderRadius: 8,
              fontSize: 12,
            }}
            labelStyle={{ color: "#9CA3AF" }}
          />
          <Area type="monotone" dataKey="pending" stroke="#22D3EE" fill="url(#gp)" strokeWidth={1.5} />
          <Area type="monotone" dataKey="running" stroke="#10B981" fill="url(#gr)" strokeWidth={1.5} />
          <Area type="monotone" dataKey="failed" stroke="#EF4444" fill="url(#gf)" strokeWidth={1.5} />
        </AreaChart>
      </ResponsiveContainer>
    </div>
  );
}
