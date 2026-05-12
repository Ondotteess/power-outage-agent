import { Bar, BarChart, Cell, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";

export function ConfidenceBars({
  high,
  medium,
  low,
}: {
  high: number;
  medium: number;
  low: number;
}) {
  const rows = [
    { name: "low <50%", value: low, fill: "#EF4444" },
    { name: "medium 50–80%", value: medium, fill: "#F59E0B" },
    { name: "high ≥80%", value: high, fill: "#10B981" },
  ];
  return (
    <div className="h-40 w-full">
      <ResponsiveContainer>
        <BarChart data={rows} layout="vertical" margin={{ top: 4, right: 16, bottom: 4, left: -8 }}>
          <XAxis type="number" stroke="#6B7280" fontSize={10} tickLine={false} axisLine={false} />
          <YAxis
            type="category"
            dataKey="name"
            stroke="#6B7280"
            fontSize={11}
            tickLine={false}
            axisLine={false}
            width={110}
          />
          <Tooltip
            contentStyle={{
              background: "#111827",
              border: "1px solid #1F2937",
              borderRadius: 8,
              fontSize: 12,
            }}
            cursor={{ fill: "#1F293733" }}
          />
          <Bar dataKey="value" radius={[0, 4, 4, 0]}>
            {rows.map((r, i) => (
              <Cell key={i} fill={r.fill} />
            ))}
          </Bar>
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}
