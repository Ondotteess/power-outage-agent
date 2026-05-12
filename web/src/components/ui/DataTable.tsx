import { ReactNode } from "react";

export interface Column<T> {
  key: string;
  header: ReactNode;
  cell: (row: T) => ReactNode;
  className?: string;
  width?: string;
}

interface Props<T> {
  columns: Column<T>[];
  rows: T[] | undefined;
  isLoading?: boolean;
  error?: unknown;
  empty?: ReactNode;
  rowKey?: (row: T) => string;
}

export function DataTable<T>({
  columns,
  rows,
  isLoading,
  error,
  empty = "No records",
  rowKey,
}: Props<T>) {
  if (isLoading) {
    return (
      <div className="space-y-2 p-4">
        {Array.from({ length: 6 }).map((_, i) => (
          <div key={i} className="h-8 animate-pulse rounded bg-bg-elevated/60" />
        ))}
      </div>
    );
  }
  if (error) {
    return (
      <div className="p-6 text-sm text-accent-red">
        Failed to load: {(error as Error).message}
      </div>
    );
  }
  if (!rows || rows.length === 0) {
    return <div className="p-10 text-center text-sm text-ink-muted">{empty}</div>;
  }
  return (
    <div className="table-wrap">
      <table className="data-table">
        <thead>
          <tr>
            {columns.map((c) => (
              <th key={c.key} className={c.className} style={c.width ? { width: c.width } : undefined}>
                {c.header}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, i) => (
            <tr key={rowKey ? rowKey(row) : i}>
              {columns.map((c) => (
                <td key={c.key} className={c.className}>
                  {c.cell(row)}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
