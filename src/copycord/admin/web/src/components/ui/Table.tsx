// Typed table: columns pick a key or render a cell; shows skeleton rows while
// loading and an empty state when there are no rows.

import type { ReactNode } from "react";
import { cx } from "./cx";
import { EmptyState } from "./EmptyState";

export interface Column<T> {
  key: string;
  header: ReactNode;
  /** Cell content; defaults to String(row[key]) when key is a field of T. */
  cell?: (row: T, index: number) => ReactNode;
  className?: string;
  align?: "left" | "right" | "center";
}

export interface TableProps<T> {
  columns: Column<T>[];
  rows: readonly T[];
  rowKey: (row: T, index: number) => string | number;
  loading?: boolean;
  skeletonRows?: number;
  empty?: ReactNode;
  caption?: string;
  onRowClick?: (row: T) => void;
  className?: string;
}

const ALIGN = { left: "text-left", right: "text-right", center: "text-center" } as const;

function defaultCell<T>(row: T, key: string): ReactNode {
  const v = (row as Record<string, unknown>)[key];
  if (v === null || v === undefined) return "";
  return typeof v === "object" ? JSON.stringify(v) : String(v);
}

export function Table<T>({
  columns,
  rows,
  rowKey,
  loading = false,
  skeletonRows = 3,
  empty = "Nothing here yet.",
  caption,
  onRowClick,
  className,
}: TableProps<T>) {
  return (
    <div className={cx("overflow-x-auto", className)}>
      <table className="w-full border-collapse text-sm" aria-busy={loading || undefined}>
        {caption ? <caption className="sr-only">{caption}</caption> : null}
        <thead>
          <tr>
            {columns.map((c) => (
              <th
                key={c.key}
                scope="col"
                className={cx(
                  "border-b border-border px-2 py-2.5 text-xs font-semibold uppercase tracking-wide text-muted",
                  ALIGN[c.align ?? "left"],
                  c.className,
                )}
              >
                {c.header}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {loading
            ? Array.from({ length: skeletonRows }, (_, i) => (
                <tr key={`skeleton-${i}`} data-testid="table-skeleton">
                  {columns.map((c) => (
                    <td key={c.key} className="border-b border-border px-2 py-2.5">
                      <span className="block h-3.5 w-3/4 animate-pulse rounded bg-line-2" />
                    </td>
                  ))}
                </tr>
              ))
            : rows.length === 0
              ? (
                  <tr>
                    <td colSpan={columns.length} className="py-3">
                      {typeof empty === "string" ? <EmptyState title={empty} /> : empty}
                    </td>
                  </tr>
                )
              : rows.map((row, i) => (
                  <tr
                    key={rowKey(row, i)}
                    onClick={onRowClick ? () => onRowClick(row) : undefined}
                    className={cx("transition-colors hover:bg-white/3", onRowClick && "cursor-pointer")}
                  >
                    {columns.map((c) => (
                      <td
                        key={c.key}
                        className={cx("border-b border-border px-2 py-2.5 text-text-1", ALIGN[c.align ?? "left"], c.className)}
                      >
                        {c.cell ? c.cell(row, i) : defaultCell(row, c.key)}
                      </td>
                    ))}
                  </tr>
                ))}
        </tbody>
      </table>
    </div>
  );
}
