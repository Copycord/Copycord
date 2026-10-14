// Page buttons under the table, same shape as the old .logs-pagination.

import { cx } from "../../components/ui/cx";
import { pageRange } from "./model";

const BTN =
  "cursor-pointer rounded-lg border px-3 py-1.5 text-[0.85rem] transition-colors " +
  "disabled:cursor-default disabled:opacity-30";
const IDLE = "border-border bg-transparent text-text enabled:hover:border-accent enabled:hover:bg-[rgba(139,92,246,0.15)]";
const ACTIVE = "border-accent bg-accent text-white";

// With one page the old .logs-pagination stayed in the flow, empty, keeping its 16px padding.
const NAV = "flex flex-wrap items-center justify-center gap-1 py-4";

export function Pagination({ page, pages, onPage }: { page: number; pages: number; onPage: (p: number) => void }) {
  const range = pageRange(page, pages);
  if (range.length === 0) return <nav id="logs-pagination" aria-label="Pagination" className={NAV} />;
  return (
    <nav id="logs-pagination" aria-label="Pagination" className={NAV}>
      <button
        type="button"
        className={cx(BTN, IDLE)}
        disabled={page <= 1}
        aria-label="Previous page"
        onClick={() => onPage(page - 1)}
      >
        {"«"}
      </button>
      {range.map((p, i) =>
        p === null ? (
          <span key={`gap-${i}`} className="px-1 text-[0.85rem] text-muted">
            {"…"}
          </span>
        ) : (
          <button
            key={p}
            type="button"
            className={cx(BTN, p === page ? ACTIVE : IDLE)}
            aria-current={p === page ? "page" : undefined}
            aria-label={`Page ${p}`}
            onClick={() => onPage(p)}
          >
            {p}
          </button>
        ),
      )}
      <button
        type="button"
        className={cx(BTN, IDLE)}
        disabled={page >= pages}
        aria-label="Next page"
        onClick={() => onPage(page + 1)}
      >
        {"»"}
      </button>
    </nav>
  );
}
