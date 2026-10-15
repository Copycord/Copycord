// Page buttons under the table, same shape as the old .logs-pagination. The pager is always there,
// one page included, so the row keeps its height from the first paint to the last page.

import { cx } from "../../components/ui/cx";
import { pageRange } from "./model";

export const PAGINATION = "flex flex-wrap items-center justify-center gap-1 py-4";
const BTN =
  "cursor-pointer rounded-lg border px-3 py-1.5 text-[0.85rem] transition-colors " +
  "disabled:cursor-default disabled:opacity-30";
const IDLE = "border-border bg-transparent text-text enabled:hover:border-accent enabled:hover:bg-[rgba(139,92,246,0.15)]";
const ACTIVE = "border-accent bg-accent text-white";

const NOOP = () => {};

/** inert: the skeleton's copy, without the id and the handlers. */
export function Pagination({
  page,
  pages,
  onPage = NOOP,
  inert = false,
}: {
  page: number;
  pages: number;
  onPage?: (p: number) => void;
  inert?: boolean;
}) {
  const range = pages <= 1 ? [1] : pageRange(page, pages);
  return (
    <nav id={inert ? undefined : "logs-pagination"} aria-label="Pagination" className={PAGINATION} inert={inert || undefined}>
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
