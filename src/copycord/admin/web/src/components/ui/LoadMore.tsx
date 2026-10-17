// Infinite scroll for a list: a sentinel under the last row that calls onMore when it nears the
// viewport. While a page is out a small "Loading more" row stands there; once nothing is left it renders
// nothing. A page that lands without filling the screen keeps the sentinel visible, so the observer is
// rebuilt after each load and fires again for the next page.
//   <LoadMore hasMore={runs.length < total} loading={loadingMore} onMore={loadMore} />

import { useEffect, useRef } from "react";
import { cx } from "./cx";

/** How far below the viewport the sentinel starts the next page, so it usually lands before the user gets there. */
export const LOAD_MORE_MARGIN = "300px";
export const LOAD_MORE_TEXT = "Loading more";

export interface LoadMoreProps {
  hasMore: boolean;
  /** A page is out: the row says so and no second fetch starts. */
  loading: boolean;
  onMore: () => void;
  className?: string;
}

export function LoadMore({ hasMore, loading, onMore, className }: LoadMoreProps) {
  const ref = useRef<HTMLDivElement>(null);
  const onMoreRef = useRef(onMore);
  onMoreRef.current = onMore;

  useEffect(() => {
    const el = ref.current;
    if (!el || !hasMore || loading || typeof IntersectionObserver === "undefined") return;
    const io = new IntersectionObserver(
      (entries) => {
        if (entries.some((e) => e.isIntersecting)) onMoreRef.current();
      },
      { rootMargin: `0px 0px ${LOAD_MORE_MARGIN} 0px` },
    );
    io.observe(el);
    return () => io.disconnect();
  }, [hasMore, loading]);

  if (!hasMore) return null;
  return (
    <div ref={ref} data-load-more="" className={cx(loading ? "px-4 py-3 text-center text-sm text-muted" : "h-px", className)}>
      {loading ? (
        <span role="status" aria-live="polite">
          {LOAD_MORE_TEXT}
        </span>
      ) : null}
    </div>
  );
}
