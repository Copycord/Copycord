// The Tasks page while its chunk downloads: the type rail, the header and the list frame with bars where
// the data goes. Kept out of Tasks.tsx so the app shell can show it without pulling the page chunk in;
// the page takes its shared classes and the loading rows from here.

import { cx } from "../../components/ui/cx";
import { Skeleton, SkeletonSection } from "../../components/ui/Skeleton";

export const PAGE = "my-3 flex w-full gap-6 max-md:flex-col";
export const RAIL = "w-[240px] shrink-0 max-md:w-full";
export const RAIL_ITEM =
  "relative flex items-center justify-between gap-2 rounded-rail px-3 py-1.5 text-sm text-text no-underline transition-colors " +
  "focus-visible:outline-2 focus-visible:outline-accent/70";
export const RAIL_HEADING = "mt-4 mb-1 px-3 text-xs font-semibold text-muted";
export const MAIN = "min-w-0 flex-1";
export const FRAME = "overflow-hidden rounded-card border border-border bg-card";
export const FRAME_HEAD = "flex flex-wrap items-center justify-between gap-3 border-b border-border px-4 py-2.5 text-sm";
export const ROW = "flex items-start gap-3 border-b border-border px-4 py-3 last:border-b-0 hover:bg-rail-hover";

export function TasksHeading() {
  return <h1 className="sr-only">Tasks</h1>;
}

/** The list body while a page of runs loads. */
export function RunRowsLoading({ rows = 6 }: { rows?: number }) {
  return (
    <div className="px-4 py-2">
      <SkeletonSection rows={rows} rowHeight={44} label="Loading task runs" />
    </div>
  );
}

export function TasksSkeleton() {
  return (
    <section role="status" aria-busy="true" aria-label="Loading page" className="w-full">
      <TasksHeading />
      <div inert className={PAGE}>
        <nav className={RAIL}>
          <div className={cx(RAIL_ITEM, "bg-rail-active")}>All tasks</div>
          <div className={RAIL_HEADING}>Task types</div>
          {[70, 55].map((w) => (
            <div key={w} className={RAIL_ITEM}>
              <Skeleton.Line width={`${w}%`} />
            </div>
          ))}
        </nav>
        <div className={MAIN}>
          <div className="mb-4 flex items-start justify-between gap-3">
            <div className="min-w-0 flex-1">
              <h2 className="m-0 text-[22px] font-bold text-text">All tasks</h2>
              <Skeleton.Line width={240} />
            </div>
            <Skeleton.Block width={96} height={34} />
          </div>
          <div className={FRAME}>
            <div className={FRAME_HEAD}>
              <Skeleton.Line width={110} />
              <Skeleton.Line width={220} />
            </div>
            <RunRowsLoading />
          </div>
        </div>
      </div>
    </section>
  );
}
