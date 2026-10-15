// The Tasks page while its chunk downloads: the type rail, the header and the list frame with bars where
// the data goes. Kept out of Tasks.tsx so the app shell can show it without pulling the page chunk in;
// the page takes its shared classes and the loading rows from here.

import { cx } from "../../components/ui/cx";
import { Skeleton } from "../../components/ui/Skeleton";

export const PAGE = "my-3 flex w-full gap-6 max-md:flex-col";
export const RAIL = "w-[240px] shrink-0 max-md:w-full";
export const RAIL_ITEM =
  "relative flex items-center justify-between gap-2 rounded-rail px-3 py-1.5 text-sm text-text no-underline transition-colors " +
  "focus-visible:outline-2 focus-visible:outline-accent/70";
/** The selected rail entry: tinted pill plus the accent bar on the rail's left edge, like the sidebar's active item. */
export const RAIL_ACTIVE =
  "bg-rail-active font-semibold before:absolute before:top-1 before:bottom-1 before:left-0 before:w-[3px] before:rounded-full " +
  "before:bg-primary before:content-['']";
export const RAIL_HEADING = "mt-4 mb-1 px-3 text-xs font-semibold text-muted";
export const MAIN = "min-w-0 flex-1";
export const FRAME = "overflow-hidden rounded-card border border-border bg-card";
export const FRAME_HEAD = "flex flex-wrap items-center justify-between gap-3 border-b border-border px-4 py-2.5 text-sm";
/** The box of a run row; ROW adds the hover tint. The skeleton row uses the box alone so both stand equally tall. */
export const ROW_BOX = "flex items-start gap-3 border-b border-border px-4 py-3 last:border-b-0";
export const ROW = `${ROW_BOX} hover:bg-rail-hover`;

export function TasksHeading() {
  return <h1 className="sr-only">Tasks</h1>;
}

const TITLE_WIDTHS = ["58%", "44%", "66%", "50%", "38%", "62%"];

/** A bar centred in one line box of the text it stands in for, so it is exactly that line's height. */
function TextBar({ width, height = 10, className }: { width: number | string; height?: number; className?: string }) {
  return (
    <div className={cx("flex h-[1lh] items-center", className)}>
      <Skeleton width={width} height={height} rounded="rounded-full" />
    </div>
  );
}

/**
 * One loading row in the shape of RunRow (RunList.tsx): the same box, the status glyph, the title and
 * type lines, the mapping badge column, the two timing lines and the row menu button, each in the same
 * type size and padding as the real element, so the first page lands without the list jumping.
 */
function RunRowLoading({ titleWidth }: { titleWidth: string }) {
  return (
    <li className={ROW_BOX}>
      <span className="mt-0.5 inline-flex shrink-0">
        <Skeleton width={16} height={16} rounded="rounded-full" />
      </span>
      <div className="min-w-0 flex-1">
        <TextBar width={titleWidth} height={12} className="font-semibold" />
        <TextBar width="34%" className="mt-0.5 text-xs" />
      </div>
      <div className="w-[180px] shrink-0 max-md:hidden">
        {/* The Badge box: text-xs normal line height, py-0.5 and a border. */}
        <span className="cc-skeleton inline-flex rounded-full border border-transparent px-2 py-0.5 text-xs leading-normal font-semibold text-transparent">
          Mapping name
        </span>
      </div>
      <div className="flex w-[170px] shrink-0 flex-col gap-0.5 text-xs max-md:w-auto">
        <TextBar width={128} />
        <TextBar width={36} />
      </div>
      <div className="shrink-0 self-start">
        {/* The row menu button box with its glyph hidden. */}
        <span className="cc-skeleton inline-block rounded-btn border border-transparent px-2 py-0.5 text-transparent">⋯</span>
      </div>
    </li>
  );
}

/** The list body while a page of runs loads: rows shaped like the real ones. */
export function RunRowsLoading({ rows = 6 }: { rows?: number }) {
  return (
    <div role="status" aria-label="Loading task runs" aria-busy="true">
      <ul aria-hidden="true" className="m-0 list-none p-0">
        {Array.from({ length: rows }, (_, i) => (
          <RunRowLoading key={i} titleWidth={TITLE_WIDTHS[i % TITLE_WIDTHS.length]!} />
        ))}
      </ul>
    </div>
  );
}

export function TasksSkeleton() {
  return (
    <section role="status" aria-busy="true" aria-label="Loading page" className="w-full">
      <TasksHeading />
      <div inert className={PAGE}>
        <nav className={RAIL}>
          <div className={cx(RAIL_ITEM, RAIL_ACTIVE)}>All tasks</div>
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
