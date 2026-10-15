// The Tasks page while its chunk downloads: the page's own rail, header, search box and list frame with its
// toolbar, in the same boxes and with the same static text, bars only where data goes; on a run url the run
// page's header and cards instead. Kept out of Tasks.tsx so the app shell can show it without pulling the
// page chunk in; the page takes its shared classes, the header, the search chrome, the toolbar labels and
// the loading pieces from here, so the two cannot drift.

import type { ReactNode } from "react";
import { Link, useMatch, useSearchParams } from "react-router";
import { Button } from "../../components/ui/Button";
import { cx } from "../../components/ui/cx";
import { Input } from "../../components/ui/Field";
import { Skeleton } from "../../components/ui/Skeleton";
import { ToolbarSelect } from "../channels/ToolbarSelect";

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
export const RAIL_LIST = "m-0 flex list-none flex-col gap-0.5 p-0";
export const MAIN = "min-w-0 flex-1";
export const FRAME = "overflow-hidden rounded-card border border-border bg-card";
export const FRAME_HEAD = "flex flex-wrap items-center justify-between gap-3 border-b border-border px-4 py-2.5 text-sm";
/** Left of the frame head: the run count and Auto-refresh. */
export const FRAME_HEAD_LEFT = "flex items-center gap-4";
/** Right of the frame head: the filter dropdowns. */
export const FRAME_HEAD_FILTERS = "flex flex-wrap items-center gap-3";
export const RUN_COUNT = "font-semibold text-text";
/** The box of a run row; ROW adds the hover tint. The skeleton row uses the box alone so both stand equally tall. */
export const ROW_BOX = "flex items-start gap-3 border-b border-border px-4 py-3 last:border-b-0";
export const ROW = `${ROW_BOX} hover:bg-rail-hover`;
export const SEARCH_FORM = "mb-4 flex";
export const SEARCH_INPUT = "min-w-0 flex-1 rounded-r-none";
export const SEARCH_BUTTON = "rounded-l-none border-l-0";

// The run page (RunDetail.tsx): header, then one column of cards.
export const RUN_HEADER = "mb-4";
export const RUN_BREADCRUMB = "mb-2 text-sm text-muted";
export const RUN_CRUMB = "text-accent no-underline hover:underline";
export const RUN_TITLE_ROW = "flex items-center gap-2";
export const RUN_TITLE = "m-0 text-[22px] font-bold text-text";
export const RUN_META = "mt-1 flex flex-wrap items-center gap-x-3 gap-y-1 text-sm text-muted";
export const RUN_BODY = "flex flex-col gap-4";
export const CARD_TITLE = "m-0 border-b border-border px-4 py-2.5 text-sm font-semibold text-text";
/** The Inputs list under the stat tiles, and its small heading. */
export const INPUTS_BOX = "px-4 py-3";
export const INPUTS_TITLE = "m-0 mb-2 text-xs font-semibold text-muted";
export const INPUTS_LIST = "m-0 grid grid-cols-[minmax(110px,max-content)_1fr] gap-x-6 gap-y-1 text-sm";
/** One step row: glyph, title and note on one line, the progress bar under it while running. */
export const STEP_ROW = "border-b border-border px-4 py-2.5 text-sm last:border-b-0";
export const STEP_LINE = "flex items-center gap-2.5";
export const REPORT_SUMMARY =
  "cursor-pointer list-none px-4 py-2.5 text-sm font-semibold text-text marker:hidden [&::-webkit-details-marker]:hidden";

/** The Report panel's summary line, collapsed: the chevron and "Report (JSON)". */
export function ReportSummaryText() {
  return (
    <>
      <span aria-hidden="true" className="mr-2 inline-block text-muted transition-transform group-open:rotate-90">
        ›
      </span>
      Report (JSON)
    </>
  );
}

export const ALL_TASKS_DESCRIPTION = "Showing runs from all task types";

export const AUTO_REFRESH_KEY = "tasks:autorefresh";
export const AUTO_REFRESH_OPTIONS = [
  { value: "0", label: "Auto-refresh: Off" },
  { value: "10", label: "Auto-refresh: 10s" },
  { value: "30", label: "Auto-refresh: 30s" },
  { value: "60", label: "Auto-refresh: 60s" },
] as const;

/** The stored auto-refresh interval, 30 s by default. */
export function readAutoRefresh(): string {
  try {
    const v = localStorage.getItem(AUTO_REFRESH_KEY);
    if (v && AUTO_REFRESH_OPTIONS.some((o) => o.value === v)) return v;
  } catch {
    // Storage blocked: use the default.
  }
  return "30";
}

/** The filter dropdowns of the frame head in order; Task type shows on All tasks only. */
export const FILTER_LABELS = { type: "Task type", status: "Status", context: "Mapping", actor: "Actor" } as const;
export type FilterKey = keyof typeof FILTER_LABELS;

/** The first option of a filter dropdown, "Status: All". */
export function allOption(key: FilterKey) {
  return { value: "", label: `${FILTER_LABELS[key]}: All` };
}

export function runCountText(total: number): string {
  return total === 1 ? "1 task run" : `${total} task runs`;
}

export function TasksHeading() {
  return <h1 className="sr-only">Tasks</h1>;
}

const TITLE_WIDTHS = ["58%", "44%", "66%", "50%", "38%", "62%"];

/** A bar centred in one line box of the text it stands in for, so it is exactly that line's height. */
function TextBar({ width, height = 10, className }: { width: number | string; height?: number; className?: string }) {
  return (
    <span className={cx("flex h-[1lh] items-center", className)}>
      <Skeleton width={width} height={height} rounded="rounded-full" />
    </span>
  );
}

export interface TasksHeaderProps {
  /** null while the type is not known yet: a bar in the title's line. */
  title: string | null;
  description: string | null;
  action: ReactNode;
}

export function TasksHeader({ title, description, action }: TasksHeaderProps) {
  return (
    <div className="mb-4 flex items-start justify-between gap-3">
      <div className="min-w-0">
        <h2 className="m-0 text-[22px] font-bold text-text">{title ?? <TextBar width={180} height={16} />}</h2>
        <p className="mt-1 mb-0 text-sm text-muted">{description ?? <TextBar width={260} />}</p>
      </div>
      {action}
    </div>
  );
}

export function SearchGlyph() {
  return (
    <svg aria-hidden viewBox="0 0 16 16" width="14" height="14" fill="none" stroke="currentColor" strokeWidth="1.6">
      <circle cx="7" cy="7" r="4.5" />
      <path d="M10.5 10.5 14 14" strokeLinecap="round" />
    </svg>
  );
}

/** The run count while the first page loads: a bar the size of the count text. */
export function RunCountLoading() {
  return (
    <span data-skeleton="" className={cx(RUN_COUNT, "cc-skeleton inline-block rounded-md align-middle text-transparent")}>
      {runCountText(0)}
    </span>
  );
}

/** One rail entry while the task types load. */
export function RailEntryLoading() {
  return (
    <li aria-hidden="true">
      <div className={RAIL_ITEM}>
        <TextBar width="60%" className="min-w-0 flex-1" />
      </div>
    </li>
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
export function RunRowsLoading({ rows = 3 }: { rows?: number }) {
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

/** A bar inside a line of text, as wide as the words it stands in for. */
function InlineBar({ width }: { width: number }) {
  return (
    <span className="inline-flex h-[1lh] items-center align-top">
      <Skeleton width={width} height={10} rounded="rounded-full" />
    </span>
  );
}

const STEP_WIDTHS = [["34%", 72], ["26%", 96]] as const;

/**
 * The run page until the run answers, also the page's own loading state: the header with bars for the
 * type, glyph, title and meta line (the actions depend on the status, so they wait), the summary card
 * with its Inputs heading and bar rows, the Steps card with bar rows and the collapsed Report line.
 */
export function RunDetailSkeleton() {
  return (
    <div role="status" aria-busy="true" aria-label="Loading run" className="flex flex-col">
      <div inert>
        <header className={RUN_HEADER}>
          <nav aria-label="Breadcrumb" className={RUN_BREADCRUMB}>
            <Link to="/tasks" className={RUN_CRUMB}>
              All tasks
            </Link>
            {" / "}
            <InlineBar width={110} />
          </nav>
          <div className={RUN_TITLE_ROW}>
            <Skeleton width={20} height={20} rounded="rounded-full" />
            <h2 className={RUN_TITLE}>
              <TextBar width={240} height={16} />
            </h2>
          </div>
          <div className={RUN_META}>
            <TextBar width={130} />
            <TextBar width={90} />
            <TextBar width={110} />
          </div>
        </header>
        <div className={RUN_BODY}>
          <section aria-label="Summary" className={FRAME}>
            <div className={INPUTS_BOX}>
              <h3 className={INPUTS_TITLE}>Inputs</h3>
              <div className={INPUTS_LIST}>
                {[60, 90, 70, 80].map((w, i) => (
                  <div key={i} className="contents">
                    <TextBar width={w} />
                    <TextBar width={w + 40} />
                  </div>
                ))}
              </div>
            </div>
          </section>
          <section aria-label="Steps" className={FRAME}>
            <h3 className={CARD_TITLE}>Steps</h3>
            <ul className="m-0 list-none p-0">
              {STEP_WIDTHS.map(([title, note], i) => (
                <li key={i} className={STEP_ROW}>
                  <div className={STEP_LINE}>
                    <Skeleton width={14} height={14} rounded="rounded-full" />
                    <TextBar width={title} className="min-w-0 flex-1" />
                    <TextBar width={note} className="text-xs" />
                  </div>
                </li>
              ))}
            </ul>
          </section>
          <section aria-label="Report">
            <details className={cx(FRAME, "group")}>
              <summary className={REPORT_SUMMARY}>
                <ReportSummaryText />
              </summary>
            </details>
          </section>
        </div>
      </div>
    </div>
  );
}

const noop = () => {};

/** The page as Tasks first paints it, the controls inert under the section: the same rail, header, search and toolbar. */
export function TasksSkeleton() {
  const [params] = useSearchParams();
  const runPage = useMatch("/tasks/runs/:id") !== null;
  const typeKey = params.get("type") ?? "";
  const filters: FilterKey[] = typeKey ? ["status", "context", "actor"] : ["type", "status", "context", "actor"];
  return (
    <section role="status" aria-busy="true" aria-label="Loading page" className="w-full">
      <TasksHeading />
      <div inert className={PAGE}>
        <nav className={RAIL}>
          {/* A run page lights All tasks, as the page's rail does. */}
          <div className={cx(RAIL_ITEM, typeKey && !runPage ? null : RAIL_ACTIVE)}>All tasks</div>
          <div className={RAIL_HEADING}>Task types</div>
          <ul className={RAIL_LIST}>
            <RailEntryLoading />
          </ul>
        </nav>
        <div className={MAIN}>
          {runPage ? (
            <RunDetailSkeleton />
          ) : (
            <>
              {typeKey ? (
                <TasksHeader title={null} description={null} action={null} />
              ) : (
                <TasksHeader title="All tasks" description={ALL_TASKS_DESCRIPTION} action={null} />
              )}
              <form role="search" className={SEARCH_FORM} onSubmit={(e) => e.preventDefault()}>
                <Input
                  type="search"
                  aria-label="Search runs"
                  placeholder="Search runs"
                  value={params.get("q") ?? ""}
                  readOnly
                  className={SEARCH_INPUT}
                />
                <Button type="button" variant="ghost" attached aria-label="Search" className={SEARCH_BUTTON}>
                  <SearchGlyph />
                </Button>
              </form>
              <div className={FRAME}>
                <div className={FRAME_HEAD}>
                  <div className={FRAME_HEAD_LEFT}>
                    <RunCountLoading />
                    <ToolbarSelect
                      id="tasks-autorefresh-skeleton"
                      label="Auto-refresh"
                      value={readAutoRefresh()}
                      options={AUTO_REFRESH_OPTIONS}
                      onChange={noop}
                    />
                  </div>
                  <div className={FRAME_HEAD_FILTERS}>
                    {filters.map((k) => (
                      <ToolbarSelect key={k} id={`tasks-filter-${k}-skeleton`} label={FILTER_LABELS[k]} value="" options={[allOption(k)]} onChange={noop} />
                    ))}
                  </div>
                </div>
                <RunRowsLoading />
              </div>
            </>
          )}
        </div>
      </div>
    </section>
  );
}
