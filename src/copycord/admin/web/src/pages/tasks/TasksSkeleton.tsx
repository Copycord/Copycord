// The Tasks page while its chunk downloads: the page's own rail, header, search box and list frame with its
// toolbar, in the same boxes and with the same static text, bars only where data goes; on a run url the run
// page's header and cards instead. Kept out of Tasks.tsx so the app shell can show it without pulling the
// page chunk in; the page takes its shared classes, the header, the search chrome, the toolbar labels and
// the loading pieces from here, so the two cannot drift.

import type { ReactNode } from "react";
import { Link, useMatch, useSearchParams } from "react-router";
import { Button } from "../../components/ui/Button";
import { cx } from "../../components/ui/cx";
import { EmptyState } from "../../components/ui/EmptyState";
import { Input } from "../../components/ui/Field";
import { readLastCount } from "../../components/ui/lastCount";
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
/** Content that takes a placeholder's place fades in over 180 ms; no fade under reduced motion. */
export const FADE_IN = "transition-opacity duration-[180ms] ease-out starting:opacity-0 motion-reduce:transition-none";
/** Held content while a refetch is out: dimmed only once the fetch passes 300 ms, so a quick answer never flickers. */
export const BUSY = "opacity-60 delay-300";
export const FRAME = "overflow-hidden rounded-card border border-border bg-card";
export const FRAME_HEAD = "flex flex-wrap items-center justify-between gap-3 border-b border-border px-4 py-2.5 text-sm";
/** Left of the frame head: the run count and Auto-refresh. */
export const FRAME_HEAD_LEFT = "flex items-center gap-4";
/** Right of the frame head: the filter dropdowns. */
export const FRAME_HEAD_FILTERS = "flex flex-wrap items-center gap-3";
export const RUN_COUNT = "font-semibold text-text";
/** The pager under the runs (RunPager). */
export const PAGER = "flex items-center justify-between gap-3 border-t border-border px-4 py-3 text-sm text-muted";
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
/** The Inputs list under the stat tiles, and its small heading. */
export const INPUTS_BOX = "px-4 py-3";
export const INPUTS_TITLE = "m-0 mb-2 text-xs font-semibold text-muted";
export const INPUTS_LIST = "m-0 grid grid-cols-[minmax(110px,max-content)_1fr] gap-x-6 gap-y-1 text-sm";
/** The status glyph's box (StatusIcon): one line of the row's text tall with the glyph centred, so the
 * glyph centres on the first text line in the run list, the step list and the run header alike. */
export const STATUS_BOX = "inline-flex h-[1lh] shrink-0 items-center";
/** The Steps card head: the title, the counts line, the overall bar and the status filter on one line. */
export const STEPS_HEAD = "flex flex-wrap items-center gap-x-3 gap-y-1.5 border-b border-border px-4 py-1.5 text-sm";
export const STEPS_TITLE = "m-0 text-sm font-semibold text-text";
export const STEPS_SUMMARY = "min-w-0 flex-1 truncate text-xs text-muted";
/** The overall bar's track in the head, and a running step's thin bar in its row. */
export const STEPS_BAR = "h-1 w-28 shrink-0 overflow-hidden rounded-full bg-line-1 max-sm:hidden";
export const STEP_BAR = "h-1 w-24 shrink-0 overflow-hidden rounded-full bg-line-1 max-sm:w-12";
/** Rows are one fixed line each, so the list stops at STEP_ROWS_MAX rows and scrolls past that. */
export const STEP_ROWS_MAX = 12;
export const STEP_LIST = "m-0 max-h-[384px] list-none overflow-y-auto p-0";
/** One step row, 32 px: glyph, title, the thin bar while running, the note on the right. */
export const STEP_ROW = "flex h-8 items-center gap-2.5 border-b border-border px-4 text-sm last:border-b-0";
export const STEP_EMPTY = "m-0 px-4 py-1.5 text-sm text-muted";
/** The steps the run page showed last time (lastCount.ts), so the loading card stands as tall. */
export const STEPS_COUNT_KEY = "tasks.steps";
export const STEP_FILTER_OPTIONS = [
  { value: "", label: "Status: All" },
  { value: "running", label: "Status: Running" },
  { value: "queued", label: "Status: Queued" },
  { value: "failed", label: "Status: Failed" },
  { value: "success", label: "Status: Done" },
] as const;
export type StepFilter = (typeof STEP_FILTER_OPTIONS)[number]["value"];

export interface StepsHeadProps {
  /** The counts line, or its bar while the run loads. */
  summary: ReactNode;
  /** The overall bar, or its placeholder. */
  progress: ReactNode;
  filter: StepFilter;
  onFilter: (value: StepFilter) => void;
  id: string;
}

/** The Steps card head; the loaded card and the run skeleton both draw it, so they cannot drift. */
export function StepsHead({ summary, progress, filter, onFilter, id }: StepsHeadProps) {
  return (
    <div className={STEPS_HEAD}>
      <h3 className={STEPS_TITLE}>Steps</h3>
      <span data-testid="steps-summary" className={STEPS_SUMMARY}>
        {summary}
      </span>
      {progress}
      <ToolbarSelect id={id} label="Step status" value={filter} options={STEP_FILTER_OPTIONS} onChange={(v) => onFilter(v as StepFilter)} />
    </div>
  );
}
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

/** Runs per page (runStore.ts asks for this many). */
export const PER_PAGE = 25;
/** The rows the run list showed last time (lastCount.ts), so the loading list stands as tall. */
export const RUNS_COUNT_KEY = "tasks.runs";

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

/** A Skeleton.Text on a line of its own; the wrapper carries the layout and font classes, so it is that line's height. */
function TextBar({ width, className }: { width: number | string; className?: string }) {
  return (
    <span className={cx("block", className)}>
      <Skeleton.Text block width={width} />
    </span>
  );
}

export interface TasksHeaderProps {
  /** null while the type is not known yet: a bar in the title's line. */
  title: string | null;
  description: string | null;
  action: ReactNode;
  /** A type page, whose title and description take the place of bars: they fade in. All tasks is static text. */
  fade?: boolean;
}

export function TasksHeader({ title, description, action, fade = false }: TasksHeaderProps) {
  const text = (v: string) => (fade ? <span className={FADE_IN}>{v}</span> : v);
  return (
    <div className="mb-4 flex items-start justify-between gap-3">
      <div className="min-w-0">
        <h2 className="m-0 text-[22px] font-bold text-text">{title != null ? text(title) : <TextBar width={180} />}</h2>
        <p className="mt-1 mb-0 text-sm text-muted">{description != null ? text(description) : <TextBar width={260} />}</p>
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

/**
 * Previous, the page label and Next under the runs. Always there, one page included, so the frame keeps
 * its height; pages is null until the first page lands (a bar on the label, both buttons disabled).
 */
export function RunPager({ page, pages, onPage = noop }: { page: number; pages: number | null; onPage?: (p: number) => void }) {
  return (
    <nav aria-label="Pagination" className={PAGER}>
      <Button variant="ghost" size="sm" disabled={page <= 1} onClick={() => onPage(page - 1)}>
        Previous
      </Button>
      {pages === null ? <Skeleton.Text>Page 1 of 1</Skeleton.Text> : <span>{`Page ${page} of ${pages}`}</span>}
      <Button variant="ghost" size="sm" disabled={pages === null || page >= pages} onClick={() => onPage(page + 1)}>
        Next
      </Button>
    </nav>
  );
}

/** The run count while the first page loads: a bar the size of the count text. */
export function RunCountLoading() {
  return <Skeleton.Text className={cx(RUN_COUNT, "align-middle")}>{runCountText(0)}</Skeleton.Text>;
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
      <span className={STATUS_BOX}>
        <Skeleton width={16} height={16} rounded="rounded-full" />
      </span>
      <div className="min-w-0 flex-1">
        <TextBar width={titleWidth} className="font-semibold" />
        <TextBar width="34%" className="mt-0.5 text-xs" />
      </div>
      <div className="w-[180px] shrink-0 max-md:hidden">
        {/* The Badge box: text-xs normal line height, py-0.5 and a border. */}
        <Skeleton.Box className="inline-flex rounded-full border px-2 py-0.5 text-xs leading-normal font-semibold">
          Mapping name
        </Skeleton.Box>
      </div>
      <div className="flex w-[170px] shrink-0 flex-col gap-0.5 text-xs max-md:w-auto">
        <TextBar width={128} />
        <TextBar width={36} />
      </div>
      <div className="shrink-0 self-start">
        {/* The row menu button box with its glyph hidden. */}
        <Skeleton.Box className="inline-block rounded-btn border px-2 py-0.5">⋯</Skeleton.Box>
      </div>
    </li>
  );
}

/** The empty state (Tasks.tsx) with bars on its two lines, for a list that had no runs last time. */
function RunsEmptyLoading() {
  return (
    <EmptyState
      plain
      title={<Skeleton.Text>No task runs yet</Skeleton.Text>}
      description={<Skeleton.Text>Pick a task type on the left to run one</Skeleton.Text>}
    />
  );
}

/**
 * The list body while a page of runs loads: as many rows shaped like the real ones as the list had last
 * time (at most a page), or the empty state's shape when it had none or this is the first visit.
 */
export function RunRowsLoading({ rows = readLastCount(RUNS_COUNT_KEY, 0, PER_PAGE) }: { rows?: number }) {
  return (
    <div role="status" aria-label="Loading task runs" aria-busy="true">
      {rows === 0 ? (
        <div aria-hidden="true">
          <RunsEmptyLoading />
        </div>
      ) : (
        <ul aria-hidden="true" className="m-0 list-none p-0">
          {Array.from({ length: rows }, (_, i) => (
            <RunRowLoading key={i} titleWidth={TITLE_WIDTHS[i % TITLE_WIDTHS.length]!} />
          ))}
        </ul>
      )}
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

const STEP_WIDTHS = [["34%", 72], ["26%", 96], ["30%", 84]] as const;

/**
 * The run page until the run answers, also the page's own loading state: the header with bars for the
 * type, glyph, title and meta line (the actions depend on the status, so they wait), the summary card
 * with its Inputs heading and bar rows, the Steps card with its real head (bars for the counts and the
 * overall bar, the real filter) and as many bar rows as the last run showed, and the collapsed Report line.
 */
export function RunDetailSkeleton({ steps = readLastCount(STEPS_COUNT_KEY, 2, STEP_ROWS_MAX) }: { steps?: number }) {
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
            <span className={STATUS_BOX}>
              <Skeleton width={20} height={20} rounded="rounded-full" />
            </span>
            <h2 className={RUN_TITLE}>
              <TextBar width={240} />
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
            <StepsHead
              id="steps-filter-skeleton"
              summary={<Skeleton.Text width={180} />}
              progress={<Skeleton width={112} height={4} rounded="rounded-full" className="shrink-0 max-sm:hidden" />}
              filter=""
              onFilter={noop}
            />
            {steps === 0 ? (
              <p className={STEP_EMPTY}>
                <Skeleton.Text>No steps yet</Skeleton.Text>
              </p>
            ) : (
              <ul className={STEP_LIST}>
                {Array.from({ length: steps }, (_, i) => {
                  const [title, note] = STEP_WIDTHS[i % STEP_WIDTHS.length]!;
                  return (
                    <li key={i} className={STEP_ROW}>
                      <span className={STATUS_BOX}>
                        <Skeleton width={14} height={14} rounded="rounded-full" />
                      </span>
                      <TextBar width={title} className="min-w-0 flex-1" />
                      <TextBar width={note} className="text-xs" />
                    </li>
                  );
                })}
              </ul>
            )}
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
                <RunPager page={Number(params.get("page")) || 1} pages={null} />
              </div>
            </>
          )}
        </div>
      </div>
    </section>
  );
}
