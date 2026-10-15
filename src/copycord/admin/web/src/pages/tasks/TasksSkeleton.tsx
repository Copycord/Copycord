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
import { readLastCount, readLastLabels } from "../../components/ui/lastCount";
import { Skeleton, useReveal } from "../../components/ui/Skeleton";
import { ToolbarSelect } from "../channels/ToolbarSelect";
import { runTypeOf, typeLabel } from "./typeLabels";

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
/** The header's main block on the left, the run's actions on the right. */
export const RUN_HEAD_ROW = "flex flex-wrap items-start justify-between gap-3";
/** The header's glyph beside its two lines (the title row, the meta line), as in a run list row. */
export const RUN_HEAD_MAIN = "flex min-w-0 items-center gap-3";
export const RUN_TITLE_ROW = "flex items-center gap-2";
export const RUN_TITLE = "m-0 text-[22px] font-bold text-text";
export const RUN_NUMBER = "text-[22px] text-muted";

/** A type's page, where the run page's breadcrumb leads. */
export function typeUrl(key: string): string {
  return `/tasks?type=${encodeURIComponent(key)}`;
}
/** The meta line holds the mapping badge (24 px), so it is that tall with or without one. */
export const RUN_META = "mt-1 flex min-h-6 flex-wrap items-center gap-x-3 gap-y-1 text-sm text-muted";
/** The run's status word in the meta line, in its status colour; the run's error follows it, quiet. */
export const RUN_STATUS = "font-semibold";
export const RUN_ERROR = "max-w-[48ch] truncate text-muted";
/** The header's Delete and Cancel run: the height of the Steps filter (ToolbarSelect, 34 px). */
export const RUN_ACTION = "min-h-[34px] px-3 text-[13px]";
export const RUN_BODY = "flex flex-col gap-4";
/** The cards of the run page: FRAME without the clip, so the Steps filter's list can open over the next card. */
export const RUN_CARD = "rounded-card border border-border bg-card";
/** One head for every run card (Summary, Steps, Report): 48 px, the title, a 13 px muted meta, controls on the right. */
export const CARD_HEAD = "flex min-h-12 flex-wrap items-center gap-x-3 gap-y-1.5 border-b border-border px-4 py-1.5 text-sm";
export const CARD_TITLE = "m-0 text-sm font-semibold text-text";
export const CARD_META = "min-w-0 flex-1 truncate text-[13px] text-muted";
/** The stat tiles at the top of the summary card: equal columns split by hairlines, a small label over a 20 px count. */
export const STATS_LIST = "m-0 grid auto-cols-fr grid-flow-col divide-x divide-border border-b border-border";
export const STAT_TILE = "min-w-0 px-4 py-3";
export const STAT_LABEL = "truncate text-xs text-muted";
export const STAT_VALUE = "m-0 mt-1 text-xl leading-7 font-semibold tabular-nums text-text";
/** The tile and Inputs labels the run page showed last time (lastCount.ts), printed by the skeleton. */
export const STATS_LABELS_KEY = "tasks.stat-labels";
export const INPUTS_LABELS_KEY = "tasks.input-labels";
/** The Inputs list under the stat tiles, and its small heading; its left edge is the tiles' text edge. */
export const INPUTS_BOX = "px-4 py-3";
export const INPUTS_TITLE = "m-0 mb-2 text-xs font-semibold text-muted";
export const INPUTS_LIST = "m-0 grid grid-cols-[minmax(110px,max-content)_1fr] gap-x-6 gap-y-1 text-sm leading-5";
/** The status glyph's box (StatusIcon): it centres itself on the whole row it sits in, whatever the row
 * aligns its other columns to. A run list row (title and meta line), a step row and the run header (title
 * and meta line, RUN_HEAD_MAIN) all hold it as a direct flex child, so it sits mid-row in each. */
export const STATUS_BOX = "inline-flex shrink-0 items-center self-center";
/** The Steps card head: the title, the counts line, the overall bar with its percent and the status filter on one line. */
export const STEPS_HEAD = CARD_HEAD;
export const STEPS_TITLE = CARD_TITLE;
export const STEPS_SUMMARY = CARD_META;
/** The overall bar in the head: its percent caption, then the track. */
export const STEPS_PROGRESS = "flex shrink-0 items-center gap-2 max-sm:hidden";
export const STEPS_PCT = "w-10 text-right text-xs tabular-nums text-muted";
/** The overall bar's track in the head, and a running step's thin bar in its row. */
export const STEPS_BAR = "h-1 w-28 shrink-0 overflow-hidden rounded-full bg-line-1";
export const STEP_BAR = "h-1 w-24 shrink-0 overflow-hidden rounded-full bg-line-1 max-sm:w-12";
/** Rows are one fixed line each, so the list stops at STEP_ROWS_MAX rows and scrolls past that. */
export const STEP_ROWS_MAX = 12;
/** One step row's height in px (h-9 in STEP_ROW). */
export const STEP_ROW_PX = 36;
export const STEP_LIST = "m-0 max-h-[432px] list-none overflow-y-auto p-0";
/** One step row, 36 px: glyph, title, the thin bar while running, the note on the right. */
export const STEP_ROW = "flex h-9 items-center gap-2.5 border-b border-border px-4 text-sm last:border-b-0";
export const STEP_TITLE = "min-w-0 flex-1 truncate font-medium text-text";
export const STEP_NOTE = "max-w-[50%] truncate text-right text-xs";
/** No steps, or none in the filter: one muted line centred in the rows' place, as tall as they stood. */
export const STEP_EMPTY = "m-0 flex min-h-9 items-center justify-center px-4 text-[13px] text-muted";
/** The steps the run page showed last time (lastCount.ts), so the loading card stands as tall. */
export const STEPS_COUNT_KEY = "tasks.steps";
/** The filter's options; the closed control reads "Status: All" (ToolbarSelect prefix). */
export const STEP_FILTER_OPTIONS = [
  { value: "", label: "All" },
  { value: "running", label: "Running" },
  { value: "queued", label: "Queued" },
  { value: "failed", label: "Failed" },
  { value: "success", label: "Done" },
  { value: "cancelled", label: "Cancelled" },
  { value: "skipped", label: "Skipped" },
] as const;
export type StepFilter = (typeof STEP_FILTER_OPTIONS)[number]["value"];

/** The height the rows of n steps take, capped where the list scrolls; an empty filter keeps it. */
export function stepRowsHeight(n: number): number {
  return Math.max(1, Math.min(n, STEP_ROWS_MAX)) * STEP_ROW_PX;
}

export interface StepsHeadProps {
  /** The counts line, or its bar while the run loads. */
  summary: ReactNode;
  /** The overall percent, or its placeholder. */
  percent: ReactNode;
  /** The overall bar, or its placeholder. */
  progress: ReactNode;
  filter: StepFilter;
  onFilter: (value: StepFilter) => void;
  id: string;
}

/** The Steps card head; the loaded card and the run skeleton both draw it, so they cannot drift. */
export function StepsHead({ summary, percent, progress, filter, onFilter, id }: StepsHeadProps) {
  return (
    <div className={STEPS_HEAD}>
      <h3 className={STEPS_TITLE}>Steps</h3>
      <span data-testid="steps-summary" className={STEPS_SUMMARY}>
        {summary}
      </span>
      <span className={STEPS_PROGRESS}>
        <span data-testid="steps-percent" className={STEPS_PCT}>
          {percent}
        </span>
        {progress}
      </span>
      <ToolbarSelect
        id={id}
        label="Step status"
        prefix="Status"
        value={filter}
        options={STEP_FILTER_OPTIONS}
        onChange={(v) => onFilter(v as StepFilter)}
      />
    </div>
  );
}

/** The Summary card's head: its title alone, in the same box as the other cards' heads. */
export function SummaryHead() {
  return (
    <div className={CARD_HEAD}>
      <h3 className={CARD_TITLE}>Summary</h3>
    </div>
  );
}

export const REPORT_SUMMARY =
  "flex min-h-12 cursor-pointer list-none items-center gap-x-3 px-4 py-1.5 text-sm marker:hidden [&::-webkit-details-marker]:hidden";

/** The Report panel's summary line, collapsed: the chevron and "Report (JSON)", in the card head's type. */
export function ReportSummaryText() {
  return (
    <>
      <span aria-hidden="true" className="inline-block w-2 text-muted transition-transform group-open:rotate-90">
        ›
      </span>
      <span className={CARD_TITLE}>Report (JSON)</span>
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
  /** A label, never a bar: All tasks, or the type's title (typeLabels.ts has it before the types load). */
  title: string;
  /** "" while a first visit does not know the type's description yet: the line keeps its height. */
  description: string;
  action: ReactNode;
}

export function TasksHeader({ title, description, action }: TasksHeaderProps) {
  // A description that arrives after the first paint fades in; one there from the start does not.
  const reveal = useReveal(description !== "");
  return (
    <div className="mb-4 flex items-start justify-between gap-3">
      <div className="min-w-0">
        <h2 className="m-0 text-[22px] font-bold text-text">{title}</h2>
        <p className="mt-1 mb-0 text-sm text-muted">{description ? <span className={reveal}>{description}</span> : " "}</p>
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
 * time (at most a page), one on a first visit, or the empty state's shape when it had none.
 */
export function RunRowsLoading({ rows = readLastCount(RUNS_COUNT_KEY, 1, PER_PAGE) }: { rows?: number }) {
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
const INPUT_WIDTHS = [60, 90, 70, 80];
/** A first visit draws a backfill run's shape: its five tiles, its six inputs and six steps, bars for every label. */
const FIRST_VISIT_TILES = 5;
const FIRST_VISIT_INPUTS = 6;
const FIRST_VISIT_STEPS = 6;
const TILE_WIDTHS = [36, 40, 44, 52, 48];

export interface RunDetailSkeletonProps {
  /** The run's type key when known before the run loads (typeLabels.ts runTypeOf): its title shows as text. */
  runType?: string | null;
  /** Step rows to draw; the count the last run page showed by default. */
  steps?: number;
}

/**
 * The run page until the run answers, also the page's own loading state: the header with the type's
 * title as text when the run's type is known (bars in its place only when it cannot be), bars for the
 * glyph, number and meta line (the actions depend on the status, so they wait), the summary card with
 * the tile and Inputs labels the last run page showed and bars for their values (a backfill run's five
 * tiles and six rows, all bars, on a first visit), the Steps card with its real head (bars for the counts
 * and the overall bar, the real filter) and as many bar rows as the last run showed, and the collapsed
 * Report line.
 */
export function RunDetailSkeleton({
  runType = null,
  steps = readLastCount(STEPS_COUNT_KEY, FIRST_VISIT_STEPS, STEP_ROWS_MAX),
}: RunDetailSkeletonProps) {
  const title = runType ? typeLabel(runType).title : null;
  // The tile and Inputs labels the last run page showed, as text; a first visit has bars in their place.
  const lastStats = readLastLabels(STATS_LABELS_KEY, 8);
  const inputs = readLastLabels(INPUTS_LABELS_KEY, 12);
  const stats: (string | null)[] = lastStats ?? Array.from({ length: FIRST_VISIT_TILES }, () => null);
  const inputRows: (string | null)[] = inputs ?? Array.from({ length: FIRST_VISIT_INPUTS }, () => null);
  return (
    <div role="status" aria-busy="true" aria-label="Loading run" className="flex flex-col">
      <div inert>
        <header className={RUN_HEADER}>
          <nav aria-label="Breadcrumb" className={RUN_BREADCRUMB}>
            <Link to="/tasks" className={RUN_CRUMB}>
              All tasks
            </Link>
            {" / "}
            {runType && title ? (
              <Link to={typeUrl(runType)} className={RUN_CRUMB}>
                {title}
              </Link>
            ) : (
              <InlineBar width={110} />
            )}
          </nav>
          <div className={RUN_HEAD_ROW}>
            <div className={RUN_HEAD_MAIN}>
              <span className={STATUS_BOX}>
                <Skeleton width={20} height={20} rounded="rounded-full" />
              </span>
              <div className="min-w-0">
                <div className={RUN_TITLE_ROW}>
                  <h2 className={RUN_TITLE}>{title ?? <TextBar width={180} />}</h2>
                  <Skeleton.Text className="text-[22px]">#00</Skeleton.Text>
                </div>
                <div className={RUN_META}>
                  <TextBar width={130} />
                  <TextBar width={90} />
                  <TextBar width={110} />
                  <TextBar width={60} />
                </div>
              </div>
            </div>
          </div>
        </header>
        <div className={RUN_BODY}>
          <section aria-label="Summary" className={RUN_CARD}>
            <SummaryHead />
            {stats.length ? (
              <dl className={STATS_LIST}>
                {stats.map((label, i) => (
                  <div key={i} className={STAT_TILE}>
                    <dt className={STAT_LABEL}>{label ?? <TextBar width={TILE_WIDTHS[i % TILE_WIDTHS.length]!} />}</dt>
                    <dd className={STAT_VALUE}>
                      <Skeleton.Text>00</Skeleton.Text>
                    </dd>
                  </div>
                ))}
              </dl>
            ) : null}
            <div className={INPUTS_BOX}>
              <h3 className={INPUTS_TITLE}>Inputs</h3>
              {inputs?.length === 0 ? (
                <p className="m-0 text-sm text-muted">
                  <Skeleton.Text>No inputs</Skeleton.Text>
                </p>
              ) : (
                <dl className={INPUTS_LIST}>
                  {inputRows.map((label, i) => (
                    <div key={i} className="contents">
                      <dt className="text-muted">{label ?? <TextBar width={INPUT_WIDTHS[i % INPUT_WIDTHS.length]!} />}</dt>
                      <dd className="m-0">
                        <TextBar width={INPUT_WIDTHS[(i + 1) % INPUT_WIDTHS.length]! + 40} />
                      </dd>
                    </div>
                  ))}
                </dl>
              )}
            </div>
          </section>
          <section aria-label="Steps" className={RUN_CARD}>
            <StepsHead
              id="steps-filter-skeleton"
              summary={<Skeleton.Text width={180} />}
              percent={null}
              progress={<Skeleton width={112} height={4} rounded="rounded-full" className="shrink-0" />}
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
  const runMatch = useMatch("/tasks/runs/:id");
  const runPage = runMatch !== null;
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
          {runMatch ? (
            <RunDetailSkeleton runType={runTypeOf(runMatch.params.id ?? "")} />
          ) : (
            <>
              {typeKey ? (
                <TasksHeader {...typeLabel(typeKey)} action={null} />
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
