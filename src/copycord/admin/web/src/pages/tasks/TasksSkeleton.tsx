// The Tasks page while its chunk downloads: the page's own rail, header, search box and list frame with its
// toolbar, in the same boxes and with the same static text, bars only where data goes; on a run url the run
// page's header and cards instead. Kept out of Tasks.tsx so the app shell can show it without pulling the
// page chunk in; the page takes its shared classes, the header, the search chrome, the toolbar labels and
// the loading pieces from here, so the two cannot drift.

import type { ReactNode } from "react";
import { Link, useMatch, useSearchParams } from "react-router";
import { Badge } from "../../components/ui/Badge";
import { Button } from "../../components/ui/Button";
import { cx } from "../../components/ui/cx";
import { EmptyState } from "../../components/ui/EmptyState";
import { Input } from "../../components/ui/Field";
import { readLastCount, readLastLabels } from "../../components/ui/lastCount";
import { Skeleton, useReveal } from "../../components/ui/Skeleton";
import { RowMenu } from "../channels/actions/RowMenu";
import { ToolbarSelect } from "../channels/ToolbarSelect";
import { runTypeOf, typeLabel } from "./typeLabels";

/** The rail and the page's column, at most 1280 px wide and centred, so a wide screen keeps a row together. */
export const PAGE = "mx-auto my-3 flex w-full max-w-[1280px] gap-6 max-md:flex-col";
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
/** The runs card: FRAME without the clip, so an open filter list hangs past the card's bottom edge, over the page. */
export const LIST_FRAME = "rounded-card border border-border bg-card";
export const FRAME_HEAD = "flex flex-wrap items-center justify-between gap-3 border-b border-border px-4 py-2.5 text-sm";
/** Left of the frame head: the run count and Auto-refresh. */
export const FRAME_HEAD_LEFT = "flex items-center gap-4";
/** Right of the frame head: the filter dropdowns. */
export const FRAME_HEAD_FILTERS = "flex flex-wrap items-center gap-3";
export const RUN_COUNT = "font-semibold text-text";
/** The pager under the runs (RunPager). */
export const PAGER = "flex items-center justify-between gap-3 border-t border-border px-4 py-3 text-sm text-muted";
/** The box of a run row; ROW adds the hover tint. The skeleton row uses the box alone so both stand equally tall. */
export const ROW_BOX = "relative flex min-h-16 items-center gap-3 border-b border-border px-4 py-2.5 last:rounded-b-card last:border-b-0";
export const ROW = `${ROW_BOX} transition-colors duration-150 hover:bg-rail-hover`;
/** A run row's left column: the task name and its status badge over the meta line. */
export const ROW_MAIN = "min-w-0 flex-1";
/** The name line: the task name and number, then the run's one coloured badge. */
export const ROW_HEAD = "flex h-6 min-w-0 items-center gap-2";
export const ROW_NAME = "min-w-0 truncate text-sm leading-5 font-semibold";
/** The name is the row's link: its ::before covers the whole row, so the row clicks through and shows one focus ring. */
export const ROW_LINK =
  `${ROW_NAME} text-text no-underline outline-none hover:underline before:absolute before:inset-0 before:content-[''] ` +
  "focus-visible:before:outline-2 focus-visible:before:-outline-offset-2 focus-visible:before:outline-accent";
/** "0d88ac5a · Clone-1 · Manual · admin": 13 px muted text with 12 px glyphs, the mapping a quiet outlined chip. */
export const ROW_META = "mt-1 flex h-[18px] min-w-0 items-center gap-1.5 overflow-hidden whitespace-nowrap text-[13px] leading-[18px] text-muted";
/** A run ID in mono, the list row and the run page alike. */
export const ROW_ID = "font-mono text-xs";
/** One item of a meta line: a 12 px glyph and its words. */
export const META_ITEM = "inline-flex shrink-0 items-center gap-1";
export const META_DOT = "text-muted/50";
/** The mapping on a meta line: a link-like name, so a subtle outlined chip rather than plain text. */
export const MAPPING_CHIP = "inline-block max-w-[20ch] truncate rounded-full border border-line-2 px-1.5 text-xs leading-4 text-text-2";
/** The outcome column: "6 steps · 2 done · 4 cancelled · 92 delivered" over the thin segmented bar. */
export const ROW_OUTCOME = "flex w-[300px] shrink-0 flex-col gap-2 max-lg:hidden";
export const ROW_OUTCOME_TEXT = "h-[18px] truncate text-[13px] leading-[18px] text-muted tabular-nums";
export const ROW_SEG = "h-1 w-full";
/** Started (relative) over duration, each after a small glyph, 13 px muted with tabular numbers. */
export const ROW_TIMING = "flex w-[96px] shrink-0 flex-col gap-1 text-[13px] leading-[18px] text-muted tabular-nums max-sm:hidden";
export const ROW_TIME = "flex items-center gap-1.5 whitespace-nowrap";
/** Above the row link's cover, so the kebab opens its menu instead of the run. */
export const ROW_MENU = "relative z-10 shrink-0";
export const SEARCH_FORM = "mb-4 flex";
export const SEARCH_INPUT = "min-w-0 flex-1 rounded-r-none";
export const SEARCH_BUTTON = "rounded-l-none border-l-0";

// The run page (RunDetail.tsx): header, then one column of cards.
export const RUN_HEADER = "mb-4";
export const RUN_BREADCRUMB = "mb-2 text-sm text-muted";
export const RUN_CRUMB = "text-accent no-underline hover:underline";
/** The title and status badge on the left, Cancel (while active) and the actions menu on the right, one centred line. */
export const RUN_HEAD_ROW = "flex min-h-9 items-center justify-between gap-3";
/** The type's title, the run's number, its status badge and a failed run's error, on one line. */
export const RUN_TITLE_ROW = "flex min-w-0 items-center gap-2";
export const RUN_TITLE = "m-0 shrink-0 text-[22px] leading-8 font-bold text-text";
export const RUN_NUMBER = "shrink-0 text-[22px] leading-8 text-muted";
/** The status badge: 24 px, 12 px text, a 12 px glyph and the status word; the run page header and each run row. */
export const RUN_PILL = "ml-1 shrink-0";
export const RUN_PILL_BADGE = "h-6 gap-1.5 px-2.5 py-0! leading-none! font-medium!";
/** Right of the title line: Cancel run while the run is active, then the Run actions menu. */
export const RUN_ACTIONS = "flex shrink-0 items-center gap-2";
/** "Triggered manually by admin · started ... · ran 1m 55s · mapping Clone-1 · ID 0d88ac5a": muted inline text under the title. */
export const RUN_META = "m-0 mt-1.5 flex min-h-5 flex-wrap items-center gap-x-2 gap-y-1 text-[13px] leading-5 text-muted";

/** A type's page, where the run page's breadcrumb leads. */
export function typeUrl(key: string): string {
  return `/tasks?type=${encodeURIComponent(key)}`;
}
/** A failed run's error after the status badge: quiet text, cut to one line, the whole of it in the title. */
export const RUN_ERROR = "min-w-0 max-w-[48ch] truncate text-sm text-muted";
/** The run ID's copy button on the meta line, a 20 px hit box around a 12 px glyph, inside the line's height. */
export const RUN_ID_COPY =
  "inline-flex size-5 cursor-pointer items-center justify-center rounded-rail border-0 bg-transparent p-0 text-muted " +
  "hover:bg-rail-hover hover:text-text focus-visible:outline-2 focus-visible:outline-accent disabled:cursor-default";
/** How many of a run's channels its Inputs show as chips before "+N more". */
export const INPUT_CHIPS_MAX = 4;
/** The chips of one Inputs value: one 20 px line that never wraps, so the row keeps its text height. */
export const INPUT_CHIPS = "flex min-w-0 items-center gap-1 overflow-hidden";

const META_GLYPHS = {
  started: (
    <>
      <rect x="2.25" y="3.25" width="11.5" height="10.5" rx="1.75" fill="none" stroke="currentColor" strokeWidth="1.5" />
      <path d="M2.25 6.75h11.5M5.5 1.75v3M10.5 1.75v3" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" />
    </>
  ),
  duration: (
    <>
      <circle cx="8" cy="8" r="6.25" fill="none" stroke="currentColor" strokeWidth="1.5" />
      <path d="M8 4.75V8l2.25 1.5" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" />
    </>
  ),
  copy: (
    <>
      <rect x="5.25" y="5.25" width="8.5" height="8.5" rx="1.5" fill="none" stroke="currentColor" strokeWidth="1.5" />
      <path d="M10.75 5.25V3.75a1.5 1.5 0 0 0-1.5-1.5h-5.5a1.5 1.5 0 0 0-1.5 1.5v5.5a1.5 1.5 0 0 0 1.5 1.5h1.5" fill="none" stroke="currentColor" strokeWidth="1.5" />
    </>
  ),
  trigger: <path d="M9.25 1.75 3.5 9.25h4.25l-1 5 5.75-7.5H8.25l1-5Z" fill="none" stroke="currentColor" strokeWidth="1.4" strokeLinejoin="round" />,
  actor: (
    <>
      <circle cx="8" cy="5.25" r="2.75" fill="none" stroke="currentColor" strokeWidth="1.5" />
      <path d="M2.75 14c.6-2.6 2.7-4.25 5.25-4.25s4.65 1.65 5.25 4.25" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" />
    </>
  ),
  mapping: (
    <path
      d="M2.5 5.25h10.5M10.5 2.75l2.5 2.5-2.5 2.5M13.5 10.75H3M5.5 8.25 3 10.75l2.5 2.5"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.5"
      strokeLinecap="round"
      strokeLinejoin="round"
    />
  ),
} as const;

/** A small muted glyph before a value (meta lines, the row's timing, the run ID copy button): a label, so the skeleton draws it too. */
export function MetaGlyph({ name, size = 12 }: { name: keyof typeof META_GLYPHS; size?: number }) {
  return (
    <svg viewBox="0 0 16 16" width={size} height={size} aria-hidden="true" focusable="false" className="shrink-0 text-muted">
      {META_GLYPHS[name]}
    </svg>
  );
}

/** The middle dot between the items of a meta line. */
export function MetaDot() {
  return (
    <span aria-hidden="true" className={META_DOT}>
      ·
    </span>
  );
}

/** The status badge's box as itself; its glyph and word are the run's status, so they shimmer. */
export function StatusPillLoading({ testId, className }: { testId?: string; className?: string }) {
  return (
    <span data-testid={testId} className={cx("inline-flex", className)}>
      <Badge className={RUN_PILL_BADGE}>
        <Skeleton width={12} height={12} rounded="rounded-full" />
        <Skeleton.Text>Cancelled</Skeleton.Text>
      </Badge>
    </span>
  );
}

export interface RunMetaValues {
  /** "Triggered manually by admin". */
  trigger: ReactNode;
  /** After the static word "started": the date and the relative time. */
  started: ReactNode;
  /** "ran 1m 55s", or "running 12s" while active. */
  ran: ReactNode;
  /** After the static word "mapping": the mapping chip; null leaves the item out. */
  mapping: ReactNode | null;
  /** After the static word "ID": the short id and its copy button. */
  id: ReactNode;
}

/** The run page's meta line under the title: the static words as text, the run's values (or the skeleton's bars) between them. */
export function RunMetaLine({ values }: { values: RunMetaValues }) {
  return (
    <p data-testid="run-meta" className={RUN_META}>
      <span className={META_ITEM}>
        <MetaGlyph name="trigger" />
        {values.trigger}
      </span>
      <MetaDot />
      <span className={META_ITEM}>
        <MetaGlyph name="started" />
        started {values.started}
      </span>
      <MetaDot />
      <span className={META_ITEM}>
        <MetaGlyph name="duration" />
        {values.ran}
      </span>
      {values.mapping != null ? (
        <>
          <MetaDot />
          <span className={META_ITEM}>
            <MetaGlyph name="mapping" />
            mapping {values.mapping}
          </span>
        </>
      ) : null}
      <MetaDot />
      <span className={META_ITEM}>ID {values.id}</span>
    </p>
  );
}

/** The run ID value: the first 8 characters in mono and the copy button; the full id in the title. id null: a bar, the button inert. */
export function RunIdValue({ id, onCopy }: { id: string | null; onCopy?: () => void }) {
  return (
    <>
      {id ? (
        <span data-testid="run-id" className={ROW_ID} title={`Run ID ${id}`}>
          {id.slice(0, 8)}
        </span>
      ) : (
        <Skeleton.Text className={ROW_ID}>00000000</Skeleton.Text>
      )}
      <button type="button" aria-label="Copy run ID" title="Copy run ID" disabled={!id} onClick={onCopy} className={RUN_ID_COPY}>
        <MetaGlyph name="copy" size={12} />
      </button>
    </>
  );
}

/** The Summary card's top: the hero count (messages delivered) and "6/6 steps finished" over the segmented bar. */
export const OVERVIEW = "border-b border-border px-4 py-3";
export const OVERVIEW_ROW = "flex h-10 items-end justify-between gap-4";
export const HERO = "flex min-w-0 items-baseline gap-2";
export const HERO_VALUE = "text-[32px] leading-10 font-semibold tabular-nums text-accent";
export const HERO_LABEL = "truncate text-sm text-muted";
export const FINISHED = "shrink-0 pb-1 text-[13px] leading-5 tabular-nums text-muted";
export const OVERVIEW_BAR = "mt-3 h-1.5 w-full";

/** The overview's frame; the run page and its skeleton pass the hero, the finished line and the bar. */
export function RunOverview({ hero, finished, bar }: { hero: ReactNode; finished: ReactNode; bar: ReactNode }) {
  return (
    <div className={OVERVIEW}>
      <div className={OVERVIEW_ROW}>
        <div data-testid="run-hero" className={HERO}>
          {hero}
        </div>
        <span data-testid="run-finished" className={FINISHED}>
          {finished}
        </span>
      </div>
      {bar}
    </div>
  );
}

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
export const STAT_VALUE = "m-0 mt-1 text-xl leading-7 font-semibold tabular-nums";
/** A count of zero reads dimmed, so the counts that matter stand out. */
export const STAT_ZERO = "font-medium! text-muted/50";
/** The tile and Inputs labels the run page showed last time (lastCount.ts), printed by the skeleton. */
export const STATS_LABELS_KEY = "tasks.stat-labels";
export const INPUTS_LABELS_KEY = "tasks.input-labels";
/** The Inputs list under the stat tiles, and its small heading; its left edge is the tiles' text edge. */
export const INPUTS_BOX = "px-4 py-3";
export const INPUTS_TITLE = "m-0 mb-2 text-xs font-semibold text-muted";
export const INPUTS_LIST = "m-0 grid grid-cols-[minmax(110px,max-content)_1fr] gap-x-6 gap-y-1 text-sm leading-5";
/** The status glyph's box (StatusIcon): it centres itself on the line it sits in, a step row or the inside
 * of a status badge, whatever that line aligns its other parts to. */
export const STATUS_BOX = "inline-flex shrink-0 items-center self-center";
/** The Steps card head: the title, the counts line and the status filter on one line (the run's bar is in the Summary). */
export const STEPS_HEAD = CARD_HEAD;
export const STEPS_TITLE = CARD_TITLE;
export const STEPS_SUMMARY = CARD_META;
/** A running step's thin bar in its row. */
export const STEP_BAR = "h-1 w-24 shrink-0 overflow-hidden rounded-full bg-line-1 max-sm:w-12";
/** Rows are one fixed line each, so the list stops at STEP_ROWS_MAX rows and scrolls past that. */
export const STEP_ROWS_MAX = 12;
/** One step row's height in px (h-9 in STEP_ROW). */
export const STEP_ROW_PX = 36;
export const STEP_LIST = "m-0 max-h-[432px] list-none overflow-y-auto p-0";
/** One step row, 36 px: glyph, title, the thin bar while running, the note, then the state in its colour. */
export const STEP_ROW = "flex h-9 items-center gap-2.5 border-b border-border px-4 text-sm last:border-b-0";
export const STEP_TITLE = "min-w-0 flex-1 truncate font-medium text-text";
export const STEP_NOTE = "max-w-[50%] truncate text-right text-xs";
/** The step's state in words at the row's right edge, coloured like its glyph, so colour is never the only signal. */
export const STEP_STATE = "w-[72px] shrink-0 text-right text-xs font-medium";
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
  filter: StepFilter;
  onFilter: (value: StepFilter) => void;
  id: string;
}

/** The Steps card head; the loaded card and the run skeleton both draw it, so they cannot drift. */
export function StepsHead({ summary, filter, onFilter, id }: StepsHeadProps) {
  return (
    <div className={STEPS_HEAD}>
      <h3 className={STEPS_TITLE}>Steps</h3>
      <span data-testid="steps-summary" className={STEPS_SUMMARY}>
        {summary}
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
/** The pages the run list had last time: the loading list draws the pager only when there was more than one. */
export const RUN_PAGES_KEY = "tasks.pages";

/** Whether the loading list draws the pager: the list had more than one page last time. */
export function readPagerShown(): boolean {
  return readLastCount(RUN_PAGES_KEY, 1, 10000) > 1;
}

export const AUTO_REFRESH_KEY = "tasks:autorefresh";
/** The closed control reads "Auto-refresh: Off" (ToolbarSelect prefix); the options stay plain. */
export const AUTO_REFRESH_LABEL = "Auto-refresh";
export const AUTO_REFRESH_OPTIONS = [
  { value: "0", label: "Off" },
  { value: "10", label: "10s" },
  { value: "30", label: "30s" },
  { value: "60", label: "60s" },
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

/** The first option of a filter dropdown; its label is the prefix on the closed control, "Status: All". */
export const ALL_OPTION = { value: "", label: "All" } as const;

export function runCountText(total: number): string {
  return total === 1 ? "1 task run" : `${total} task runs`;
}

export function TasksHeading() {
  return <h1 className="sr-only">Tasks</h1>;
}

/** About the width of "Backfill channels #12" and its kin. */
const NAME_WIDTHS = [150, 132, 164, 140, 124, 156];

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
 * Previous, the page label and Next under the runs, only when there is more than one page (the loading
 * list draws it when the last list had more, readPagerShown); pages is null until the first page lands
 * (a bar on the label, both buttons disabled).
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
 * One loading row in the shape of RunRow (RunList.tsx): the same box, a bar for the name beside the status
 * badge's box, a bar on the meta line, bars for the outcome line and its segmented bar, the two timing
 * lines with their glyphs as themselves and bars for the values, and the row menu button, each in the
 * same type size and padding as the real element, so the first page lands without the list jumping.
 */
function RunRowLoading({ nameWidth }: { nameWidth: number }) {
  return (
    <li className={ROW_BOX}>
      <div className={ROW_MAIN}>
        <div className={ROW_HEAD}>
          <TextBar width={nameWidth} className={ROW_NAME} />
          <StatusPillLoading />
        </div>
        <div className={ROW_META}>
          <Skeleton.Text width={nameWidth + 110} />
        </div>
      </div>
      <div className={ROW_OUTCOME}>
        <span className={ROW_OUTCOME_TEXT}>
          <Skeleton.Text>6 steps · 2 done · 4 cancelled · 92 delivered</Skeleton.Text>
        </span>
        <Skeleton rounded="rounded-full" className={ROW_SEG} />
      </div>
      <div className={ROW_TIMING}>
        <span className={ROW_TIME}>
          <MetaGlyph name="started" />
          <Skeleton.Text>5h ago</Skeleton.Text>
        </span>
        <span className={ROW_TIME}>
          <MetaGlyph name="duration" />
          <Skeleton.Text>1m 55s</Skeleton.Text>
        </span>
      </div>
      <div className={ROW_MENU}>
        {/* The row menu button box with its glyph hidden. */}
        <Skeleton.Box className="inline-block rounded-btn border px-2 py-0.5">⋯</Skeleton.Box>
      </div>
    </li>
  );
}

/** The runs empty state's icon (Tasks.tsx): a static mark, so the skeleton draws it as itself. */
export function RunsEmptyIcon() {
  return (
    <svg viewBox="0 0 24 24" width="28" height="28" fill="none" stroke="currentColor" strokeWidth="1.6" aria-hidden="true">
      <rect x="3.5" y="4.5" width="17" height="15" rx="2.5" />
      <path d="M7.5 9.5h9M7.5 13h6M7.5 16.5h4" strokeLinecap="round" />
    </svg>
  );
}

/** The runs empty state: icon, title, a line under it and one button, the same shape whichever variant shows. */
export const RUNS_EMPTY = "py-12";

/** The empty state (Tasks.tsx) with bars on its lines and its button's box, for a list that had no runs last time. */
function RunsEmptyLoading() {
  return (
    <EmptyState
      plain
      className={RUNS_EMPTY}
      icon={<RunsEmptyIcon />}
      title={<Skeleton.Text>No task runs yet</Skeleton.Text>}
      description={<Skeleton.Text>Runs show up here once a task starts</Skeleton.Text>}
      action={
        <Button variant="ghost" disabled>
          <Skeleton.Text>Run Backfill channels</Skeleton.Text>
        </Button>
      }
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
            <RunRowLoading key={i} nameWidth={NAME_WIDTHS[i % NAME_WIDTHS.length]!} />
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
/** A first visit draws a backfill run's shape: its four step tiles, its six inputs and six steps, bars for every label. */
const FIRST_VISIT_TILES = 4;
const FIRST_VISIT_INPUTS = 6;
const FIRST_VISIT_STEPS = 6;
const TILE_WIDTHS = [64, 68, 76, 84];

export interface RunDetailSkeletonProps {
  /** The run's type key when known before the run loads (typeLabels.ts runTypeOf): its title shows as text. */
  runType?: string | null;
  /** Step rows to draw; the count the last run page showed by default. */
  steps?: number;
}

/**
 * The run page until the run answers, also the page's own loading state: the header with the type's
 * title as text when the run's type is known (bars in its place only when it cannot be), a bar for the
 * number, the status badge's box with bars inside and the "Run actions" button as itself and inert on the
 * same line, the meta line with its static words as text and bars for the run's values, the summary card
 * with bars for the hero count, the finished line and the segmented bar, then the tile and Inputs labels
 * the last run page showed and bars for their values (a backfill run's four tiles and six rows, all bars,
 * on a first visit), the Steps card with its real head (a bar for the counts, the real filter) and as many
 * bar rows as the last run showed, and the collapsed Report line.
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
            <div className={RUN_TITLE_ROW}>
              <h2 className={RUN_TITLE}>{title ?? <TextBar width={180} />}</h2>
              <Skeleton.Text className="shrink-0 text-[22px] leading-8">#00</Skeleton.Text>
              <StatusPillLoading testId="run-status" className={RUN_PILL} />
            </div>
            <div className={RUN_ACTIONS}>
              <RowMenu kind="channel" label="Run actions" items={[]} disabled />
            </div>
          </div>
          <RunMetaLine
            values={{
              trigger: <Skeleton.Text>Triggered manually by admin</Skeleton.Text>,
              started: <Skeleton.Text>Oct 15, 5:06 PM EDT (5h ago)</Skeleton.Text>,
              ran: <Skeleton.Text>ran 1m 55s</Skeleton.Text>,
              mapping: <Skeleton.Box className={MAPPING_CHIP}>Clone-1</Skeleton.Box>,
              id: <RunIdValue id={null} />,
            }}
          />
        </header>
        <div className={RUN_BODY}>
          <section aria-label="Summary" className={RUN_CARD}>
            <SummaryHead />
            <RunOverview
              hero={
                <>
                  <Skeleton.Text className={HERO_VALUE}>000</Skeleton.Text>
                  <Skeleton.Text className={HERO_LABEL}>messages delivered</Skeleton.Text>
                </>
              }
              finished={<Skeleton.Text>6/6 steps finished</Skeleton.Text>}
              bar={<Skeleton rounded="rounded-full" className={OVERVIEW_BAR} />}
            />
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
            <StepsHead id="steps-filter-skeleton" summary={<Skeleton.Text width={180} />} filter="" onFilter={noop} />
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
                      <span className={STEP_STATE}>
                        <Skeleton.Text>Cancelled</Skeleton.Text>
                      </span>
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
              <div className={LIST_FRAME}>
                <div className={FRAME_HEAD}>
                  <div className={FRAME_HEAD_LEFT}>
                    <RunCountLoading />
                    <ToolbarSelect
                      id="tasks-autorefresh-skeleton"
                      label={AUTO_REFRESH_LABEL}
                      prefix={AUTO_REFRESH_LABEL}
                      value={readAutoRefresh()}
                      options={AUTO_REFRESH_OPTIONS}
                      onChange={noop}
                    />
                  </div>
                  <div className={FRAME_HEAD_FILTERS}>
                    {filters.map((k) => (
                      <ToolbarSelect
                        key={k}
                        id={`tasks-filter-${k}-skeleton`}
                        label={FILTER_LABELS[k]}
                        prefix={FILTER_LABELS[k]}
                        value=""
                        options={[ALL_OPTION]}
                        onChange={noop}
                      />
                    ))}
                  </div>
                </div>
                <RunRowsLoading />
                {readPagerShown() ? <RunPager page={Number(params.get("page")) || 1} pages={null} /> : null}
              </div>
            </>
          )}
        </div>
      </div>
    </section>
  );
}
