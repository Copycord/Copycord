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
import { Skeleton, useReveal } from "../../components/ui/Skeleton";
import { RowMenu } from "../channels/actions/RowMenu";
import { ToolbarSelect } from "../channels/ToolbarSelect";
import type { RunCounts } from "../../types/tasks";
import { runTypeOf, typeLabel } from "./typeLabels";

/** The rail and the page's column side by side, filling the shared centered page column (--layout-max). */
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
/** The runs card: FRAME without the clip, so an open filter list hangs past the card's bottom edge, over the page. */
export const LIST_FRAME = "rounded-card border border-border bg-card";
export const FRAME_HEAD = "flex flex-wrap items-center justify-between gap-3 border-b border-border px-4 py-2.5 text-sm";
/** Left of the frame head: the run count. */
export const FRAME_HEAD_LEFT = "flex items-center gap-4";
/** Right of the frame head: the filter dropdowns. */
export const FRAME_HEAD_FILTERS = "flex flex-wrap items-center gap-3";
export const RUN_COUNT = "font-semibold text-text";
/** The pager under the runs (RunPager). */
export const PAGER = "flex items-center justify-between gap-3 border-t border-border px-4 py-3 text-sm text-muted";
/** The box of a run row; ROW adds the hover tint. The skeleton row uses the box alone so both stand equally tall. */
export const ROW_BOX = "relative flex min-h-16 items-center gap-3 border-b border-border px-4 py-2.5 last:rounded-b-card last:border-b-0";
export const ROW = `${ROW_BOX} transition-colors duration-150 hover:bg-rail-hover`;
/**
 * A run row's left column: the task name over the meta line, taking the room up to the timing, its meta
 * line cut at the end when short; from 1024 px it keeps 220 px.
 */
export const ROW_MAIN = "min-w-0 flex-1 lg:min-w-[220px]";
/** The name line: the task name and number. */
export const ROW_HEAD = "flex h-6 min-w-0 items-center gap-2";
export const ROW_NAME = "min-w-0 truncate text-sm leading-5 font-semibold";
/** The name is the row's link: its ::before covers the whole row, so the row clicks through and shows one focus ring. */
export const ROW_LINK =
  `${ROW_NAME} text-text no-underline outline-none hover:underline before:absolute before:inset-0 before:content-[''] ` +
  "focus-visible:before:outline-2 focus-visible:before:-outline-offset-2 focus-visible:before:outline-accent";
/**
 * "Triggered by admin": one line of 13 px muted text, cut with an ellipsis at its end when the row is narrow.
 */
export const ROW_META = "mt-1 h-[18px] min-w-0 truncate text-[13px] leading-[18px] text-muted [&>*+*]:ml-1.5";
/** The mapping on a row's meta line: plain text like the trigger and actor, a long name cut at 24 characters. */
export const ROW_MAPPING = "min-w-0 max-w-[24ch] truncate";
/** A run ID in mono, the list row and the run page alike. */
export const ROW_ID = "font-mono text-xs";
/** One item of a meta line: a 12 px glyph and its words. */
export const META_ITEM = "inline-flex shrink-0 items-center gap-1";
export const META_DOT = "text-muted/50";
/** Started (relative) over duration, each after a small glyph, 13 px muted with tabular numbers. */
export const ROW_TIMING = "flex w-[80px] shrink-0 flex-col gap-1 text-[13px] leading-[18px] text-muted tabular-nums max-sm:hidden";
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
/** The glyph and title on the left, Cancel (while active) and the actions menu on the right, one centred line. */
export const RUN_HEAD_ROW = "flex min-h-9 items-center justify-between gap-3";
/** The status glyph, the type's title, the run's number and a failed run's error, on one line. */
export const RUN_TITLE_ROW = "flex min-w-0 items-center gap-2";
export const RUN_TITLE = "m-0 shrink-0 text-[22px] leading-8 font-bold text-text";
export const RUN_NUMBER = "shrink-0 text-[22px] leading-8 text-muted";
/** Right of the title line: Cancel run while the run is active, then the Run actions menu. */
export const RUN_ACTIONS = "flex shrink-0 items-center gap-2";
/** "Manual · admin · 5h ago · 1m 55s · Clone-1 · 0d88ac5a": plain muted text under the title, no word labels. */
export const RUN_META = "m-0 mt-1.5 flex min-h-5 flex-wrap items-center gap-x-2 gap-y-1 text-[13px] leading-5 text-muted";

/** A type's page, where the run page's breadcrumb leads. */
export function typeUrl(key: string): string {
  return `/tasks?type=${encodeURIComponent(key)}`;
}
/** A failed run's error after the number: quiet text, cut to one line, the whole of it in the title. */
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
} as const;

/** A small muted glyph before a value (a run row's meta line and timing, the run ID copy button): a label, so the skeleton draws it too. */
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

/** The status glyph's box (StatusIcon) with a round bar: the glyph is the run's status, so it shimmers. */
export function StatusIconLoading({ size = 16, className }: { size?: number; className?: string }) {
  return (
    <span data-testid="status-icon-loading" className={cx(STATUS_BOX, className)}>
      <Skeleton width={size} height={size} rounded="rounded-full" />
    </span>
  );
}

export interface RunMetaValues {
  /** "Manual · admin". */
  trigger: ReactNode;
  /** The relative start time, the date in its title. */
  started: ReactNode;
  /** How long it ran, "1m 55s". */
  ran: ReactNode;
  /** The mapping's name; null leaves the item out. */
  mapping: ReactNode | null;
  /** The short id and its copy button. */
  id: ReactNode;
}

/** The run page's meta line under the title: the run's values (or the skeleton's bars) between dots, no word labels. */
export function RunMetaLine({ values }: { values: RunMetaValues }) {
  return (
    <p data-testid="run-meta" className={RUN_META}>
      <span className={META_ITEM}>{values.trigger}</span>
      <MetaDot />
      <span className={META_ITEM}>{values.started}</span>
      <MetaDot />
      <span className={META_ITEM}>{values.ran}</span>
      {values.mapping != null ? (
        <>
          <MetaDot />
          <span className={META_ITEM}>{values.mapping}</span>
        </>
      ) : null}
      <MetaDot />
      <span className={META_ITEM}>{values.id}</span>
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

/** The Summary card's top: the hero count (messages delivered), the segmented bar, then the counts in one muted line. */
export const OVERVIEW = "border-b border-border px-4 py-3";
export const HERO = "flex h-10 min-w-0 items-baseline gap-2";
export const HERO_VALUE = "text-[32px] leading-10 font-semibold tabular-nums text-accent";
export const HERO_LABEL = "truncate text-sm text-muted";
export const OVERVIEW_BAR = "mt-3 h-1.5 w-full";
/** "4 done · 1 failed · 1 cancelled" under the bar: one 20 px line, held even when empty, so the card keeps its height. */
export const COUNTS_LINE = "mt-2 h-5 truncate text-[13px] leading-5 tabular-nums text-muted";

/** The overview's frame; the run page and its skeleton pass the hero, the bar and the counts line. */
export function RunOverview({ hero, bar, counts }: { hero: ReactNode; bar: ReactNode; counts: ReactNode }) {
  return (
    <div className={OVERVIEW}>
      <div data-testid="run-hero" className={HERO}>
        {hero}
      </div>
      {bar}
      <p data-testid="run-counts" className={COUNTS_LINE}>
        {counts}
      </p>
    </div>
  );
}

export const RUN_BODY = "flex flex-col gap-4";
/** The cards of the run page: FRAME without the clip, so the Steps filter's list can open over the next card. */
export const RUN_CARD = "rounded-card border border-border bg-card";
/** A card head (the Steps card; the Summary card opens on its hero instead): 48 px, the title on the left, controls on the right. */
export const CARD_HEAD = "flex min-h-12 flex-wrap items-center justify-between gap-x-3 gap-y-1.5 border-b border-border px-4 py-1.5 text-sm";
export const CARD_TITLE = "m-0 text-sm font-semibold text-text";
/** The Inputs list inside its opened disclosure: labels in a column at least 110 px wide, then the values. */
export const INPUTS_LIST = "m-0 grid grid-cols-[minmax(110px,max-content)_1fr] gap-x-6 gap-y-1 text-sm leading-5";
/** The status glyph's box (StatusIcon): it centres itself on the line it sits in, a run row, the run title
 * or a step row, whatever that line aligns its other parts to. */
export const STATUS_BOX = "inline-flex shrink-0 items-center self-center";
/** The Steps card head: the title and the status filter (the counts are in the Summary). */
export const STEPS_HEAD = CARD_HEAD;
/** A running step's thin bar in its row. */
export const STEP_BAR = "h-1 w-24 shrink-0 overflow-hidden rounded-full bg-line-1 max-sm:w-12";
/** Rows are one fixed line each, so the list stops at STEP_ROWS_MAX rows and scrolls past that. */
export const STEP_ROWS_MAX = 12;
/** One step row's height in px (h-9 in STEP_ROW). */
export const STEP_ROW_PX = 36;
export const STEP_LIST = "m-0 max-h-[432px] list-none overflow-y-auto p-0";
/** One step row, 36 px: the glyph (its state, labelled), the title, the thin bar while running, then the note. */
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
  filter: StepFilter;
  onFilter: (value: StepFilter) => void;
  id: string;
}

/** The Steps card head; the loaded card and the run skeleton both draw it, so they cannot drift. */
export function StepsHead({ filter, onFilter, id }: StepsHeadProps) {
  return (
    <div className={STEPS_HEAD}>
      <h3 className={CARD_TITLE}>Steps</h3>
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

/** A collapsed disclosure's row (Inputs, Report (JSON)): 48 px like a card head, the chevron and the title. */
export const DISCLOSURE_SUMMARY =
  "flex min-h-12 cursor-pointer list-none items-center gap-x-3 px-4 py-1.5 text-sm transition-colors hover:bg-white/5 marker:hidden [&::-webkit-details-marker]:hidden";
/** What an opened disclosure shows, under a hairline. */
export const DISCLOSURE_BODY = "border-t border-border px-4 py-3";

/**
 * A disclosure's summary line: the chevron, turned while open, and the title in the card head's type.
 * `open` turns it from a button toggle's own state (a details element turns it by itself).
 */
export function DisclosureTitle({ title, open }: { title: string; open?: boolean }) {
  return (
    <>
      <span aria-hidden="true" className={cx("inline-flex w-3 justify-center text-lg leading-none text-text-2 transition-transform group-open:rotate-90", open && "rotate-90")}>
        ›
      </span>
      <span className={CARD_TITLE}>{title}</span>
    </>
  );
}

/** The Summary card's Inputs, collapsed by default; the skeleton draws the closed row as itself. */
export function InputsDisclosure({ children }: { children?: ReactNode }) {
  return (
    <details data-testid="run-inputs" className="group">
      <summary className={DISCLOSURE_SUMMARY}>
        <DisclosureTitle title="Inputs" />
      </summary>
      {children ? <div className={DISCLOSURE_BODY}>{children}</div> : null}
    </details>
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

/** The quick status tabs over the runs frame: each label as text, its count from the server. */
export const STATUS_TABS = "mb-3 flex flex-wrap items-center gap-1 text-sm";
export const STATUS_TAB =
  "inline-flex h-8 items-center gap-1.5 rounded-control px-3 text-text-2 transition-colors hover:bg-rail-hover hover:text-text " +
  "focus-visible:outline-2 focus-visible:outline-accent/70";
export const STATUS_TAB_ON = "bg-rail-active font-semibold text-text";
export const STATUS_TAB_COUNT = "min-w-[1ch] text-muted tabular-nums";
/** The tabs in order: the status filter value ("" for every run) and the word on the tab. */
export const STATUS_TAB_LIST = [
  { value: "", key: "all", label: "All" },
  { value: "running", key: "running", label: "Running" },
  { value: "success", key: "success", label: "Done" },
  { value: "failed", key: "failed", label: "Failed" },
  { value: "cancelled", key: "cancelled", label: "Cancelled" },
] as const;

export interface StatusTabsProps {
  /** Runs per status; null while the first page loads, so the counts shimmer and the labels stay text. */
  counts: Partial<RunCounts> | null;
  /** The status filter in the URL, "" for All. */
  value: string;
  onSelect?: (value: string) => void;
}

/** "All (n) · Running (n) · Done (n) · Failed (n) · Cancelled (n)", wired to the status filter. */
export function StatusTabs({ counts, value, onSelect = noop }: StatusTabsProps) {
  return (
    <div role="group" aria-label="Runs by status" className={STATUS_TABS}>
      {STATUS_TAB_LIST.map((t) => {
        const on = t.value === value;
        return (
          <button
            key={t.key}
            type="button"
            aria-pressed={on}
            onClick={() => onSelect(t.value)}
            className={cx(STATUS_TAB, on && STATUS_TAB_ON)}
          >
            {t.label}
            {counts ? (
              <span data-testid={`tab-count-${t.key}`} className={cx(STATUS_TAB_COUNT, FADE_IN)}>
                {counts[t.key] ?? 0}
              </span>
            ) : (
              <Skeleton.Text className={STATUS_TAB_COUNT}>0</Skeleton.Text>
            )}
          </button>
        );
      })}
    </div>
  );
}

/** A rail entry's right side: a purple running badge (the running status colour) when the type has active runs, then its run count. */
export const RAIL_COUNTS = "ml-auto inline-flex shrink-0 items-center gap-2";
export const RAIL_COUNT = "text-xs text-muted tabular-nums";
export const RAIL_RUNNING = "inline-flex items-center gap-1 text-xs font-medium text-accent tabular-nums";
export const RAIL_RUNNING_DOT = "size-1.5 rounded-full bg-accent animate-pulse";
/** The same badge when every active run of the type is still queued: the queued state's blue, the dot still. */
export const RAIL_QUEUED = "inline-flex items-center gap-1 text-xs font-medium text-info tabular-nums";
export const RAIL_QUEUED_DOT = "size-1.5 rounded-full bg-info";

/** "1 running, 2 queued": the active runs in words, for the rail entry's title. */
export function activeText(active: number, queued = 0): string {
  const running = Math.max(0, active - queued);
  return [running > 0 ? `${running} running` : "", queued > 0 ? `${queued} queued` : ""].filter(Boolean).join(", ");
}

/** The rail badge, kept short: the running runs only ("1 running"), or "2 queued" when none runs yet. */
export function badgeText(active: number, queued = 0): string {
  const running = Math.max(0, active - queued);
  return running > 0 ? `${running} running` : `${queued} queued`;
}
// The active count says "running" in words too (rule a): the pulsing accent dot alone was a colour-only signal.

export function RailCounts({ total, active = 0, queued = 0 }: { total: number | null; active?: number; queued?: number }) {
  const onlyQueued = active > 0 && queued >= active;
  return (
    // Hidden from the link's name, which stays the type's title; the link's title says the counts.
    <span aria-hidden="true" className={RAIL_COUNTS}>
      {active > 0 ? (
        <span className={onlyQueued ? RAIL_QUEUED : RAIL_RUNNING}>
          <span aria-hidden="true" className={onlyQueued ? RAIL_QUEUED_DOT : RAIL_RUNNING_DOT} />
          {badgeText(active, queued)}
        </span>
      ) : null}
      {total == null ? (
        <Skeleton.Text className={RAIL_COUNT}>0</Skeleton.Text>
      ) : (
        <span data-testid="rail-count" className={cx(RAIL_COUNT, FADE_IN)}>
          {total}
        </span>
      )}
    </span>
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
        <RailCounts total={null} />
      </div>
    </li>
  );
}

/**
 * One loading row in the shape of RunRow (RunList.tsx): the same box, a round bar for the status glyph,
 * a bar for the name, a bar on the meta line, the two timing lines with their
 * glyphs as themselves and bars for the values, and the row menu button, each in the same type size and
 * padding as the real element, so the first page lands without the list jumping.
 */
function RunRowLoading({ nameWidth }: { nameWidth: number }) {
  return (
    <li className={ROW_BOX}>
      <StatusIconLoading />
      <div className={ROW_MAIN}>
        <div className={ROW_HEAD}>
          <TextBar width={nameWidth} className={ROW_NAME} />
        </div>
        <div className={ROW_META}>
          <Skeleton.Text className="align-top">Triggered by admin</Skeleton.Text>
        </div>
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

/** The runs empty state: icon, title and a line under it; the filtered variant adds Clear filters. */
export const RUNS_EMPTY = "py-12";

/** The empty state (Tasks.tsx) with bars on its lines, for a list that had no runs last time. */
function RunsEmptyLoading() {
  return (
    <EmptyState
      plain
      className={RUNS_EMPTY}
      icon={<RunsEmptyIcon />}
      title={<Skeleton.Text>No task runs yet</Skeleton.Text>}
      description={<Skeleton.Text>Runs show up here once a task starts</Skeleton.Text>}
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
/** A first visit draws a backfill run's six steps. */
const FIRST_VISIT_STEPS = 6;

export interface RunDetailSkeletonProps {
  /** The run's type key when known before the run loads (typeLabels.ts runTypeOf): its title shows as text. */
  runType?: string | null;
  /** Step rows to draw; the count the last run page showed by default. */
  steps?: number;
}

/**
 * The run page until the run answers, also the page's own loading state: the header with the type's
 * title as text when the run's type is known (bars in its place only when it cannot be), a bar for the
 * number and the "Run actions" button as itself and inert on the same line, the meta line with bars for
 * the run's values between its dots, the summary card with bars for the hero count, the segmented bar and
 * the counts line, then the closed Inputs row as itself, the Steps card with its real head and as many bar
 * rows as the last run showed, and the collapsed Report line.
 */
export function RunDetailSkeleton({
  runType = null,
  steps = readLastCount(STEPS_COUNT_KEY, FIRST_VISIT_STEPS, STEP_ROWS_MAX),
}: RunDetailSkeletonProps) {
  const title = runType ? typeLabel(runType).title : null;
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
              <StatusIconLoading size={20} />
              <h2 className={RUN_TITLE}>{title ?? <TextBar width={180} />}</h2>
              <Skeleton.Text className="shrink-0 text-[22px] leading-8">#00</Skeleton.Text>
            </div>
            <div className={RUN_ACTIONS}>
              <RowMenu kind="channel" label="Run actions" items={[]} disabled />
            </div>
          </div>
          <RunMetaLine
            values={{
              trigger: <Skeleton.Text>Manual · admin</Skeleton.Text>,
              started: <Skeleton.Text>5h ago</Skeleton.Text>,
              ran: <Skeleton.Text>1m 55s</Skeleton.Text>,
              mapping: <Skeleton.Text>Clone-1</Skeleton.Text>,
              id: <RunIdValue id={null} />,
            }}
          />
        </header>
        <div className={RUN_BODY}>
          <section aria-label="Summary" className={RUN_CARD}>
            <RunOverview
              hero={
                <>
                  <Skeleton.Text className={HERO_VALUE}>000</Skeleton.Text>
                  <Skeleton.Text className={HERO_LABEL}>messages delivered</Skeleton.Text>
                </>
              }
              bar={<Skeleton rounded="rounded-full" className={OVERVIEW_BAR} />}
              counts={<Skeleton.Text>4 done · 1 failed · 1 cancelled</Skeleton.Text>}
            />
            <InputsDisclosure />
          </section>
          <section aria-label="Steps" className={RUN_CARD}>
            <StepsHead id="steps-filter-skeleton" filter="" onFilter={noop} />
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
              <summary className={DISCLOSURE_SUMMARY}>
                <DisclosureTitle title="Report (JSON)" />
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
          <div className={cx(RAIL_ITEM, typeKey && !runPage ? null : RAIL_ACTIVE)}>
            All tasks
            <RailCounts total={null} />
          </div>
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
              <StatusTabs counts={null} value={params.get("status") ?? ""} />
              <div className={LIST_FRAME}>
                <div className={FRAME_HEAD}>
                  <div className={FRAME_HEAD_LEFT}>
                    <RunCountLoading />
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
