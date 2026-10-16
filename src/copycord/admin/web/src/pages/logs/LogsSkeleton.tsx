// The Logs page while its chunk downloads: the same header, toolbar, table head, rows and pager as
// the page. Kept out of Logs.tsx so the app shell can show it without pulling the page chunk in; the
// page and LogRow take their shared classes and loading rows from here.

import { cx } from "../../components/ui/cx";
import { Skeleton } from "../../components/ui/Skeleton";
import { ToolbarSelect } from "../channels/ToolbarSelect";
import { LogsIcon } from "./icons";
import type { SortColumn } from "./model";
import { Pagination } from "./Pagination";

export const CONTROL =
  "rounded-lg border border-border bg-panel-a60 text-[13px] text-text outline-none transition-colors focus:border-accent";
export const SEARCH_INPUT = cx(CONTROL, "w-[200px] py-[7px] pr-3 pl-[30px] placeholder:text-muted/60 max-md:w-full");
export const TYPE_BOX = "w-[300px] min-w-[300px] [&_select]:w-[300px] [&_select]:max-w-[300px] [&>span]:block";
export const ICON_BTN =
  "inline-flex size-8 shrink-0 cursor-pointer items-center justify-center rounded-lg border border-border bg-transparent px-1.5 py-px text-muted " +
  "transition-colors disabled:cursor-wait disabled:opacity-55 focus-visible:ring-2 focus-visible:ring-accent/60 outline-none";
export const REFRESH_BTN = cx(ICON_BTN, "enabled:hover:border-accent/40 enabled:hover:bg-primary-a10 enabled:hover:text-accent");
export const CLEAR_ALL_BTN = cx(
  ICON_BTN,
  "enabled:hover:border-danger-a30 enabled:hover:bg-danger-a10 enabled:hover:text-danger-soft",
);

export const TOOLBAR = cx(
  "mb-3 flex flex-wrap items-center justify-between gap-2.5 rounded-xl border border-border px-3.5 py-2.5",
  "bg-linear-135 from-card-hover to-panel-a40 max-md:flex-col max-md:items-stretch",
);
export const TOOLBAR_LEFT = "flex flex-wrap items-center gap-2.5 max-md:flex-col";
export const TOOLBAR_RIGHT = "flex items-center gap-3";
export const COUNT = "text-xs whitespace-nowrap text-muted";

// The table height leaves room for the header and toolbar so the page fits the screen. On phones the
// table keeps the old fixed layout without Guild, so Details runs past the edge and the wrap scrolls.
export const TABLE_WRAP = cx(
  "h-[calc(100vh-280px)] min-h-[400px] overflow-auto rounded-xl border border-border",
  "bg-linear-to-b from-card-a50 to-well-a60 max-md:h-[calc(100vh-320px)] max-md:min-h-[300px]",
);
export const TABLE = "mt-2 w-full table-fixed border-collapse border-[gray]";
export const HEAD_ROW = "border-[gray] bg-panel-a95 backdrop-blur-sm";

export const HEADERS: Array<{ key: SortColumn; label: string; className: string }> = [
  { key: "created_at", label: "Timestamp", className: "w-[200px] min-w-[200px] max-md:w-[130px]" },
  { key: "event_type", label: "Type", className: "w-[160px]" },
  { key: "guild_name", label: "Guild", className: "w-[140px] max-md:hidden" },
  { key: "details", label: "Details", className: "" },
];
export const COLS = HEADERS.length + 1;

// Padding and alignment are per header so no two utilities fight over one property.
export const TH = "relative border-b border-b-border py-2.5 text-[11px] leading-[normal] font-semibold tracking-[0.5px] whitespace-nowrap text-muted uppercase";
export const SORT_BTN = "cursor-pointer uppercase outline-none select-none hover:text-text focus-visible:text-text";
export const ACTIONS_TH = cx(TH, "w-11 px-3.5 text-center");

// One log row: the row, its five cells and what each cell holds. LogRow and the loading rows both
// build from these, so a loading row has the cells, paddings and line boxes of a loaded one.
export const ROW = "group border-[gray] transition-colors hover:bg-primary-a6";
const TD = "border-b py-2.5 align-middle text-[13px] text-text";
export function rowCells(expanded: boolean) {
  const edge = cx(TD, expanded ? "border-b-transparent" : "border-b-white/4");
  const td = cx(edge, "px-3.5 text-left");
  return { td, guild: cx(td, "max-md:hidden"), actions: cx(edge, "px-3.5 text-center") };
}
export const TIMESTAMP_TEXT = "whitespace-nowrap font-mono text-xs text-muted";
// The event type badge: fill and text only, the 1px border stays transparent.
export const BADGE =
  "inline-block whitespace-nowrap rounded-md border border-transparent px-2.5 py-[3px] text-[11px] font-semibold tracking-[0.2px] capitalize";
export const GUILD_TEXT = "text-xs font-medium text-muted";
export const DETAILS_TEXT = "text-[13px] text-white/80";
export const DELETE_BTN = cx(
  "inline-flex cursor-pointer items-center justify-center rounded-md p-1 text-muted opacity-0 transition",
  "group-hover:opacity-100 focus-visible:opacity-100 max-md:opacity-100",
  "hover:bg-danger-a10 hover:text-danger-soft disabled:cursor-wait",
);

// A timestamp is 19 monospace characters; the type, guild and details widths vary like real rows do.
const ROW_WIDTHS: Array<[number, number, number]> = [
  [104, 72, 260],
  [92, 88, 340],
  [118, 64, 220],
  [86, 80, 300],
  [110, 72, 380],
  [96, 88, 240],
];
export const LOADING_ROWS = 14;

/** The table body while the first page of logs loads: rows shaped like LogRow, a shimmer where each value lands. */
export function LogsLoadingRows({ rows = LOADING_ROWS }: { rows?: number }) {
  const cells = rowCells(false);
  return (
    <>
      {Array.from({ length: rows }, (_, i) => {
        const [type, guild, details] = ROW_WIDTHS[i % ROW_WIDTHS.length]!;
        return (
          <tr key={i} data-skeleton-row="" className={ROW}>
            <td className={cells.td}>
              {i === 0 ? <span role="status" aria-label="Loading event logs" className="sr-only" /> : null}
              <Skeleton.Text className={TIMESTAMP_TEXT} width={137} />
            </td>
            <td className={cells.td}>
              <Skeleton.Text className={cx(BADGE, "border-transparent")} width={type} />
            </td>
            <td className={cells.guild}>
              <Skeleton.Text className={GUILD_TEXT} width={guild} />
            </td>
            <td className={cells.td}>
              <Skeleton.Text className={DETAILS_TEXT} width={details} />
            </td>
            <td className={cells.actions}>
              <button type="button" aria-hidden="true" tabIndex={-1} className={DELETE_BTN}>
                <LogsIcon name="x" size={14} />
              </button>
            </td>
          </tr>
        );
      })}
    </>
  );
}

/** The count while the first page loads, in the line box of "Showing 1-50 of 120". */
export function CountBar() {
  return <Skeleton.Text className={COUNT} width={112} />;
}

export function LogsHeader() {
  return (
    <>
      <h1 className="sr-only">Logs</h1>
      <div className="my-3 flex items-center gap-3">
        <h2 className="m-0 text-[22px] font-bold text-text">Event Logs</h2>
      </div>
    </>
  );
}

const ALL_EVENTS = [{ value: "", label: "All Events" }];

export function LogsSkeleton() {
  return (
    <section role="status" aria-busy="true" aria-label="Loading page" className="w-full scroll-mt-4">
      <LogsHeader />

      <div inert className={TOOLBAR}>
        <div className={TOOLBAR_LEFT}>
          <div className="relative flex items-center">
            <LogsIcon name="search" size={15} className="pointer-events-none absolute left-2.5 text-muted" />
            <input type="text" className={SEARCH_INPUT} placeholder="Search logs" aria-label="Search logs" readOnly />
          </div>
          <div className={TYPE_BOX}>
            <ToolbarSelect id="logs-filter-type-skeleton" label="Filter by type" value="" options={ALL_EVENTS} onChange={() => {}} />
          </div>
        </div>
        <div className={TOOLBAR_RIGHT}>
          <span className={COUNT}>
            <CountBar />
          </span>
          <button type="button" aria-label="Refresh logs" className={REFRESH_BTN}>
            <LogsIcon name="refresh" size={16} className="animate-spin" />
          </button>
          <button type="button" aria-label="Clear all logs" className={CLEAR_ALL_BTN}>
            <LogsIcon name="trash" size={16} />
          </button>
        </div>
      </div>

      <div inert className={TABLE_WRAP}>
        <table className={TABLE}>
          <thead className="sticky top-0 z-[2]">
            <tr className={HEAD_ROW}>
              {HEADERS.map((h) => (
                <th key={h.key} scope="col" className={cx(TH, "px-3.5 text-left", h.className)}>
                  <button type="button" className={SORT_BTN}>
                    {h.label}
                    <span aria-hidden="true" className="ml-1 text-[11px] opacity-30 transition-opacity">
                      {h.key === "created_at" ? "▼" : ""}
                    </span>
                  </button>
                </th>
              ))}
              <th scope="col" className={ACTIONS_TH}>
                <span className="sr-only">Actions</span>
              </th>
            </tr>
          </thead>
          <tbody>
            <LogsLoadingRows />
          </tbody>
        </table>
      </div>

      <Pagination page={1} pages={1} inert />
    </section>
  );
}
