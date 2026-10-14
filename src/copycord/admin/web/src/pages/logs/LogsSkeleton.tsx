// The Logs page while its chunk downloads: the same header, toolbar, table head and loading rows as
// the page, and the empty pagination row it opens with. Kept out of Logs.tsx so the app shell can show
// it without pulling the page chunk in; the page takes its shared classes and placeholder from here.

import { cx } from "../../components/ui/cx";
import { SkeletonSection } from "../../components/ui/Skeleton";
import { ToolbarSelect } from "../channels/ToolbarSelect";
import { LogsIcon } from "./icons";
import type { SortColumn } from "./model";

export const CONTROL =
  "rounded-lg border border-border bg-[rgba(20,10,40,0.6)] text-[13px] text-text outline-none transition-colors focus:border-accent";
export const SEARCH_INPUT = cx(CONTROL, "w-[200px] py-[7px] pr-3 pl-[30px] placeholder:text-muted/60 max-md:w-full");
export const TYPE_BOX = "w-[300px] min-w-[300px] [&_select]:w-[300px] [&_select]:max-w-[300px] [&>span]:block";
export const ICON_BTN =
  "inline-flex size-8 shrink-0 cursor-pointer items-center justify-center rounded-lg border border-border bg-transparent px-1.5 py-px text-muted " +
  "transition-colors disabled:cursor-wait disabled:opacity-55 focus-visible:ring-2 focus-visible:ring-accent/60 outline-none";
export const REFRESH_BTN = cx(ICON_BTN, "enabled:hover:border-accent/40 enabled:hover:bg-[rgba(139,92,246,0.1)] enabled:hover:text-accent");
export const CLEAR_ALL_BTN = cx(
  ICON_BTN,
  "enabled:hover:border-[rgba(239,68,68,0.3)] enabled:hover:bg-[rgba(239,68,68,0.1)] enabled:hover:text-[#f87171]",
);

export const TOOLBAR = cx(
  "mb-3 flex flex-wrap items-center justify-between gap-2.5 rounded-xl border border-border px-3.5 py-2.5",
  "bg-linear-135 from-[rgba(27,16,48,0.6)] to-[rgba(20,10,40,0.4)] max-md:flex-col max-md:items-stretch",
);
export const TOOLBAR_LEFT = "flex flex-wrap items-center gap-2.5 max-md:flex-col";
export const TOOLBAR_RIGHT = "flex items-center gap-3";
export const COUNT = "text-xs whitespace-nowrap text-muted";

// The table height leaves room for the header and toolbar so the page fits the screen. On phones the
// table keeps the old fixed layout without Guild, so Details runs past the edge and the wrap scrolls.
export const TABLE_WRAP = cx(
  "h-[calc(100vh-280px)] min-h-[400px] overflow-auto rounded-xl border border-border",
  "bg-linear-to-b from-[rgba(27,16,48,0.5)] to-[rgba(15,8,30,0.6)] max-md:h-[calc(100vh-320px)] max-md:min-h-[300px]",
);
export const TABLE = "mt-2 w-full table-fixed border-collapse border-[gray]";
export const HEAD_ROW = "border-[gray] bg-[rgba(20,10,40,0.95)] backdrop-blur-sm";

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

// With one page the old .logs-pagination stayed in the flow, empty, keeping its 16px padding.
export const PAGINATION = "flex flex-wrap items-center justify-center gap-1 py-4";

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

/** The table body while the first page of logs loads. */
export function LogsLoadingRow() {
  return (
    <tr>
      <td colSpan={COLS} className="px-3.5 py-2">
        <SkeletonSection rows={8} rowHeight={41} label="Loading event logs" />
      </td>
    </tr>
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
          <span className={COUNT} />
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
            <LogsLoadingRow />
          </tbody>
        </table>
      </div>

      <nav aria-label="Pagination" className={PAGINATION} />
    </section>
  );
}
