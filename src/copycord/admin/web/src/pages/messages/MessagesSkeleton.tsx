// The Messages page while its chunk or first page downloads, and the pieces the page shares with it:
// header, toolbar, table head and loading rows. Kept out of Messages.tsx so the app shell can show it
// without pulling the page chunk in.

import type { ReactNode } from "react";
import { cx } from "../../components/ui/cx";
import { Skeleton } from "../../components/ui/Skeleton";
import { ToolbarSelect } from "../channels/ToolbarSelect";
import type { ToolbarOption } from "../channels/ToolbarSelect";
import { CONTROL, COUNT, HEAD_ROW, ICON_BTN, TH } from "../logs/LogsSkeleton";
import { MessagesIcon } from "./icons";
import { COLUMNS, NO_FILTERS, PREVIEW_WIDTH, SORT_OPTIONS, loadColumns, tableMinWidth } from "./model";
import type { ColumnKey, Filters, SortKey } from "./model";

export const TOOLBAR = cx(
  "mb-3 flex flex-col gap-2.5 rounded-xl border border-border px-3.5 py-2.5",
  "bg-linear-135 from-card-hover to-panel-a40",
);
export const TOOLBAR_ROW = "flex flex-wrap items-center gap-2.5 max-md:grid max-md:grid-cols-2 max-md:items-end";
export const SEARCH_INPUT = cx(CONTROL, "w-full py-[7px] pr-3 pl-[30px] placeholder:text-muted/60");
export const DATE_INPUT = cx(CONTROL, "h-[34px] w-[150px] px-2.5 [color-scheme:dark] max-md:w-full");
export const PILL_BTN =
  "inline-flex h-[34px] cursor-pointer items-center gap-1.5 whitespace-nowrap rounded-lg border px-3 text-[13px] transition-colors " +
  "outline-none focus-visible:ring-2 focus-visible:ring-accent/60";
export const COLUMNS_BTN = cx(
  PILL_BTN,
  "border-border-input bg-panel-a60 text-text hover:border-accent/50 aria-expanded:border-accent",
);
export const RESET_BTN = cx(
  PILL_BTN,
  "border-primary-a30 bg-primary-a10 text-accent-soft hover:border-primary-a50 hover:bg-primary-a20",
);

// The wrap scrolls the rows under a sticky head; its height leaves room for the header and the
// two toolbar rows so the page fits the screen, which keeps its sideways scrollbar (cc-hscroll) at
// the bottom of the visible table instead of after the last loaded row.
export const TABLE_WRAP = cx(
  "cc-hscroll relative h-[calc(100vh-318px)] min-h-[360px] overflow-auto rounded-xl border border-border",
  "bg-linear-to-b from-card-a50 to-well-a60 transition-opacity max-md:h-[calc(100vh-260px)]",
);
export const TABLE = "w-full table-fixed border-collapse";
export const ROW = "group transition-colors hover:bg-primary-a6";
export const TD = "overflow-hidden border-b border-b-white/4 px-3 py-2 align-middle text-[13px] text-text";
export const CELL_TEXT = "block truncate";
export const MUTED = "text-xs text-muted";
export const MONO = "font-mono text-xs";
// Preview is a plain last column: it scrolls with the others and takes the row's background and hover.
export const PREVIEW_TH = cx(TH, "px-3 text-right");
export const PREVIEW_TD = "text-right";

const ALL_MAPPINGS: ToolbarOption[] = [{ value: "", label: "All mappings" }];
const ALL_CHANNELS: ToolbarOption[] = [{ value: "", label: "All channels" }];
export const ATTACHMENT_OPTIONS: ToolbarOption[] = [
  { value: "", label: "Any attachments" },
  { value: "yes", label: "With attachments" },
  { value: "no", label: "No attachments" },
];

export function MessagesHeader({ count }: { count?: ReactNode }) {
  return (
    <div className="my-3 flex flex-wrap items-end justify-between gap-x-4 gap-y-1">
      <div className="min-w-0">
        <h1 className="m-0 text-[22px] font-bold text-text">Messages</h1>
        <p className="m-0 mt-0.5 text-[13px] text-muted md:truncate">
          Every message Copycord cloned. Search, filter and preview them live from Discord.
        </p>
      </div>
      <span id="messages-count" className={COUNT} aria-live="polite">
        {count}
      </span>
    </div>
  );
}

export interface ToolbarProps {
  searchInput: string;
  onSearch: (v: string) => void;
  filters: Filters;
  onFilter: (patch: Partial<Filters>) => void;
  sort: SortKey;
  onSort: (s: SortKey) => void;
  mappingOptions?: ToolbarOption[];
  channelOptions?: ToolbarOption[];
  facetsPending?: boolean;
  columnsMenu?: ReactNode;
  reset?: ReactNode;
}

/** The two toolbar rows: search, sort and columns; then the filters and Reset filters. */
export function MessagesToolbar({
  searchInput,
  onSearch,
  filters,
  onFilter,
  sort,
  onSort,
  mappingOptions = ALL_MAPPINGS,
  channelOptions = ALL_CHANNELS,
  facetsPending,
  columnsMenu,
  reset,
}: ToolbarProps) {
  return (
    <div className={TOOLBAR}>
      <div className={TOOLBAR_ROW}>
        <div className="relative flex min-w-[240px] flex-1 items-center max-md:col-span-2 max-md:min-w-0">
          <MessagesIcon name="search" size={15} className="pointer-events-none absolute left-2.5 text-muted" />
          <input
            type="search"
            id="messages-search"
            className={SEARCH_INPUT}
            placeholder="Search by author name or ID"
            aria-label="Search messages"
            autoComplete="off"
            value={searchInput}
            onChange={(e) => onSearch(e.target.value)}
          />
        </div>
        <ToolbarSelect
          id="messages-sort"
          label="Sort messages"
          prefix="Sort"
          value={sort}
          options={SORT_OPTIONS}
          onChange={(v) => onSort(v as SortKey)}
        />
        {columnsMenu ?? (
          <button type="button" className={COLUMNS_BTN} aria-expanded={false}>
            <MessagesIcon name="columns" size={15} />
            Columns
          </button>
        )}
      </div>
      <div className={TOOLBAR_ROW}>
        <ToolbarSelect
          id="messages-filter-mapping"
          label="Filter by mapping"
          value={filters.mapping}
          options={mappingOptions}
          pending={facetsPending}
          pendingLabel="All mappings"
          onChange={(v) => onFilter({ mapping: v, channel: "" })}
        />
        <ToolbarSelect
          id="messages-filter-channel"
          label="Filter by channel"
          value={filters.channel}
          options={channelOptions}
          pending={facetsPending}
          pendingLabel="All channels"
          onChange={(v) => onFilter({ channel: v })}
        />
        <ToolbarSelect
          id="messages-filter-attachments"
          label="Filter by attachments"
          value={filters.attachments}
          options={ATTACHMENT_OPTIONS}
          className="max-md:col-span-2"
          onChange={(v) => onFilter({ attachments: v })}
        />
        <label className="flex items-center gap-1.5 text-xs text-muted max-md:flex-col max-md:items-stretch">
          From
          <input
            type="date"
            id="messages-from"
            className={DATE_INPUT}
            value={filters.from}
            max={filters.to || undefined}
            onChange={(e) => onFilter({ from: e.target.value })}
          />
        </label>
        <label className="flex items-center gap-1.5 text-xs text-muted max-md:flex-col max-md:items-stretch">
          To
          <input
            type="date"
            id="messages-to"
            className={DATE_INPUT}
            value={filters.to}
            min={filters.from || undefined}
            onChange={(e) => onFilter({ to: e.target.value })}
          />
        </label>
        {reset}
      </div>
    </div>
  );
}

export function MessagesHead({ columns }: { columns: readonly ColumnKey[] }) {
  const on = new Set(columns);
  return (
    <thead className="sticky top-0 z-[2]">
      <tr className={HEAD_ROW}>
        {COLUMNS.filter((c) => on.has(c.key)).map((c) => (
          <th key={c.key} scope="col" className={cx(TH, "px-3 text-left")} style={{ width: c.width }}>
            {c.label}
          </th>
        ))}
        <th scope="col" className={PREVIEW_TH} style={{ width: PREVIEW_WIDTH }}>
          <span className="sr-only">Preview</span>
        </th>
      </tr>
    </thead>
  );
}

// Bar widths per column so loading rows vary like real ones.
const BAR: Record<ColumnKey, number[]> = {
  time: [52, 44, 60],
  created: [150, 140, 150],
  author_name: [90, 70, 110],
  username: [80, 96, 64],
  author_id: [140, 140, 140],
  original_channel: [90, 110, 70],
  original_channel_id: [140, 140, 140],
  cloned_channel: [90, 110, 70],
  cloned_channel_id: [140, 140, 140],
  original_guild: [100, 80, 120],
  original_guild_id: [140, 140, 140],
  cloned_guild: [100, 80, 120],
  cloned_guild_id: [140, 140, 140],
  mapping: [70, 90, 60],
  attachments: [24, 24, 24],
  embeds: [24, 24, 24],
  links: [110, 110, 110],
  message_id: [140, 140, 140],
  cloned_message_id: [140, 140, 140],
};

/** The table element's sizing for these columns: full width, never narrower than the columns. */
export function tableStyle(cols: readonly ColumnKey[]): { minWidth: number } {
  return { minWidth: tableMinWidth(cols) };
}
export const LOADING_ROWS = 16;

/** The table body while the first page loads: one row per message row, a bar where each value lands. */
export function MessagesLoadingRows({ columns, rows = LOADING_ROWS }: { columns: readonly ColumnKey[]; rows?: number }) {
  return (
    <>
      {Array.from({ length: rows }, (_, i) => (
        <tr key={i} data-skeleton-row="" className={ROW}>
          {columns.map((k, j) => {
            const widths = BAR[k];
            const w = widths[i % widths.length]!;
            return (
              <td key={k} className={TD}>
                {i === 0 && j === 0 ? <span role="status" aria-label="Loading messages" className="sr-only" /> : null}
                {w ? <Skeleton.Text className={cx(CELL_TEXT, "leading-6")} width={w} /> : <span className="block leading-6">&nbsp;</span>}
              </td>
            );
          })}
          <td className={cx(TD, PREVIEW_TD)}>
            <Skeleton.Text className="inline-block leading-6" width={64} />
          </td>
        </tr>
      ))}
    </>
  );
}

export function MessagesSkeleton() {
  const columns = loadColumns();
  return (
    <section role="status" aria-busy="true" aria-label="Loading page" className="w-full">
      <MessagesHeader count={<Skeleton.Text className={COUNT} width={90} />} />
      <div inert>
        <MessagesToolbar
          searchInput=""
          onSearch={() => {}}
          filters={NO_FILTERS}
          onFilter={() => {}}
          sort="newest"
          onSort={() => {}}
          facetsPending
        />
      </div>
      <div inert className={TABLE_WRAP}>
        <table className={TABLE} style={tableStyle(columns)}>
          <MessagesHead columns={columns} />
          <tbody>
            <MessagesLoadingRows columns={columns} />
          </tbody>
        </table>
      </div>
    </section>
  );
}

export { ICON_BTN };
