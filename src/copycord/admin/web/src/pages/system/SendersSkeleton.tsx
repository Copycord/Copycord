// The sending accounts section's shared pieces: the header, the toolbar, the wrap, the head, the cell
// classes and the loading rows. The page section and the chunk skeleton both build from these, so a
// loading row has the cells, paddings and line boxes of a loaded one. Styled like the Messages page.

import type { CSSProperties, ReactNode } from "react";
import { Skeleton, cx } from "../../components/ui";
import { readLastCount } from "../../components/ui/lastCount";
import { COUNT, HEAD_ROW, SORT_BTN, TH } from "../logs/LogsSkeleton";
import type { SenderState } from "../../types/system";
import { ToolbarSelect } from "../channels/ToolbarSelect";
import type { ToolbarOption } from "../channels/ToolbarSelect";
import { MessagesIcon } from "../messages/icons";
import { CELL_TEXT, ROW, SEARCH_INPUT, TABLE, TD, TOOLBAR, TOOLBAR_ROW } from "../messages/MessagesSkeleton";
import type { SenderSort, SenderSortKey } from "./senders";

export type SenderColumn = SenderSortKey;

export const SENDER_COLUMNS: ReadonlyArray<{ key: SenderColumn; label: string; width: string; right?: boolean }> = [
  { key: "account", label: "Account", width: "26%" },
  { key: "username", label: "Username", width: "18%" },
  { key: "status", label: "Status", width: "15%" },
  { key: "online", label: "Online for", width: "14%" },
  { key: "last", label: "Last send", width: "13%" },
  { key: "sent", label: "Messages sent", width: "14%", right: true },
];

export const SENDERS_SECTION = "mt-[18px]";
// The Messages wrap's look and scrolling (cc-hscroll, rows under a sticky head) at a fixed height of
// about twelve rows, capped so the page fits the screen. The height never follows the row count:
// three accounts leave empty space below, a hundred scroll inside.
export const SENDERS_WRAP = cx(
  "cc-hscroll relative h-[520px] max-h-[calc(100vh-500px)] min-h-[360px] overflow-auto rounded-xl border border-border",
  "bg-linear-to-b from-card-a50 to-well-a60 transition-opacity",
);
export const SENDERS_TABLE = TABLE;
export const SENDERS_TABLE_STYLE: CSSProperties = { minWidth: 760 };
export const SENDER_ROW = ROW;
export const SENDER_TD = cx(TD, "leading-6");
export const SENDER_TD_RIGHT = cx(SENDER_TD, "text-right");
export const SENDER_TEXT = cx(CELL_TEXT, "leading-6");
export const AVATAR = "size-6 flex-none rounded-full";
export const ACCOUNT = "flex h-6 min-w-0 items-center gap-2.5";
/** The empty, no match or failed line, centred in the wrap whatever its height. */
export const SENDERS_NOTE = "pointer-events-none absolute inset-0 flex items-center justify-center px-3 text-center text-[13px] text-muted";

export const SENDERS_COUNT_KEY = "system.senders";

/** The count until the first answer: a bar in the line box of "7 accounts". */
export function SendersCountBar() {
  return <Skeleton.Text className={COUNT} width={72} />;
}

// The Enabled or Disabled chip of the mapping popup's section headers (SectionSummary in
// dashboard/mappings/modal/parts.tsx). Both labels share one grid cell, the other one invisible, so the
// box is as wide as "Disabled" in either state and the loading box matches it exactly.
const CHIP = "inline-grid shrink-0 rounded-[20px] px-2.5 py-1 text-center text-[11px] leading-none font-semibold";
const CHIP_ON = "bg-ok-a12 text-ok";
const CHIP_OFF = "bg-wash-6 text-muted";
const CHIP_CELL = "[grid-area:1/1]";

function ChipLabels({ on }: { on: boolean }) {
  return (
    <>
      <span data-chip-label="" className={CHIP_CELL}>
        {on ? "Enabled" : "Disabled"}
      </span>
      <span aria-hidden="true" className={cx(CHIP_CELL, "invisible")}>
        {on ? "Disabled" : "Enabled"}
      </span>
    </>
  );
}

/** Whether token message cloning is on for any mapping, in the mapping popup's chip. */
export function SendersChip({ on }: { on: boolean }) {
  return (
    <span id="system-senders-chip" data-section-chip={on ? "on" : "off"} className={cx(CHIP, on ? CHIP_ON : CHIP_OFF)}>
      <ChipLabels on={on} />
    </span>
  );
}

/** The chip until the first answer: its exact box, shimmering. */
export function SendersChipBox() {
  return (
    <Skeleton.Box id="system-senders-chip" className={CHIP}>
      <ChipLabels on={false} />
    </Skeleton.Box>
  );
}

/** The status line until the first answer: a bar in its line box. */
export function SendersStatusBar() {
  return <Skeleton.Text className={COUNT} width={150} />;
}

/** How the listed accounts stand: a green dot and the online count, then the other states muted, zeros left out. */
export function SendersStatus({ counts }: { counts: ReadonlyArray<{ state: SenderState; n: number }> }) {
  return counts.map(({ state, n }, i) => (
    <span key={state} data-count={state} className={cx("inline-flex items-center gap-1.5", i > 0 && "ml-3")}>
      {state === "online" ? <span aria-hidden="true" className="inline-block size-2 flex-none rounded-full bg-good" /> : null}
      <span className={state === "online" ? "text-text" : undefined}>
        {n.toLocaleString()} {state}
      </span>
    </span>
  ));
}

/** Title left; the status line, the account count and the Enabled or Disabled chip right, as the Messages header
 *  without its subtitle. The same rows whatever loads or fails: from md up one line, the status line truncating,
 *  below md the count and chip on the line under the title and the status line under them. Each piece keeps
 *  its 20 px line box when empty. */
export function SendersHeader({ status = null, count, chip = null }: { status?: ReactNode; count: ReactNode; chip?: ReactNode }) {
  return (
    <div className="mb-3 flex items-end justify-between gap-x-4 gap-y-1 max-md:flex-col max-md:items-stretch">
      <h2 id="system-senders-title" className="m-0 shrink-0 text-[22px] font-bold text-text">
        Token Message Cloning
      </h2>
      <div className="flex min-w-0 items-center justify-end gap-x-2.5 gap-y-1 max-md:flex-wrap max-md:justify-start">
        <span id="system-senders-status" className={cx(COUNT, "h-5 min-w-0 truncate leading-5 tabular-nums max-md:order-last max-md:basis-full")}>
          {status}
        </span>
        <span id="system-senders-count" className={cx(COUNT, "h-5 shrink-0 leading-5 tabular-nums")} aria-live="polite">
          {count}
        </span>
        {chip}
      </div>
    </div>
  );
}

export interface SendersToolbarProps {
  query: string;
  onQuery: (q: string) => void;
  mapping?: string;
  mappingOptions?: readonly ToolbarOption[];
  onMapping?: (id: string) => void;
  /** The mappings are still loading: a bar stands where All mappings goes. */
  mappingsPending?: boolean;
}

const NO_MAPPINGS: readonly ToolbarOption[] = [{ value: "", label: "All mappings" }];

/** The toolbar card: the search input (Escape clears it) and the mapping dropdown, as on the Messages page. */
export function SendersToolbar({
  query,
  onQuery,
  mapping = "",
  mappingOptions = NO_MAPPINGS,
  onMapping = () => {},
  mappingsPending,
}: SendersToolbarProps) {
  return (
    <div className={TOOLBAR}>
      <div className={TOOLBAR_ROW}>
        <div className="relative flex min-w-[240px] flex-1 items-center max-md:col-span-2 max-md:min-w-0">
          <MessagesIcon name="search" size={15} className="pointer-events-none absolute left-2.5 text-muted" />
          <input
            type="search"
            id="system-senders-search"
            className={SEARCH_INPUT}
            placeholder="Search accounts"
            aria-label="Search accounts"
            autoComplete="off"
            value={query}
            onChange={(e) => onQuery(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === "Escape" && query) {
                e.preventDefault();
                onQuery("");
              }
            }}
          />
        </div>
        <ToolbarSelect
          id="system-senders-mapping"
          label="Filter by mapping"
          value={mapping}
          options={mappingOptions}
          pending={mappingsPending}
          pendingLabel="All mappings"
          className="max-md:col-span-2"
          onChange={onMapping}
        />
      </div>
    </div>
  );
}

/** The head: every header is a sort button with the arrow and aria-sort on the active one, as on Messages. */
export function SendersHead({ sort = null, onSort }: { sort?: SenderSort | null; onSort?: (by: SenderColumn) => void }) {
  return (
    <thead className="sticky top-0 z-[2]">
      <tr className={HEAD_ROW}>
        {SENDER_COLUMNS.map((c) => {
          const dir = sort?.by === c.key ? sort.dir : undefined;
          return (
            <th
              key={c.key}
              scope="col"
              className={cx(TH, "px-3", c.right ? "text-right" : "text-left")}
              style={{ width: c.width }}
              aria-sort={dir ? (dir === "asc" ? "ascending" : "descending") : undefined}
            >
              {/* [font:inherit]: the base layer reverts button fonts, the label keeps the header's size and weight */}
              <button
                type="button"
                data-sort={c.key}
                className={cx(SORT_BTN, "[font:inherit]", dir && "text-text")}
                onClick={() => onSort?.(c.key)}
              >
                {c.label}
                <span aria-hidden="true" className="ml-1 text-[11px] text-accent">
                  {dir ? (dir === "asc" ? "▲" : "▼") : ""}
                </span>
              </button>
            </th>
          );
        })}
      </tr>
    </thead>
  );
}

// Bar widths per column so loading rows vary like real ones.
const BAR: Record<Exclude<SenderColumn, "account">, number[]> = {
  username: [80, 96, 64],
  status: [60, 48, 72],
  online: [64, 56, 72],
  last: [52, 44, 60],
  sent: [36, 28, 44],
};
const NAME_BAR = [96, 120, 84];

/** Rows while the first answer loads: as many as last time (up to a wrap full), three on a first visit. */
export function SendersLoadingRows({ rows = readLastCount(SENDERS_COUNT_KEY, 3, 12) }: { rows?: number }) {
  return (
    <>
      {Array.from({ length: rows }, (_, i) => (
        <tr key={i} data-skeleton-row="" className={SENDER_ROW}>
          {SENDER_COLUMNS.map((c, j) => (
            <td key={c.key} className={c.right ? SENDER_TD_RIGHT : SENDER_TD}>
              {i === 0 && j === 0 ? <span role="status" aria-label="Loading sending accounts" className="sr-only" /> : null}
              {c.key === "account" ? (
                <span className={ACCOUNT}>
                  <Skeleton.Box className={AVATAR} />
                  <Skeleton.Text className="leading-6" width={NAME_BAR[i % NAME_BAR.length]} />
                </span>
              ) : (
                // A block bar in the cell's 24 px line box: an inline-block with overflow hidden would
                // sit on the baseline and make the row taller than a loaded one.
                <Skeleton.Text block className={cx("leading-6", c.right && "ml-auto")} width={BAR[c.key][i % BAR[c.key].length]} />
              )}
            </td>
          ))}
        </tr>
      ))}
    </>
  );
}

/** The section while the page chunk downloads: the real header, toolbar and head as inert chrome, loading rows. */
export function SendersTableSkeleton() {
  return (
    <section aria-labelledby="system-senders-title" className={SENDERS_SECTION}>
      <SendersHeader status={<SendersStatusBar />} count={<SendersCountBar />} chip={<SendersChipBox />} />
      <div inert>
        <SendersToolbar query="" onQuery={() => {}} mappingsPending />
      </div>
      <div inert className={SENDERS_WRAP}>
        <table aria-label="Sending accounts" className={SENDERS_TABLE} style={SENDERS_TABLE_STYLE}>
          <SendersHead />
          <tbody>
            <SendersLoadingRows />
          </tbody>
        </table>
      </div>
    </section>
  );
}
