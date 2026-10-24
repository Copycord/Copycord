// The sending accounts section's shared pieces: the header, the toolbar, the wrap, the head, the cell
// classes and the loading rows. The page section and the chunk skeleton both build from these, so a
// loading row has the cells, paddings and line boxes of a loaded one. Styled like the Messages page.

import type { CSSProperties, ReactNode } from "react";
import { Skeleton, cx } from "../../components/ui";
import { readLastCount } from "../../components/ui/lastCount";
import { COUNT, HEAD_ROW, SORT_BTN, TH } from "../logs/LogsSkeleton";
import { MessagesIcon } from "../messages/icons";
import { CELL_TEXT, ROW, SEARCH_INPUT, TABLE, TD, TOOLBAR } from "../messages/MessagesSkeleton";
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

/** Title left, the account count right, as the Messages header without its subtitle. */
export function SendersHeader({ count }: { count: ReactNode }) {
  return (
    <div className="mb-3 flex flex-wrap items-end justify-between gap-x-4 gap-y-1">
      <h2 id="system-senders-title" className="m-0 text-[22px] font-bold text-text">
        Token Message Cloning
      </h2>
      <span id="system-senders-count" className={COUNT} aria-live="polite">
        {count}
      </span>
    </div>
  );
}

/** The toolbar card with the one search input; Escape clears it. */
export function SendersToolbar({ query, onQuery }: { query: string; onQuery: (q: string) => void }) {
  return (
    <div className={TOOLBAR}>
      <div className="relative flex w-full items-center">
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
      <SendersHeader count={<SendersCountBar />} />
      <div inert>
        <SendersToolbar query="" onQuery={() => {}} />
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
