// The sending accounts table's shared pieces: the wrap, the head, the cell classes and the loading
// rows. The page table and the chunk skeleton both build from these, so a loading row has the
// cells, paddings and line boxes of a loaded one. Styled like the Messages table.

import type { CSSProperties } from "react";
import { Skeleton, cx } from "../../components/ui";
import { readLastCount } from "../../components/ui/lastCount";
import { HEAD_ROW, TH } from "../logs/LogsSkeleton";
import { CELL_TEXT, ROW, TABLE, TABLE_WRAP, TD } from "../messages/MessagesSkeleton";

export type SenderColumn = "account" | "username" | "status" | "online" | "last" | "sent";

export const SENDER_COLUMNS: ReadonlyArray<{ key: SenderColumn; label: string; width: string; right?: boolean }> = [
  { key: "account", label: "Account", width: "26%" },
  { key: "username", label: "Username", width: "18%" },
  { key: "status", label: "Status", width: "15%" },
  { key: "online", label: "Online for", width: "14%" },
  { key: "last", label: "Last send", width: "13%" },
  { key: "sent", label: "Messages sent", width: "14%", right: true },
];

export const SENDERS_WRAP = cx(TABLE_WRAP, "mt-[18px]");
// The Messages wrap fills the screen; this one is as tall as its rows and scrolls under the head past that.
export const SENDERS_WRAP_STYLE: CSSProperties = { height: "auto", minHeight: 0, maxHeight: "min(70vh, 640px)" };
export const SENDERS_TABLE = TABLE;
export const SENDERS_TABLE_STYLE: CSSProperties = { minWidth: 760 };
export const SENDER_ROW = ROW;
export const SENDER_TD = cx(TD, "leading-6");
export const SENDER_TD_RIGHT = cx(SENDER_TD, "text-right");
export const SENDER_TEXT = cx(CELL_TEXT, "leading-6");
export const AVATAR = "size-6 flex-none rounded-full";
export const ACCOUNT = "flex h-6 min-w-0 items-center gap-2.5";
/** The empty or failed line keeps the wrap about three rows tall. */
export const SENDERS_NOTE = "h-[123px] px-3 text-center text-[13px] text-muted";

export const SENDERS_COUNT_KEY = "system.senders";

export function SendersHead() {
  return (
    <thead className="sticky top-0 z-[2]">
      <tr className={HEAD_ROW}>
        {SENDER_COLUMNS.map((c) => (
          <th key={c.key} scope="col" className={cx(TH, "px-3", c.right ? "text-right" : "text-left")} style={{ width: c.width }}>
            {c.label}
          </th>
        ))}
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

/** Rows while the first answer loads: as many as last time, three on a first visit. */
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

/** The table while the page chunk downloads: the real head, loading rows. */
export function SendersTableSkeleton() {
  return (
    <div inert className={SENDERS_WRAP} style={SENDERS_WRAP_STYLE}>
      <table aria-label="Sending accounts" className={SENDERS_TABLE} style={SENDERS_TABLE_STYLE}>
        <SendersHead />
        <tbody>
          <SendersLoadingRows />
        </tbody>
      </table>
    </div>
  );
}
