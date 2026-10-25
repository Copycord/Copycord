// The sending accounts under the Database card: one row per account that posts cloned messages,
// live from /api/system/senders. Avatar and name, username, status dot, how long it has been online
// (ticking), the last send and how many messages it sent. Revoked and disabled rows are dimmed.
// A search and a column sort over the loaded rows live in state, so each 5 s poll swaps the rows
// underneath without losing them.

import { useEffect, useMemo, useState } from "react";
import { cx, useReveal } from "../../components/ui";
import { useLastCount } from "../../components/ui/lastCount";
import type { Sender } from "../../types/system";
import { formatUptime } from "../dashboard/model";
import { MUTED } from "../messages/MessagesSkeleton";
import { absoluteTime, relativeTime } from "../messages/model";
import {
  DEFAULT_AVATAR_COLORS,
  STATUS_LABEL,
  defaultAvatarIndex,
  filterSenders,
  nextSort,
  senderName,
  senderStatus,
  sortSenders,
} from "./senders";
import type { SenderSort, SenderStatus } from "./senders";
import {
  ACCOUNT,
  AVATAR,
  SENDER_ROW,
  SENDER_TD,
  SENDER_TD_RIGHT,
  SENDER_TEXT,
  SENDERS_COUNT_KEY,
  SENDERS_NOTE,
  SENDERS_SECTION,
  SENDERS_TABLE,
  SENDERS_TABLE_STYLE,
  SENDERS_WRAP,
  SendersChip,
  SendersChipBox,
  SendersCountBar,
  SendersHead,
  SendersHeader,
  SendersLoadingRows,
  SendersToolbar,
} from "./SendersSkeleton";
import { useSenders } from "./useSenders";

const DASH = "-";

// Filled dots for a live presence, a hollow ring while connecting or offline (pulsing only while
// connecting), no dot for revoked and disabled: their label says it.
const DOT = "inline-block size-2 flex-none rounded-full";
const RING = cx(DOT, "border-[1.5px] border-muted");
export const STATUS: Record<SenderStatus, { label: string; dot?: string; text?: string }> = {
  online: { label: STATUS_LABEL.online, dot: cx(DOT, "bg-good") },
  // Discord's own idle yellow; the theme has no token for it.
  idle: { label: STATUS_LABEL.idle, dot: cx(DOT, "bg-[#F0B232]") },
  busy: { label: STATUS_LABEL.busy, dot: cx(DOT, "bg-danger") },
  connecting: { label: STATUS_LABEL.connecting, dot: cx(RING, "motion-safe:animate-pulse") },
  offline: { label: STATUS_LABEL.offline, dot: RING },
  revoked: { label: STATUS_LABEL.revoked, text: "text-warn" },
  disabled: { label: STATUS_LABEL.disabled, text: "text-muted" },
};

const DIMMED = new Set<SenderStatus>(["revoked", "disabled"]);

/** The avatar the API names (a Discord default until the account has connected, then the real one) over a
 *  circle in the account's default colour, which shows while an image loads and when it fails or is missing.
 *  A new URL on a later poll swaps the image in the same 24 px box. */
function Avatar({ url, id }: { url: string | null; id: string }) {
  const [failedUrl, setFailedUrl] = useState<string | null>(null);
  const index = defaultAvatarIndex(url, id);
  const color = { backgroundColor: DEFAULT_AVATAR_COLORS[index] };
  if (!url || failedUrl === url) {
    return (
      <span
        aria-hidden="true"
        data-avatar="default"
        data-avatar-index={index}
        className={cx(AVATAR, "select-none")}
        style={color}
      />
    );
  }
  // Locked in place: the browser's image drag ghost and text selection are switched off.
  return (
    <img
      src={url}
      alt=""
      loading="lazy"
      draggable={false}
      data-avatar="img"
      data-avatar-index={index}
      className={cx(AVATAR, "pointer-events-none select-none object-cover")}
      style={color}
      onDragStart={(e) => e.preventDefault()}
      onError={() => setFailedUrl(url)}
    />
  );
}

/** Time online from `since` (unix seconds), redrawn on each whole second. */
function OnlineFor({ since }: { since: number }) {
  const [, redraw] = useState(0);
  const start = since * 1000;
  useEffect(() => {
    const into = (((Date.now() - start) % 1000) + 1000) % 1000;
    const id = setTimeout(() => redraw((n) => n + 1), 1000 - into);
    return () => clearTimeout(id);
  });
  const text = formatUptime(Math.floor(Math.max(0, Date.now() - start) / 1000));
  return (
    <span className={cx(SENDER_TEXT, "tabular-nums")} title={`Online since ${absoluteTime(since)}`}>
      {text}
    </span>
  );
}

function Text({ text, className }: { text: string; className?: string }) {
  return (
    <span className={cx(SENDER_TEXT, className)} title={text}>
      {text}
    </span>
  );
}

function SenderRow({ s, now }: { s: Sender; now: number }) {
  const status = senderStatus(s);
  const st = STATUS[status];
  return (
    <tr className={cx(SENDER_ROW, DIMMED.has(status) && "opacity-60")} data-sender={s.id} data-status={status}>
      <td className={SENDER_TD}>
        <span className={ACCOUNT}>
          <Avatar url={s.avatar_url} id={s.id} />
          <Text text={senderName(s)} className="font-medium" />
        </span>
      </td>
      <td className={SENDER_TD}>
        <Text text={s.username || DASH} className={MUTED} />
      </td>
      <td className={SENDER_TD}>
        <span className="flex h-6 min-w-0 items-center gap-2" title={st.label}>
          {st.dot ? <span aria-hidden="true" data-dot={status} className={st.dot} /> : null}
          <span className={cx("truncate", st.text)}>{st.label}</span>
        </span>
      </td>
      <td className={SENDER_TD}>
        {s.state === "online" && s.since !== null ? <OnlineFor since={s.since} /> : <Text text={DASH} className={MUTED} />}
      </td>
      <td className={SENDER_TD}>
        <span className={cx(SENDER_TEXT, MUTED)} title={s.last_used === null ? DASH : absoluteTime(s.last_used)}>
          {relativeTime(s.last_used, now)}
        </span>
      </td>
      <td className={SENDER_TD_RIGHT}>
        <Text text={s.use_count.toLocaleString()} className="tabular-nums" />
      </td>
    </tr>
  );
}

const accounts = (n: number) => `${n.toLocaleString()} ${n === 1 ? "account" : "accounts"}`;

export function SendersTable() {
  const { items, enabled, error, busy } = useSenders();
  const [query, setQuery] = useState("");
  const [sort, setSort] = useState<SenderSort | null>(null);
  const reveal = useReveal(items !== undefined || error);
  useLastCount(SENDERS_COUNT_KEY, items?.length);
  const now = Date.now();
  const searching = query.trim() !== "";
  const rows = useMemo(() => (items ? sortSenders(filterSenders(items, query), sort) : undefined), [items, query, sort]);

  const count =
    items === undefined ? (error ? null : <SendersCountBar />) : searching ? `${rows!.length.toLocaleString()} of ${accounts(items.length)}` : accounts(items.length);
  // The chip's box is held until the first answer; a first poll that fails shows no chip.
  const chip =
    enabled !== undefined ? <SendersChip on={enabled} /> : items === undefined && !error ? <SendersChipBox /> : null;
  const note =
    items === undefined
      ? error
        ? "Could not load sending accounts"
        : null
      : items.length === 0
        ? "No sending accounts yet"
        : rows!.length === 0
          ? "No matching accounts"
          : null;

  return (
    <section aria-labelledby="system-senders-title" className={SENDERS_SECTION}>
      <SendersHeader count={count} chip={chip} />
      <div>
        <SendersToolbar query={query} onQuery={setQuery} />
      </div>
      <div id="system-senders" className={SENDERS_WRAP} aria-busy={busy && items !== undefined ? true : undefined}>
        <table aria-label="Sending accounts" className={SENDERS_TABLE} style={SENDERS_TABLE_STYLE}>
          <SendersHead sort={sort} onSort={(by) => setSort((cur) => nextSort(cur, by))} />
          <tbody className={reveal}>
            {items === undefined && !error ? <SendersLoadingRows /> : null}
            {rows?.map((s) => <SenderRow key={s.id} s={s} now={now} />)}
          </tbody>
        </table>
        {note ? <p className={SENDERS_NOTE}>{note}</p> : null}
      </div>
    </section>
  );
}
