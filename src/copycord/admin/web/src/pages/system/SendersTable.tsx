// The sending accounts under the Database card: one row per account that posts cloned messages,
// live from /api/system/senders. Avatar and name, username, status dot, how long it has been online
// (ticking), the last send and how many messages it sent. Revoked and disabled rows are dimmed.

import { useEffect, useState } from "react";
import { cx, useReveal } from "../../components/ui";
import { useLastCount } from "../../components/ui/lastCount";
import type { Sender } from "../../types/system";
import { formatUptime } from "../dashboard/model";
import { MUTED } from "../messages/MessagesSkeleton";
import { absoluteTime, relativeTime } from "../messages/model";
import {
  ACCOUNT,
  AVATAR,
  SENDER_COLUMNS,
  SENDER_ROW,
  SENDER_TD,
  SENDER_TD_RIGHT,
  SENDER_TEXT,
  SENDERS_COUNT_KEY,
  SENDERS_NOTE,
  SENDERS_TABLE,
  SENDERS_TABLE_STYLE,
  SENDERS_WRAP,
  SENDERS_WRAP_STYLE,
  SendersHead,
  SendersLoadingRows,
} from "./SendersSkeleton";
import { useSenders } from "./useSenders";

const DASH = "-";

export type SenderStatus = "online" | "busy" | "invisible" | "connecting" | "offline" | "revoked" | "disabled";

/** The Status column of one account: the presence while online, else the state. */
export function senderStatus(s: Pick<Sender, "state" | "presence">): SenderStatus {
  if (s.state !== "online") return s.state in STATUS ? s.state : "offline";
  if (s.presence === "dnd") return "busy";
  return s.presence === "invisible" ? "invisible" : "online";
}

// Filled dots for a live presence, a hollow ring while connecting or offline (pulsing only while
// connecting), no dot for revoked and disabled: their label says it.
const DOT = "inline-block size-2 flex-none rounded-full";
const RING = cx(DOT, "border-[1.5px] border-muted");
export const STATUS: Record<SenderStatus, { label: string; dot?: string; text?: string }> = {
  online: { label: "Online", dot: cx(DOT, "bg-good") },
  busy: { label: "Busy", dot: cx(DOT, "bg-danger") },
  invisible: { label: "Invisible", dot: cx(DOT, "bg-muted") },
  connecting: { label: "Connecting", dot: cx(RING, "motion-safe:animate-pulse") },
  offline: { label: "Offline", dot: RING },
  revoked: { label: "Revoked", text: "text-warn" },
  disabled: { label: "Disabled", text: "text-muted" },
};

const DIMMED = new Set<SenderStatus>(["revoked", "disabled"]);

function Avatar({ url }: { url: string | null }) {
  const [failed, setFailed] = useState(false);
  if (!url || failed) return <span aria-hidden="true" data-avatar="none" className={cx(AVATAR, "bg-wash-10")} />;
  return (
    <img
      src={url}
      alt=""
      loading="lazy"
      data-avatar="img"
      className={cx(AVATAR, "bg-wash-10 object-cover")}
      onError={() => setFailed(true)}
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
  const name = s.display_name || s.username || s.id;
  return (
    <tr className={cx(SENDER_ROW, DIMMED.has(status) && "opacity-60")} data-sender={s.id} data-status={status}>
      <td className={SENDER_TD}>
        <span className={ACCOUNT}>
          <Avatar key={s.avatar_url ?? ""} url={s.avatar_url} />
          <Text text={name} className="font-medium" />
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

export function SendersTable() {
  const { items, error, busy } = useSenders();
  const reveal = useReveal(items !== undefined || error);
  useLastCount(SENDERS_COUNT_KEY, items?.length);
  const now = Date.now();
  const note = items === undefined ? (error ? "Could not load sending accounts" : null) : items.length === 0 ? "No sending accounts yet" : null;

  return (
    <div
      id="system-senders"
      className={SENDERS_WRAP}
      style={SENDERS_WRAP_STYLE}
      aria-busy={busy && items !== undefined ? true : undefined}
    >
      <table aria-label="Sending accounts" className={SENDERS_TABLE} style={SENDERS_TABLE_STYLE}>
        <SendersHead />
        <tbody className={reveal}>
          {items === undefined && !error ? <SendersLoadingRows /> : null}
          {items?.map((s) => <SenderRow key={s.id} s={s} now={now} />)}
          {note ? (
            <tr>
              <td colSpan={SENDER_COLUMNS.length} className={SENDERS_NOTE}>
                {note}
              </td>
            </tr>
          ) : null}
        </tbody>
      </table>
    </div>
  );
}
