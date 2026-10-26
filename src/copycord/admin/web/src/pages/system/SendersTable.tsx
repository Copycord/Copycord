// The sending accounts under the Database card: one row per account that posts cloned messages,
// live from /api/system/senders. Avatar and name, username, status dot, how long it has been online
// (ticking), the last send and how many messages it sent. Revoked and disabled rows are dimmed.
// A mapping dropdown, a search and a column sort over the loaded rows live in state, so each 5 s poll
// swaps the rows underneath without losing them. The status line over the table counts the rows shown.

import { useEffect, useMemo, useState } from "react";
import { cx, useReveal } from "../../components/ui";
import { useLastCount } from "../../components/ui/lastCount";
import type { Sender } from "../../types/system";
import { formatUptime } from "../dashboard/model";
import { MUTED } from "../messages/MessagesSkeleton";
import { absoluteTime, relativeTime } from "../messages/model";
import {
  ALL_MAPPINGS,
  STATUS_LABEL,
  filterSenders,
  liveMapping,
  mappingOptions,
  nextSort,
  senderInMapping,
  senderName,
  senderStatus,
  sortSenders,
  stateCounts,
} from "./senders";
import type { SenderSort, SenderStatus } from "./senders";
import { SenderAvatar } from "./SenderAvatar";
import {
  ACCOUNT,
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
  SendersStatus,
  SendersStatusBar,
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
          <SenderAvatar url={s.avatar_url} id={s.id} />
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
  const { items, enabled, mappings, error, busy } = useSenders();
  const [query, setQuery] = useState("");
  const [sort, setSort] = useState<SenderSort | null>(null);
  const [picked, setPicked] = useState(ALL_MAPPINGS);
  // A mapping gone from a later poll reads as All mappings on that same render, then the choice is dropped.
  const mapping = liveMapping(mappings, picked);
  useEffect(() => {
    if (mappings !== undefined && mapping !== picked) setPicked(mapping);
  }, [mappings, mapping, picked]);
  const reveal = useReveal(items !== undefined || error);
  useLastCount(SENDERS_COUNT_KEY, items?.length);
  const now = Date.now();
  const searching = query.trim() !== "";
  const scoped = useMemo(() => items?.filter((s) => senderInMapping(s, mapping)), [items, mapping]);
  const rows = useMemo(() => (scoped ? sortSenders(filterSenders(scoped, query), sort) : undefined), [scoped, query, sort]);
  const options = useMemo(() => mappingOptions(mappings), [mappings]);

  const count =
    scoped === undefined ? (error ? null : <SendersCountBar />) : searching ? `${rows!.length.toLocaleString()} of ${accounts(scoped.length)}` : accounts(scoped.length);
  // The same rows as the table: the mapping and the search both apply.
  const status = rows === undefined ? (error ? null : <SendersStatusBar />) : <SendersStatus counts={stateCounts(rows)} />;
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
      <SendersHeader status={status} count={count} chip={chip} />
      <div>
        <SendersToolbar
          query={query}
          onQuery={setQuery}
          mapping={mapping}
          mappingOptions={options}
          onMapping={setPicked}
          mappingsPending={mappings === undefined && !error}
        />
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
