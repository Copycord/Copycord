// Pure logic of the sending accounts table: the status of a row, the client side search and column
// sort over the rows already loaded, and the Discord default avatar colour of an account.

import type { Sender } from "../../types/system";

export type SenderStatus = "online" | "busy" | "invisible" | "connecting" | "offline" | "revoked" | "disabled";

export const STATUS_LABEL: Record<SenderStatus, string> = {
  online: "Online",
  busy: "Busy",
  invisible: "Invisible",
  connecting: "Connecting",
  offline: "Offline",
  revoked: "Revoked",
  disabled: "Disabled",
};

/** The Status column of one account: the presence while online, else the state. */
export function senderStatus(s: Pick<Sender, "state" | "presence">): SenderStatus {
  if (s.state !== "online") return s.state in STATUS_LABEL ? s.state : "offline";
  if (s.presence === "dnd") return "busy";
  return s.presence === "invisible" ? "invisible" : "online";
}

/** The name the Account column shows: display name, then username, then id. */
export function senderName(s: Pick<Sender, "id" | "username" | "display_name">): string {
  return s.display_name || s.username || s.id;
}

/** Rows whose name, username or status label holds the query, case-insensitive. A blank query keeps all. */
export function filterSenders(items: readonly Sender[], query: string): Sender[] {
  const q = query.trim().toLowerCase();
  if (!q) return [...items];
  return items.filter((s) =>
    [senderName(s), s.username ?? "", STATUS_LABEL[senderStatus(s)]].some((v) => v.toLowerCase().includes(q)),
  );
}

export type SenderSortKey = "account" | "username" | "status" | "online" | "last" | "sent";
export interface SenderSort {
  by: SenderSortKey;
  dir: "asc" | "desc";
}

/** Status sort order, ascending. */
export const STATUS_ORDER: readonly SenderStatus[] = ["online", "busy", "invisible", "connecting", "offline", "disabled", "revoked"];

/** A header click: a new column sorts ascending, then descending, then back to the API order (null). */
export function nextSort(current: SenderSort | null, by: SenderSortKey): SenderSort | null {
  if (!current || current.by !== by) return { by, dir: "asc" };
  return current.dir === "asc" ? { by, dir: "desc" } : null;
}

const text = new Intl.Collator(undefined, { sensitivity: "base", numeric: true });

// The value each column sorts by; null sorts last in both directions.
function sortValue(s: Sender, by: SenderSortKey): string | number | null {
  switch (by) {
    case "account":
      return senderName(s);
    case "username":
      return s.username || null;
    case "status":
      return STATUS_ORDER.indexOf(senderStatus(s));
    case "online":
      // Online for is a duration: a later start is a shorter time online.
      return s.state === "online" && s.since !== null ? -s.since : null;
    case "last":
      return s.last_used;
    case "sent":
      return s.use_count;
  }
}

/** The rows in the column order; null keeps the API order (online first, then username). Ties keep the API order. */
export function sortSenders(items: readonly Sender[], sort: SenderSort | null): Sender[] {
  if (!sort) return [...items];
  const sign = sort.dir === "asc" ? 1 : -1;
  return items
    .map((s, i) => ({ s, i, v: sortValue(s, sort.by) }))
    .sort((a, b) => {
      if (a.v === null || b.v === null) return a.v === b.v ? a.i - b.i : a.v === null ? 1 : -1;
      const d = typeof a.v === "string" ? text.compare(a.v, String(b.v)) : a.v - (b.v as number);
      return d ? d * sign : a.i - b.i;
    })
    .map((x) => x.s);
}

/** Discord's six default avatar colours, by avatar index. */
export const DEFAULT_AVATAR_COLORS = ["#5865F2", "#747F8D", "#3BA55D", "#FAA61A", "#ED4245", "#EB459E"] as const;

/** The default avatar index: from a /embed/avatars/<n>.png URL, else a stable hash of the account id. */
export function defaultAvatarIndex(url: string | null, id: string): number {
  const m = url ? /\/embed\/avatars\/(\d+)\.png/.exec(url) : null;
  if (m) return Number(m[1]) % DEFAULT_AVATAR_COLORS.length;
  let h = 0;
  for (let i = 0; i < id.length; i += 1) h = (h * 31 + id.charCodeAt(i)) >>> 0;
  return h % DEFAULT_AVATAR_COLORS.length;
}
