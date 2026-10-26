// Pure logic of the sending accounts table: the status of a row, the mapping filter, the status
// counts, the client side search and column sort over the rows already loaded, and the images an
// account's avatar tries (its own, then the Discord default).

import type { Sender, SenderMapping, SenderState } from "../../types/system";

export type SenderStatus = "online" | "idle" | "busy" | "connecting" | "offline" | "revoked" | "disabled";

export const STATUS_LABEL: Record<SenderStatus, string> = {
  online: "Online",
  idle: "Idle",
  busy: "Busy",
  connecting: "Connecting",
  offline: "Offline",
  revoked: "Revoked",
  disabled: "Disabled",
};

/** The Status column of one account: the presence while online (Idle, Busy, else Online), else the state. */
export function senderStatus(s: Pick<Sender, "state" | "presence">): SenderStatus {
  if (s.state !== "online") return s.state in STATUS_LABEL ? s.state : "offline";
  if (s.presence === "idle") return "idle";
  if (s.presence === "dnd") return "busy";
  return "online";
}

/** The name the Account column shows: display name, then username, then id. */
export function senderName(s: Pick<Sender, "id" | "username" | "display_name">): string {
  return s.display_name || s.username || s.id;
}

/** The mapping dropdown's value for every account. */
export const ALL_MAPPINGS = "";

/** The dropdown's options: All mappings, then each mapping as the API orders them, a paused one marked. */
export function mappingOptions(mappings: readonly SenderMapping[] | undefined): Array<{ value: string; label: string }> {
  return [
    { value: ALL_MAPPINGS, label: "All mappings" },
    ...(mappings ?? []).map((m) => ({ value: m.id, label: m.status === "paused" ? `${m.name} (paused)` : m.name })),
  ];
}

/** The chosen mapping while the API still lists it, else All mappings. */
export function liveMapping(mappings: readonly SenderMapping[] | undefined, id: string): string {
  return id !== ALL_MAPPINGS && mappings?.some((m) => m.id === id) ? id : ALL_MAPPINGS;
}

/** The accounts of one mapping (a shared token is in each of its mappings); All mappings keeps every one. */
export function senderInMapping(s: Pick<Sender, "mapping_ids">, id: string): boolean {
  return id === ALL_MAPPINGS || (s.mapping_ids ?? []).includes(id);
}

/** The status line's order; an online account counts as online whatever its presence. */
export const STATE_ORDER: readonly SenderState[] = ["online", "connecting", "offline", "disabled", "revoked"];

/** How many accounts are in each state, the ones that have none left out, in STATE_ORDER. */
export function stateCounts(items: readonly Pick<Sender, "state">[]): Array<{ state: SenderState; n: number }> {
  const n = new Map<SenderState, number>();
  for (const s of items) n.set(s.state, (n.get(s.state) ?? 0) + 1);
  return STATE_ORDER.flatMap((state) => (n.get(state) ? [{ state, n: n.get(state)! }] : []));
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
export const STATUS_ORDER: readonly SenderStatus[] = ["online", "idle", "busy", "connecting", "offline", "disabled", "revoked"];

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

/** How many default avatars Discord has. */
const DEFAULT_AVATARS = 6;

const EMBED_AVATAR = /\/embed\/avatars\/(\d+)\.png/;

/** The default avatar index: from a /embed/avatars/<n>.png URL, else from the Discord user id of an
 *  /avatars/<user id>/ URL as Discord picks it, else a stable hash of the row id. */
export function defaultAvatarIndex(url: string | null, id: string): number {
  const embed = url ? EMBED_AVATAR.exec(url) : null;
  if (embed) return Number(embed[1]) % DEFAULT_AVATARS;
  const user = url ? /\/avatars\/(\d+)\//.exec(url) : null;
  if (user) return Number((BigInt(user[1]!) >> 22n) % BigInt(DEFAULT_AVATARS));
  let h = 0;
  for (let i = 0; i < id.length; i += 1) h = (h * 31 + id.charCodeAt(i)) >>> 0;
  return h % DEFAULT_AVATARS;
}

/** The images an avatar tries in order: the URL the API names, then the Discord default avatar image
 *  (only the URL itself when it already is a default). */
export function avatarSources(url: string | null, id: string): string[] {
  const fallback = `https://cdn.discordapp.com/embed/avatars/${defaultAvatarIndex(url, id)}.png`;
  if (!url || EMBED_AVATAR.test(url)) return [url || fallback];
  return [url, fallback];
}
