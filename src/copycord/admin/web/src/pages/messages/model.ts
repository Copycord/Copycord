// Messages page state that is not React: the columns and their saved choice, the filters and the
// list url, time text and the preview error wording.

import { ApiError } from "../../lib/api";
import type { MessageRow } from "../../types/messages";

export const PAGE_SIZE = 50;
export const SEARCH_DEBOUNCE_MS = 300;

export type ColumnKey =
  | "time"
  | "created"
  | "author_name"
  | "username"
  | "author_id"
  | "original_channel"
  | "original_channel_id"
  | "cloned_channel"
  | "cloned_channel_id"
  | "original_guild"
  | "original_guild_id"
  | "cloned_guild"
  | "cloned_guild_id"
  | "mapping"
  | "attachments"
  | "embeds"
  | "links"
  | "message_id"
  | "cloned_message_id";

export type ColumnGroup = "Message" | "Author" | "Channel" | "Guild" | "Other";

export interface ColumnDef {
  key: ColumnKey;
  label: string;
  /** Header cell width in px; the table never gets narrower than the sum, it scrolls instead. */
  width: number;
  group: ColumnGroup;
  defaultOn: boolean;
}

/** In table order. Preview is not here: it is always the last column. */
export const COLUMNS: readonly ColumnDef[] = [
  { key: "time", label: "Time", width: 96, group: "Message", defaultOn: true },
  { key: "created", label: "Created", width: 190, group: "Message", defaultOn: false },
  { key: "author_name", label: "Author name", width: 150, group: "Author", defaultOn: true },
  { key: "username", label: "Username", width: 140, group: "Author", defaultOn: true },
  { key: "author_id", label: "Author ID", width: 190, group: "Author", defaultOn: false },
  { key: "original_channel", label: "Original channel", width: 160, group: "Channel", defaultOn: true },
  { key: "original_channel_id", label: "Original channel ID", width: 190, group: "Channel", defaultOn: false },
  { key: "cloned_channel", label: "Cloned channel", width: 160, group: "Channel", defaultOn: false },
  { key: "cloned_channel_id", label: "Cloned channel ID", width: 190, group: "Channel", defaultOn: false },
  { key: "original_guild", label: "Original guild", width: 160, group: "Guild", defaultOn: false },
  { key: "original_guild_id", label: "Original guild ID", width: 190, group: "Guild", defaultOn: false },
  { key: "cloned_guild", label: "Cloned guild", width: 160, group: "Guild", defaultOn: false },
  { key: "cloned_guild_id", label: "Cloned guild ID", width: 190, group: "Guild", defaultOn: false },
  { key: "mapping", label: "Mapping", width: 130, group: "Other", defaultOn: true },
  { key: "attachments", label: "Attachments", width: 116, group: "Other", defaultOn: true },
  { key: "embeds", label: "Embeds", width: 88, group: "Other", defaultOn: false },
  { key: "links", label: "Links", width: 150, group: "Other", defaultOn: true },
  { key: "message_id", label: "Message ID", width: 200, group: "Message", defaultOn: true },
  { key: "cloned_message_id", label: "Cloned message ID", width: 200, group: "Message", defaultOn: false },
];

export const COLUMN_GROUPS: readonly ColumnGroup[] = ["Message", "Author", "Channel", "Guild", "Other"];

export const PREVIEW_WIDTH = 96;

/** Min width of the table for these columns: the sum of their widths plus the Preview column. */
export function tableMinWidth(cols: readonly ColumnKey[]): number {
  const on = new Set(cols);
  return COLUMNS.reduce((sum, c) => (on.has(c.key) ? sum + c.width : sum), PREVIEW_WIDTH);
}

/** Versioned: a new column set bumps the key so an old saved list cannot hide a new default. */
export const COLUMNS_KEY = "cc.messages.columns.v2";
export const COLUMNS_KEY_V1 = "cc.messages.columns.v1";

/** v1 column keys and what each became in v2; a v1 key missing here was dropped. */
const V1_TO_V2: Record<string, readonly ColumnKey[]> = {
  time: ["time"],
  author: ["author_name", "username"],
  channel: ["original_channel"],
  mapping: ["mapping"],
  counts: ["attachments", "embeds"],
  links: ["links"],
  message_id: ["message_id"],
  author_id: ["author_id"],
  original_channel_id: ["original_channel_id"],
  cloned_message_id: ["cloned_message_id"],
  cloned_channel_id: ["cloned_channel_id"],
  guild_ids: ["original_guild_id", "cloned_guild_id"],
};

export const DEFAULT_COLUMNS: readonly ColumnKey[] = COLUMNS.filter((c) => c.defaultOn).map((c) => c.key);

function inTableOrder(keys: Iterable<string>): ColumnKey[] {
  const keep = new Set(keys);
  return COLUMNS.filter((c) => keep.has(c.key)).map((c) => c.key);
}

function readList(storage: Storage | null, key: string): string[] | null {
  const raw = storage?.getItem(key);
  if (!raw) return null;
  const parsed: unknown = JSON.parse(raw);
  return Array.isArray(parsed) ? parsed.filter((k): k is string => typeof k === "string") : [];
}

/** A v1 saved choice in v2 keys: each v1 column brings its v2 columns, new columns stay off. */
export function migrateV1(v1: readonly string[]): ColumnKey[] {
  return inTableOrder(v1.flatMap((k) => V1_TO_V2[k] ?? []));
}

/** The saved column choice, or the defaults when nothing usable is stored. Always in table order.
 * Without a v2 choice a v1 one is migrated, saved as v2 and the v1 entry removed. */
export function loadColumns(storage: Storage | null = safeStorage()): ColumnKey[] {
  try {
    const saved = readList(storage, COLUMNS_KEY);
    if (saved) {
      const cols = inTableOrder(saved);
      return cols.length ? cols : [...DEFAULT_COLUMNS];
    }
    const v1 = readList(storage, COLUMNS_KEY_V1);
    if (!v1) return [...DEFAULT_COLUMNS];
    const cols = migrateV1(v1);
    if (!cols.length) return [...DEFAULT_COLUMNS];
    storage?.setItem(COLUMNS_KEY, JSON.stringify(cols));
    storage?.removeItem(COLUMNS_KEY_V1);
    return cols;
  } catch {
    return [...DEFAULT_COLUMNS];
  }
}

export function saveColumns(cols: readonly ColumnKey[], storage: Storage | null = safeStorage()): void {
  try {
    storage?.setItem(COLUMNS_KEY, JSON.stringify(cols));
  } catch {
    // Private mode or a full quota: the choice lasts for this visit only.
  }
}

/** Turns one column on or off; the last visible column stays on so the table never goes blank. */
export function toggleColumn(cols: readonly ColumnKey[], key: ColumnKey): ColumnKey[] {
  const on = new Set(cols);
  if (on.has(key)) {
    if (on.size === 1) return [...cols];
    on.delete(key);
  } else on.add(key);
  return inTableOrder(on);
}

function safeStorage(): Storage | null {
  try {
    return typeof localStorage === "undefined" ? null : localStorage;
  } catch {
    return null;
  }
}

export type SortKey = "newest" | "oldest" | "author" | "username" | "channel" | "guild";

export const SORT_OPTIONS: ReadonlyArray<{ value: SortKey; label: string }> = [
  { value: "newest", label: "Newest" },
  { value: "oldest", label: "Oldest" },
  { value: "author", label: "Author name" },
  { value: "username", label: "Username" },
  { value: "channel", label: "Channel" },
  { value: "guild", label: "Original guild" },
];

const SORT_PARAMS: Record<SortKey, { sort: string; order: string }> = {
  newest: { sort: "created_at", order: "desc" },
  oldest: { sort: "created_at", order: "asc" },
  author: { sort: "author", order: "asc" },
  username: { sort: "username", order: "asc" },
  channel: { sort: "channel", order: "asc" },
  guild: { sort: "guild", order: "asc" },
};

export interface Filters {
  q: string;
  mapping: string;
  channel: string;
  /** "", "yes" or "no". */
  attachments: string;
  /** yyyy-mm-dd from a date input, local time. */
  from: string;
  to: string;
}

export const NO_FILTERS: Filters = { q: "", mapping: "", channel: "", attachments: "", from: "", to: "" };

export function filtersActive(f: Filters): boolean {
  return Boolean(f.q || f.mapping || f.channel || f.attachments || f.from || f.to);
}

/** Unix seconds of local midnight (start) or the last second of that local day (end). */
export function dayBound(day: string, end: boolean): number | null {
  const m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(day);
  if (!m) return null;
  const d = new Date(Number(m[1]), Number(m[2]) - 1, Number(m[3]), end ? 23 : 0, end ? 59 : 0, end ? 59 : 0);
  return Math.floor(d.getTime() / 1000);
}

export function messagesUrl(f: Filters, sort: SortKey, cursor: string | null = null, limit = PAGE_SIZE): string {
  const p = new URLSearchParams();
  const q = f.q.trim();
  if (q) p.set("q", q);
  if (f.mapping) p.set("mapping_id", f.mapping);
  if (f.channel) p.set("channel_id", f.channel);
  if (f.attachments) p.set("has_attachments", f.attachments === "yes" ? "true" : "false");
  const from = dayBound(f.from, false);
  const to = dayBound(f.to, true);
  if (from !== null) p.set("created_from", String(from));
  if (to !== null) p.set("created_to", String(to));
  const s = SORT_PARAMS[sort];
  p.set("sort", s.sort);
  p.set("order", s.order);
  p.set("limit", String(limit));
  if (cursor) p.set("cursor", cursor);
  return `/api/messages?${p.toString()}`;
}

export function contentUrl(row: MessageRow): string {
  const id = encodeURIComponent(row.original_message_id);
  return row.cloned_guild_id
    ? `/api/messages/${id}/content?cloned_guild_id=${encodeURIComponent(row.cloned_guild_id)}`
    : `/api/messages/${id}/content`;
}

/** A row is one (original message, cloned guild) pair. */
export function rowKey(r: MessageRow): string {
  return `${r.original_message_id}:${r.cloned_guild_id ?? ""}`;
}

/** "just now", "5m ago", "3h ago", "4d ago", then the date. */
export function relativeTime(sec: number | null, now: number = Date.now()): string {
  if (sec === null || sec === undefined) return "-";
  const diff = Math.max(0, Math.round(now / 1000 - sec));
  if (diff < 45) return "just now";
  if (diff < 3600) return `${Math.max(1, Math.round(diff / 60))}m ago`;
  if (diff < 86400) return `${Math.round(diff / 3600)}h ago`;
  if (diff < 86400 * 30) return `${Math.round(diff / 86400)}d ago`;
  return new Date(sec * 1000).toLocaleDateString(undefined, { year: "numeric", month: "short", day: "numeric" });
}

export function absoluteTime(sec: number | null): string {
  if (sec === null || sec === undefined) return "Unknown time";
  return new Date(sec * 1000).toLocaleString(undefined, {
    year: "numeric",
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
}

export function channelLabel(name: string | null, id: string | null): string {
  if (name) return `#${name}`;
  return id ?? "-";
}

/** The display name, or the username when Discord had no separate display name. */
export function authorDisplayName(r: Pick<MessageRow, "author_name" | "author_username">): string | null {
  return r.author_name || r.author_username || null;
}

export interface PreviewError {
  title: string;
  hint: string;
  /** Seconds to wait before a retry (429). */
  retryAfter?: number;
}

/** What the preview says for a failed live fetch. */
export function previewError(e: unknown): PreviewError {
  if (e instanceof ApiError) {
    const body = (e.body ?? {}) as Record<string, unknown>;
    switch (e.status) {
      case 503:
        return { title: "The server bot is not running", hint: "Start the server bot, then try again." };
      case 404:
        return body.detail === "message-not-recorded"
          ? { title: "This message is no longer recorded", hint: "It was removed from the database." }
          : { title: "This message was deleted on Discord", hint: "Neither the clone nor the original exists anymore." };
      case 403:
        return { title: "The bot cannot read this channel", hint: "Give the server bot access to the channel to preview it." };
      case 429: {
        const n = Number(body.retry_after);
        const retryAfter = Number.isFinite(n) && n > 0 ? n : 1;
        return {
          title: "Discord is rate limiting the bot",
          hint: `Try again in ${Math.ceil(retryAfter)} s.`,
          retryAfter,
        };
      }
      default:
        return { title: "Could not load the message", hint: `Discord or the server answered with status ${e.status}.` };
    }
  }
  return { title: "Could not load the message", hint: "Network error. Check the connection and try again." };
}
