// Messages page state that is not React: the columns and their saved choice, the filters and the
// list url, time text and the preview error wording.

import { ApiError } from "../../lib/api";
import type { MessageRow } from "../../types/messages";

export const PAGE_SIZE = 50;
export const SEARCH_DEBOUNCE_MS = 300;

export type ColumnKey =
  | "time"
  | "author"
  | "channel"
  | "mapping"
  | "counts"
  | "links"
  | "message_id"
  | "author_id"
  | "original_channel_id"
  | "cloned_message_id"
  | "cloned_channel_id"
  | "guild_ids";

export interface ColumnDef {
  key: ColumnKey;
  label: string;
  /** Width class of the header cell (table-fixed). */
  width: string;
  defaultOn: boolean;
}

export const COLUMNS: readonly ColumnDef[] = [
  { key: "time", label: "Time", width: "w-[104px]", defaultOn: true },
  { key: "author", label: "Author", width: "w-[200px]", defaultOn: true },
  { key: "channel", label: "Channel", width: "w-[160px]", defaultOn: true },
  { key: "mapping", label: "Mapping", width: "w-[130px]", defaultOn: true },
  { key: "counts", label: "Files", width: "w-[84px]", defaultOn: true },
  { key: "links", label: "Links", width: "w-[84px]", defaultOn: true },
  { key: "message_id", label: "Message ID", width: "w-[210px]", defaultOn: true },
  { key: "author_id", label: "Author ID", width: "w-[200px]", defaultOn: false },
  { key: "original_channel_id", label: "Original channel ID", width: "w-[200px]", defaultOn: false },
  { key: "cloned_message_id", label: "Cloned message ID", width: "w-[210px]", defaultOn: false },
  { key: "cloned_channel_id", label: "Cloned channel ID", width: "w-[200px]", defaultOn: false },
  { key: "guild_ids", label: "Guild IDs", width: "w-[200px]", defaultOn: false },
];

/** Versioned: a new column set bumps the key so an old saved list cannot hide a new default. */
export const COLUMNS_KEY = "cc.messages.columns.v1";

export const DEFAULT_COLUMNS: readonly ColumnKey[] = COLUMNS.filter((c) => c.defaultOn).map((c) => c.key);

/** The saved column choice, or the defaults when nothing usable is stored. Always in table order. */
export function loadColumns(storage: Storage | null = safeStorage()): ColumnKey[] {
  try {
    const raw = storage?.getItem(COLUMNS_KEY);
    if (!raw) return [...DEFAULT_COLUMNS];
    const parsed: unknown = JSON.parse(raw);
    if (!Array.isArray(parsed)) return [...DEFAULT_COLUMNS];
    const keep = new Set(parsed.filter((k): k is string => typeof k === "string"));
    const cols = COLUMNS.filter((c) => keep.has(c.key)).map((c) => c.key);
    return cols.length ? cols : [...DEFAULT_COLUMNS];
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
  return COLUMNS.filter((c) => on.has(c.key)).map((c) => c.key);
}

function safeStorage(): Storage | null {
  try {
    return typeof localStorage === "undefined" ? null : localStorage;
  } catch {
    return null;
  }
}

export type SortKey = "newest" | "oldest" | "author" | "channel";

export const SORT_OPTIONS: ReadonlyArray<{ value: SortKey; label: string }> = [
  { value: "newest", label: "Newest" },
  { value: "oldest", label: "Oldest" },
  { value: "author", label: "Author" },
  { value: "channel", label: "Channel" },
];

const SORT_PARAMS: Record<SortKey, { sort: string; order: string }> = {
  newest: { sort: "created_at", order: "desc" },
  oldest: { sort: "created_at", order: "asc" },
  author: { sort: "author", order: "asc" },
  channel: { sort: "channel", order: "asc" },
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
