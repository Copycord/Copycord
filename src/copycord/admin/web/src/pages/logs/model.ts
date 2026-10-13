// Pure helpers for the Event logs page, ported from frontend/src/js/logs.js.

import type { EventLog, EventLogBulkDeleteRequest } from "../../types/logs";

export const PAGE_SIZE = 50;

export type TypeTone =
  | "created"
  | "deleted"
  | "renamed"
  | "moved"
  | "converted"
  | "thread"
  | "role"
  | "emoji"
  | "sticker"
  | "guild"
  | "permissions"
  | "metadata"
  | "webhook"
  | "error"
  | "default";

export interface TypeMeta {
  label: string;
  tone: TypeTone;
}

const TYPE_META: Record<string, TypeMeta> = {
  channel_created: { label: "Channel Created", tone: "created" },
  channel_deleted: { label: "Channel Deleted", tone: "deleted" },
  channel_renamed: { label: "Channel Renamed", tone: "renamed" },
  channel_moved: { label: "Channel Moved", tone: "moved" },
  channel_converted: { label: "Channel Converted", tone: "converted" },
  category_created: { label: "Category Created", tone: "created" },
  category_deleted: { label: "Category Deleted", tone: "deleted" },
  category_renamed: { label: "Category Renamed", tone: "renamed" },
  thread_created: { label: "Thread Created", tone: "thread" },
  thread_deleted: { label: "Thread Deleted", tone: "deleted" },
  thread_renamed: { label: "Thread Renamed", tone: "renamed" },
  forum_created: { label: "Forum Created", tone: "created" },
  forum_renamed: { label: "Forum Renamed", tone: "renamed" },
  forum_moved: { label: "Forum Moved", tone: "moved" },
  role_created: { label: "Role Created", tone: "role" },
  role_deleted: { label: "Role Deleted", tone: "deleted" },
  role_updated: { label: "Role Updated", tone: "role" },
  emoji_created: { label: "Emoji Created", tone: "emoji" },
  emoji_deleted: { label: "Emoji Deleted", tone: "deleted" },
  emoji_renamed: { label: "Emoji Renamed", tone: "emoji" },
  emoji_synced: { label: "Emoji Synced", tone: "emoji" },
  sticker_created: { label: "Sticker Created", tone: "sticker" },
  sticker_deleted: { label: "Sticker Deleted", tone: "deleted" },
  sticker_renamed: { label: "Sticker Renamed", tone: "sticker" },
  sticker_synced: { label: "Sticker Synced", tone: "sticker" },
  guild_metadata: { label: "Guild Metadata", tone: "guild" },
  channel_metadata_updated: { label: "Channel Metadata", tone: "metadata" },
  voice_metadata_updated: { label: "Voice Metadata", tone: "metadata" },
  stage_metadata_updated: { label: "Stage Metadata", tone: "metadata" },
  forum_metadata_updated: { label: "Forum Metadata", tone: "metadata" },
  permissions_synced: { label: "Permissions Synced", tone: "permissions" },
  webhook_created: { label: "Webhook Created", tone: "webhook" },
  error: { label: "Error", tone: "error" },
};

/** "some_snake_key" -> "Some Snake Key". */
export function titleCase(key: string): string {
  return key.replace(/_/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());
}

export function typeMeta(type: string): TypeMeta {
  return TYPE_META[type] ?? { label: titleCase(type), tone: "default" };
}

/** Dropdown options sorted by label, as the old filter did. */
export function typeOptions(types: readonly string[]): Array<{ value: string; label: string }> {
  return [...new Set(types)]
    .map((t) => ({ value: t, label: typeMeta(t).label }))
    .sort((a, b) => a.label.toLowerCase().localeCompare(b.label.toLowerCase()));
}

/** Unix seconds -> "YYYY-MM-DD HH:MM:SS" in local time, or a dash. */
export function fmtTimestamp(epoch: number | null | undefined): string {
  if (!epoch) return "—";
  const d = new Date(epoch * 1000);
  if (Number.isNaN(d.getTime())) return "—";
  const pad = (n: number) => String(n).padStart(2, "0");
  return (
    `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())} ` +
    `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`
  );
}

export function guildText(log: EventLog): string {
  if (log.guild_name) return log.guild_name;
  return log.guild_id ? String(log.guild_id) : "SYSTEM";
}

export function parseExtra(log: EventLog): Record<string, unknown> | null {
  const raw = log.extra_json;
  if (!raw) return null;
  if (typeof raw !== "string") return raw;
  try {
    const v: unknown = JSON.parse(raw);
    return v && typeof v === "object" && !Array.isArray(v) ? (v as Record<string, unknown>) : null;
  } catch {
    return null;
  }
}

const EXTRA_LABELS: Record<string, string> = {
  sync_task_id: "Sync Task",
  original_channel_id: "Source Channel ID",
  clone_channel_id: "Clone Channel ID",
  original_category_id: "Source Category ID",
  clone_category_id: "Clone Category ID",
  original_role_id: "Source Role ID",
  clone_role_id: "Clone Role ID",
  original_emoji_id: "Source Emoji ID",
  clone_emoji_id: "Clone Emoji ID",
  original_sticker_id: "Source Sticker ID",
  clone_sticker_id: "Clone Sticker ID",
  original_thread_id: "Source Thread ID",
  clone_thread_id: "Clone Thread ID",
  changed_fields: "Changed Fields",
  changes: "Changes",
};

const ORDERED_EXTRA = [
  "original_channel_id",
  "clone_channel_id",
  "original_category_id",
  "clone_category_id",
  "original_role_id",
  "clone_role_id",
  "original_emoji_id",
  "clone_emoji_id",
  "original_sticker_id",
  "clone_sticker_id",
  "original_thread_id",
  "clone_thread_id",
  "changed_fields",
  "changes",
];

function show(v: unknown): string {
  if (Array.isArray(v)) return v.map((x) => show(x)).join(", ");
  if (v !== null && typeof v === "object") return JSON.stringify(v);
  return String(v);
}

/** Label/value pairs for the expandable detail row, in the old page's order. */
export function metadataRows(log: EventLog): Array<[string, string]> {
  const extra = parseExtra(log);
  const rows: Array<[string, string]> = [];
  const handled = new Set<string>();

  if (extra?.sync_task_id) {
    rows.push(["Sync Task", show(extra.sync_task_id)]);
    handled.add("sync_task_id");
  }
  if (log.guild_id) rows.push(["Guild ID", String(log.guild_id)]);
  if (log.channel_name) rows.push(["Channel", log.channel_name]);
  if (log.category_name) rows.push(["Category", log.category_name]);
  if (log.category_id) rows.push(["Category ID", String(log.category_id)]);

  if (extra) {
    for (const k of ORDERED_EXTRA) {
      if (extra[k] != null) {
        rows.push([EXTRA_LABELS[k] ?? k, show(extra[k])]);
        handled.add(k);
      }
    }
    for (const [k, v] of Object.entries(extra)) {
      if (handled.has(k)) continue;
      rows.push([EXTRA_LABELS[k] ?? titleCase(k), show(v)]);
    }
  }
  return rows;
}

export function hasMetadata(log: EventLog): boolean {
  return Boolean(
    log.channel_id || log.channel_name || log.category_id || log.category_name || log.guild_id || parseExtra(log),
  );
}

export interface LogsQuery {
  page: number;
  type: string;
  search: string;
}

/** The query string for GET /api/event-logs. */
export function buildLogsQuery({ page, type, search }: LogsQuery): string {
  const params = new URLSearchParams();
  params.set("limit", String(PAGE_SIZE));
  params.set("offset", String((Math.max(1, page) - 1) * PAGE_SIZE));
  if (type) params.set("event_type", type);
  const q = search.trim();
  if (q) params.set("search", q);
  return params.toString();
}

export function logsUrl(q: LogsQuery): string {
  return `/api/event-logs?${buildLogsQuery(q)}`;
}

export function totalPages(total: number): number {
  return Math.max(1, Math.ceil(total / PAGE_SIZE));
}

/** Page buttons: first, last, and two either side of the current page; null marks a gap. */
export function pageRange(current: number, pages: number): Array<number | null> {
  if (pages <= 1) return [];
  const set = new Set<number>([1, pages]);
  for (let i = Math.max(2, current - 2); i <= Math.min(pages - 1, current + 2); i++) set.add(i);
  const sorted = [...set].sort((a, b) => a - b);
  const out: Array<number | null> = [];
  let last = 0;
  for (const p of sorted) {
    if (last && p - last > 1) out.push(null);
    out.push(p);
    last = p;
  }
  return out;
}

/** "Showing 1-50 of 120", or the old wording when the page came back empty. */
export function countText(page: number, shown: number, total: number): string {
  if (shown === 0) return total > 0 ? `No results for current filters (${total} total)` : "";
  const start = (page - 1) * PAGE_SIZE + 1;
  return `Showing ${start}-${start + shown - 1} of ${total}`;
}

export type SortColumn = "created_at" | "event_type" | "guild_name" | "details";
export type SortDir = "asc" | "desc";
export interface SortState {
  column: SortColumn;
  dir: SortDir;
}

export const DEFAULT_SORT: SortState = { column: "created_at", dir: "desc" };

/** Header click: same column flips, a new column starts desc for time and asc otherwise. */
export function nextSort(prev: SortState, column: SortColumn): SortState {
  if (prev.column === column) return { column, dir: prev.dir === "asc" ? "desc" : "asc" };
  return { column, dir: column === "created_at" ? "desc" : "asc" };
}

function sortKey(log: EventLog, column: SortColumn): string | number {
  switch (column) {
    case "created_at":
      return log.created_at ?? 0;
    case "event_type":
      return typeMeta(log.event_type).label.toLowerCase();
    case "guild_name":
      return guildText(log).toLowerCase();
    case "details":
      return (log.details ?? "").toLowerCase();
  }
}

/** Sorts the rows of the current page; the server always pages newest first. */
export function sortLogs(logs: readonly EventLog[], sort: SortState): EventLog[] {
  const sign = sort.dir === "asc" ? 1 : -1;
  return [...logs].sort((a, b) => {
    const ka = sortKey(a, sort.column);
    const kb = sortKey(b, sort.column);
    const c = typeof ka === "number" && typeof kb === "number" ? ka - kb : String(ka).localeCompare(String(kb));
    return c * sign;
  });
}

/** Body for POST /api/event-logs/delete-bulk, limited to rows still on screen. */
export function bulkDeletePayload(selected: ReadonlySet<string>, logs: readonly EventLog[]): EventLogBulkDeleteRequest {
  return { ids: logs.filter((l) => selected.has(l.log_id)).map((l) => l.log_id) };
}
