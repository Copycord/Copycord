// Pure logic of the Guilds page: search and sort of the grid, the export payload and the menu position.

import type { ExportMessagesRequest, GuildSummary, MediaTypes } from "../../types/guilds";

export type SortDir = "asc" | "desc";

export const FALLBACK_ICON = "/static/logo.png";

const norm = (s: unknown) => String(s ?? "").toLowerCase();

/** Guilds whose name contains the query, case-insensitive, sorted by name. */
export function visibleGuilds(guilds: readonly GuildSummary[], query: string, dir: SortDir): GuildSummary[] {
  const q = norm(query.trim());
  const list = q ? guilds.filter((g) => norm(g.name).includes(q)) : [...guilds];
  const sign = dir === "asc" ? 1 : -1;
  return list.sort((a, b) => {
    const x = norm(a.name);
    const y = norm(b.name);
    return x === y ? 0 : (x > y ? 1 : -1) * sign;
  });
}

export function sortLabel(dir: SortDir): string {
  return dir === "asc" ? "A → Z" : "Z → A";
}

export function formatMembers(count: number | null | undefined): string {
  if (count == null || Number.isNaN(count)) return "Member count unknown";
  return `${new Intl.NumberFormat("en-US").format(count)} member${count === 1 ? "" : "s"}`;
}

/** Everything the export dialog collects, before it becomes a request body. */
export interface ExportForm {
  channelId: string;
  userId: string;
  hasContent: boolean;
  embeds: boolean;
  attachments: boolean;
  attTypes: MediaTypes;
  links: boolean;
  emojis: boolean;
  wordOn: boolean;
  word: string;
  replies: boolean;
  bots: boolean;
  system: boolean;
  pinned: boolean;
  stickers: boolean;
  mentions: boolean;
  threads: boolean;
  minLength: string;
  minReactions: string;
  webhookUrl: string;
  after: string;
  before: string;
  download: MediaTypes;
}

export const ALL_MEDIA: MediaTypes = { images: true, videos: true, audio: true, other: true };
export const NO_MEDIA: MediaTypes = { images: false, videos: false, audio: false, other: false };

export const MEDIA_LABELS: ReadonlyArray<[keyof MediaTypes, string]> = [
  ["images", "Images"],
  ["videos", "Videos"],
  ["audio", "Audio"],
  ["other", "Other files"],
];

/** Defaults of the export dialog's checkboxes. */
export const DEFAULT_EXPORT_FORM: ExportForm = {
  channelId: "",
  userId: "",
  hasContent: true,
  embeds: true,
  attachments: true,
  attTypes: ALL_MEDIA,
  links: true,
  emojis: true,
  wordOn: false,
  word: "",
  replies: true,
  bots: true,
  system: false,
  pinned: true,
  stickers: true,
  mentions: true,
  threads: true,
  minLength: "0",
  minReactions: "0",
  webhookUrl: "",
  after: "",
  before: "",
  download: NO_MEDIA,
};

/** A non-negative whole number from a number input; anything else counts as 0. */
export function parseCount(v: string): number {
  const n = parseInt(v || "0", 10);
  return Number.isFinite(n) ? Math.max(0, n) : 0;
}

/** A datetime-local value read in the browser's timezone, as an ISO string in UTC. */
export function localToIso(v: string): string | null {
  const s = v.trim();
  if (!s) return null;
  const d = new Date(s);
  return Number.isNaN(d.getTime()) ? null : d.toISOString();
}

const orNull = (s: string) => s.trim() || null;

export function buildExportPayload(guildId: string, f: ExportForm): ExportMessagesRequest {
  return {
    guild_id: guildId,
    channel_id: orNull(f.channelId),
    user_id: orNull(f.userId),
    webhook_url: orNull(f.webhookUrl),
    has_attachments: false,
    after_iso: localToIso(f.after),
    before_iso: localToIso(f.before),
    filters: {
      has_content: f.hasContent,
      embeds: f.embeds,
      attachments: f.attachments,
      att_types: f.attachments ? { ...f.attTypes } : { ...NO_MEDIA },
      links: f.links,
      emojis: f.emojis,
      word_on: f.wordOn,
      word: f.word.trim(),
      replies: f.replies,
      bots: f.bots,
      system: f.system,
      min_length: parseCount(f.minLength),
      min_reactions: parseCount(f.minReactions),
      pinned: f.pinned,
      stickers: f.stickers,
      mentions: f.mentions,
      threads: f.threads,
      forum_threads: f.threads,
      private_threads: f.threads,
      download_media: { ...f.download },
    },
  };
}

/** Short note next to "Include attachments" when only some types are picked. */
export function attachmentSummary(f: ExportForm): string {
  if (!f.attachments) return "";
  const on = MEDIA_LABELS.filter(([k]) => f.attTypes[k]).map(([, label]) => label);
  if (on.length === MEDIA_LABELS.length) return "";
  return on.length ? on.join(", ") : "No types selected";
}

export interface Point {
  x: number;
  y: number;
}

/**
 * Top-left corner of the card menu: 8px below and right of the click, kept
 * inside the viewport and below the header.
 */
export function placeMenu(
  at: Point,
  size: { width: number; height: number },
  viewport: { width: number; height: number },
  headerHeight = 60,
): Point {
  const pad = 8;
  let x = at.x + pad;
  let y = at.y + pad;
  if (x + size.width > viewport.width - pad) x = viewport.width - pad - size.width;
  if (y + size.height > viewport.height - pad) y = viewport.height - pad - size.height;
  return { x: Math.round(Math.max(pad, x)), y: Math.round(Math.max(headerHeight + pad, y)) };
}
