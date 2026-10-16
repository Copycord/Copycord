// Pure logic of the Channels page, ported from frontend/src/js/channels.js:
// category pins, search, the orphan merge, the filter and the sort of the
// tree (applyFilterAndSort, mergeOrphansIntoGroups and render).

import type { ChannelRow, GuildMapping, Orphans } from "../../types/channels";

export const UNGROUPED_LABEL = "Uncategorized";

export type FilterMode = "all" | "orphans" | "nonorphans";
export type SortMode = "name" | "category" | "type";
export type SortDir = "asc" | "desc";

export const NO_ORPHANS: Orphans = { categories: [], channels: [] };

export const FILTER_OPTIONS: ReadonlyArray<{ value: FilterMode; label: string }> = [
  { value: "all", label: "All channels" },
  { value: "orphans", label: "Orphans only" },
  { value: "nonorphans", label: "Non-orphans only" },
];

/** Lowercase, no leading "#", no diacritics, as normalize() did. */
export function normalize(s: unknown): string {
  const v = String(s ?? "")
    .toLowerCase()
    .replace(/^#\s*/, "");
  return v.normalize("NFKD").replace(/\p{Diacritic}/gu, "");
}

export function chTypeLabel(t: number | "" | null | undefined): string {
  const map: Record<number, string> = { 0: "Text", 2: "Voice", 5: "Announcements", 15: "Forum", 13: "Stage" };
  return (typeof t === "number" && map[t]) || `Type ${t ?? "-"}`;
}

/** The icon beside a channel name, Discord style; any other type falls back to the text channel's #. */
export type ChannelTypeIconKey = "text" | "voice" | "announcement" | "forum" | "stage";

export function chTypeIcon(t: number): ChannelTypeIconKey {
  const map: Record<number, ChannelTypeIconKey> = { 2: "voice", 5: "announcement", 15: "forum", 13: "stage" };
  return map[t] ?? "text";
}

/** A card's badges: cloned is the normal state and has none, so only the exceptions show. */
export type ChannelBadge = "orphan" | "notcloned" | "custom";

export function channelBadges(c: { orphan: boolean; clonedId: string; customized: boolean }): ChannelBadge[] {
  if (c.orphan) return ["orphan"];
  const out: ChannelBadge[] = [];
  if (!c.clonedId) out.push("notcloned");
  if (c.customized) out.push("custom");
  return out;
}

/** Option text of #mapping-select, as the Jinja template rendered it. */
export function mappingLabel(m: GuildMapping): string {
  const name = (m.mapping_name ?? "").trim();
  return name || `${m.original_guild_name ?? ""} → ${m.cloned_guild_name ?? ""}`;
}

// The labels stay plain: updateSortUI bailed out early because channels.html has no #ch-sortdir.
export const SORT_OPTIONS: ReadonlyArray<{ value: SortMode; label: string }> = [
  { value: "name", label: "Name" },
  { value: "category", label: "Category" },
  { value: "type", label: "Type" },
];

/** Picking a new sort starts ascending; picking the same one flips the direction. */
export function nextSort(cur: { sort: SortMode; dir: SortDir }, picked: string): { sort: SortMode; dir: SortDir } {
  const raw = picked.toLowerCase();
  const sort: SortMode = raw.includes("cat") ? "category" : raw.includes("type") ? "type" : "name";
  if (sort !== cur.sort) return { sort, dir: "asc" };
  return { sort, dir: cur.dir === "asc" ? "desc" : "asc" };
}

function originalCategory(ch: ChannelRow): string {
  const v = String(
    ch.original_category_name ?? ch.category_original_name ?? ch.category_upstream_name ?? ch.category_name ?? "",
  ).trim();
  return v || UNGROUPED_LABEL;
}

export interface CategoryPins {
  /** Original category name to its customized name in the clone. */
  pinByOrig: Map<string, string>;
  /** Lowercased original or customized name to the original name. */
  origByEither: Map<string, string>;
}

/** rebuildCategoryPinMaps without the window globals. */
export function buildCategoryPins(rows: readonly ChannelRow[]): CategoryPins {
  const pinByOrig = new Map<string, string>();
  const origByEither = new Map<string, string>();
  for (const ch of rows) {
    const orig = String(
      ch.original_category_name ?? ch.category_original_name ?? ch.category_upstream_name ?? ch.category_name ?? "",
    ).trim();
    if (!orig) continue;
    const pin = String(ch.cloned_category_name ?? ch.category_name ?? "").trim();
    if (!origByEither.has(orig.toLowerCase())) origByEither.set(orig.toLowerCase(), orig);
    if (pin) origByEither.set(pin.toLowerCase(), orig);
    if (pin && pin !== orig) pinByOrig.set(orig, pin);
  }
  return { pinByOrig, origByEither };
}

/** Rows whose channel names, category names or ids contain the query. */
export function searchRows(rows: readonly ChannelRow[], query: string, pins: CategoryPins): ChannelRow[] {
  const q = normalize(query);
  if (!q) return [...rows];
  return rows.filter((ch) => {
    const resolvedOrig = originalCategory(ch);
    const pinned = pins.pinByOrig.get(resolvedOrig) ?? "";
    const catName = ch.category_name?.trim() || UNGROUPED_LABEL;
    return [
      ch.original_channel_name,
      ch.clone_channel_name,
      catName,
      resolvedOrig,
      pinned,
      ch.original_channel_id,
      ch.cloned_channel_id,
    ].some((s) => normalize(s).includes(q));
  });
}

/** One channel card of the tree. */
export interface TreeChannel {
  /** Original channel id, the card's data-cid. */
  id: string;
  /** The API row; null for an orphan from the verify scan. */
  row: ChannelRow | null;
  orphan: boolean;
  originalName: string;
  displayName: string;
  customized: boolean;
  /** Title of the name: the custom name tooltip, else the original name. */
  title: string;
  type: number;
  typeLabel: string;
  clonedId: string;
}

/** One category section of the tree. */
export interface TreeSection {
  /** Grouping key: the row's category_name or the orphan category name. */
  key: string;
  /** Original category name, the section's data-cat-name. */
  originalName: string;
  displayName: string;
  customized: boolean;
  tooltip: string;
  orphanCategory: boolean;
  orphanCategoryId: string | null;
  uncategorized: boolean;
  channels: TreeChannel[];
}

export interface TreeOptions {
  query: string;
  filter: FilterMode;
  sort: SortMode;
  dir: SortDir;
}

export interface ChannelTree {
  sections: TreeSection[];
  /** "No channels found." shows when nothing matched and there are no orphans. */
  empty: boolean;
}

function channelTooltip(orig: string, custom: string): string {
  if (!custom || !custom.trim() || custom === orig) return "";
  return `Cloned channel = #${orig}\nCustomized channel = #${custom}`;
}

function categoryTooltip(orig: string, pin: string | undefined): string {
  if (!pin || pin.trim() === "" || pin === orig) return "";
  return `Cloned category = ${orig}\nCustomized category = ${pin}`;
}

function fromRow(ch: ChannelRow): TreeChannel {
  const customized = !!ch.clone_channel_name?.trim();
  const type = typeof ch.channel_type === "number" ? ch.channel_type : 0;
  return {
    id: String(ch.original_channel_id),
    row: ch,
    orphan: false,
    originalName: ch.original_channel_name ?? "",
    displayName: customized ? ch.clone_channel_name : (ch.original_channel_name ?? ""),
    customized,
    title: channelTooltip(ch.original_channel_name, ch.clone_channel_name) || (ch.original_channel_name ?? ""),
    type,
    typeLabel: chTypeLabel(ch.channel_type),
    clonedId: ch.cloned_channel_id ?? "",
  };
}

interface Group {
  channels: TreeChannel[];
  orphanCategory: boolean;
  orphanCategoryId: string | null;
}

/**
 * Groups the searched rows by category, merges the orphans in, then filters
 * and sorts, as render() did. Sections keep API order unless sorted by
 * category; channels inside a section sort by name, or by type then name.
 */
export function buildTree(
  rows: readonly ChannelRow[],
  orphans: Orphans,
  pins: CategoryPins,
  opts: TreeOptions,
): ChannelTree {
  const filtered = searchRows(rows, opts.query, pins);
  const hasOrphans = orphans.categories.length + orphans.channels.length > 0;
  if (!filtered.length && !hasOrphans) return { sections: [], empty: true };

  const q = normalize(opts.query);
  const groups = new Map<string, Group>();
  const group = (name: string) => {
    let g = groups.get(name);
    if (!g) {
      g = { channels: [], orphanCategory: false, orphanCategoryId: null };
      groups.set(name, g);
    }
    return g;
  };

  for (const ch of filtered) group(ch.category_name?.trim() || UNGROUPED_LABEL).channels.push(fromRow(ch));

  const catNameById = new Map<string, string>();
  for (const c of orphans.categories) {
    if (c.id != null) catNameById.set(String(c.id), c.name);
    if (q && !normalize(c.name).includes(q)) continue;
    const g = group(c.name);
    g.orphanCategory = true;
    g.orphanCategoryId = String(c.id);
  }
  for (const ch of orphans.channels) {
    const catId = ch.parent_id ?? ch.category_id ?? null;
    const catName =
      (ch.category_name ?? "").trim() || (catId != null ? catNameById.get(String(catId)) : undefined) || UNGROUPED_LABEL;
    if (q && !(normalize(ch.name).includes(q) || normalize(catName).includes(q))) continue;
    group(catName).channels.push({
      id: String(ch.id),
      row: null,
      orphan: true,
      originalName: ch.name,
      displayName: ch.name,
      customized: false,
      title: ch.name,
      type: ch.type ?? 0,
      typeLabel: chTypeLabel(ch.type ?? 0),
      clonedId: "",
    });
  }

  const entries = [...groups.entries()];
  if (opts.sort === "category") {
    entries.sort(([a], [b]) => a.localeCompare(b));
    if (opts.dir === "desc") entries.reverse();
  }

  const byName = (a: TreeChannel, b: TreeChannel) => a.originalName.localeCompare(b.originalName);
  const base = opts.sort === "type" ? (a: TreeChannel, b: TreeChannel) => a.type - b.type || byName(a, b) : byName;
  const cmp = opts.dir === "desc" ? (a: TreeChannel, b: TreeChannel) => -base(a, b) : base;

  const sections: TreeSection[] = [];
  for (const [key, g] of entries) {
    const channels = g.channels
      .filter((c) => (opts.filter === "orphans" ? c.orphan : opts.filter === "nonorphans" ? !c.orphan : true))
      .sort(cmp);
    const orphanCategory = g.orphanCategory && key !== UNGROUPED_LABEL;
    if (!channels.length && !orphanCategory) continue;

    const originalName = pins.origByEither.get(key.toLowerCase()) ?? key;
    const pin = pins.pinByOrig.get(originalName);
    const customized = !!(pin && pin.trim() && pin !== originalName);
    sections.push({
      key,
      originalName,
      displayName: customized && pin ? pin : originalName,
      customized,
      tooltip: categoryTooltip(originalName, pin),
      orphanCategory,
      orphanCategoryId: orphanCategory ? g.orphanCategoryId : null,
      uncategorized: originalName === UNGROUPED_LABEL,
      channels,
    });
  }
  return { sections, empty: false };
}

/** The GET url for a mapping; no mapping lists every channel, as load() did. */
export function channelsUrl(mappingId: string): string {
  return mappingId ? `/api/channels?mapping_id=${encodeURIComponent(mappingId)}` : "/api/channels";
}
