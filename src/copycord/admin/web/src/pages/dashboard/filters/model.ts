// Calls and pure helpers for the Filters modal and its two picker modals.

import { api } from "../../../lib/api";
import type {
  FilterKey,
  FiltersResponse,
  FiltersState,
  GuildObject,
  GuildRole,
  GuildTree,
  PickerKey,
  PickerKind,
  RolesResponse,
} from "../../../types/dashboard-filters";

const filtersUrl = (id: string) => `/filters/${encodeURIComponent(id)}`;
const mappingApi = (id: string) => `/api/mappings/${encodeURIComponent(id)}`;

export const EMPTY_FILTERS: FiltersState = {
  wl_categories: [],
  wl_channels: [],
  ex_categories: [],
  ex_channels: [],
  channel_name_blacklist: [],
  blocked_role_ids: [],
  blocked_words: [],
  wl_users: [],
  bl_users: [],
};

/** Field order of the form post; every key is a form field name the server reads. */
export const FILTER_KEYS = Object.keys(EMPTY_FILTERS) as FilterKey[];

const strings = (v: unknown): string[] => (Array.isArray(v) ? v.map((x) => String(x)) : []);

export function parseFilters(data: FiltersResponse | null | undefined): FiltersState {
  const d = data ?? {};
  return {
    wl_categories: strings(d.wl_categories),
    wl_channels: strings(d.wl_channels),
    ex_categories: strings(d.ex_categories),
    ex_channels: strings(d.ex_channels),
    channel_name_blacklist: strings(d.channel_name_blacklist),
    blocked_role_ids: strings(d.blocked_role_ids),
    blocked_words: strings(d.blocked_words),
    wl_users: strings(d.wl_users),
    bl_users: strings(d.bl_users),
  };
}

export const fetchFilters = async (id: string): Promise<FiltersState> =>
  parseFilters(await api.get<FiltersResponse>(filtersUrl(id)));

/** The save route reads a form: one comma joined field per list, as the hidden inputs held them. */
export function buildSaveForm(state: FiltersState): FormData {
  const fd = new FormData();
  for (const key of FILTER_KEYS) fd.append(key, state[key].join(","));
  return fd;
}

export const saveFilters = (id: string, state: FiltersState) =>
  api.post<{ ok?: boolean }>(`${filtersUrl(id)}/save`, buildSaveForm(state));

export function isDirty(a: FiltersState, b: FiltersState): boolean {
  return FILTER_KEYS.some((k) => a[k].join(",") !== b[k].join(","));
}

export const fetchGuildTree = (id: string) => api.get<GuildTree>(`${mappingApi(id)}/channels`);

export async function fetchRoles(id: string): Promise<GuildRole[]> {
  const res = await api.get<RolesResponse>(`${mappingApi(id)}/roles`);
  return Array.isArray(res.roles) ? res.roles : [];
}

export const PICKER_KIND: Record<PickerKey, PickerKind> = {
  wl_categories: "category",
  wl_channels: "channel",
  ex_categories: "category",
  ex_channels: "channel",
};

export function pickerTitle(key: PickerKey): string {
  const prefix = key.startsWith("wl_") ? "Allow" : "Block";
  return `${prefix} ${PICKER_KIND[key] === "category" ? "Categories" : "Channels & Threads"}`;
}

export function pickerHelp(key: PickerKey): string {
  const allow = key.startsWith("wl_");
  if (PICKER_KIND[key] === "category") {
    return allow
      ? "Only the selected categories will be cloned (including their channels)."
      : "The selected categories (and their channels) will not be cloned.";
  }
  return allow
    ? "Only the selected channels and threads will be cloned."
    : "The selected channels and threads will not be cloned. Blocking a channel also covers its threads.";
}

export function categoryNames(categories: GuildObject[]): Map<string, string> {
  return new Map(categories.map((c) => [String(c.id), c.name || `Category ${c.id}`]));
}

/** Category column of a channel row; a thread hangs off a channel so the endpoint sends its parent name. */
export function parentLabel(obj: GuildObject, cats: Map<string, string>): string {
  if (obj.is_thread) return obj.parent_name || "";
  const pid = obj.parent_id ? String(obj.parent_id) : "";
  return (pid && cats.get(pid)) || "";
}

/** Search over name, ID and (for channels) category name. */
export function filterObjects(
  items: GuildObject[],
  query: string,
  kind: PickerKind,
  cats: Map<string, string>,
): GuildObject[] {
  const q = query.trim().toLowerCase();
  if (!q) return items;
  return items.filter((o) => {
    const cat = kind === "channel" ? (cats.get(o.parent_id ? String(o.parent_id) : "") ?? "").toLowerCase() : "";
    return String(o.name || "").toLowerCase().includes(q) || String(o.id || "").includes(q) || (cat !== "" && cat.includes(q));
  });
}

export function filterRoles(roles: GuildRole[], query: string): GuildRole[] {
  const q = query.trim().toLowerCase();
  if (!q) return roles;
  return roles.filter((r) => String(r.name || "").toLowerCase().includes(q) || String(r.id || "").includes(q));
}

/** Check or uncheck a set of IDs (the rows on screen) in a selection. */
export function setMany(selected: ReadonlySet<string>, ids: readonly string[], on: boolean): Set<string> {
  const next = new Set(selected);
  for (const id of ids) {
    if (on) next.add(id);
    else next.delete(id);
  }
  return next;
}

export function toggleOne(selected: ReadonlySet<string>, id: string): Set<string> {
  return setMany(selected, [id], !selected.has(id));
}

export type SelectState = "none" | "some" | "all";

export function selectState(selected: ReadonlySet<string>, ids: readonly string[]): SelectState {
  const n = ids.filter((id) => selected.has(id)).length;
  return n === 0 ? "none" : n === ids.length ? "all" : "some";
}
