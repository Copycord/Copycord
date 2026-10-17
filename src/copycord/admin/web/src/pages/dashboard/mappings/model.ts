// Calls and pure helpers for the mapping cards.

import { api } from "../../../lib/api";
import type {
  DashboardMapping,
  DashboardMappingList,
  ToggleStatusResponse,
} from "../../../types/dashboard-mappings";

export const MAPPINGS_URL = "/api/guild-mappings";
export const DEFAULT_ICON = "/static/logo.png";

const mappingUrl = (id: string) => `${MAPPINGS_URL}/${encodeURIComponent(id)}`;

export async function fetchMappings(): Promise<DashboardMapping[]> {
  const res = await api.get<DashboardMappingList>(MAPPINGS_URL);
  return res.mappings ?? [];
}

export const toggleMappingStatus = (id: string) =>
  api.post<ToggleStatusResponse>(`${mappingUrl(id)}/toggle-status`);

export const deleteMapping = (id: string) => api.del<unknown>(mappingUrl(id));

export function isPaused(m: DashboardMapping): boolean {
  return (m.status || "active").toLowerCase() === "paused";
}

/** Case insensitive match over the names and IDs. */
export function filterMappings(mappings: DashboardMapping[], query: string): DashboardMapping[] {
  const q = query.trim().toLowerCase();
  if (!q) return mappings;
  return mappings.filter((m) => {
    const parts = [
      m.mapping_name,
      m.original_guild_name,
      m.cloned_guild_name,
      m.original_guild_id ? String(m.original_guild_id) : "",
      m.cloned_guild_id ? String(m.cloned_guild_id) : "",
    ];
    return parts.filter(Boolean).join(" ").toLowerCase().includes(q);
  });
}

export function hostLabel(m: DashboardMapping): string {
  return m.original_guild_name || `ID: ${m.original_guild_id}`;
}

export function deleteBody(m: DashboardMapping): string {
  const name = (m.mapping_name || "").trim();
  const label = name ? `“${name}”` : `ID ${m.mapping_id}`;
  return `This will remove the mapping ${label}. This cannot be undone.`;
}

export function statusToast(status: string): string {
  return status.toLowerCase() === "paused"
    ? "Mapping paused. It will no longer clone until resumed."
    : "Mapping resumed. Cloning is active again.";
}

export function statusLabel(paused: boolean): string {
  return paused ? "Resume cloning for this mapping" : "Pause cloning for this mapping";
}
