// Shapes of the guild mapping routes in admin/app.py used by the Configuration page mapping cards.

/** One row of GET /api/guild-mappings. */
export interface DashboardMapping {
  mapping_id: string;
  mapping_name?: string | null;
  original_guild_id: string | number;
  original_guild_name?: string | null;
  original_guild_icon_url?: string | null;
  cloned_guild_id: string | number;
  cloned_guild_name?: string | null;
  status?: string | null;
}

export interface DashboardMappingList {
  ok?: boolean;
  mappings?: DashboardMapping[];
}

/** POST /api/guild-mappings/{id}/toggle-status */
export interface ToggleStatusResponse {
  ok?: boolean;
  status?: string;
}

export interface MappingModalOptions {
  mode?: "clone";
}
