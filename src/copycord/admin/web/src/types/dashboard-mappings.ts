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
  /** Per mapping settings; the list route includes them, the modal reads them for Settings and Clone. */
  settings?: Record<string, unknown> | null;
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

/** A per-mapping setting value as stored in the mapping row: booleans, plus the user token strings and numbers. */
export type MappingSettingValue = boolean | string | number;
export type MappingSettings = Record<string, MappingSettingValue>;

/** Form state of the mapping modal. The user tokens block reads and writes `settings` through the same state. */
export interface MappingFormState {
  mapping_id: string;
  mapping_name: string;
  original_guild_id: string;
  cloned_guild_id: string;
  settings: MappingSettings;
}

/** Body of POST /api/guild-mappings and PATCH /api/guild-mappings/{id}. */
export interface MappingSavePayload {
  mapping_name: string;
  original_guild_id: string;
  cloned_guild_id: string;
  original_guild_name: string;
  cloned_guild_name: string;
  settings: MappingSettings;
}

export interface MappingSaveResponse {
  ok?: boolean;
  mapping_id?: string;
}

/** The six per mapping message options edited in the Message Features sub-modal. */
export type MsgFeatureKey =
  | "TAG_REPLY_MSG"
  | "ANONYMIZE_USERS"
  | "DISABLE_EVERYONE_MENTIONS"
  | "DISABLE_ROLE_MENTIONS"
  | "APPEND_TIMESTAMP"
  | "APPEND_AUTHOR";
