// Shapes of the filters routes in admin/app.py used by the mapping Filters modal.

/** GET /filters/{mapping_id}. */
export interface FiltersResponse {
  wl_categories?: Array<string | number>;
  wl_channels?: Array<string | number>;
  ex_categories?: Array<string | number>;
  ex_channels?: Array<string | number>;
  blocked_words?: string[];
  channel_name_blacklist?: string[];
  blocked_role_ids?: Array<string | number>;
  wl_users?: Array<string | number>;
  bl_users?: Array<string | number>;
}

/** The chip lists of the form, keyed by their form field names. */
export interface FiltersState {
  wl_categories: string[];
  wl_channels: string[];
  ex_categories: string[];
  ex_channels: string[];
  channel_name_blacklist: string[];
  blocked_role_ids: string[];
  blocked_words: string[];
  wl_users: string[];
  bl_users: string[];
}

export type FilterKey = keyof FiltersState;

/** The four lists that have a Browse picker. */
export type PickerKey = "wl_categories" | "wl_channels" | "ex_categories" | "ex_channels";

export type PickerKind = "category" | "channel";

/** One category, channel or thread of GET /api/mappings/{id}/channels. */
export interface GuildObject {
  id: string;
  name?: string;
  type?: number;
  position?: number;
  parent_id?: string | null;
  parent_name?: string | null;
  is_thread?: boolean;
}

export interface GuildTree {
  ok?: boolean;
  categories?: GuildObject[];
  channels?: GuildObject[];
}

/** One role of GET /api/mappings/{id}/roles. */
export interface GuildRole {
  id: string;
  name?: string;
  position?: number;
  color?: number;
  color_hex?: string | null;
}

export interface RolesResponse {
  ok?: boolean;
  roles?: GuildRole[];
}
