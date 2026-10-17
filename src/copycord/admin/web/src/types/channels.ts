// Shapes of the channel routes in admin/app.py (api_channels, api_list_guild_mappings).

/** One row of GET /api/channels: a channel mapping flattened with its category. */
export interface ChannelRow {
  original_channel_id: string;
  original_channel_name: string;
  /** Empty string when the channel has not been cloned yet. */
  cloned_channel_id: string;
  /** Customized name in the clone; empty when not customized. */
  clone_channel_name: string;
  is_thread?: boolean;
  pin_count?: number;
  channel_webhook_url?: string;
  /** Discord channel type; empty string when the DB has none. */
  channel_type: number | "" | null;
  original_guild_id?: string;
  cloned_guild_id?: string;
  /** cloned_category_name, else original_category_name, else "Uncategorized". */
  category_name: string;
  original_category_id?: string;
  cloned_category_id?: string;
  // Older payloads and other routes name the upstream category in these fields.
  original_category_name?: string;
  category_original_name?: string;
  category_upstream_name?: string;
  cloned_category_name?: string;
}

export interface ChannelListResponse {
  items: ChannelRow[];
}

/** One row of GET /api/guild-mappings, the options of #mapping-select. */
export interface GuildMapping {
  mapping_id: string;
  mapping_name: string | null;
  original_guild_id: string | number;
  original_guild_name: string | null;
  cloned_guild_id: string | number;
  cloned_guild_name: string | null;
  status?: string;
}

export interface GuildMappingListResponse {
  ok: boolean;
  mappings: GuildMapping[];
}

/** An orphan category from the verify scan. */
export interface OrphanCategory {
  id: string;
  name: string;
}

/** An orphan channel from the verify scan. */
export interface OrphanChannel {
  id: string;
  name: string;
  type?: number | null;
  category_name?: string | null;
  parent_id?: string | number | null;
  category_id?: string | number | null;
}

export interface Orphans {
  categories: OrphanCategory[];
  channels: OrphanChannel[];
}

/** POST /api/channels/customize; a null name clears the customization. */
export interface CustomizeChannelBody {
  original_channel_id: string;
  cloned_guild_id: string;
  clone_channel_name: string | null;
}

/** POST /api/categories/customize; a null name clears the customization. */
export interface CustomizeCategoryBody {
  original_category_id: string;
  cloned_guild_id: string;
  custom_category_name: string | null;
}

/** Reply of both customize routes. */
export interface CustomizeResponse {
  ok?: boolean;
  error?: string;
}

/** POST /api/filters/blacklist. */
export interface BlacklistBody {
  scope: "channel" | "category";
  obj_id: string;
  mapping_id: string;
  original_guild_id: string;
  cloned_guild_id: string;
}

/** Ids of a category resolved from the channel rows. */
export interface CategoryIds {
  originalCatId: string | null;
  clonedCatId: string | null;
  originalGuildId: string | null;
  clonedGuildId: string | null;
}

/** Message sent on /ws/in to delete orphans (envelope kind "verify"). */
export type OrphanDeletePayload =
  | { action: "delete_one"; kind: "channel" | "category"; id: string }
  | { action: "delete_all"; ids: string[] }
  | { action: "list" };

/** One entry of the "deleted" verify event on /ws/out. */
export interface DeletedResult {
  id?: string | number;
  channel_id?: string | number;
  category_id?: string | number;
  target_id?: string | number;
  orphan_id?: string | number;
  original_id?: string | number;
  name?: string;
  channel_name?: string;
  category_name?: string;
  status?: string;
  reason?: string;
  deleted?: boolean;
  ok?: boolean;
  success?: boolean;
}

// Verify results.

/** The "orphans" verify event on /ws/out. */
export interface OrphansEvent {
  type: "orphans";
  mapping_id?: string | number;
  categories?: OrphanCategory[];
  channels?: OrphanChannel[];
}
