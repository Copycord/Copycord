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

/** An orphan category from the verify scan (lane 23 fills these in). */
export interface OrphanCategory {
  id: string;
  name: string;
}

/** An orphan channel from the verify scan (lane 23 fills these in). */
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
