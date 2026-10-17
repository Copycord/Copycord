// Shapes of the guild routes in admin/app.py (guilds_api, guild_details) and of
// the Export messages task's inputs.

/** One row of GET /api/guilds. */
export interface GuildSummary {
  id: string;
  name: string;
  icon_url: string | null;
  member_count: number | null;
}

export interface GuildListResponse {
  items: GuildSummary[];
}

/** The item of GET /api/guilds/{id}; the extra fields come straight from the DB row. */
export interface GuildDetails extends GuildSummary {
  owner_id: string | number | null;
  created_at: string | null;
  description: string | null;
}

export interface GuildDetailsResponse {
  ok: boolean;
  item?: GuildDetails;
  error?: string;
}

export interface MediaTypes {
  images: boolean;
  videos: boolean;
  audio: boolean;
  other: boolean;
}

/** The filters object the client agent reads for an export. */
export interface ExportFilters {
  has_content: boolean;
  embeds: boolean;
  attachments: boolean;
  att_types: MediaTypes;
  links: boolean;
  emojis: boolean;
  word_on: boolean;
  word: string;
  replies: boolean;
  bots: boolean;
  system: boolean;
  min_length: number;
  min_reactions: number;
  pinned: boolean;
  stickers: boolean;
  mentions: boolean;
  threads: boolean;
  forum_threads: boolean;
  private_threads: boolean;
  download_media: MediaTypes;
}

/** The Export messages task's inputs. */
export interface ExportMessagesRequest {
  guild_id: string;
  channel_id: string | null;
  user_id: string | null;
  webhook_url: string | null;
  has_attachments: boolean;
  after_iso: string | null;
  before_iso: string | null;
  filters: ExportFilters;
}
