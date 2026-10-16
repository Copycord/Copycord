// GET /api/messages, /api/messages/facets and /api/messages/{id}/content (admin/messages.py).
// Discord ids are strings; timestamps on rows are Unix seconds.

export interface MessageRow {
  original_message_id: string;
  original_guild_id: string | null;
  original_guild_name: string | null;
  original_channel_id: string | null;
  original_channel_name: string | null;
  cloned_message_id: string | null;
  cloned_guild_id: string | null;
  cloned_guild_name: string | null;
  cloned_channel_id: string | null;
  cloned_channel_name: string | null;
  mapping_id: string | null;
  mapping_name: string | null;
  author_id: string | null;
  author_name: string | null;
  author_username: string | null;
  attachment_count: number | null;
  embed_count: number | null;
  created_at: number | null;
  cloned_at: number | null;
  original_url: string | null;
  cloned_url: string | null;
}

export interface MessagesResponse {
  ok: boolean;
  items: MessageRow[];
  next_cursor: string | null;
  total?: number;
}

export interface MessageFacetMapping {
  mapping_id: string;
  mapping_name: string | null;
  original_guild_id: string | null;
  cloned_guild_id: string | null;
}

export interface MessageFacetChannel {
  original_channel_id: string;
  original_guild_id: string | null;
  cloned_guild_id: string | null;
  name: string | null;
  count: number;
}

export interface MessageFacetsResponse {
  ok: boolean;
  mappings: MessageFacetMapping[];
  channels: MessageFacetChannel[];
}

export interface LiveAttachment {
  filename: string;
  url: string;
  size?: number | null;
  content_type?: string | null;
}

export interface LiveEmbed {
  title?: string | null;
  description?: string | null;
  url?: string | null;
  author?: string | { name?: string | null } | null;
  footer?: string | { text?: string | null } | null;
  image?: string | { url?: string | null } | null;
  thumbnail?: string | { url?: string | null } | null;
  fields?: Array<{ name: string; value: string }> | null;
}

export interface LiveMessage {
  id: string;
  channel_id: string;
  content: string;
  author: { id: string; name: string | null; username: string | null; avatar_url?: string | null };
  created_at: string | null;
  edited_at: string | null;
  attachments: LiveAttachment[];
  embeds: LiveEmbed[];
}

export interface MessageContentResponse {
  ok: boolean;
  cached: boolean;
  source: "clone" | "original";
  message: LiveMessage;
  row?: MessageRow;
}
