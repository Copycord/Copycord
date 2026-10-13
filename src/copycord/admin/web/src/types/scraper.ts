// Replies of the Scraper page endpoints (app.py /api/scraper/* routes).

/** One saved token; the server only ever sends the masked value. */
export interface ScraperToken {
  token_id: string;
  label: string;
  /** First 6 and last 4 characters, as app.py _mask_token. */
  masked: string;
  is_valid: boolean;
  last_validated?: string | null;
  username?: string | null;
  user_id?: string | null;
  added_at?: string | null;
  last_used?: string | null;
  use_count?: number;
}

/** GET /api/scraper/tokens */
export interface ScraperTokensResponse {
  ok: boolean;
  tokens?: ScraperToken[];
  error?: string;
}

/** POST /api/scraper/tokens/add */
export interface ScraperTokenAddResponse {
  ok: boolean;
  token_id?: string;
  is_valid?: boolean;
  username?: string | null;
  user_id?: string | null;
  error?: string;
}

/** POST /api/scraper/tokens/{id}/validate */
export interface ScraperTokenValidateResponse {
  ok: boolean;
  is_valid?: boolean;
  username?: string | null;
  user_id?: string | null;
  error?: string;
}

/** POST /api/scraper/tokens/{id}/update and DELETE /api/scraper/tokens/{id} */
export interface ScraperOk {
  ok: boolean;
  error?: string;
}

/** POST /api/scraper/validate-setup */
export interface ScraperValidateSetupResponse {
  ok: boolean;
  tokens_in_guild?: number;
  tokens_missing?: number;
  results?: Array<{ token_id: string; label: string; has_access: boolean }>;
  error?: string;
}

/** Body of POST /api/scraper/start and POST /api/scraper/queue/add. */
export interface ScraperPayload {
  guild_id: string;
  include_username: boolean;
  include_avatar_url: boolean;
  include_bio: boolean;
  include_roles: boolean;
  proxies?: string[];
}

/** POST /api/scraper/start */
export interface ScraperStartResponse {
  ok: boolean;
  queue_position?: number;
  tokens_used?: number;
  error?: string;
}

/** POST /api/scraper/queue/add */
export interface ScraperQueueAddResponse {
  ok: boolean;
  queue_position?: number;
  error?: string;
}

export interface ScraperQueueItem {
  id: string;
  guild_id: string | number;
  status: "pending" | "running" | string;
}

/** GET /api/scraper/status */
export interface ScraperStatus {
  running?: boolean;
  guild_id?: string | null;
  started_at?: number | null;
  queue?: ScraperQueueItem[];
}

/** GET and PUT /api/scraper/proxies */
export interface ScraperProxiesResponse {
  ok: boolean;
  proxies?: string[];
  error?: string;
}

/** GET /api/scraper/queue and the queue field of a queue_update message. */
export interface ScraperQueueResponse {
  ok?: boolean;
  queue?: ScraperQueueItem[];
  running?: boolean;
  error?: string;
}

/** One entry of GET /api/scraper/scrapes; unreadable files only carry the name and an error. */
export interface ScraperScrape {
  filename: string;
  /** Relative to data/scrapes, either "name.json" or "{guild_id}/name.json". */
  path: string;
  guild_id?: string | number | null;
  guild_name?: string | null;
  total_count?: number | null;
  elapsed_seconds?: number | null;
  scraped_at?: string | null;
  metadata?: { method?: string; [key: string]: unknown } | null;
  file_size?: number | null;
  error?: string;
}

/** GET /api/scraper/scrapes */
export interface ScraperScrapesResponse {
  ok: boolean;
  scrapes?: ScraperScrape[];
  error?: string;
}

/** Payload of a /ws/out message whose kind is "scraper". */
export type ScraperEvent =
  | { type: "progress"; current?: number; total?: number; message?: string }
  | {
      type: "complete";
      success?: boolean;
      guild_id?: string | number | null;
      total_count?: number;
      elapsed_seconds?: number | null;
      error?: string | null;
    }
  | { type: "queue_update"; queue?: ScraperQueueItem[]; running?: boolean };

/** A /ws/out message as the scraper sends it. */
export interface ScraperWsMessage {
  kind?: string;
  type?: string;
  payload?: ScraperEvent;
  [key: string]: unknown;
}

/** One line of /logs/stream/scraper: a tail batch, a single new line, or raw text. */
export interface ScraperLogEvent {
  lines?: unknown[];
  line?: string;
}
