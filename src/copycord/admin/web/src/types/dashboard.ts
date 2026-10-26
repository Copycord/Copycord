// Shapes of the replies behind the Configuration (dashboard) page.

/** GET /version. */
export interface VersionInfo {
  current: string;
  latest: string;
  url: string;
  update_available: boolean;
}

export type BotRole = "server" | "client";

/** A bot's state: stopping from a Stop click until the role has really exited. */
export type BotState = "running" | "stopping" | "stopped";

/** One row of the Bots table. running is true while stopping too (the role is still up). */
export interface BotStatus {
  running: boolean;
  state?: BotState;
  uptime_sec?: number | null;
  status?: string;
}

/** One token in the server's warm pool. */
export interface TokenPoolEntry {
  state: "connecting" | "online" | "offline";
  presence: "online" | "dnd" | "invisible";
  since: number | null;
  dead: boolean;
  session: string;
}

/** The server's warm token pool, null while it has not started. Tokens are keyed by token id. */
export interface TokenPoolStatus {
  keep_online: number;
  live: number;
  max_loop_lag_ms: number;
  tokens: Record<string, TokenPoolEntry>;
}

/** GET /api/status, only the fields the Bots panel, the Tasks Run task button and the token block read. */
export interface StatusResponse {
  server?: Partial<BotStatus> & { token_pool?: TokenPoolStatus | null } & Record<string, unknown>;
  client?: Partial<BotStatus> & Record<string, unknown>;
  /** Both bots in the running state, as the server derives it. */
  both_running?: boolean;
}

export type BotStatuses = Record<BotRole, BotStatus>;

/** One SSE message on /logs/stream/{which}: a batch, a single line, or plain text. */
export interface LogStreamPayload {
  lines?: unknown[];
  line?: string;
}
