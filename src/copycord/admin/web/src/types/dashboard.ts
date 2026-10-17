// Shapes of the replies behind the Configuration (dashboard) page.

/** GET /version. */
export interface VersionInfo {
  current: string;
  latest: string;
  url: string;
  update_available: boolean;
}

export type BotRole = "server" | "client";

/** One row of the Bots table. */
export interface BotStatus {
  running: boolean;
  uptime_sec?: number | null;
  status?: string;
}

/** GET /api/status, only the fields the Bots panel and the Tasks Run task button read. */
export interface StatusResponse {
  server?: Partial<BotStatus> & Record<string, unknown>;
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
