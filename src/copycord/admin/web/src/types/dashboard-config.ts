// Shapes behind the Global Configuration card and the backup tokens modal.

export type LogLevel = "INFO" | "DEBUG";

/** The six keys POST /save accepts (ALLOWED_ENV in app.py). */
export interface GlobalConfig {
  SERVER_TOKEN: string;
  CLIENT_TOKEN: string;
  /** Discord user ids, one chip each. */
  COMMAND_USERS: string[];
  COPYCORD_AUTOSTART: boolean;
  LOG_LEVEL: LogLevel;
  LOG_MAX_SIZE_MB: string;
}

export type TokenKey = "SERVER_TOKEN" | "CLIENT_TOKEN";

/** GET /api/validate-tokens. */
export interface TokenValidation {
  ok: boolean;
  has_tokens: boolean;
  errors: string[];
  bot_client_id?: string | null;
}

/** One row of GET /api/backup-tokens; the value itself stays on the server. */
export interface BackupToken {
  token_id: string;
  masked: string;
  note: string;
  added_at?: number | null;
  last_used?: number | null;
}

export interface BackupTokensResponse {
  ok: boolean;
  tokens: BackupToken[];
}

export interface BackupTokenResult {
  ok: boolean;
  token_id?: string;
  not_found?: boolean;
}
