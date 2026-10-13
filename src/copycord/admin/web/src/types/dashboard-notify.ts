// Shapes of the replies and payloads behind the Notifications and Sync Settings cards.

export type NotifEvent = "CLIENT_OFFLINE" | "SERVER_OFFLINE" | "PROXIES_DEAD" | "TOKEN_INVALID";

export type NotifEvents = Record<NotifEvent, boolean>;

/** GET /api/notifications/settings. */
export interface NotifSettingsReply {
  ok: boolean;
  webhook_url?: string;
  events?: Partial<Record<string, boolean>>;
}

/** PUT /api/notifications/settings body. */
export interface NotifSettingsPayload {
  webhook_url: string;
  events: NotifEvents;
}

/** POST /api/notifications/test. */
export interface NotifTestReply {
  ok: boolean;
  error?: string | null;
}

export interface SyncSettings {
  SYNC_STARTUP_DELAY: number;
  SYNC_INTER_GUILD_DELAY: number;
  SYNC_RANDOMIZE_ORDER: 0 | 1;
}

/** GET /api/sync/settings; the server may send any of the keys. */
export interface SyncSettingsReply {
  ok: boolean;
  settings?: Partial<Record<keyof SyncSettings, number | boolean | null>>;
}

/** What the inputs hold: delays stay text while typing. */
export interface SyncForm {
  startupDelay: string;
  guildDelay: string;
  randomize: boolean;
}
