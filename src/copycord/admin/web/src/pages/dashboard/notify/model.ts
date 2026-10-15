// State and payload logic for the Notifications and Sync Settings cards.

import type {
  NotifEvent,
  NotifEvents,
  NotifSettingsPayload,
  NotifSettingsReply,
  SyncForm,
  SyncSettings,
  SyncSettingsReply,
} from "../../../types/dashboard-notify";

/** Debounce before a change is written, as on the old page. */
export const SAVE_DELAY_MS = 600;

export const NOTIF_HINT =
  "Get notified via Discord webhook when something goes wrong \u2014 client goes offline, proxies die, tokens get revoked.";

export const NOTIF_ROWS: Array<{ event: NotifEvent; label: string; hint: string }> = [
  { event: "CLIENT_OFFLINE", label: "Client Offline", hint: "When the client bot disconnects or stops" },
  { event: "SERVER_OFFLINE", label: "Server Offline", hint: "When the server bot disconnects or stops" },
  { event: "PROXIES_DEAD", label: "All Proxies Dead", hint: "When every proxy fails and the client can't connect" },
  { event: "TOKEN_INVALID", label: "Token Invalid", hint: "When a token is revoked or becomes invalid" },
];

export const DEFAULT_EVENTS: NotifEvents = {
  CLIENT_OFFLINE: true,
  SERVER_OFFLINE: true,
  PROXIES_DEAD: true,
  TOKEN_INVALID: true,
};

export const DEFAULT_SYNC: SyncForm = { startupDelay: "15", guildDelay: "3", randomize: true };

/** The Sync Settings card text, shared with the dashboard skeleton. */
export const SYNC_HINT =
  "When the client starts, it scans each server you're cloning and sends the full structure (channels, roles, " +
  "emojis) to the bot so it knows what to sync. These settings control how fast that happens. Slower is always " +
  "safer if you have many servers. Changes apply on next client restart.";

export const SYNC_DELAYS: Array<{ key: "startupDelay" | "guildDelay"; id: string; label: string; hint: string; min: number; max: number }> = [
  { key: "startupDelay", id: "sync-startup-delay", label: "Startup Delay", hint: "Wait time after login before sending sitemaps", min: 1, max: 120 },
  { key: "guildDelay", id: "sync-guild-delay", label: "Inter-Guild Delay", hint: "Pause between each server sitemap to help avoid rate limits.", min: 1, max: 30 },
];

export const SYNC_RANDOMIZE = {
  label: "Randomize Order",
  hint: "Process servers in random order each startup to avoid predictable patterns",
} as const;

/** Events the server knows about replace the defaults; unknown keys are ignored. */
export function mergeEvents(reply: NotifSettingsReply["events"]): NotifEvents {
  const out = { ...DEFAULT_EVENTS };
  for (const { event } of NOTIF_ROWS) {
    const v = reply?.[event];
    if (v !== undefined) out[event] = Boolean(v);
  }
  return out;
}

export function notifPayload(webhookUrl: string, events: NotifEvents): NotifSettingsPayload {
  return { webhook_url: webhookUrl.trim(), events };
}

/** Falsy server values fall back to the defaults, as the old `|| 15` did. */
export function syncFormFrom(reply: SyncSettingsReply["settings"]): SyncForm {
  const s = reply ?? {};
  return {
    startupDelay: String(s.SYNC_STARTUP_DELAY || 15),
    guildDelay: String(s.SYNC_INTER_GUILD_DELAY || 3),
    randomize: Boolean(s.SYNC_RANDOMIZE_ORDER ?? 1),
  };
}

export function syncPayload(form: SyncForm): { settings: SyncSettings } {
  return {
    settings: {
      SYNC_STARTUP_DELAY: parseInt(form.startupDelay, 10) || 15,
      SYNC_INTER_GUILD_DELAY: parseInt(form.guildDelay, 10) || 3,
      SYNC_RANDOMIZE_ORDER: form.randomize ? 1 : 0,
    },
  };
}
