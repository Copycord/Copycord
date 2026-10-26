// Calls and pure helpers for the Bot settings popup: how each bot appears on
// Discord (SERVER_STATUS and CLIENT_STATUS). Its own endpoint, so a save here
// never touches the Global Configuration form.

import { api } from "../../lib/api";
import type { ToolbarOption } from "../channels/ToolbarSelect";

export const BOT_STATUS_URL = "/api/bot-status";

export type BotPresence = "online" | "idle" | "dnd" | "invisible";

export interface BotPresences {
  SERVER_STATUS: BotPresence;
  CLIENT_STATUS: BotPresence;
}

export type BotPresenceKey = keyof BotPresences;

export const DEFAULT_PRESENCES: BotPresences = { SERVER_STATUS: "online", CLIENT_STATUS: "online" };

export const PRESENCE_OPTIONS: readonly ToolbarOption[] = [
  { value: "online", label: "Online", dot: "#3ecf8e" },
  { value: "idle", label: "Idle", dot: "#f5a524" },
  { value: "dnd", label: "Busy", dot: "#f0506e" },
  { value: "invisible", label: "Invisible", dot: "#8b8b9e" },
];

export function toPresence(v: unknown): BotPresence {
  const s = String(v ?? "").toLowerCase();
  return PRESENCE_OPTIONS.some((o) => o.value === s) ? (s as BotPresence) : "online";
}

export async function loadPresences(): Promise<BotPresences> {
  const r = await api.get<Partial<Record<BotPresenceKey, string>>>(BOT_STATUS_URL);
  return { SERVER_STATUS: toPresence(r?.SERVER_STATUS), CLIENT_STATUS: toPresence(r?.CLIENT_STATUS) };
}

export interface SavePresencesResponse extends BotPresences {
  ok: boolean;
  /** Which bots were running and took the new status right away. */
  live?: { server?: boolean; client?: boolean };
}

export const savePresences = (p: BotPresences) => api.post<SavePresencesResponse>(BOT_STATUS_URL, p);

export const presencesDirty = (a: BotPresences, b: BotPresences) =>
  a.SERVER_STATUS !== b.SERVER_STATUS || a.CLIENT_STATUS !== b.CLIENT_STATUS;

export function savedMessage(res: SavePresencesResponse | null | undefined): string {
  return res?.live?.server || res?.live?.client ? "Status updated" : "Saved, applies at next start";
}
