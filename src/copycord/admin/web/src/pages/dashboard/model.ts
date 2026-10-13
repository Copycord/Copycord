// Calls and pure helpers for the Bots panel and the log modal, ported from app.js.

import { api } from "../../lib/api";
import type { BusMessage, LogStream } from "../../types/api";
import type { BotRole, BotStatus, BotStatuses, LogStreamPayload, StatusResponse } from "../../types/dashboard";

export const STATUS_URL = "/api/status";
export const VERSION_URL = "/version";
export const MAX_LOG_LINES = 10000;

export const EMPTY_STATUSES: BotStatuses = {
  server: { running: false },
  client: { running: false },
};

// The old /start and /stop are form posts that answer with a 303 to "/"; any
// 2xx after the redirect is success. The CSRF header comes from lib/api.ts.
export const startBots = () => api.post<unknown>("/start");
export const stopBots = () => api.post<unknown>("/stop");
export const clearLogs = (which: LogStream) => api.post<unknown>(`/logs/clear/${encodeURIComponent(which)}`);

export function toBotStatus(raw: Partial<BotStatus> | undefined): BotStatus {
  const r = raw ?? {};
  return {
    running: !!r.running,
    uptime_sec: r.uptime_sec ?? null,
    status: typeof r.status === "string" ? r.status : "",
  };
}

export function statusesFrom(res: StatusResponse | undefined): BotStatuses {
  return { server: toBotStatus(res?.server), client: toBotStatus(res?.client) };
}

/** A status push on /ws/ui, as app.js read it, or null for any other message. */
export function statusFromWs(msg: BusMessage): { role: BotRole; status: BotStatus } | null {
  if (msg.type !== "status") return null;
  const data = (msg.data ?? {}) as Record<string, unknown>;
  if (msg.kind === "agent") {
    return { role: msg.role === "server" ? "server" : "client", status: toBotStatus(data as Partial<BotStatus>) };
  }
  if (msg.source !== "server" && msg.source !== "client") return null;
  return {
    role: msg.source,
    status: toBotStatus({
      running: !!data.running,
      uptime_sec: typeof data.uptime_sec === "number" ? data.uptime_sec : null,
      status: typeof data.note === "string" ? data.note : "",
    }),
  };
}

export function anyRunning(s: BotStatuses): boolean {
  return s.server.running || s.client.running;
}

export function formatUptime(sec: number | null | undefined): string {
  const s = Math.max(0, Math.floor(Number(sec ?? 0)));
  const d = Math.floor(s / 86400);
  const h = Math.floor((s % 86400) / 3600);
  const m = Math.floor((s % 3600) / 60);
  const r = s % 60;
  const parts: string[] = [];
  if (d) parts.push(`${d}d`);
  if (d || h) parts.push(`${h}h`);
  if (d || h || m) parts.push(`${m}m`);
  parts.push(`${r}s`);
  return parts.join(" ");
}

/** The old page hides the plain "active" and "stopped" notes. */
export function displayStatus(status: string | undefined): string {
  const raw = (status ?? "").trim();
  return /^(active|stopped)$/i.test(raw) ? "" : raw;
}

/** Lines carried by one /logs/stream message; non JSON data is one line. */
export function parseLogData(raw: string): string[] {
  try {
    const obj = JSON.parse(raw) as LogStreamPayload | null;
    if (obj && Array.isArray(obj.lines)) return obj.lines.map((l) => String(l ?? ""));
    if (obj && typeof obj.line === "string") return [obj.line];
    return [raw];
  } catch {
    return [raw];
  }
}

export function appendCapped(lines: string[], more: string[], max = MAX_LOG_LINES): string[] {
  const next = lines.concat(more);
  return next.length > max ? next.slice(next.length - max) : next;
}

export function filterLines(lines: string[], query: string): string[] {
  const q = query.trim().toLowerCase();
  return q ? lines.filter((l) => l.toLowerCase().includes(q)) : lines;
}
