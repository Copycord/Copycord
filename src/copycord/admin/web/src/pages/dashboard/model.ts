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

// /start and /stop answer {"ok": true} to a JSON request (a 303 to "/" only
// for a plain form post). The CSRF header comes from lib/api.ts.
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

/** What one status push says about a role: only the fields the frame carries. */
export type BotStatusPatch = Partial<BotStatus>;

function patchFrom(raw: Record<string, unknown>, noteKey = "status"): BotStatusPatch {
  const out: BotStatusPatch = {};
  if (raw.running != null) out.running = !!raw.running;
  if (typeof raw.uptime_sec === "number") out.uptime_sec = raw.uptime_sec;
  if (typeof raw[noteKey] === "string") out.status = raw[noteKey] as string;
  return out;
}

/**
 * A role's status after an update: fields the update leaves out keep their last value, so a push with
 * only the status note (the bots' AdminBus.status() never sends uptime_sec) does not blank the uptime
 * or flip running. A stopped role has no uptime, and a note does not outlive a change of running.
 */
export function mergeStatus(prev: BotStatus, patch: BotStatusPatch): BotStatus {
  const running = patch.running ?? prev.running;
  const flipped = running !== prev.running;
  const uptime_sec = running ? (patch.uptime_sec ?? (flipped ? null : (prev.uptime_sec ?? null))) : null;
  return { running, uptime_sec, status: patch.status ?? (flipped ? "" : (prev.status ?? "")) };
}

/** A status push on /ws/ui, or null for any other message. */
export function statusFromWs(frame: BusMessage): { role: BotRole; status: BotStatusPatch } | null {
  const p = (frame.payload && typeof frame.payload === "object" ? frame.payload : {}) as Record<string, unknown>;
  // AdminBus.status() and the hub's replay on connect: {kind: "status", role, payload: {running, status}}.
  if (frame.kind === "status" && !p.type) {
    if (frame.role !== "server" && frame.role !== "client") return null;
    return { role: frame.role, status: patchFrom(p) };
  }
  // hub.broadcast() keeps kind and role and moves the rest into payload, so
  // {type: "status", source, data} and agent frames arrive with payload.type.
  const msg: BusMessage = frame.type ? frame : { ...p, kind: frame.kind, role: frame.role };
  if (msg.type !== "status") return null;
  const data = (msg.data ?? {}) as Record<string, unknown>;
  if (msg.kind === "agent") {
    return { role: msg.role === "server" ? "server" : "client", status: patchFrom(data) };
  }
  if (msg.source !== "server" && msg.source !== "client") return null;
  return { role: msg.source, status: patchFrom(data, "note") };
}

/** Whether both roles in /api/status report no error, as app.js checked before slowing its poll. */
export function statusHealthy(res: StatusResponse | undefined): boolean {
  const ok = (r: StatusResponse["server"]) => !!r && r.ok !== false && !r.error;
  return ok(res?.server) && ok(res?.client);
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
