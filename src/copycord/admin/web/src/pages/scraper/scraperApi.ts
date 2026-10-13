// Scraper calls for the tokens, configuration and proxy cards. Token and
// guild forms are multipart, as the FastAPI Form(...) parameters expect;
// start, queue and proxies are JSON. A non 2xx reply throws an ApiError
// whose message is the server's error or detail, which the cards toast.

import { api } from "../../lib/api";
import type {
  ScraperOk,
  ScraperPayload,
  ScraperProxiesResponse,
  ScraperQueueAddResponse,
  ScraperStartResponse,
  ScraperTokenAddResponse,
  ScraperTokenValidateResponse,
  ScraperValidateSetupResponse,
} from "../../types/scraper";

export const TOKENS_URL = "/api/scraper/tokens";
export const TOKEN_ADD_URL = "/api/scraper/tokens/add";
export const VALIDATE_SETUP_URL = "/api/scraper/validate-setup";
export const START_URL = "/api/scraper/start";
export const CANCEL_URL = "/api/scraper/cancel";
export const QUEUE_ADD_URL = "/api/scraper/queue/add";
export const QUEUE_START_URL = "/api/scraper/queue/start";
export const STATUS_URL = "/api/scraper/status";
export const PROXIES_URL = "/api/scraper/proxies";

export function tokenUrl(id: string): string {
  return `${TOKENS_URL}/${encodeURIComponent(id)}`;
}

function form(fields: Record<string, string>): FormData {
  const fd = new FormData();
  for (const [key, value] of Object.entries(fields)) fd.append(key, value);
  return fd;
}

export function addToken(value: string, label: string): Promise<ScraperTokenAddResponse> {
  return api.post<ScraperTokenAddResponse>(TOKEN_ADD_URL, form({ token_value: value, label }));
}

export function validateToken(id: string): Promise<ScraperTokenValidateResponse> {
  return api.post<ScraperTokenValidateResponse>(`${tokenUrl(id)}/validate`);
}

export function updateTokenLabel(id: string, label: string): Promise<ScraperOk> {
  return api.post<ScraperOk>(`${tokenUrl(id)}/update`, form({ label }));
}

export function deleteToken(id: string): Promise<ScraperOk> {
  return api.del<ScraperOk>(tokenUrl(id));
}

export function validateSetup(guildId: string): Promise<ScraperValidateSetupResponse> {
  return api.post<ScraperValidateSetupResponse>(VALIDATE_SETUP_URL, form({ guild_id: guildId }));
}

export function startScrape(payload: ScraperPayload): Promise<ScraperStartResponse> {
  return api.post<ScraperStartResponse>(START_URL, payload);
}

export function queueAdd(payload: ScraperPayload): Promise<ScraperQueueAddResponse> {
  return api.post<ScraperQueueAddResponse>(QUEUE_ADD_URL, payload);
}

export function queueStart(): Promise<ScraperOk> {
  return api.post<ScraperOk>(QUEUE_START_URL);
}

export function cancelScrape(): Promise<ScraperOk> {
  return api.post<ScraperOk>(CANCEL_URL);
}

/** The saved proxy list; an unreadable reply counts as no proxies, as on the old page. */
export async function getProxies(): Promise<string[]> {
  try {
    const j = await api.get<ScraperProxiesResponse>(PROXIES_URL);
    return j?.ok ? (j.proxies ?? []) : [];
  } catch {
    return [];
  }
}

export function saveProxies(proxies: string[]): Promise<ScraperProxiesResponse> {
  return api.put<ScraperProxiesResponse>(PROXIES_URL, { proxies });
}

/** The server's reason when it sent one, otherwise the fallback. */
export function reason(e: unknown, fallback: string): string {
  return e instanceof Error && e.message && !e.message.startsWith("request to ") ? e.message : fallback;
}

/** "1 token", "2 tokens". */
export function plural(n: number, word: string): string {
  return `${n} ${word}${n !== 1 ? "s" : ""}`;
}

export const QUEUE_CLEAR_URL = "/api/scraper/queue/clear";
export const CANCEL_ALL_URL = "/api/scraper/cancel-all";
export const SCRAPES_URL = "/api/scraper/scrapes";
export const LOGS_CLEAR_URL = "/api/scraper/logs/clear";
export const LOG_STREAM_URL = "/logs/stream/scraper";

export function queueItemUrl(id: string | number): string {
  return `/api/scraper/queue/${encodeURIComponent(String(id))}`;
}

/** Download and delete address of a saved scrape; the whole path is one encoded segment, as on the old page. */
export function scrapeUrl(path: string): string {
  return `${SCRAPES_URL}/${encodeURIComponent(path)}`;
}

export function queueRemove(id: string | number): Promise<ScraperOk> {
  return api.del<ScraperOk>(queueItemUrl(id));
}

export function queueClear(): Promise<ScraperOk> {
  return api.post<ScraperOk>(QUEUE_CLEAR_URL);
}

export function cancelAll(): Promise<ScraperOk> {
  return api.post<ScraperOk>(CANCEL_ALL_URL);
}

export function deleteScrape(path: string): Promise<ScraperOk> {
  return api.del<ScraperOk>(scrapeUrl(path));
}

/** Empties scraper.out so a reload does not bring the lines back; failures are ignored. */
export function clearLogFile(): void {
  api.post<ScraperOk>(LOGS_CLEAR_URL).catch(() => undefined);
}

/** "42s", "3m 7s", or a dash when unknown, as the old fmtSec. */
export function fmtSec(s: number | null | undefined): string {
  if (s == null || !Number.isFinite(s)) return "\u2014";
  const r = Math.round(s);
  return r < 60 ? `${r}s` : `${Math.floor(r / 60)}m ${r % 60}s`;
}
