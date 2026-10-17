// Calls and pure helpers for the Proxy card.

import { api } from "../../../lib/api";
import type {
  IntervalUnit,
  ProxyChipResult,
  ProxyConfig,
  ProxyOk,
  ProxySettingKey,
  ProxySettings,
  ProxySettingsResponse,
  ProxyTestEvent,
  ProxyTestResult,
  ProxyTestStatus,
} from "../../../types/dashboard-proxy";

const BASE = "/api/server/proxies";

export const PROXY_CARD_OPEN_KEY = "cpc.proxy-card-open";

export const PS_DEFAULTS: ProxySettings = {
  PROXY_SUSPEND_DURATION: 300,
  PROXY_TEST_BATCH_SIZE: 50,
  PROXY_SLOW_THRESHOLD: 3,
};

export const loadProxies = () => api.get<ProxyConfig>(BASE);
export const saveProxyList = (proxies: string[]) => api.put<ProxyOk>(BASE, { proxies });
export const saveRotation = (interval: number) => api.put<ProxyOk>(`${BASE}/rotation-interval`, { interval });
export const saveFavorites = (favorites: string[]) => api.put<ProxyOk>(`${BASE}/favorites`, { favorites });
export const saveEnabled = (enabled: boolean) => api.put<ProxyOk>(`${BASE}/toggle`, { enabled });
export const saveRecheck = (enabled: boolean, interval: number) =>
  api.put<ProxyOk>(`${BASE}/favorite-recheck`, { enabled, interval });
export const startTest = (proxies: string[]) => api.post<ProxyOk>(`${BASE}/test`, { proxies });
export const stopTest = () => api.post<ProxyOk>(`${BASE}/test/stop`);
export const testStatus = () => api.get<ProxyTestStatus>(`${BASE}/test/status`);
export const loadSettings = () => api.get<ProxySettingsResponse>(`${BASE}/settings`);
export const saveSettings = (settings: ProxySettings) => api.put<ProxyOk>(`${BASE}/settings`, { settings });

export function pluralProxy(n: number): string {
  return n === 1 ? "1 proxy" : `${n} proxies`;
}

/** Splits pasted or typed text on newlines and commas and drops what the list already holds. */
export function addProxies(current: readonly string[], text: string): string[] {
  const seen = new Set(current);
  const out = [...current];
  for (const line of text.split(/[\n\r,]+/)) {
    const p = line.trim();
    if (p && !seen.has(p)) {
      seen.add(p);
      out.push(p);
    }
  }
  return out;
}

export function statusChip(enabled: boolean, count: number): { text: string; tone: "on" | "warn" | "off" } {
  if (enabled && count > 0) return { text: `Enabled · ${pluralProxy(count)}`, tone: "on" };
  if (enabled) return { text: "Enabled · no proxies", tone: "warn" };
  return { text: "Disabled", tone: "off" };
}

/** Seconds into the number and unit the inputs show. */
export function splitInterval(sec: number): { value: number; unit: IntervalUnit } {
  if (sec >= 3600 && sec % 3600 === 0) return { value: sec / 3600, unit: "3600" };
  return { value: Math.max(1, Math.round(sec / 60)), unit: "60" };
}

export function toSeconds(value: number | string, unit: IntervalUnit, fallback = 1): number {
  const n = Math.max(1, parseInt(String(value), 10) || fallback);
  return n * (parseInt(unit, 10) || 60);
}

/** The server's saved rotation interval: 0 means off. */
export function rotationSeconds(on: boolean, value: number | string, unit: IntervalUnit): number {
  return on ? toSeconds(value, unit) : 0;
}

/** The re-check interval is at least a minute and defaults to five. */
export function recheckSplit(sec: number | undefined) {
  return splitInterval(Math.max(60, sec || 300));
}

export function maskProxy(p: string): string {
  return p.includes("@") ? p.replace(/\/\/[^@]+@/, "//***@") : p;
}

export function classify(r: ProxyTestResult, slowMs: number): "pass" | "slow" | "fail" {
  if (!r.ok) return "fail";
  return (r.ms ?? 0) >= slowMs ? "slow" : "pass";
}

export function chipResult(r: ProxyTestResult, slowMs: number): ProxyChipResult {
  const state = classify(r, slowMs);
  return { state, label: state === "fail" ? r.error || "failed" : `${r.ms ?? 0}ms` };
}

export function groupResults(results: readonly ProxyTestResult[], slowMs: number) {
  const good: ProxyTestResult[] = [];
  const slow: ProxyTestResult[] = [];
  const failed: ProxyTestResult[] = [];
  for (const r of results) {
    const c = classify(r, slowMs);
    (c === "fail" ? failed : c === "slow" ? slow : good).push(r);
  }
  return { good, slow, failed };
}

export function settingsPayload(raw: Record<ProxySettingKey, string | number>): ProxySettings {
  const out = { ...PS_DEFAULTS };
  for (const key of Object.keys(out) as ProxySettingKey[]) out[key] = parseInt(String(raw[key]), 10) || 1;
  return out;
}

export function isProxyTestEvent(msg: { kind?: string; payload?: unknown }): msg is { payload: ProxyTestEvent } {
  const p = msg.payload as ProxyTestEvent | undefined;
  return msg.kind === "proxy_test" && !!p && typeof p.type === "string";
}
