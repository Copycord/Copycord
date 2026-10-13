import path from "node:path";
import { fileURLToPath } from "node:url";

export const E2E_DIR = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
export const REPO_DIR = path.resolve(E2E_DIR, "..");
export const RUN_DIR = path.join(E2E_DIR, ".run");
export const STATE_FILE = path.join(E2E_DIR, ".auth", "state.json");
export const RUNTIME_FILE = path.join(RUN_DIR, "runtime.json");

export const PASSWORD = process.env.E2E_PASSWORD ?? "e2e-test-password-1";
export const HOME = process.env.E2E_COPYCORD_HOME ?? "/tmp/cc-e2e-home";
export const PORT = Number(process.env.E2E_ADMIN_PORT ?? "18080");
export const BASE_URL = process.env.E2E_BASE_URL ?? `http://127.0.0.1:${PORT}`;

// Old UI pages live at the root; the SPA lives under /app. E2E_BASE_PATH=/app reruns the same specs there.
export const BASE_PATH = (process.env.E2E_BASE_PATH ?? "").replace(/\/+$/, "");

export const PAGES = {
  dashboard: "/",
  channels: "/channels",
  guilds: "/guilds",
  forwarding: "/forwarding",
  scraper: "/scraper",
  system: "/system",
  "event-logs": "/event-logs",
} as const;

export type PageName = keyof typeof PAGES;

export function pageUrl(name: PageName): string {
  const p = PAGES[name];
  if (!BASE_PATH) return p;
  // The SPA serves the logs page at /logs; the old UI calls it /event-logs.
  if (name === "event-logs") return `${BASE_PATH}/logs`;
  return `${BASE_PATH}${p === "/" ? "/" : p}`;
}

// The login and 404 pages stay server routes until the SPA login lane replaces them.
export const LOGIN_PATH = process.env.E2E_LOGIN_PATH ?? "/login";
export const NOT_FOUND_PATH = `${BASE_PATH}/this-page-does-not-exist`;

export const CSRF_HEADERS = { "X-Copycord-CSRF": "1", Origin: BASE_URL };
