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

// Dashboard pages, all served by the SPA at the root. The key event-logs is the old name of the logs page.
export const PAGES = {
  dashboard: "/",
  channels: "/channels",
  guilds: "/guilds",
  forwarding: "/forwarding",
  scraper: "/scraper",
  system: "/system",
  "event-logs": "/logs",
} as const;

export type PageName = keyof typeof PAGES;

export function pageUrl(name: PageName): string {
  return PAGES[name];
}

export const LOGIN_PATH = "/login";
export const NOT_FOUND_PATH = "/this-page-does-not-exist";

export const CSRF_HEADERS = { "X-Copycord-CSRF": "1", Origin: BASE_URL };
