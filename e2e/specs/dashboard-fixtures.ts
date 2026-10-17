// Stubs for the Configuration page bots panel and log modal,
// shared by parity.spec.ts and baseline.spec.ts. Both UIs read the same endpoints.
import type { Page } from "@playwright/test";

export const LOG_LINES = ["e2e alpha ready", "e2e beta warning", "e2e gamma done"] as const;

const RUNNING = {
  server: { running: true, uptime_sec: 65, status: "active" },
  client: { running: true, uptime_sec: 3700, status: "e2e syncing" },
};

/** Serve /version with or without a newer release. */
export async function stubVersion(page: Page, update: boolean): Promise<void> {
  await page.route("**/version", (route) =>
    route.fulfill({
      json: { current: "v4.0.0", latest: update ? "v4.1.0" : "v4.0.0", url: "https://example.test/release", update_available: update },
    }),
  );
}

/** Report both bots running on /api/status. */
export async function stubRunning(page: Page): Promise<void> {
  await page.route("**/api/status", (route) => route.fulfill({ json: RUNNING }));
}

/** First /logs/stream/{which} connection carries LOG_LINES; reconnects carry only a heartbeat. */
export async function stubLogStream(page: Page): Promise<void> {
  let served = 0;
  await page.route(/\/logs\/stream\/(server|client)$/, (route) => {
    const body = served++ === 0 ? `data: ${JSON.stringify({ lines: LOG_LINES })}\n\n` : ": ping\n\n";
    return route.fulfill({ status: 200, headers: { "content-type": "text/event-stream", "cache-control": "no-cache" }, body });
  });
}
