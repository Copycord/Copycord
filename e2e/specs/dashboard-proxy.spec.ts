// Parity for the Configuration page proxy card.
// Every proxy endpoint is stubbed: no real proxy is ever contacted. The bus websocket is stubbed too,
// so the test pushes the proxy_test messages itself.
import { expect, test, type Page, type WebSocketRoute } from "@playwright/test";
import { pageUrl } from "../lib/env.ts";
import { settle } from "../lib/ui.ts";

const PROXIES = ["a.e2e.test:1111", "b.e2e.test:2222", "c.e2e.test:3333"];

interface Call {
  url: string;
  method: string;
  body: unknown;
  csrf: string | null;
}

interface Stub {
  calls: Call[];
  sockets: WebSocketRoute[];
  of: (path: string, method: string) => Call[];
  send: (payload: unknown) => void;
}

async function stubProxies(page: Page): Promise<Stub> {
  const calls: Call[] = [];
  const sockets: WebSocketRoute[] = [];
  await page.routeWebSocket(/\/ws\/(ui|out)$/, (ws) => {
    sockets.push(ws);
  });
  await page.route("**/api/server/proxies**", (route) => {
    const req = route.request();
    const url = new URL(req.url()).pathname;
    const raw = req.postData();
    calls.push({ url, method: req.method(), body: raw ? JSON.parse(raw) : undefined, csrf: req.headers()["x-copycord-csrf"] ?? null });
    if (url.endsWith("/api/server/proxies") && req.method() === "GET") {
      return route.fulfill({
        json: {
          ok: true,
          proxies: PROXIES,
          enabled: true,
          rotation_interval: 1800,
          favorites: [PROXIES[0]],
          favorite_recheck: false,
          favorite_recheck_interval: 300,
        },
      });
    }
    if (url.endsWith("/test/status")) return route.fulfill({ json: { ok: true, running: false } });
    if (url.endsWith("/favorites")) return route.fulfill({ json: { ok: true, favorites: (JSON.parse(raw ?? "{}") as { favorites: string[] }).favorites } });
    if (url.endsWith("/settings") && req.method() === "GET") {
      return route.fulfill({ json: { ok: true, settings: { PROXY_SUSPEND_DURATION: 120, PROXY_TEST_BATCH_SIZE: 20, PROXY_SLOW_THRESHOLD: 2 } } });
    }
    return route.fulfill({ json: { ok: true } });
  });
  return {
    calls,
    sockets,
    of: (path, method) => calls.filter((c) => c.url === path && c.method === method),
    send: (payload) => {
      for (const s of sockets) s.send(JSON.stringify({ kind: "proxy_test", role: "admin", payload }));
    },
  };
}

async function openCard(page: Page): Promise<void> {
  await page.goto(pageUrl("dashboard"));
  await settle(page);
  await page.locator("#srv-proxy-card > summary").click();
  await expect(page.locator(".srv-proxy-chip-item")).toHaveCount(PROXIES.length);
}

const chip = (page: Page, proxy: string) => page.locator(`.srv-proxy-chip-item[data-proxy="${proxy}"]`);

test.describe("dashboard proxy", () => {
  test("25 loads the list, enable state, favorite and rotation", async ({ page }) => {
    await stubProxies(page);
    await openCard(page);
    await expect(page.locator("#srv-proxy-toggle")).toBeChecked();
    await expect(page.locator("#srv-proxy-status")).toHaveText(/Enabled\s*·\s*3 proxies/);
    await expect(chip(page, PROXIES[0]!)).toHaveClass(/is-favorite/);
    await expect(chip(page, PROXIES[1]!)).not.toHaveClass(/is-favorite/);
    await expect(page.locator("#srv-proxy-rotation-toggle")).toBeChecked();
    await expect(page.locator("#srv-proxy-interval")).toHaveValue("30");
    await expect(page.locator("#srv-proxy-interval-unit")).toHaveValue("60");
  });

  test("25 adding and removing chips saves the list with CSRF", async ({ page }) => {
    const stub = await stubProxies(page);
    await openCard(page);
    const input = page.locator("#srv-proxy-chips-input");
    await input.fill("d.e2e.test:4444");
    await input.press("Enter");
    await expect(page.locator(".srv-proxy-chip-item")).toHaveCount(4);
    await expect.poll(() => stub.of("/api/server/proxies", "PUT").length).toBeGreaterThan(0);
    const put = stub.of("/api/server/proxies", "PUT").at(-1)!;
    expect(put.body).toEqual({ proxies: [...PROXIES, "d.e2e.test:4444"] });
    expect(put.csrf).toBe("1");
    await chip(page, "d.e2e.test:4444").locator(".srv-proxy-chip-x").click();
    await expect(page.locator(".srv-proxy-chip-item")).toHaveCount(3);
    await expect.poll(() => (stub.of("/api/server/proxies", "PUT").at(-1)!.body as { proxies: string[] }).proxies.length).toBe(3);
  });

  test("25 the star saves favorites and the toggle saves enabled", async ({ page }) => {
    const stub = await stubProxies(page);
    await openCard(page);
    await chip(page, PROXIES[1]!).locator(".srv-proxy-chip-star").click();
    await expect.poll(() => stub.of("/api/server/proxies/favorites", "PUT").length).toBe(1);
    expect(stub.of("/api/server/proxies/favorites", "PUT")[0]!.body).toEqual({ favorites: [PROXIES[0], PROXIES[1]] });
    await expect(chip(page, PROXIES[1]!)).toHaveClass(/is-favorite/);

    await page.locator("#srv-proxy-toggle").uncheck();
    await expect.poll(() => stub.of("/api/server/proxies/toggle", "PUT").length).toBe(1);
    expect(stub.of("/api/server/proxies/toggle", "PUT")[0]!.body).toEqual({ enabled: false });
    await expect(page.locator("#srv-proxy-status")).toHaveText("Disabled");
    await expect(page.locator("#srv-proxy-interval-unit")).toBeDisabled();
  });

  test("25 rotation and favorite re-check save their intervals", async ({ page }) => {
    const stub = await stubProxies(page);
    await openCard(page);
    await page.locator("#srv-proxy-interval").fill("2");
    await page.locator("#srv-proxy-interval-unit").selectOption("3600");
    await expect.poll(() => stub.of("/api/server/proxies/rotation-interval", "PUT").at(-1)?.body).toEqual({ interval: 7200 });
    await page.locator("#srv-proxy-fav-recheck-toggle").check();
    await expect.poll(() => stub.of("/api/server/proxies/favorite-recheck", "PUT").at(-1)?.body).toEqual({ enabled: true, interval: 300 });
  });

  test("26 test shows live progress, per chip results and removes failed proxies; Stop posts", async ({ page }) => {
    const stub = await stubProxies(page);
    await openCard(page);
    await page.waitForTimeout(1800); // the page ignores bus messages that arrive right after load (history replay)
    await page.locator("#srv-proxy-test").click();
    await expect.poll(() => stub.of("/api/server/proxies/test", "POST").length).toBe(1);
    expect(stub.of("/api/server/proxies/test", "POST")[0]!.body).toEqual({ proxies: PROXIES });
    await expect.poll(() => stub.sockets.length).toBeGreaterThan(0);

    stub.send({ type: "started", total: 3 });
    await expect(page.locator("#srv-proxy-test")).toHaveText(/Stop/);
    await expect(page.locator("#srv-proxy-progress")).toBeVisible();
    stub.send({
      type: "progress",
      current: 2,
      total: 3,
      results: [
        { proxy: PROXIES[0], ok: true, ms: 120 },
        { proxy: PROXIES[1], ok: false, error: "timeout" },
      ],
    });
    await expect(page.locator("#srv-proxy-progress-text")).toHaveText("2 / 3");
    await expect(chip(page, PROXIES[0]!)).toContainText("120ms");
    await expect(chip(page, PROXIES[1]!)).toContainText("timeout");

    await page.locator("#srv-proxy-test").click();
    await expect.poll(() => stub.of("/api/server/proxies/test/stop", "POST").length).toBe(1);

    stub.send({
      type: "complete",
      results: [
        { proxy: PROXIES[0], ok: true, ms: 120 },
        { proxy: PROXIES[1], ok: false, error: "timeout" },
        { proxy: PROXIES[2], ok: true, ms: 120 },
      ],
    });
    await expect(page.locator("#ptr-remove-failed")).toBeChecked();
    await page.getByRole("button", { name: /Remove Selected/ }).click();
    await expect(page.locator(".srv-proxy-chip-item")).toHaveCount(2);
    await expect.poll(() => stub.of("/api/server/proxies", "PUT").at(-1)?.body).toEqual({ proxies: [PROXIES[0], PROXIES[2]] });
    await expect(page.locator("#srv-proxy-test")).toHaveText(/Test All/);
  });

  test("27 settings modal loads, saves and resets; favorites help opens and closes", async ({ page }) => {
    const stub = await stubProxies(page);
    await openCard(page);
    await page.locator("#srv-proxy-settings-btn").click();
    const settings = page.locator("#proxy-settings-modal.show");
    await expect(settings).toHaveCount(1);
    await expect(page.locator("#ps-suspend-duration")).toHaveValue("120");
    await expect(page.locator("#ps-test-batch")).toHaveValue("20");
    await expect(page.locator("#ps-slow-threshold")).toHaveValue("2");
    await page.locator("#ps-test-batch").fill("30");
    await page.locator("#ps-test-batch").blur();
    await expect.poll(() => stub.of("/api/server/proxies/settings", "PUT").at(-1)?.body).toEqual({
      settings: { PROXY_SUSPEND_DURATION: 120, PROXY_TEST_BATCH_SIZE: 30, PROXY_SLOW_THRESHOLD: 2 },
    });
    await page.locator("#ps-reset-defaults").click();
    await expect.poll(() => stub.of("/api/server/proxies/settings", "PUT").at(-1)?.body).toEqual({
      settings: { PROXY_SUSPEND_DURATION: 300, PROXY_TEST_BATCH_SIZE: 50, PROXY_SLOW_THRESHOLD: 3 },
    });
    await page.locator("#proxy-settings-close").click();
    await expect(settings).toHaveCount(0);

    await page.locator("#srv-proxy-fav-help-btn").click();
    const help = page.locator("#proxy-fav-help-modal.show");
    await expect(help).toHaveCount(1);
    await expect(page.locator("#srv-proxy-card")).toHaveJSProperty("open", true);
    await page.locator("#proxy-fav-help-close").click();
    await expect(help).toHaveCount(0);
  });
});
