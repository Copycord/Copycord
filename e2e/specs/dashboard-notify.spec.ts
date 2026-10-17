// Parity for the Notifications and Sync Settings cards on the Configuration page.
// The test endpoint is always stubbed, so no webhook is ever called.
import { expect, test, type Locator, type Page, type Route } from "@playwright/test";
import { pageUrl } from "../lib/env.ts";
import { settle } from "../lib/ui.ts";

const EVENTS = ["CLIENT_OFFLINE", "SERVER_OFFLINE", "PROXIES_DEAD", "TOKEN_INVALID"] as const;
type Ev = (typeof EVENTS)[number];

async function open(page: Page): Promise<void> {
  const errors: string[] = [];
  page.on("pageerror", (e) => errors.push(e.message));
  const r = await page.goto(pageUrl("dashboard"));
  expect(r?.status()).toBeLessThan(400);
  await settle(page);
  expect(errors).toEqual([]);
}

/** Open a collapsed card by clicking its title. */
async function openCard(page: Page, id: string): Promise<void> {
  await page.locator(`#${id} summary h3`).click();
  await expect(page.locator(`#${id}`)).toHaveJSProperty("open", true);
}

/** Old UI: a hidden checkbox with data-event. SPA: a role=switch button. */
const toggle = (page: Page, ev: Ev): Locator => page.locator(`#notif-toggle-${ev}, input[data-event="${ev}"]`);

const isOn = (loc: Locator): Promise<boolean> =>
  loc.evaluate((el) => (el instanceof HTMLInputElement ? el.checked : el.getAttribute("aria-checked") === "true"));

const flip = (loc: Locator): Promise<void> => loc.evaluate((el) => (el as HTMLElement).click());

function bodyOf(route: Route): unknown {
  return JSON.parse(route.request().postData() ?? "null");
}

test.describe("dashboard notifications and sync", () => {
  test("23 notifications load, save debounced PUT and test is stubbed", async ({ page }) => {
    const puts: unknown[] = [];
    await page.route("**/api/notifications/settings", (route) => {
      if (route.request().method() === "PUT") {
        puts.push(bodyOf(route));
        return route.fulfill({ json: { ok: true } });
      }
      return route.fulfill({
        json: { ok: true, webhook_url: "https://example.test/hook", events: { CLIENT_OFFLINE: true, SERVER_OFFLINE: false, PROXIES_DEAD: true, TOKEN_INVALID: true } },
      });
    });
    let tests = 0;
    await page.route("**/api/notifications/test", (route) => {
      tests += 1;
      return route.fulfill({ json: { ok: false, error: "e2e stub refused" } });
    });
    await open(page);
    await openCard(page, "notifications-card");

    await expect(page.locator("#notif-webhook-url")).toHaveValue("https://example.test/hook");
    expect(await isOn(toggle(page, "CLIENT_OFFLINE"))).toBe(true);
    expect(await isOn(toggle(page, "SERVER_OFFLINE"))).toBe(false);
    expect(puts).toEqual([]);

    await flip(toggle(page, "SERVER_OFFLINE"));
    await page.locator("#notif-webhook-url").fill("  https://example.test/other  ");
    await expect.poll(() => puts.length, { timeout: 5000 }).toBe(1);
    expect(puts[0]).toEqual({
      webhook_url: "https://example.test/other",
      events: { CLIENT_OFFLINE: true, SERVER_OFFLINE: true, PROXIES_DEAD: true, TOKEN_INVALID: true },
    });

    await page.locator("#notif-test-btn").click();
    await expect.poll(() => tests).toBe(1);
    await expect(page.getByText("e2e stub refused")).toBeVisible();
    await expect(page.locator("#notif-test-btn")).toHaveText("Test");
    await expect(page.locator("#notif-test-btn")).toBeEnabled();
  });

  test("23 test success toasts", async ({ page }) => {
    await page.route("**/api/notifications/settings", (route) => route.fulfill({ json: { ok: true, webhook_url: "", events: {} } }));
    await page.route("**/api/notifications/test", (route) => route.fulfill({ json: { ok: true, error: null } }));
    await open(page);
    await openCard(page, "notifications-card");
    await page.locator("#notif-test-btn").click();
    await expect(page.getByText("Test notification sent!")).toBeVisible();
  });

  test("24 sync settings load, save with the old fallbacks", async ({ page }) => {
    const puts: unknown[] = [];
    await page.route("**/api/sync/settings", (route) => {
      if (route.request().method() === "PUT") {
        puts.push(bodyOf(route));
        return route.fulfill({ json: { ok: true } });
      }
      return route.fulfill({ json: { ok: true, settings: { SYNC_STARTUP_DELAY: 20, SYNC_INTER_GUILD_DELAY: 5, SYNC_RANDOMIZE_ORDER: 0 } } });
    });
    await open(page);
    await openCard(page, "sync-settings-card");

    await expect(page.locator("#sync-startup-delay")).toHaveValue("20");
    await expect(page.locator("#sync-guild-delay")).toHaveValue("5");
    expect(await isOn(page.locator("#sync-randomize"))).toBe(false);
    expect(puts).toEqual([]);

    await page.locator("#sync-startup-delay").fill("30");
    await page.locator("#sync-startup-delay").blur();
    await flip(page.locator("#sync-randomize"));
    await expect.poll(() => puts.length, { timeout: 5000 }).toBeGreaterThan(0);
    await expect
      .poll(() => puts[puts.length - 1])
      .toEqual({ settings: { SYNC_STARTUP_DELAY: 30, SYNC_INTER_GUILD_DELAY: 5, SYNC_RANDOMIZE_ORDER: 1 } });
  });

  test("24 sync card open state is remembered", async ({ page }) => {
    await open(page);
    await expect(page.locator("#sync-settings-card")).toHaveJSProperty("open", false);
    await openCard(page, "sync-settings-card");
    expect(await page.evaluate(() => localStorage.getItem("cpc.sync-card-open"))).toBe("1");
    await page.reload();
    await settle(page);
    await expect(page.locator("#sync-settings-card")).toHaveJSProperty("open", true);
  });
});
