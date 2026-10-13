// Parity smoke for inventory items reachable without Discord (numbers refer to copycord-frontend-inventory.md).
// Runs against the old UI by default; E2E_BASE_PATH=/app runs the same checks against the SPA.
import { expect, test, type Page } from "@playwright/test";
import { CSRF_HEADERS, LOGIN_PATH, NOT_FOUND_PATH, PAGES, PASSWORD, pageUrl, type PageName } from "../lib/env.ts";
import { SEED } from "../lib/seed.ts";
import { closeX, expectClosed, expectOpen, modal, settle } from "../lib/ui.ts";

async function open(page: Page, name: PageName): Promise<void> {
  const errors: string[] = [];
  page.on("pageerror", (e) => errors.push(e.message));
  const r = await page.goto(pageUrl(name));
  expect(r?.status(), `${name} status`).toBeLessThan(400);
  await settle(page);
  expect(errors, `${name} page errors`).toEqual([]);
}

test.describe("shared", () => {
  test("1 sidebar links reach every page", async ({ page }) => {
    await open(page, "dashboard");
    for (const name of Object.keys(PAGES) as PageName[]) {
      const href = pageUrl(name);
      const link = page.locator(`a[href="${href}"]`).first();
      await expect(link, `nav link ${href}`).toBeAttached();
    }
    for (const name of Object.keys(PAGES) as PageName[]) await open(page, name);
  });

  test("3 mobile drawer opens and Escape closes", async ({ page }) => {
    await page.setViewportSize({ width: 390, height: 844 });
    await open(page, "dashboard");
    const menu = page.locator("#side-menu");
    await page.locator("#menu-toggle").click();
    await expect(menu).toBeInViewport();
    await page.keyboard.press("Escape");
    await expect(menu).not.toBeInViewport();
  });

  test("4 unsafe API calls need the CSRF header", async ({ page }) => {
    await open(page, "dashboard");
    const bad = await page.request.put("/api/sync/settings", { data: {} });
    expect(bad.status()).toBeGreaterThanOrEqual(400);
  });

  test("5 status endpoint answers", async ({ page }) => {
    const r = await page.request.get("/api/status");
    expect(r.ok()).toBeTruthy();
  });

  test("8 version endpoint answers", async ({ page }) => {
    const r = await page.request.get("/version");
    expect(r.ok()).toBeTruthy();
  });
});

test.describe("auth", () => {
  test.use({ storageState: { cookies: [], origins: [] } });

  test("11 logged out pages redirect to login", async ({ page }) => {
    await page.goto(pageUrl("system"));
    await expect(page).toHaveURL(/\/login/);
  });

  test("10 bad password shows an error, good password logs in, 2 logout", async ({ page }) => {
    await page.goto(LOGIN_PATH);
    await page.locator("#password").fill("wrong-password");
    await page.locator("#login-form button[type=submit]").click();
    await expect(page).toHaveURL(/\/login/);
    await page.locator("#password").fill(PASSWORD);
    await page.locator("#login-form button[type=submit]").click();
    await page.waitForURL((u) => !u.pathname.startsWith("/login"));
    await settle(page);
    const out = page.locator("[data-logout]").first();
    if (!(await out.isVisible())) await page.locator("#menu-toggle").click().catch(() => undefined);
    await out.click();
    await expect(page).toHaveURL(/\/login/);
  });
});

test("12 not found page links home", async ({ page }) => {
  await page.goto(NOT_FOUND_PATH);
  await settle(page);
  await expect(page.locator(`a[href="${pageUrl("dashboard")}"]`).first()).toBeAttached();
});

test.describe("dashboard", () => {
  test("15 config form loads with save and cancel", async ({ page }) => {
    await open(page, "dashboard");
    await expect(page.locator("#cfg-form")).toBeAttached();
    await expect(page.locator("#cfg-save-btn")).toBeAttached();
    await expect(page.locator("#cfg-cancel-btn")).toBeAttached();
  });

  test("17 backup tokens modal opens and closes", async ({ page }) => {
    await open(page, "dashboard");
    const m = modal(page, "backup-token-modal");
    await page.locator("#backupTokensBtn").click();
    await expectOpen(m);
    await page.keyboard.press("Escape");
    await expectClosed(m);
  });

  test("18 seeded mapping card is listed and search filters it", async ({ page }) => {
    await open(page, "dashboard");
    const list = page.locator("#guild-mapping-list");
    await expect(list).toContainText(SEED.mappingName);
    await page.locator("#mappingSearchInput").fill("zzz-no-match");
    await expect(list.getByText(SEED.mappingName)).toBeHidden();
    await page.locator("#mappingSearchInput").fill("");
    await expect(list).toContainText(SEED.mappingName);
  });

  test("19 mapping settings modal opens with the seeded guild IDs and closes", async ({ page }) => {
    await open(page, "dashboard");
    await page.locator('[data-act="edit"], [data-action="edit"], .mapping-card button:has-text("Settings")').first().click();
    const m = modal(page, "mapping-modal");
    await expectOpen(m);
    await expect(m.locator(`input[value="${SEED.guildId}"]`).first()).toBeAttached();
    await closeX(m).click();
    await expectClosed(m);
  });

  test("22 filters modal opens and closes", async ({ page }) => {
    await open(page, "dashboard");
    await page.locator('[data-act="filters"], [data-action="filters"], .mapping-card button:has-text("Filters")').first().click();
    const m = modal(page, "filters-modal");
    await expectOpen(m);
    await page.keyboard.press("Escape");
    await expectClosed(m);
  });

  test("23 24 notification and sync settings round trip", async ({ page }) => {
    for (const url of ["/api/notifications/settings", "/api/sync/settings"]) {
      const r = await page.request.get(url);
      expect(r.ok(), url).toBeTruthy();
      const put = await page.request.put(url, { data: await r.json(), headers: CSRF_HEADERS });
      expect(put.status(), `${url} put`).toBeLessThan(500);
    }
  });

  test("27 proxy settings and favorites help modals open and close", async ({ page }) => {
    await open(page, "dashboard");
    for (const [btn, id] of [["#srv-proxy-settings-btn", "proxy-settings-modal"], ["#srv-proxy-fav-help-btn", "proxy-fav-help-modal"]]) {
      const m = modal(page, id);
      await page.locator(btn).click();
      await expectOpen(m);
      await page.keyboard.press("Escape");
      await expectClosed(m);
    }
  });
});

test.describe("channels", () => {
  test("29 30 mapping select and search controls", async ({ page }) => {
    await open(page, "channels");
    for (const id of ["mapping-select", "ch-search", "ch-filter", "ch-sort"]) {
      await expect(page.locator(`#${id}`), id).toBeAttached();
    }
  });
});

test.describe("guilds", () => {
  test("37 search and sort controls, guild list endpoint", async ({ page }) => {
    await open(page, "guilds");
    await expect(page.locator("#g-search")).toBeAttached();
    await expect(page.locator("#g-sortdir")).toBeAttached();
    expect((await page.request.get("/api/guilds")).ok()).toBeTruthy();
  });
});

test.describe("forwarding", () => {
  test("40 seeded rule is listed", async ({ page }) => {
    await open(page, "forwarding");
    await expect(page.locator("#forwarding-list")).toContainText(SEED.ruleLabel);
  });

  test("41 new rule modal opens and closes", async ({ page }) => {
    await open(page, "forwarding");
    const m = modal(page, "fwdModal");
    await page.locator("#fwdCreateBtn").click();
    await expectOpen(m);
    await page.keyboard.press("Escape");
    await expectClosed(m);
  });
});

test.describe("scraper", () => {
  test("48 49 log and past scrapes cards", async ({ page }) => {
    await open(page, "scraper");
    for (const id of ["sc-config-card", "sc-log-card", "sc-log-search", "sc-log-autoclear", "sc-scrapes-card", "sc-scrapes-refresh"]) {
      await expect(page.locator(`#${id}`), id).toBeAttached();
    }
    expect((await page.request.get("/api/scraper/scrapes")).ok()).toBeTruthy();
  });
});

test.describe("system", () => {
  test("50 database card with backup now and info", async ({ page }) => {
    await open(page, "system");
    await expect(page.locator("#adv-backup-now")).toBeAttached();
    expect((await page.request.get("/api/backup/info")).ok()).toBeTruthy();
  });
});

test.describe("event logs", () => {
  test("51 seeded events listed, search filters, clear filters restores", async ({ page }) => {
    await open(page, "event-logs");
    const body = page.locator("#logs-tbody");
    await expect(body).toContainText("Created channel #general");
    await expect(body).toContainText("Created role Moderator");
    await page.locator("#logs-search").fill("Moderator");
    await expect(body).not.toContainText("Created channel #general");
    await page.locator("#logs-clear-filters").click();
    await expect(body).toContainText("Created channel #general");
  });
});
