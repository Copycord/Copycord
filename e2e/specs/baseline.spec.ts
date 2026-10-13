import fs from "node:fs";
import path from "node:path";
import { expect, test, type Page } from "@playwright/test";
import { BASE_PATH, E2E_DIR, LOGIN_PATH, NOT_FOUND_PATH, PAGES, pageUrl, type PageName } from "../lib/env.ts";
import { FWD_CREATE_BTN, MAPPING_EDIT_BTN, MAPPING_FILTERS_BTN, expectOpen, modal, settle } from "../lib/ui.ts";
import { stubClientGuilds } from "../lib/seed.ts";
import { DETAILS, FIRST, dialog as guildDialog, guildAction, guildCard, stubGuilds } from "./guilds-fixtures.ts";
import { LOG_LINES, stubLogStream, stubRunning, stubVersion } from "./dashboard-fixtures.ts";

// E2E_SHOTS_DIR=baseline/new captures the SPA into a separate folder for scripts/compare.ts.
const OUT = path.resolve(E2E_DIR, process.env.E2E_SHOTS_DIR ?? "baseline/old");
const VIEWPORTS = [
  { name: "1440x900", width: 1440, height: 900 },
  { name: "390x844", width: 390, height: 844 },
] as const;

interface ModalShot {
  name: string;
  page: PageName;
  open: (page: Page) => Promise<void>;
  id: string;
}

const MODALS: ModalShot[] = [
  { name: "dashboard-backup-tokens", page: "dashboard", id: "backup-token-modal",
    open: (p) => p.locator("#backupTokensBtn").click() },
  { name: "dashboard-proxy-settings", page: "dashboard", id: "proxy-settings-modal",
    open: (p) => p.locator("#srv-proxy-settings-btn").click() },
  { name: "dashboard-proxy-fav-help", page: "dashboard", id: "proxy-fav-help-modal",
    open: (p) => p.locator("#srv-proxy-fav-help-btn").click() },
  { name: "dashboard-mapping-settings", page: "dashboard", id: "mapping-modal",
    open: (p) => p.locator(MAPPING_EDIT_BTN).first().click() },
  { name: "dashboard-filters", page: "dashboard", id: "filters-modal",
    open: (p) => p.locator(MAPPING_FILTERS_BTN).first().click() },
  { name: "forwarding-new-rule", page: "forwarding", id: "fwdModal",
    open: (p) => p.locator(FWD_CREATE_BTN).filter({ visible: true }).first().click() },
];

const GUILD_SHOTS: Array<{ name: string; fullPage: boolean; open: (page: Page) => Promise<void> }> = [
  { name: "guilds-grid", fullPage: true, open: async () => undefined },
  { name: "guilds-menu", fullPage: false, open: (p) => guildAction(p, FIRST.id) },
  { name: "guilds-details", fullPage: false, open: async (p) => {
    await guildAction(p, FIRST.id, "View details");
    await expect(guildDialog(p, "Guild details")).toContainText(DETAILS.owner_id);
  } },
  { name: "guilds-export", fullPage: false, open: async (p) => {
    await guildAction(p, FIRST.id, "Export messages");
    await expect(guildDialog(p, "Export messages")).toBeVisible();
  } },
];

// The bots panel with stubbed running bots and the log modal with a stubbed stream (items 13, 14).
const DASHBOARD_SHOTS: Array<{ name: string; stub: (page: Page) => Promise<void>; open: (page: Page) => Promise<void> }> = [
  { name: "dashboard-bots-running", stub: stubRunning, open: async () => undefined },
  { name: "dashboard-log-modal", stub: stubLogStream, open: async (p) => {
    await p.locator('[data-log="server"]').click();
    await expect(p.locator("#log-body")).toContainText(LOG_LINES[2]);
  } },
  { name: "dashboard-log-search", stub: stubLogStream, open: async (p) => {
    await p.locator('[data-log="server"]').click();
    await expect(p.locator("#log-body")).toContainText(LOG_LINES[2]);
    await p.locator("#log-search-input").fill("beta");
    await expect(p.locator("#log-body")).not.toContainText(LOG_LINES[0]);
  } },
];

fs.mkdirSync(OUT, { recursive: true });

for (const vp of VIEWPORTS) {
  test.describe(`baseline ${vp.name}`, () => {
    test.use({ viewport: { width: vp.width, height: vp.height } });

    for (const name of Object.keys(PAGES) as PageName[]) {
      test(`${name}`, async ({ page }) => {
        // The SPA serves the old /event-logs page at /app/logs (App.tsx PAGES).
        await page.goto(name === "event-logs" && BASE_PATH ? `${BASE_PATH}/logs` : pageUrl(name));
        await settle(page);
        await page.screenshot({ path: path.join(OUT, `${name}-${vp.name}.png`), fullPage: true });
      });
    }

    test("404", async ({ page }) => {
      await page.goto(NOT_FOUND_PATH);
      await settle(page);
      await page.screenshot({ path: path.join(OUT, `404-${vp.name}.png`), fullPage: true });
    });

    test.describe("logged out", () => {
      test.use({ storageState: { cookies: [], origins: [] } });
      test("login", async ({ page }) => {
        await page.goto(LOGIN_PATH);
        await settle(page);
        await page.screenshot({ path: path.join(OUT, `login-${vp.name}.png`), fullPage: true });
      });
    });

    if (vp.width < 600) {
      test("mobile-drawer", async ({ page }) => {
        await page.goto(pageUrl("dashboard"));
        await settle(page);
        await page.locator("#menu-toggle").click();
        await page.waitForTimeout(300);
        await page.screenshot({ path: path.join(OUT, `mobile-drawer-${vp.name}.png`) });
      });
    }

    // The guilds page with stubbed guilds (it stays gated without Discord), its card menu and dialogs.
    for (const g of GUILD_SHOTS) {
      test(`guilds ${g.name}`, async ({ page }) => {
        await stubGuilds(page);
        await page.goto(pageUrl("guilds"));
        await settle(page);
        await expect(guildCard(page, FIRST.id)).toBeVisible();
        await g.open(page);
        await page.waitForTimeout(400);
        await page.screenshot({ path: path.join(OUT, `${g.name}-${vp.name}.png`), fullPage: g.fullPage });
      });
    }

    for (const d of DASHBOARD_SHOTS) {
      test(`${d.name}`, async ({ page }) => {
        await stubVersion(page, false);
        await d.stub(page);
        await page.goto(pageUrl("dashboard"));
        await settle(page);
        await d.open(page);
        await page.waitForTimeout(400);
        await page.screenshot({ path: path.join(OUT, `${d.name}-${vp.name}.png`) });
      });
    }

    for (const m of MODALS) {
      test(`modal ${m.name}`, async ({ page }) => {
        await stubClientGuilds(page);
        await page.goto(pageUrl(m.page));
        await settle(page);
        test.setTimeout(20_000);
        await m.open(page);
        await expectOpen(modal(page, m.id));
        await page.waitForTimeout(400);
        await page.screenshot({ path: path.join(OUT, `${m.name}-${vp.name}.png`) });
      });
    }
  });
}
