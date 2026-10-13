import fs from "node:fs";
import path from "node:path";
import { test, type Page } from "@playwright/test";
import { BASE_PATH, E2E_DIR, LOGIN_PATH, NOT_FOUND_PATH, PAGES, pageUrl, type PageName } from "../lib/env.ts";
import { FWD_CREATE_BTN, MAPPING_EDIT_BTN, MAPPING_FILTERS_BTN, expectOpen, modal, settle } from "../lib/ui.ts";
import { stubClientGuilds } from "../lib/seed.ts";

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
