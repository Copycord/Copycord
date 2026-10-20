// Screenshots of the Redeem code modal in each state, both /api/redeem routes mocked.
// Usage: E2E_BASE_URL=http://127.0.0.1:18182 node --experimental-strip-types scripts/roleclaims-shots.ts [out-dir]
// States: menu-hover, idle, waiting, result-claimed, result-already, result-invalid, result-server-down; written as
// roleclaims-<state>.png at 1900x1000. menu-hover is the open side menu with the pointer on the gold Redeem code entry. result-claimed catches the gift animation mid-play. waiting holds the start
// POST unanswered so the spinner stays up.

import fs from "node:fs";
import path from "node:path";
import { chromium, type Browser, type Page } from "@playwright/test";
import { BASE_URL, LOGIN_PATH, PASSWORD } from "../lib/env.ts";

const OUT = path.resolve(process.argv[2] ?? "roleclaims-shots");
const VIEWPORT = { width: 1900, height: 1000 };
const CONFIG = { configured: true };
const CLAIMED = { status: "claimed", message: "Role claimed", reward: "role" };
const CODE = "BETA-2345-ABCD-EFGH";

interface Mock {
  config: unknown;
  /** The start answer, or "hold" to leave it unanswered. */
  start?: { status: number; body: unknown } | "hold";
}

async function login(browser: Browser) {
  const ctx = await browser.newContext({ baseURL: BASE_URL });
  const page = await ctx.newPage();
  await page.goto(LOGIN_PATH);
  await page.locator("#password").fill(PASSWORD);
  await page.locator("#login-form button[type=submit]").click();
  await page.waitForURL((u) => !u.pathname.startsWith("/login"), { timeout: 15_000 });
  const state = await ctx.storageState();
  await ctx.close();
  return state;
}

async function openModal(browser: Browser, state: Awaited<ReturnType<typeof login>>, mock: Mock): Promise<Page> {
  const ctx = await browser.newContext({ baseURL: BASE_URL, viewport: VIEWPORT, storageState: state });
  const page = await ctx.newPage();
  await page.route("**/api/redeem/config", (r) => r.fulfill({ json: mock.config }));
  await page.route("**/api/redeem/start", async (r) => {
    const start = mock.start ?? { status: 200, body: CLAIMED };
    if (start === "hold") return;
    await r.fulfill({ status: start.status, json: start.body });
  });
  await page.goto("/");
  // The side menu is a drawer at every width.
  await page.locator("#menu-toggle").click();
  await page.locator("[data-role-claims]").click();
  await page.getByRole("dialog", { name: "Redeem code" }).waitFor();
  await page.waitForFunction(() => !document.querySelector("#role-claims-modal [aria-busy=true]"));
  return page;
}

async function submit(page: Page, code: string) {
  await page.getByPlaceholder("Enter code").fill(code);
  await page.getByRole("button", { name: "Redeem" }).click();
}

/** The waiting view: the spinner and Verifying... over the faded out form. */
const verifying = (page: Page) => page.getByRole("status").filter({ hasText: "Verifying..." }).waitFor();

/** The result view showing `title`, after the cross-fade. */
const resultView = (page: Page, title: string) =>
  page.locator('[data-role-claims-view="result"]:not([aria-hidden])').getByRole("heading", { name: title }).waitFor();

async function shot(page: Page, name: string, settle = 400) {
  // Let the reveal fade and the cross-fade finish.
  await page.waitForTimeout(settle);
  const file = path.join(OUT, `roleclaims-${name}.png`);
  await page.screenshot({ path: file });
  console.log(`SHOT ${file}`);
  await page.context().close();
}

async function main() {
  fs.mkdirSync(OUT, { recursive: true });
  const browser = await chromium.launch();
  let page: Page;
  try {
    const state = await login(browser);

    const menu = await browser.newContext({ baseURL: BASE_URL, viewport: VIEWPORT, storageState: state });
    page = await menu.newPage();
    await page.goto("/");
    await page.locator("#menu-toggle").click();
    await page.locator("[data-role-claims]").hover();
    await shot(page, "menu-hover");

    page = await openModal(browser, state, { config: CONFIG });
    await shot(page, "idle");

    page = await openModal(browser, state, { config: CONFIG, start: "hold" });
    await submit(page, CODE);
    await verifying(page);
    await shot(page, "waiting");

    page = await openModal(browser, state, { config: CONFIG });
    await submit(page, CODE);
    await resultView(page, "Role claimed");
    // The gift plays once over about 1.7 s: shoot it mid-play, about 1.2 s after its svg is drawn.
    await page.locator("[data-gift-animation] svg").waitFor();
    await shot(page, "result-claimed", 1200);

    page = await openModal(browser, state, {
      config: CONFIG,
      start: { status: 409, body: { detail: "Role already claimed", status: "already" } },
    });
    await submit(page, CODE);
    await resultView(page, "Role already claimed");
    await shot(page, "result-already");

    page = await openModal(browser, state, { config: CONFIG, start: { status: 400, body: { detail: "Invalid code" } } });
    await submit(page, "WRONG-CODE");
    await resultView(page, "Invalid code");
    await shot(page, "result-invalid");

    page = await openModal(browser, state, { config: CONFIG, start: { status: 502, body: { detail: "Server is down" } } });
    await submit(page, CODE);
    await resultView(page, "Server is down");
    await shot(page, "result-server-down");
  } finally {
    await browser.close();
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
