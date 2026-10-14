// Element dump for the UI parity review: logs in to one running dashboard (old Jinja UI or the SPA),
// visits every screen baseline.spec.ts covers at both viewports, and for the probes in lib/elements.ts
// writes each matched element's text, box and computed styles to <out>/<screen>-<viewport>.json.
// With E2E_SHOTS_DIR set it also saves <screen>-<viewport>.png there, the names baseline.spec.ts uses.
//
// Usage: E2E_BASE_URL=http://127.0.0.1:18181 node --experimental-strip-types scripts/elemdump.ts \
//          --ui old|new --out <dir> [--viewport 1440x900|390x844] [--only <regex>] [--seed-python <venv python>]
// --seed-python seeds the throwaway DB (scripts/seed_db.py, E2E_COPYCORD_HOME) and the API rows from lib/seed.ts.
import fs from "node:fs";
import path from "node:path";
import { chromium, type Browser, type BrowserContext, type Locator, type Page } from "@playwright/test";
import { BASE_URL, E2E_DIR, LOGIN_PATH, NOT_FOUND_PATH, PASSWORD, PAGES, type PageName } from "../lib/env.ts";
import { FWD_CREATE_BTN, MAPPING_EDIT_BTN, MAPPING_FILTERS_BTN, NO_MOTION_CSS } from "../lib/ui.ts";
import { seedApi, seedDb, stubClientGuilds } from "../lib/seed.ts";
import {
  BACKUP_TOKENS, BOTS_RUNNING, CHANNELS, DASHBOARD, DIALOG, FILTERS_MODAL, FORWARDING, FWD_MODAL, GENERIC,
  GUILDS, GUILD_DIALOG, LOGIN, LOGS, LOG_MODAL, MAPPING_MODAL, MENU, NOT_FOUND, PROXY_SETTINGS, SCRAPER, SHELL,
  STYLE_PROPS, SYSTEM, selectorFor, type Flavour, type Probe,
} from "../lib/elements.ts";
import { DETAILS, FIRST, stubGuilds } from "../specs/guilds-fixtures.ts";
import { LOG_LINES, stubLogStream, stubRunning, stubVersion } from "../specs/dashboard-fixtures.ts";
import { CHANNELS as CHANNEL_ROWS, stubChannels } from "../specs/channels-fixtures.ts";

function arg(name: string): string | undefined {
  const i = process.argv.indexOf(`--${name}`);
  return i >= 0 ? process.argv[i + 1] : undefined;
}

const UI = arg("ui") as Flavour | undefined;
if (UI !== "old" && UI !== "new") {
  console.error("usage: elemdump.ts --ui old|new --out <dir> [--viewport WxH] [--only <regex>] [--seed-python <py>]");
  process.exit(2);
}
const OUT = path.resolve(arg("out") ?? `uicmp/dump-${UI}`);
const SHOTS = process.env.E2E_SHOTS_DIR ? path.resolve(E2E_DIR, process.env.E2E_SHOTS_DIR) : null;
const ONLY = arg("only") ? new RegExp(arg("only")!) : null;
const SEED_PY = arg("seed-python");
const VIEWPORTS = [
  { name: "1440x900", width: 1440, height: 900 },
  { name: "390x844", width: 390, height: 844 },
].filter((v) => !arg("viewport") || v.name === arg("viewport"));

// The old UI serves the logs page at /event-logs; the SPA at /logs (PAGES).
function pathOf(name: PageName): string {
  return UI === "old" && name === "event-logs" ? "/event-logs" : PAGES[name];
}

// ---- waiting helpers (no expect: this runs outside the test runner) ----

async function until(what: string, fn: () => Promise<boolean>, timeoutMs = 10_000): Promise<void> {
  const end = Date.now() + timeoutMs;
  while (Date.now() < end) {
    if (await fn().catch(() => false)) return;
    await new Promise((r) => setTimeout(r, 200));
  }
  throw new Error(`timed out waiting for ${what}`);
}

const visible = (l: Locator) => l.first().waitFor({ state: "visible", timeout: 10_000 });

async function settle(page: Page): Promise<void> {
  // Old pages keep SSE and polling open, so networkidle gets a short cap.
  await page.waitForLoadState("networkidle", { timeout: 5_000 }).catch(() => undefined);
  await page.addStyleTag({ content: NO_MOTION_CSS }).catch(() => undefined);
  await page.evaluate(async () => {
    await document.fonts.ready;
  });
  const loader = page.locator("#app-loader");
  if (await loader.count()) await loader.waitFor({ state: "hidden", timeout: 10_000 }).catch(() => undefined);
  await page.waitForTimeout(300);
}

async function guildMenu(page: Page, item?: "View details" | "Export messages"): Promise<void> {
  await page.locator(`#guilds-root article[data-gid="${FIRST.id}"]`).click({ position: { x: 40, y: 40 } });
  await visible(page.getByRole("menuitem", { name: "View details" }));
  if (item) await page.getByRole("menuitem", { name: item }).click();
}

async function openServerLog(page: Page): Promise<void> {
  await page.locator('[data-log="server"]').click();
  await until("log lines", async () => (await page.locator("#log-body").innerText()).includes(LOG_LINES[2]));
}

// ---- screens: the same names and states as baseline.spec.ts ----

interface Screen {
  name: string;
  path: string;
  probes: Probe[];
  fullPage: boolean;
  loggedOut?: boolean;
  mobileOnly?: boolean;
  stub?: (page: Page) => Promise<void>;
  ready?: (page: Page) => Promise<void>;
  open?: (page: Page) => Promise<void>;
}

const PAGE_PROBES: Record<PageName, Probe[]> = {
  dashboard: DASHBOARD,
  channels: CHANNELS,
  guilds: GUILDS,
  forwarding: FORWARDING,
  scraper: SCRAPER,
  system: SYSTEM,
  "event-logs": LOGS,
};

const pfx = (prefix: string, probes: Probe[]): Probe[] => probes.map((p) => ({ ...p, key: `${prefix}.${p.key}` }));
const pageProbes = (name: PageName) => [...pfx("shell", SHELL), ...pfx("page", GENERIC), ...pfx(name, PAGE_PROBES[name])];
const modalProbes = (extra: Probe[]) => [...pfx("modal", DIALOG), ...pfx("modal", extra)];

// An open collapsible dashboard card (details#<id>): its parts paired by text or id, so class names don't matter.
function sectionProbes(id: string): Probe[] {
  const c = `#${id}`;
  const body = `${c} > summary ~ *`;
  return [
    { key: "card", sel: c },
    { key: "summary", sel: `${c} > summary` },
    { key: "body", sel: `${c} > summary + *` },
    { key: "row", sel: `${c} > summary + * > *`, max: 20, pair: "index" },
    { key: "text", sel: `${body} :is(p, span:not(:has(*)), code)`, max: 40, pair: "text" },
    { key: "label", sel: `${body} label`, max: 20, pair: "text" },
    { key: "input", sel: `${c} :is(input:not([type=hidden]):not([type=checkbox]):not([type=radio]), textarea)`, max: 20, pair: "id" },
    { key: "select", sel: `${c} select`, max: 10, pair: "id" },
    { key: "button", sel: `${body} :is(button:not([role=switch]), a.btn)`, max: 20, pair: "text" },
    { key: "switch", sel: `${c} :is(.toggle-track, [role=switch])`, max: 20, pair: "index" },
  ];
}

async function openSection(page: Page, id: string): Promise<void> {
  await page.locator(`#${id} > summary h3`).click();
  await until(`${id} open`, async () => (await page.locator(`#${id}[open]`).count()) > 0);
}

const SCREENS: Screen[] = [
  ...(Object.keys(PAGES) as PageName[]).map((name): Screen => ({
    name,
    path: pathOf(name),
    probes: pageProbes(name),
    fullPage: true,
    // The old forwarding page stays on its loader until /api/client-guilds answers; both sides get the stub.
    stub: name === "forwarding" ? stubClientGuilds : undefined,
    ready: name === "forwarding" ? (p) => visible(p.locator(".fwd-card")) : undefined,
  })),
  { name: "404", path: NOT_FOUND_PATH, probes: [...pfx("shell", SHELL), ...pfx("404", NOT_FOUND)], fullPage: true },
  { name: "login", path: LOGIN_PATH, probes: pfx("login", LOGIN), fullPage: true, loggedOut: true },
  {
    name: "mobile-drawer", path: PAGES.dashboard, probes: pfx("shell", SHELL), fullPage: false, mobileOnly: true,
    open: async (p) => {
      await p.locator("#menu-toggle").click();
      await p.waitForTimeout(300);
    },
  },
  ...[
    { name: "guilds-grid", fullPage: true, probes: GUILDS, open: async () => undefined },
    { name: "guilds-menu", fullPage: false, probes: [...MENU, ...GUILDS], open: (p: Page) => guildMenu(p) },
    { name: "guilds-details", fullPage: false, probes: [...DIALOG, ...GUILD_DIALOG], open: async (p: Page) => {
      await guildMenu(p, "View details");
      await until("guild details", async () => (await p.getByRole("dialog", { name: "Guild details" }).innerText()).includes(DETAILS.owner_id));
    } },
    { name: "guilds-export", fullPage: false, probes: DIALOG, open: async (p: Page) => {
      await guildMenu(p, "Export messages");
      await visible(p.getByRole("dialog", { name: "Export messages" }));
    } },
  ].map((g): Screen => ({
    name: g.name, path: PAGES.guilds, fullPage: g.fullPage, probes: pfx(g.name, g.probes), stub: stubGuilds,
    ready: (p) => visible(p.locator(`#guilds-root article[data-gid="${FIRST.id}"]`)), open: g.open,
  })),
  ...[
    { name: "dashboard-bots-running", stub: stubRunning, probes: BOTS_RUNNING, open: async () => undefined },
    { name: "dashboard-log-modal", stub: stubLogStream, probes: [...DIALOG, ...LOG_MODAL], open: openServerLog },
    { name: "dashboard-log-search", stub: stubLogStream, probes: [...DIALOG, ...LOG_MODAL], open: async (p: Page) => {
      await openServerLog(p);
      await p.locator("#log-search-input").fill("beta");
      await until("log filter", async () => !(await p.locator("#log-body").innerText()).includes(LOG_LINES[0]));
    } },
  ].map((d): Screen => ({
    name: d.name, path: PAGES.dashboard, fullPage: false, probes: pfx(d.name, d.probes),
    stub: async (p) => {
      await stubVersion(p, false);
      await d.stub(p);
    },
    open: d.open,
  })),
  ...[
    { name: "channels-tree", fullPage: true, open: async () => undefined },
    { name: "channels-search", fullPage: true, open: async (p: Page) => {
      await p.locator("#ch-search").fill("i");
      await p.locator("#ch-sort").selectOption("category", { force: true });
    } },
    { name: "channels-empty", fullPage: false, open: async (p: Page) => {
      await p.locator("#ch-search").fill("zzz-nothing");
      await visible(p.locator("#channels-empty"));
    } },
  ].map((c): Screen => ({
    name: c.name, path: PAGES.channels, fullPage: c.fullPage, probes: [...pfx("page", GENERIC), ...pfx("channels", CHANNELS)],
    stub: (p) => stubChannels(p),
    ready: (p) => until("channel cards", async () => (await p.locator("#channels-root .ch-card[data-cid]").count()) === CHANNEL_ROWS.length),
    open: c.open,
  })),
  ...[
    { name: "dashboard-backup-tokens", page: "dashboard" as PageName, extra: BACKUP_TOKENS,
      open: (p: Page) => p.locator("#backupTokensBtn").click() },
    { name: "dashboard-proxy-settings", page: "dashboard" as PageName, extra: PROXY_SETTINGS,
      open: (p: Page) => p.locator("#srv-proxy-settings-btn").click() },
    { name: "dashboard-proxy-fav-help", page: "dashboard" as PageName, extra: [],
      open: (p: Page) => p.locator("#srv-proxy-fav-help-btn").click() },
    { name: "dashboard-mapping-settings", page: "dashboard" as PageName, extra: MAPPING_MODAL,
      open: (p: Page) => p.locator(MAPPING_EDIT_BTN).first().click() },
    { name: "dashboard-filters", page: "dashboard" as PageName, extra: FILTERS_MODAL,
      open: (p: Page) => p.locator(MAPPING_FILTERS_BTN).first().click() },
    { name: "forwarding-new-rule", page: "forwarding" as PageName, extra: FWD_MODAL,
      open: (p: Page) => p.locator(FWD_CREATE_BTN).filter({ visible: true }).first().click() },
  ].map((m): Screen => ({
    name: m.name, path: pathOf(m.page), fullPage: false, probes: modalProbes(m.extra), stub: stubClientGuilds,
    open: async (p) => {
      await m.open(p);
      await until("dialog", async () => (await p.locator("[role=dialog]").filter({ visible: true }).count()) > 0);
    },
  })),
  ...[
    { name: "dashboard-notifications-open", id: "notifications-card" },
    { name: "dashboard-sync-open", id: "sync-settings-card" },
    { name: "dashboard-proxy-open", id: "srv-proxy-card" },
  ].map((d): Screen => ({
    name: d.name, path: PAGES.dashboard, fullPage: true, probes: pfx(d.name, sectionProbes(d.id)),
    open: (p) => openSection(p, d.id),
  })),
];

// ---- in-page measuring ----

/** Mark the content column and the open dialog box so probes can be scoped the same way on both sides. */
async function mark(page: Page): Promise<void> {
  await page.evaluate((ui) => {
    for (const el of Array.from(document.querySelectorAll("[data-uicmp-root], [data-uicmp-dialog]"))) {
      el.removeAttribute("data-uicmp-root");
      el.removeAttribute("data-uicmp-dialog");
    }
    const shown = (el: Element) => {
      const r = el.getBoundingClientRect();
      return r.width > 0 && r.height > 0 && getComputedStyle(el).visibility !== "hidden";
    };
    const root = ui === "old" ? document.querySelector(".container, main.page-main") : document.querySelector("main");
    root?.setAttribute("data-uicmp-root", "");
    const dialogs = Array.from(document.querySelectorAll("[role=dialog]")).filter(shown);
    let box: Element | undefined = dialogs[dialogs.length - 1];
    if (box) {
      const r = box.getBoundingClientRect();
      // Old guild and channel dialogs put role=dialog on the full-screen overlay; measure the card inside.
      if (r.width >= innerWidth - 2 && r.height >= innerHeight - 2) {
        const inner =
          Array.from(box.querySelectorAll(".modal-content, .modal-card")).find(shown) ??
          Array.from(box.querySelectorAll("*")).find(
            (e) => shown(e) && e.getBoundingClientRect().width < innerWidth - 2 &&
              getComputedStyle(e).backgroundColor !== "rgba(0, 0, 0, 0)",
          );
        if (inner) box = inner;
      }
      box.setAttribute("data-uicmp-dialog", "");
    }
  }, UI);
}

interface Measured {
  probe: string;
  key: string;
  selector: string;
  index: number;
  text: string;
  visible: boolean;
  box: { x: number; y: number; w: number; h: number };
  styles: Record<string, string>;
}

async function measure(page: Page, probe: Probe): Promise<Measured[]> {
  const selector = selectorFor(probe, UI!);
  if (!selector) return [];
  const opts = { max: probe.max ?? 0, pair: probe.pair ?? "index", pick: probe.pick ?? "first", props: [...STYLE_PROPS] };
  const rows = await page.locator(selector).evaluateAll((els, o) => {
    const shown = (el: Element) => {
      const r = el.getBoundingClientRect();
      const cs = getComputedStyle(el);
      return r.width > 0 && r.height > 0 && cs.visibility !== "hidden" && cs.display !== "none";
    };
    const clean = (s: string | null | undefined) => (s ?? "").replace(/\s+/g, " ").trim();
    const textOf = (el: Element) => {
      const h = el as HTMLElement & { value?: string; placeholder?: string; type?: string };
      return clean(h.innerText) || clean(el.getAttribute("aria-label")) || clean(el.getAttribute("title")) ||
        clean(h.placeholder) || (typeof h.value === "string" && h.type !== "password" ? clean(h.value) : "");
    };
    let list = els;
    if (o.max > 0) list = els.filter(shown).slice(0, o.max);
    else if (o.pick === "visible") list = els.filter(shown).slice(0, 1);
    else if (o.pick === "last") list = els.slice(-1);
    else list = els.slice(0, 1);
    // Tailwind colors compute to oklch(); a canvas serializes any color as #rrggbb or rgba().
    const cv = document.createElement("canvas").getContext("2d")!;
    const srgb = (s: string) =>
      s.replace(/\b(oklch|oklab|lab|lch|hwb|color)\([^()]*\)/g, (c) => {
        cv.fillStyle = "#000";
        cv.fillStyle = c;
        return String(cv.fillStyle);
      });
    const seen = new Map<string, number>();
    return list.map((el, i) => {
      let k = "";
      if (o.pair === "text") k = textOf(el).slice(0, 40).toLowerCase();
      else if (o.pair === "id") k = el.id || el.getAttribute("name") || el.getAttribute("placeholder") || "";
      else if (o.pair === "heading") k = textOf(el.querySelector("h1, h2, h3, h4, h5, legend, .modal-title-main") ?? el).slice(0, 40).toLowerCase();
      else if (o.pair === "label") k = textOf(el.querySelector("label") ?? el).slice(0, 40).toLowerCase();
      else if (o.pair.startsWith("attr:")) k = el.getAttribute(o.pair.slice(5)) ?? "";
      if (!k) k = `#${i}`;
      const n = (seen.get(k) ?? 0) + 1;
      seen.set(k, n);
      if (n > 1) k = `${k} (${n})`;
      const r = el.getBoundingClientRect();
      const cs = getComputedStyle(el);
      const styles: Record<string, string> = {};
      for (const p of o.props) styles[p] = srgb(cs.getPropertyValue(p));
      return {
        key: o.max > 0 ? k : "",
        index: i,
        text: textOf(el).slice(0, 40),
        visible: shown(el),
        box: { x: r.x + scrollX, y: r.y + scrollY, w: r.width, h: r.height },
        styles,
      };
    });
  }, opts);
  return rows.map((r) => ({ probe: probe.key, selector, ...r, key: r.key ? `${probe.key} [${r.key}]` : probe.key }));
}

// ---- run ----

async function login(browser: Browser): Promise<Awaited<ReturnType<BrowserContext["storageState"]>>> {
  const ctx = await browser.newContext({ baseURL: BASE_URL });
  const page = await ctx.newPage();
  await page.goto(LOGIN_PATH);
  await page.locator("#password").fill(PASSWORD);
  await page.locator("#login-form button[type=submit]").click();
  await page.waitForURL((u) => !u.pathname.startsWith("/login"), { timeout: 15_000 });
  if (SEED_PY) {
    seedDb(SEED_PY);
    await seedApi(ctx.request);
  }
  const state = await ctx.storageState();
  await ctx.close();
  return state;
}

async function main(): Promise<void> {
  fs.mkdirSync(OUT, { recursive: true });
  if (SHOTS) fs.mkdirSync(SHOTS, { recursive: true });
  const browser = await chromium.launch();
  const state = await login(browser);
  const index: Array<{ screen: string; viewport: string; elements: number; missing: number; error?: string }> = [];

  for (const vp of VIEWPORTS) {
    for (const s of SCREENS) {
      if (s.mobileOnly && vp.width >= 600) continue;
      if (ONLY && !ONLY.test(s.name)) continue;
      const started = Date.now();
      const ctx = await browser.newContext({
        baseURL: BASE_URL,
        viewport: { width: vp.width, height: vp.height },
        storageState: s.loggedOut ? { cookies: [], origins: [] } : state,
        reducedMotion: "reduce",
        timezoneId: "UTC",
        locale: "en-US",
      });
      ctx.setDefaultTimeout(15_000);
      const page = await ctx.newPage();
      const file = `${s.name}-${vp.name}`;
      try {
        if (s.stub) await s.stub(page);
        await page.goto(s.path);
        await settle(page);
        if (s.ready) await s.ready(page);
        if (s.open) await s.open(page);
        await page.waitForTimeout(400);
        await mark(page);
        const elements: Measured[] = [];
        const missing: string[] = [];
        const errors: string[] = [];
        for (const p of s.probes) {
          try {
            const got = await measure(page, p);
            if (got.length) elements.push(...got);
            else missing.push(p.key);
          } catch (e) {
            errors.push(`${p.key}: ${String(e).split("\n")[0]}`);
          }
        }
        const dump = { ui: UI, base: BASE_URL, screen: s.name, viewport: vp.name, url: page.url(), capturedAt: new Date().toISOString(), elements, missing, errors };
        fs.writeFileSync(path.join(OUT, `${file}.json`), JSON.stringify(dump, null, 1) + "\n");
        if (SHOTS) await page.screenshot({ path: path.join(SHOTS, `${file}.png`), fullPage: s.fullPage });
        index.push({ screen: s.name, viewport: vp.name, elements: elements.length, missing: missing.length });
        console.log(`[elemdump ${UI}] ${file}: ${elements.length} elements, ${missing.length} probes unmatched, ${errors.length} errors, ${Date.now() - started} ms`);
      } catch (e) {
        const error = String(e).split("\n")[0]!;
        index.push({ screen: s.name, viewport: vp.name, elements: 0, missing: 0, error });
        console.log(`[elemdump ${UI}] ${file}: SKIPPED ${error}`);
      } finally {
        await ctx.close();
      }
    }
  }
  await browser.close();
  const indexFile = path.join(OUT, `index-${VIEWPORTS.map((v) => v.name).join("+")}${ONLY ? "-partial" : ""}.json`);
  fs.writeFileSync(indexFile, JSON.stringify(index, null, 1) + "\n");
  const total = index.reduce((n, r) => n + r.elements, 0);
  console.log(`[elemdump ${UI}] ${index.length} screens, ${index.filter((r) => r.error).length} skipped, ${total} elements -> ${OUT}`);
}

await main();
