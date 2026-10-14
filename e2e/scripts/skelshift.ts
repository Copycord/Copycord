// Skeleton versus loaded geometry per page, and the layout shift score of a cold load.
// Usage: E2E_BASE_URL=http://127.0.0.1:18182 node --experimental-strip-types scripts/skelshift.ts [path ...]
// SEED_PY=<venv python> seeds the throwaway DB and API rows first. THROTTLE=rtt_ms,kbps,cpu as in loadtime.ts.
// Skeleton state: every /api request is held until the page has painted its loading state, then released.
// Blocks are the page column's children two levels deep plus every card (a bordered, rounded box over 200 px
// wide), matched by order between the two states.

import { chromium, type Browser, type Page, type Route } from "@playwright/test";
import { BASE_URL, LOGIN_PATH, PASSWORD } from "../lib/env.ts";
import { seedApi, seedDb } from "../lib/seed.ts";

const PATHS = process.argv.slice(2).length
  ? process.argv.slice(2)
  : ["/", "/channels", "/guilds", "/forwarding", "/scraper", "/system", "/logs"];
const THROTTLE = process.env.THROTTLE ? process.env.THROTTLE.split(",").map(Number) : null;

interface Box {
  kind: string;
  label: string;
  x: number;
  y: number;
  w: number;
  h: number;
}

async function login(browser: Browser) {
  const ctx = await browser.newContext({ baseURL: BASE_URL });
  const page = await ctx.newPage();
  await page.goto(LOGIN_PATH);
  await page.locator("#password").fill(PASSWORD);
  await page.locator("#login-form button[type=submit]").click();
  await page.waitForURL((u) => !u.pathname.startsWith("/login"), { timeout: 15_000 });
  if (process.env.SEED_PY) {
    seedDb(process.env.SEED_PY);
    await seedApi(ctx.request);
  }
  const state = await ctx.storageState();
  await ctx.close();
  return state;
}

async function throttle(page: Page) {
  if (!THROTTLE) return;
  const [rtt, kbps, cpu] = THROTTLE as [number, number, number];
  const cdp = await page.context().newCDPSession(page);
  await cdp.send("Network.enable");
  await cdp.send("Network.emulateNetworkConditions", {
    offline: false,
    latency: rtt,
    downloadThroughput: (kbps * 1024) / 8,
    uploadThroughput: (kbps * 1024) / 8,
  });
  await cdp.send("Emulation.setCPUThrottlingRate", { rate: cpu });
}

function blocks(page: Page): Promise<Box[]> {
  return page.evaluate(() => {
    const main = document.querySelector("main");
    if (!main) return [];
    const out: Array<{ kind: string; label: string; x: number; y: number; w: number; h: number }> = [];
    const add = (kind: string, el: Element) => {
      const r = el.getBoundingClientRect();
      if (r.width < 2 || r.height < 2) return;
      const label = `${el.tagName.toLowerCase()} ${(el.textContent ?? "").trim().replace(/\s+/g, " ").slice(0, 24)}`;
      out.push({ kind, label, x: r.x, y: r.y + scrollY, w: r.width, h: r.height });
    };
    for (const a of Array.from(main.children)) {
      add("d1", a);
      for (const b of Array.from(a.children)) add("d2", b);
    }
    for (const el of Array.from(main.querySelectorAll("*"))) {
      const s = getComputedStyle(el);
      if (parseFloat(s.borderTopWidth) > 0 && parseFloat(s.borderTopLeftRadius) >= 8 && el.getBoundingClientRect().width > 200)
        add("card", el);
    }
    return out;
  });
}

async function states(browser: Browser, state: Awaited<ReturnType<typeof login>>, path: string) {
  const ctx = await browser.newContext({ baseURL: BASE_URL, storageState: state, viewport: { width: 1440, height: 900 } });
  const page = await ctx.newPage();
  const held: Route[] = [];
  let hold = true;
  await page.route(/\/(api\/|version)/, (r) => (hold ? void held.push(r) : void r.continue()));
  await page.goto(path, { waitUntil: "domcontentloaded" });
  await page.waitForTimeout(1500);
  const skel = await blocks(page);
  hold = false;
  for (const r of held.splice(0)) await r.continue().catch(() => {});
  await page.waitForLoadState("networkidle").catch(() => {});
  await page.waitForTimeout(1500);
  const loaded = await blocks(page);
  await ctx.close();
  return { skel, loaded };
}

async function cls(browser: Browser, state: Awaited<ReturnType<typeof login>>, path: string) {
  const ctx = await browser.newContext({ baseURL: BASE_URL, storageState: state, viewport: { width: 1440, height: 900 } });
  const page = await ctx.newPage();
  await throttle(page);
  await page.addInitScript(() => {
    const w = window as unknown as { __cls: number };
    w.__cls = 0;
    new PerformanceObserver((l) => {
      for (const e of l.getEntries() as Array<PerformanceEntry & { value: number; hadRecentInput: boolean }>)
        if (!e.hadRecentInput) w.__cls += e.value;
    }).observe({ type: "layout-shift", buffered: true });
  });
  await page.goto(path, { waitUntil: "networkidle" });
  await page.waitForTimeout(2000);
  const v = await page.evaluate(() => (window as unknown as { __cls: number }).__cls);
  await ctx.close();
  return v;
}

const r0 = (n: number) => Math.round(n);

async function main(): Promise<void> {
  const browser = await chromium.launch();
  const state = await login(browser);
  await cls(browser, state, "/");
  console.log(`SKELSHIFT ${BASE_URL} throttle=${process.env.THROTTLE ?? "none"}`);
  for (const path of PATHS) {
    const { skel, loaded } = await states(browser, state, path);
    let differ = 0;
    let max = 0;
    const lines: string[] = [];
    for (const kind of ["d1", "d2", "card"]) {
      const a = skel.filter((b) => b.kind === kind);
      const b = loaded.filter((x) => x.kind === kind);
      lines.push(`  ${kind}: skeleton ${a.length} loaded ${b.length}`);
      for (let i = 0; i < Math.max(a.length, b.length); i++) {
        const s = a[i];
        const l = b[i];
        const d = s && l ? Math.max(Math.abs(s.x - l.x), Math.abs(s.y - l.y), Math.abs(s.w - l.w), Math.abs(s.h - l.h)) : Infinity;
        if (d > 2) differ++;
        if (Number.isFinite(d)) max = Math.max(max, d);
        const fmt = (x?: Box) => (x ? `${r0(x.x)},${r0(x.y)} ${r0(x.w)}x${r0(x.h)} ${x.label}` : "-");
        if (d > 2 || process.env.VERBOSE) lines.push(`    ${kind}[${i}] d=${Number.isFinite(d) ? r0(d) : "missing"} skel ${fmt(s)} | loaded ${fmt(l)}`);
      }
    }
    const c = await cls(browser, state, path);
    console.log(`PAGE ${path} differ=${differ} maxdelta=${r0(max)} cls=${c.toFixed(4)}`);
    for (const l of lines) console.log(l);
  }
  await browser.close();
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
