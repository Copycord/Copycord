// Dashboard load timing in a real browser: first paint, entry script fetched and run, every /api request
// relative to navigation start, and when the last loading placeholder (spinner or skeleton) cleared.
// Usage: E2E_BASE_URL=http://127.0.0.1:18182 node --experimental-strip-types scripts/loadtime.ts [path] [runs]
// SEED_PY=<venv python> seeds the throwaway DB and API rows first (as elemdump.ts does).
// Each run uses a fresh context (cold HTTP cache) with the logged in session; the median run is printed in full.

import { chromium, type Browser } from "@playwright/test";
import { BASE_URL, LOGIN_PATH, PASSWORD } from "../lib/env.ts";
import { seedApi, seedDb } from "../lib/seed.ts";

const PATH = process.argv[2] ?? "/";
const RUNS = Number(process.argv[3] ?? "5");
const BUSY = ".animate-spin, [data-skeleton], [aria-busy=true]";

interface Run {
  fp: number;
  fcp: number;
  rootFilled: number;
  scripts: Array<{ name: string; start: number; end: number; size: number }>;
  api: Array<{ name: string; start: number; end: number }>;
  busyCleared: number;
  settled: number;
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

async function once(browser: Browser, state: Awaited<ReturnType<typeof login>>): Promise<Run> {
  const ctx = await browser.newContext({ baseURL: BASE_URL, storageState: state, viewport: { width: 1440, height: 900 } });
  const page = await ctx.newPage();
  await page.addInitScript((busySel: string) => {
    const w = window as unknown as { __lt: { rootFilled: number; busyCleared: number; sawBusy: boolean } };
    w.__lt = { rootFilled: 0, busyCleared: 0, sawBusy: false };
    const tick = () => {
      const root = document.getElementById("root");
      if (!w.__lt.rootFilled && root && root.childElementCount > 0) w.__lt.rootFilled = performance.now();
      const busy = !!document.querySelector(busySel);
      if (busy) w.__lt.sawBusy = true;
      else if (w.__lt.sawBusy) {
        w.__lt.busyCleared = performance.now();
        w.__lt.sawBusy = false;
      }
      if (performance.now() < 10_000) requestAnimationFrame(tick);
    };
    requestAnimationFrame(tick);
  }, BUSY);
  await page.goto(PATH, { waitUntil: "networkidle" });
  await page.waitForTimeout(1500);
  const r = await page.evaluate(() => {
    const w = window as unknown as { __lt: { rootFilled: number; busyCleared: number } };
    const paint = Object.fromEntries(performance.getEntriesByType("paint").map((p) => [p.name, p.startTime]));
    const res = performance.getEntriesByType("resource") as PerformanceResourceTiming[];
    const short = (n: string) => n.replace(location.origin, "");
    return {
      fp: paint["first-paint"] ?? 0,
      fcp: paint["first-contentful-paint"] ?? 0,
      rootFilled: w.__lt.rootFilled,
      busyCleared: w.__lt.busyCleared,
      scripts: res
        .filter((e) => e.initiatorType === "script" || e.name.endsWith(".js"))
        .map((e) => ({ name: short(e.name), start: e.startTime, end: e.responseEnd, size: e.encodedBodySize })),
      api: res
        .filter((e) => e.name.includes("/api/"))
        .map((e) => ({ name: short(e.name), start: e.startTime, end: e.responseEnd })),
    };
  });
  await ctx.close();
  const lastApi = Math.max(0, ...r.api.map((a) => a.end));
  return { ...r, settled: Math.max(lastApi, r.busyCleared, r.rootFilled) };
}

const f = (n: number) => n.toFixed(0).padStart(5);

async function main(): Promise<void> {
  const browser = await chromium.launch();
  const state = await login(browser);
  await once(browser, state); // warm the server
  const runs: Run[] = [];
  for (let i = 0; i < RUNS; i++) runs.push(await once(browser, state));
  await browser.close();
  const med = (k: keyof Run) => [...runs].map((r) => r[k] as number).sort((a, b) => a - b)[Math.floor(RUNS / 2)]!;
  console.log(`LOADTIME ${BASE_URL}${PATH} runs=${RUNS} (median ms from navigation start)`);
  console.log(`first-paint ${f(med("fp"))}  fcp ${f(med("fcp"))}  root-rendered ${f(med("rootFilled"))}  busy-cleared ${f(med("busyCleared"))}  settled ${f(med("settled"))}`);
  const m = [...runs].sort((a, b) => a.settled - b.settled)[Math.floor(RUNS / 2)]!;
  console.log("median run detail:");
  for (const s of m.scripts) console.log(`  js  ${f(s.start)} -> ${f(s.end)}  ${s.size}B  ${s.name}`);
  for (const a of [...m.api].sort((x, y) => x.start - y.start)) console.log(`  api ${f(a.start)} -> ${f(a.end)}  ${a.name}`);
  console.log(`  root ${f(m.rootFilled)}  busy-cleared ${f(m.busyCleared)}  settled ${f(m.settled)}`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
