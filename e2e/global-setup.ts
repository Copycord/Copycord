import { execFileSync, spawn } from "node:child_process";
import fs from "node:fs";
import net from "node:net";
import path from "node:path";
import { chromium, request } from "@playwright/test";
import {
  BASE_URL, HOME, LOGIN_PATH, PASSWORD, PORT, REPO_DIR, RUN_DIR, RUNTIME_FILE, STATE_FILE,
} from "./lib/env.ts";
import { seedApi, seedDb } from "./lib/seed.ts";

const VENV = process.env.E2E_VENV ?? path.join(REPO_DIR, ".venv");

function sh(cmd: string, args: string[], cwd: string, env: NodeJS.ProcessEnv = process.env): void {
  console.log(`[e2e] ${cmd} ${args.join(" ")}`);
  execFileSync(cmd, args, { cwd, stdio: "inherit", env });
}

async function freePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const srv = net.createServer();
    srv.once("error", reject);
    srv.listen(0, "127.0.0.1", () => {
      const addr = srv.address();
      const port = typeof addr === "object" && addr ? addr.port : 0;
      srv.close(() => resolve(port));
    });
  });
}

async function waitHealthy(timeoutMs: number): Promise<void> {
  const until = Date.now() + timeoutMs;
  let last = "";
  while (Date.now() < until) {
    try {
      const r = await fetch(`${BASE_URL}/health`);
      if (r.ok) return;
      last = `HTTP ${r.status}`;
    } catch (e) {
      last = String(e);
    }
    await new Promise((r) => setTimeout(r, 500));
  }
  throw new Error(`dashboard not healthy at ${BASE_URL}/health: ${last}`);
}

export default async function globalSetup(): Promise<void> {
  if (process.env.E2E_SKIP_SERVER === "1") return;
  fs.mkdirSync(RUN_DIR, { recursive: true });

  if (process.env.E2E_SKIP_BUILD !== "1") {
    const fe = path.join(REPO_DIR, "src", "copycord", "admin", "frontend");
    sh("npm", ["ci", "--no-audit", "--no-fund"], fe);
    sh("npm", ["run", "build"], fe);
  }
  const py = path.join(VENV, "bin", "python");
  if (!fs.existsSync(path.join(VENV, "bin", "copycord"))) {
    sh("python3", ["-m", "venv", VENV], REPO_DIR);
    sh(py, ["-m", "pip", "install", "-q", "-e", "."], REPO_DIR, { ...process.env, COPYCORD_SKIP_FRONTEND: "1" });
  }

  fs.rmSync(HOME, { recursive: true, force: true });
  fs.mkdirSync(HOME, { recursive: true });
  const ports = {
    CONTROL_PORT_SERVER: await freePort(),
    CONTROL_PORT_CLIENT: await freePort(),
    SERVER_WS_PORT: await freePort(),
    CLIENT_WS_PORT: await freePort(),
  };
  const envLines = [
    `PASSWORD=${PASSWORD}`,
    "DASHBOARD_HOST=127.0.0.1",
    `ADMIN_PORT=${PORT}`,
    ...Object.entries(ports).map(([k, v]) => `${k}=${v}`),
    "COPYCORD_AUTOSTART=false",
  ];
  fs.writeFileSync(path.join(HOME, ".env"), envLines.join("\n") + "\n", { mode: 0o600 });

  const childEnv: NodeJS.ProcessEnv = {
    ...process.env,
    COPYCORD_HOME: HOME,
    COPYCORD_ALLOW_ROOT: "1",
    PASSWORD,
    DASHBOARD_HOST: "127.0.0.1",
    ADMIN_PORT: String(PORT),
    ...Object.fromEntries(Object.entries(ports).map(([k, v]) => [k, String(v)])),
  };
  for (const k of ["SERVER_TOKEN", "CLIENT_TOKEN", "DISCORD_TOKEN"]) delete childEnv[k];

  const log = fs.openSync(path.join(RUN_DIR, "copycord.log"), "w");
  const child = spawn(path.join(VENV, "bin", "copycord"), ["run"], {
    cwd: HOME,
    env: childEnv,
    detached: true,
    stdio: ["ignore", log, log],
  });
  child.unref();
  fs.writeFileSync(RUNTIME_FILE, JSON.stringify({ pid: child.pid, home: HOME, port: PORT }));
  console.log(`[e2e] copycord run pid ${child.pid} on ${BASE_URL}, log ${path.join(RUN_DIR, "copycord.log")}`);

  await waitHealthy(90_000);
  seedDb(py);

  fs.mkdirSync(path.dirname(STATE_FILE), { recursive: true });
  const browser = await chromium.launch();
  const page = await browser.newPage({ baseURL: BASE_URL });
  await page.goto(LOGIN_PATH);
  await page.locator("#password").fill(PASSWORD);
  await page.locator("#login-form button[type=submit]").click();
  await page.waitForURL((u) => !u.pathname.startsWith("/login"), { timeout: 15_000 });
  await page.context().storageState({ path: STATE_FILE });
  await browser.close();

  const api = await request.newContext({ baseURL: BASE_URL, storageState: STATE_FILE });
  await seedApi(api);
  await api.dispose();
}
