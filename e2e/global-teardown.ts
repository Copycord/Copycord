import fs from "node:fs";
import { RUNTIME_FILE } from "./lib/env.ts";

function alive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}

export default async function globalTeardown(): Promise<void> {
  if (process.env.E2E_KEEP_SERVER === "1" || !fs.existsSync(RUNTIME_FILE)) return;
  const { pid } = JSON.parse(fs.readFileSync(RUNTIME_FILE, "utf8")) as { pid: number };
  // The server was spawned detached, so -pid is its whole process group.
  try {
    process.kill(-pid, "SIGTERM");
  } catch {
    /* already gone */
  }
  const until = Date.now() + 20_000;
  while (alive(pid) && Date.now() < until) await new Promise((r) => setTimeout(r, 250));
  if (alive(pid)) {
    try {
      process.kill(-pid, "SIGKILL");
    } catch {
      /* already gone */
    }
  }
  fs.rmSync(RUNTIME_FILE, { force: true });
}
