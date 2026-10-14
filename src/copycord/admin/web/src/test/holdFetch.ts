// Wraps the stubbed fetch so requests whose path starts with `prefix` wait until release(),
// which lets a test see the loading skeleton before the data lands.
import { vi } from "vitest";

export function holdFetch(prefix: string): () => void {
  const inner = globalThis.fetch;
  let release!: () => void;
  const gate = new Promise<void>((r) => (release = r));
  vi.stubGlobal("fetch", async (input: RequestInfo | URL, init?: RequestInit) => {
    if (new URL(String(input), "http://x").pathname.startsWith(prefix)) await gate;
    return inner(input, init);
  });
  return release;
}
