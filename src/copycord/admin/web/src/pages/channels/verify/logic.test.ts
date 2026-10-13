import { describe, expect, it, vi } from "vitest";
import { parseOrphans, verifyStatusText } from "./logic";
import { createOrphanStore } from "./orphanStore";

describe("orphans event", () => {
  it("reads categories and channels", () => {
    const o = parseOrphans({ type: "orphans", mapping_id: "m1", categories: [{ id: "1", name: "c" }], channels: [{ id: "2", name: "x" }] }, "m1");
    expect(o?.categories).toHaveLength(1);
    expect(o?.channels[0]?.id).toBe("2");
  });
  it("ignores another mapping and other types", () => {
    expect(parseOrphans({ type: "orphans", mapping_id: "m2" }, "m1")).toBeNull();
    expect(parseOrphans({ type: "deleted" } as never, "m1")).toBeNull();
    expect(parseOrphans({ type: "orphans" }, "m1")).toEqual({ categories: [], channels: [] });
  });
});

it("status text", () => {
  const none = { categories: [], channels: [] };
  expect(verifyStatusText(none, false, false)).toBe("Press “Fetch orphans” to scan.");
  expect(verifyStatusText(none, true, false)).toBe("All channels and categories match the last sitemap.");
  expect(verifyStatusText(none, true, true)).toBe("Scanning…");
  expect(verifyStatusText({ categories: [{ id: "1", name: "a" }], channels: [] }, true, false)).toBe("Found 1 orphan category and 0 orphan channels.");
});

it("orphan store notifies on change and accepts updaters", () => {
  const s = createOrphanStore();
  const fn = vi.fn();
  s.subscribe(fn);
  s.set({ categories: [], channels: [{ id: "1", name: "a" }] });
  s.set((cur) => ({ ...cur, channels: [] }));
  expect(fn).toHaveBeenCalledTimes(2);
  expect(s.get().channels).toEqual([]);
});
