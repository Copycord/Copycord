import { beforeEach, describe, expect, it } from "vitest";
import { clearLegacyCloneStorage, clearLegacyCloneStorageOnce } from "./legacyStorage";

beforeEach(() => {
  localStorage.clear();
  sessionStorage.clear();
});

function keys(store: Storage): string[] {
  return Array.from({ length: store.length }, (_, i) => store.key(i) ?? "").sort();
}

function seed() {
  localStorage.setItem("bf:running", "[\"1\"]");
  localStorage.setItem("bf:done_tasks", "[]");
  localStorage.setItem("toast:persist:bf:done:1", "{}");
  localStorage.setItem("toast:receipt:bf:done:1", "{}");
  localStorage.setItem("toast:persist:other", "{}");
  localStorage.setItem("theme", "dark");
  sessionStorage.setItem("bf:taskmap", "{}");
  sessionStorage.setItem("bf:cancelled:5", "1");
  sessionStorage.setItem("keep", "1");
}

describe("clearLegacyCloneStorage", () => {
  it("removes the old bf:* and bf toast keys from both storages and keeps the rest", () => {
    seed();
    expect(clearLegacyCloneStorage()).toBe(6);
    expect(keys(localStorage)).toEqual(["theme", "toast:persist:other"]);
    expect(keys(sessionStorage)).toEqual(["keep"]);
    expect(clearLegacyCloneStorage()).toBe(0);
  });

  it("runs only once per page load", () => {
    clearLegacyCloneStorageOnce();
    seed();
    clearLegacyCloneStorageOnce();
    expect(localStorage.getItem("bf:running")).not.toBeNull();
  });
});
