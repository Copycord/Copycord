import { describe, expect, it } from "vitest";
import { displayStatus } from "./model";

describe("displayStatus", () => {
  it("hides the normal notes and keeps the exceptions", () => {
    expect(displayStatus("ready")).toBe("");
    expect(displayStatus(" Ready ")).toBe("");
    expect(displayStatus("active")).toBe("");
    expect(displayStatus("stopped")).toBe("");
    expect(displayStatus(undefined)).toBe("");
    expect(displayStatus("Syncing guild 1 of 2")).toBe("Syncing guild 1 of 2");
    expect(displayStatus("Login failed")).toBe("Login failed");
  });
});
