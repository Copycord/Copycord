import { describe, expect, it } from "vitest";
import { displayStatus } from "./model";

describe("displayStatus", () => {
  it("shows every note but a bare stopped, the bare normal ones capitalized", () => {
    expect(displayStatus("ready")).toBe("Ready");
    expect(displayStatus(" READY ")).toBe("Ready");
    expect(displayStatus("active")).toBe("Active");
    expect(displayStatus("stopped")).toBe("");
    expect(displayStatus(" Stopped ")).toBe("");
    expect(displayStatus("STOPPED")).toBe("");
    expect(displayStatus("Stopped by user")).toBe("Stopped by user");
    expect(displayStatus("e2e syncing")).toBe("e2e syncing");
    expect(displayStatus("Logged in as Copycord")).toBe("Logged in as Copycord");
    expect(displayStatus(undefined)).toBe("");
    expect(displayStatus("  ")).toBe("");
    expect(displayStatus("Syncing guild 1 of 2")).toBe("Syncing guild 1 of 2");
    expect(displayStatus("Login failed")).toBe("Login failed");
  });
});
