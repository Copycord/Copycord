import { describe, expect, it } from "vitest";
import {
  addProxies,
  chipResult,
  groupResults,
  maskProxy,
  recheckSplit,
  rotationSeconds,
  settingsPayload,
  splitInterval,
  statusChip,
  toSeconds,
} from "./model";

describe("proxy model", () => {
  it("adds proxies split on lines and commas, without duplicates", () => {
    expect(addProxies(["a:1"], "a:1, b:2\n c:3\r\nb:2")).toEqual(["a:1", "b:2", "c:3"]);
    expect(addProxies([], "  ")).toEqual([]);
  });

  it("describes the status chip", () => {
    expect(statusChip(true, 1)).toEqual({ text: "Enabled · 1 proxy", tone: "on" });
    expect(statusChip(true, 3).text).toBe("Enabled · 3 proxies");
    expect(statusChip(true, 0)).toEqual({ text: "Enabled · no proxies", tone: "warn" });
    expect(statusChip(false, 5)).toEqual({ text: "Disabled", tone: "off" });
  });

  it("converts intervals both ways", () => {
    expect(splitInterval(7200)).toEqual({ value: 2, unit: "3600" });
    expect(splitInterval(5400)).toEqual({ value: 90, unit: "60" });
    expect(splitInterval(10)).toEqual({ value: 1, unit: "60" });
    expect(toSeconds("2", "3600")).toBe(7200);
    expect(toSeconds("", "60")).toBe(60);
    expect(rotationSeconds(false, 30, "60")).toBe(0);
    expect(rotationSeconds(true, 30, "60")).toBe(1800);
    expect(recheckSplit(undefined)).toEqual({ value: 5, unit: "60" });
    expect(recheckSplit(10)).toEqual({ value: 1, unit: "60" });
  });

  it("masks credentials and groups test results by the slow threshold", () => {
    expect(maskProxy("http://u:p@h:1")).toBe("http://***@h:1");
    expect(maskProxy("h:1:u:p")).toBe("h:1:u:p");
    const rows = [
      { proxy: "a", ok: true, ms: 100 },
      { proxy: "b", ok: true, ms: 3000 },
      { proxy: "c", ok: false, error: "timeout" },
    ];
    const g = groupResults(rows, 3000);
    expect([g.good.length, g.slow.length, g.failed.length]).toEqual([1, 1, 1]);
    expect(chipResult(rows[0]!, 3000)).toEqual({ state: "pass", label: "100ms" });
    expect(chipResult(rows[2]!, 3000)).toEqual({ state: "fail", label: "timeout" });
  });

  it("builds the settings payload with integers and a floor of 1", () => {
    expect(
      settingsPayload({ PROXY_SUSPEND_DURATION: "120", PROXY_TEST_BATCH_SIZE: "", PROXY_SLOW_THRESHOLD: 4 }),
    ).toEqual({ PROXY_SUSPEND_DURATION: 120, PROXY_TEST_BATCH_SIZE: 1, PROXY_SLOW_THRESHOLD: 4 });
  });
});
