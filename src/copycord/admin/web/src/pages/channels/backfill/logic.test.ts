import { beforeEach, describe, expect, it } from "vitest";
import {
  batchBody,
  cancelText,
  claimToast,
  EMPTY_FORM,
  endIso,
  eventChannelId,
  progressLabel,
  progressOf,
  progressPct,
  queueStates,
  resumeBody,
  startBody,
  stageForProgress,
  validateForm,
} from "./logic";
import type { BackfillForm } from "./logic";

const form = (over: Partial<BackfillForm>): BackfillForm => ({ ...EMPTY_FORM, ...over });

beforeEach(() => localStorage.clear());

describe("start body", () => {
  it("all history sends only the mode", () => {
    expect(startBody("10", "m1", form({}))).toEqual({ channel_id: "10", mapping_id: "m1", mode: "all" });
  });
  it("since sends since with the time, midnight by default", () => {
    expect(startBody("10", "m1", form({ mode: "since", sinceDate: "2025-03-04", sinceTime: "13:05" })).since).toBe("2025-03-04T13:05");
    expect(startBody("10", "m1", form({ mode: "since", sinceDate: "2025-03-04" })).since).toBe("2025-03-04T00:00");
  });
  it("between sends since and the end of the to day", () => {
    const b = startBody("10", "m1", form({ mode: "between", fromDate: "2025-03-04", toDate: "2025-03-31" }));
    expect(b).toMatchObject({ mode: "between", since: "2025-03-04T00:00", before_iso: "2025-04-01T00:00" });
  });
  it("last sends last_n as a number and ignore_cloned only when set", () => {
    expect(startBody("10", "m1", form({ mode: "last", lastN: "50", ignoreCloned: true }))).toEqual({
      channel_id: "10",
      mapping_id: "m1",
      mode: "last",
      last_n: 50,
      ignore_cloned: true,
    });
  });
});

describe("batch body", () => {
  it("uses after_iso and resume false", () => {
    expect(batchBody(["1", "2"], "m1", form({ mode: "since", sinceDate: "2025-01-02", ignoreCloned: true }))).toEqual({
      channel_ids: ["1", "2"],
      mapping_id: "m1",
      mode: "since",
      after_iso: "2025-01-02T00:00",
      ignore_cloned: true,
      resume: false,
    });
  });
});

it("resume body carries the run and checkpoint", () => {
  expect(resumeBody("10", "m1", { run_id: "r1", checkpoint: { last_orig_timestamp: "t" } })).toEqual({
    channel_id: "10",
    mapping_id: "m1",
    resume: true,
    run_id: "r1",
    checkpoint: { last_orig_timestamp: "t" },
  });
});

describe("validation", () => {
  it("flags missing dates, bad counts and reversed ranges", () => {
    expect(validateForm(form({ mode: "since" }))).toEqual({ since: "Pick a date." });
    expect(validateForm(form({ mode: "last", lastN: "0" }))).toEqual({ last: "Enter a valid number." });
    expect(validateForm(form({ mode: "between", fromDate: "2025-01-02" }))).toEqual({ to: "Pick a date." });
    expect(validateForm(form({ mode: "between", fromDate: "2025-02-02", toDate: "2025-01-01" })).to).toBeDefined();
    expect(validateForm(form({ mode: "between", fromDate: "2025-01-01", toDate: "2025-01-01" }))).toEqual({});
    expect(validateForm(form({}))).toEqual({});
  });
  it("end of day rolls over a month", () => {
    expect(endIso("2025-12-31", "")).toBe("2026-01-01T00:00");
    expect(endIso("2025-12-31", "08:30")).toBe("2025-12-31T08:30");
  });
});

describe("events", () => {
  it("maps a task to its channel before reading the payload", () => {
    const tm = new Map([["t1", "77"]]);
    expect(eventChannelId({ task_id: "t1", data: { channel_id: "5" } }, tm)).toBe("77");
    expect(eventChannelId({ data: { original_channel_id: 9 } }, tm)).toBe("9");
    expect(eventChannelId({}, tm)).toBeNull();
  });
  it("keeps every digit of a snowflake from the backfill_busy frame the server sends", () => {
    const frame = JSON.parse('{"kind":"client","role":"server","payload":{"type":"backfill_busy","data":{"channel_id":"1521898853337403511"}}}');
    expect(eventChannelId(frame.payload, new Map())).toBe("1521898853337403511");
  });
  it("progress never goes backwards and keeps the known total", () => {
    expect(progressOf({ data: { delivered: 5 } }, { delivered: 8, total: 100 })).toEqual({ delivered: 8, total: 100 });
    expect(progressOf({ data: { applied: 20, expected_total: 50 } }, { delivered: 8, total: null })).toEqual({ delivered: 20, total: 50 });
  });
  it("labels and percent", () => {
    expect(stageForProgress(null, null)).toBe("pulling");
    expect(progressLabel({ stage: "cloning", delivered: 1200, total: 5000 })).toBe("Cloning (1,200/5,000)");
    expect(progressLabel({ stage: "cloning", delivered: 3, total: null })).toBe("Cloning (3)");
    expect(progressLabel({ stage: "queued", delivered: null, total: null, position: 2 })).toBe("Queued (#2)");
    expect(progressPct({ stage: "cloning", delivered: 1, total: 3 })).toBe(33);
    expect(progressPct({ stage: "pulling", delivered: null, total: null })).toBeNull();
  });
  it("cancel reasons", () => {
    expect(cancelText("server_shutdown")).toBe("Clone cancelled: server is shutting down.");
    expect(cancelText("")).toBe("Clone cancelled.");
    expect(cancelText("Boom")).toBe("Clone cancelled: boom.");
  });
  it("queue puts active first and queued with position", () => {
    const m = queueStates(
      [
        { channel_id: 2, state: "queued", position: 1 },
        { channel_id: 1, state: "active" },
      ],
      () => false,
    );
    expect(m.get("1")?.stage).toBe("queued");
    expect(m.get("2")).toMatchObject({ stage: "queued", position: 1 });
  });
});

describe("persisted toasts", () => {
  it("claims once, with the old storage keys", () => {
    expect(claimToast("bf:done:1", 1000)).toBe(true);
    expect(claimToast("bf:done:1", 1000)).toBe(false);
    expect(localStorage.getItem("toast:persist:bf:done:1")).toContain("expiresAt");
    expect(localStorage.getItem("toast:receipt:bf:done:1")).toContain("deliveredAt");
  });
});
