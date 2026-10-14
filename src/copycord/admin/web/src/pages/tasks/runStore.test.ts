import { act, renderHook, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { TaskRun } from "../../types/tasks";
import { applyFrame, mergeIntoList, parseRunFrame, runsUrl, useTaskRun, useTaskRuns } from "./runStore";

function makeRun(over: Partial<TaskRun> = {}): TaskRun {
  return {
    id: "1",
    type: "backfill_channels",
    number: 1,
    title: "Backfill #general",
    context: { label: "Main mapping", mapping_id: "m1", guild_id: "g1" },
    status: "running",
    trigger: "manual",
    actor: "admin",
    created_at: "2026-10-14T18:00:00Z",
    started_at: "2026-10-14T18:00:01Z",
    finished_at: null,
    error: null,
    summary: null,
    seq: 1,
    ...over,
  };
}

class FakeWs {
  static OPEN = 1;
  static all: FakeWs[] = [];
  readyState = 0;
  onopen: (() => void) | null = null;
  onmessage: ((ev: { data: string }) => void) | null = null;
  onclose: (() => void) | null = null;
  constructor(public url: string) {
    FakeWs.all.push(this);
  }
  open() {
    this.readyState = 1;
    this.onopen?.();
  }
  emit(data: unknown) {
    this.onmessage?.({ data: JSON.stringify(data) });
  }
  close() {
    this.readyState = 3;
  }
  send() {}
}

const frame = (run: TaskRun, steps?: unknown[]) => ({ kind: "task.run.updated", role: "admin", payload: { run, steps } });

describe("runStore merging", () => {
  it("builds the list url with 25 per page", () => {
    expect(runsUrl({ type: "backfill_channels", q: "#3" })).toBe("/api/tasks/runs?type=backfill_channels&q=%233&page=1&per_page=25");
  });

  it("unwraps the hub payload and stringifies the id", () => {
    const run = { ...makeRun(), id: 7 as unknown as string };
    expect(parseRunFrame(frame(run))?.run.id).toBe("7");
    expect(parseRunFrame({ kind: "other", payload: { run } })).toBeNull();
    expect(parseRunFrame({ type: "task.run.updated", run: makeRun() })?.run.id).toBe("1");
  });

  it("patches a held run only with a newer seq and merges changed steps by key", () => {
    const held = makeRun({ seq: 5, steps: [{ key: "a", title: "#a", state: "running" }, { key: "b", title: "#b", state: "queued" }] });
    expect(applyFrame(held, { run: makeRun({ seq: 4, status: "failed" }) })).toBe(held);
    expect(applyFrame(held, { run: makeRun({ seq: 5, status: "failed" }) })).toBe(held);
    const next = applyFrame(held, { run: makeRun({ seq: 6, status: "success" }), steps: [{ key: "b", title: "#b", state: "success" }] });
    expect(next.status).toBe("success");
    expect(next.steps?.map((s) => s.state)).toEqual(["running", "success"]);
  });

  it("inserts a matching new run at the top, ignores one that does not match", () => {
    const state = { runs: [makeRun({ id: "1" })], total: 1 };
    const added = mergeIntoList(state, { run: makeRun({ id: "2", number: 2 }) }, { type: "backfill_channels" });
    expect(added.runs.map((r) => r.id)).toEqual(["2", "1"]);
    expect(added.total).toBe(2);
    expect(mergeIntoList(state, { run: makeRun({ id: "3", type: "noop" }) }, { type: "backfill_channels" })).toBe(state);
    expect(mergeIntoList(state, { run: makeRun({ id: "3" }) }, { page: 2 })).toBe(state);
  });

  it("patches a visible row in place and drops a stale frame", () => {
    const state = { runs: [makeRun({ id: "1", seq: 3 }), makeRun({ id: "2", seq: 1 })], total: 2 };
    const next = mergeIntoList(state, { run: makeRun({ id: "2", seq: 2, status: "success" }) }, {});
    expect(next.runs.map((r) => r.status)).toEqual(["running", "success"]);
    expect(mergeIntoList(state, { run: makeRun({ id: "1", seq: 2, status: "failed" }) }, {})).toBe(state);
  });
});

describe("runStore hooks", () => {
  let fetchMock: ReturnType<typeof vi.fn>;
  beforeEach(() => {
    FakeWs.all = [];
    vi.stubGlobal("WebSocket", FakeWs);
    fetchMock = vi.fn(async (url: string) => {
      if (url.startsWith("/api/tasks/runs?")) return Response.json({ runs: [makeRun({ id: "1", seq: 1 })], total: 1, page: 1, actors: [], contexts: [] });
      return Response.json({ run: makeRun({ id: "1", seq: 1, steps: [] }) });
    });
    vi.stubGlobal("fetch", fetchMock);
  });
  afterEach(() => vi.unstubAllGlobals());

  it("useTaskRuns merges live frames and refetches after a reconnect", async () => {
    const { result } = renderHook(() => useTaskRuns({}));
    await waitFor(() => expect(result.current.runs).toHaveLength(1));
    const ws = FakeWs.all[0];
    act(() => ws.open());
    act(() => ws.emit(frame(makeRun({ id: "2", number: 2, seq: 1, status: "queued" }))));
    expect(result.current.runs.map((r) => r.id)).toEqual(["2", "1"]);
    act(() => ws.emit(frame(makeRun({ id: "1", seq: 2, status: "success" }))));
    expect(result.current.runs[1].status).toBe("success");

    const calls = fetchMock.mock.calls.length;
    act(() => ws.onclose?.());
    act(() => ws.open());
    await waitFor(() => expect(fetchMock.mock.calls.length).toBe(calls + 1));
  });

  it("useTaskRun follows frames for its run only", async () => {
    const { result } = renderHook(() => useTaskRun("1"));
    await waitFor(() => expect(result.current.run?.id).toBe("1"));
    const ws = FakeWs.all[0];
    act(() => ws.open());
    act(() => ws.emit(frame(makeRun({ id: "9", seq: 5, status: "failed" }))));
    expect(result.current.run?.status).toBe("running");
    act(() => ws.emit(frame(makeRun({ id: "1", seq: 2, status: "success" }), [{ key: "a", title: "#a", state: "success" }])));
    expect(result.current.run?.status).toBe("success");
    expect(result.current.run?.steps).toHaveLength(1);
  });
});
