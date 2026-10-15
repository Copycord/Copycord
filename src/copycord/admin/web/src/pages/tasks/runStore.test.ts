import { act, renderHook, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { TaskRun, TaskStep } from "../../types/tasks";
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
    const ws = FakeWs.all[0]!;
    act(() => ws.open());
    act(() => ws.emit(frame(makeRun({ id: "2", number: 2, seq: 1, status: "queued" }))));
    expect(result.current.runs.map((r) => r.id)).toEqual(["2", "1"]);
    act(() => ws.emit(frame(makeRun({ id: "1", seq: 2, status: "success" }))));
    expect(result.current.runs[1]?.status).toBe("success");

    const calls = fetchMock.mock.calls.length;
    act(() => ws.onclose?.());
    act(() => ws.open());
    await waitFor(() => expect(fetchMock.mock.calls.length).toBe(calls + 1));
  });

  it("useTaskRun follows frames for its run only", async () => {
    const { result } = renderHook(() => useTaskRun("1"));
    await waitFor(() => expect(result.current.run?.id).toBe("1"));
    const ws = FakeWs.all[0]!;
    act(() => ws.open());
    act(() => ws.emit(frame(makeRun({ id: "9", seq: 5, status: "failed" }))));
    expect(result.current.run?.status).toBe("running");
    act(() => ws.emit(frame(makeRun({ id: "1", seq: 2, status: "success" }), [{ key: "a", title: "#a", state: "success" }])));
    expect(result.current.run?.status).toBe("success");
    expect(result.current.run?.steps).toHaveLength(1);
  });

  it("useTaskRun keeps steps and summary live and fetches the report once the run ends", async () => {
    let detail = makeRun({ id: "1", seq: 1, steps: [] });
    fetchMock.mockImplementation(async () => Response.json({ run: detail }));
    const { result } = renderHook(() => useTaskRun("1"));
    await waitFor(() => expect(result.current.run?.id).toBe("1"));
    const ws = FakeWs.all[0]!;
    act(() => ws.open());
    const calls = fetchMock.mock.calls.length;

    const running: TaskStep = { key: "a", title: "#a", state: "running", done: 12, total: 35, label: "12 of 35 delivered, 35 pulled" };
    act(() => ws.emit(frame(makeRun({ id: "1", seq: 2, summary: { success: 0, delivered: 12 } }), [running])));
    expect(result.current.run?.summary).toEqual({ success: 0, delivered: 12 });
    expect(result.current.run?.steps).toEqual([running]);
    expect(result.current.run?.report).toBeUndefined();
    expect(fetchMock.mock.calls.length).toBe(calls);

    const finished: TaskStep = { key: "a", title: "#a", state: "success", done: 35, total: 35, label: null, note: "35 messages" };
    const summary = { success: 1, delivered: 35 };
    detail = makeRun({
      id: "1",
      seq: 3,
      status: "success",
      summary,
      steps: [finished],
      report: { summary },
      log_lines: [{ n: 1, msg: "Finished #a: 35 messages" }],
    });
    act(() => ws.emit(frame(makeRun({ id: "1", seq: 3, status: "success", summary }), [finished])));
    expect(result.current.run?.status).toBe("success");
    expect(result.current.run?.steps?.[0]?.note).toBe("35 messages");
    await waitFor(() => expect(result.current.run?.report).toEqual({ summary }));
    expect(result.current.run?.log_lines).toEqual([{ n: 1, msg: "Finished #a: 35 messages" }]);
    expect(fetchMock.mock.calls.length).toBe(calls + 1);
    expect(fetchMock.mock.calls[fetchMock.mock.calls.length - 1]?.[0]).toBe("/api/tasks/runs/1");
    expect(result.current.refreshing).toBe(false);

    // A later frame for the ended run (the same seq or older) fetches nothing more.
    act(() => ws.emit(frame(makeRun({ id: "1", seq: 3, status: "success", summary }))));
    expect(fetchMock.mock.calls.length).toBe(calls + 1);
  });

  it("useTaskRun keeps a newer frame over a detail that answers late", async () => {
    let answer: ((r: Response) => void) | null = null;
    fetchMock.mockImplementation(async () =>
      answer === null && fetchMock.mock.calls.length === 1
        ? Response.json({ run: makeRun({ id: "1", seq: 1, steps: [] }) })
        : new Promise<Response>((r) => (answer = r)),
    );
    const { result } = renderHook(() => useTaskRun("1"));
    await waitFor(() => expect(result.current.run?.id).toBe("1"));
    const ws = FakeWs.all[0]!;
    act(() => ws.open());
    act(() => ws.emit(frame(makeRun({ id: "1", seq: 4, status: "failed", error: "boom" }))));
    await waitFor(() => expect(answer).not.toBeNull());
    act(() => ws.emit(frame(makeRun({ id: "1", seq: 6, status: "failed", error: "boom", summary: { failed: 1 } }))));
    await act(async () => answer?.(Response.json({ run: makeRun({ id: "1", seq: 5, status: "failed", report: { stale: true } }) })));
    expect(result.current.run?.seq).toBe(6);
    expect(result.current.run?.summary).toEqual({ failed: 1 });
  });
});

describe("runStore refetch keeps what is on screen", () => {
  let release: (() => void) | null;
  let fetchMock: ReturnType<typeof vi.fn>;
  beforeEach(() => {
    FakeWs.all = [];
    release = null;
    vi.stubGlobal("WebSocket", FakeWs);
    let n = 0;
    fetchMock = vi.fn(async (url: string) => {
      n += 1;
      // Every fetch after the first waits for release(), so the test sees the refetch in flight.
      if (n > 1) await new Promise<void>((r) => (release = r));
      if (url.startsWith("/api/tasks/runs?"))
        return Response.json({ runs: [makeRun({ id: "1", seq: n })], total: 1, page: 1, actors: [], contexts: [] });
      return Response.json({ run: makeRun({ id: "1", seq: n, steps: [] }) });
    });
    vi.stubGlobal("fetch", fetchMock);
  });
  afterEach(() => vi.unstubAllGlobals());

  it("second fetch keeps rows and loading stays false", async () => {
    const { result } = renderHook(() => useTaskRuns({}));
    expect(result.current.loading).toBe(true);
    await waitFor(() => expect(result.current.loading).toBe(false));
    expect(result.current.runs).toHaveLength(1);

    act(() => result.current.reload());
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(2));
    expect(result.current.loading).toBe(false);
    expect(result.current.refreshing).toBe(true);
    expect(result.current.runs.map((r) => r.id)).toEqual(["1"]);

    await act(async () => release?.());
    await waitFor(() => expect(result.current.refreshing).toBe(false));
    expect(result.current.loading).toBe(false);
    expect(result.current.runs[0]?.seq).toBe(2);
  });

  it("a new filter set keeps the held rows, refreshing and not settled, until its page lands", async () => {
    const { result, rerender } = renderHook(({ f }) => useTaskRuns(f), { initialProps: { f: {} as { status?: "failed" } } });
    await waitFor(() => expect(result.current.loading).toBe(false));
    expect(result.current.settled).toBe(true);
    rerender({ f: { status: "failed" } });
    expect(result.current.loading).toBe(false);
    expect(result.current.refreshing).toBe(true);
    expect(result.current.settled).toBe(false);
    expect(result.current.runs[0]?.seq).toBe(1);
    await act(async () => release?.());
    await waitFor(() => expect(result.current.settled).toBe(true));
    expect(result.current.loading).toBe(false);
    expect(result.current.refreshing).toBe(false);
    expect(result.current.runs[0]?.seq).toBe(2);
  });

  it("useTaskRun keeps the run on a refetch and loading stays false", async () => {
    const { result } = renderHook(() => useTaskRun("1"));
    expect(result.current.loading).toBe(true);
    await waitFor(() => expect(result.current.run?.id).toBe("1"));
    expect(result.current.loading).toBe(false);

    act(() => result.current.reload());
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(2));
    expect(result.current.loading).toBe(false);
    expect(result.current.refreshing).toBe(true);
    expect(result.current.run?.id).toBe("1");

    await act(async () => release?.());
    await waitFor(() => expect(result.current.run?.seq).toBe(2));
    expect(result.current.refreshing).toBe(false);
  });
});

describe("runStore search: stale while revalidate", () => {
  // Each fetch waits until the test answers it by url fragment.
  let pending: { url: string; answer: (runs: TaskRun[]) => void }[];
  beforeEach(() => {
    FakeWs.all = [];
    pending = [];
    vi.stubGlobal("WebSocket", FakeWs);
    vi.stubGlobal(
      "fetch",
      vi.fn(
        (url: string) =>
          new Promise<Response>((resolve) => {
            pending.push({
              url,
              answer: (runs) =>
                resolve(
                  url.startsWith("/api/tasks/runs?")
                    ? Response.json({ runs, total: runs.length, page: 1, actors: [], contexts: [] })
                    : Response.json({ run: runs[0] }),
                ),
            });
          }),
      ),
    );
  });
  afterEach(() => vi.unstubAllGlobals());

  const answer = async (part: string, runs: TaskRun[]) => {
    const hit = pending.find((p) => p.url.includes(part));
    if (!hit) throw new Error(`no fetch for ${part}`);
    await act(async () => hit.answer(runs));
  };

  it("a query change after the first answer keeps the rows, refreshing until the new answer replaces them", async () => {
    const { result, rerender } = renderHook(({ q }) => useTaskRuns(q ? { q } : {}), { initialProps: { q: "" } });
    expect(result.current.loading).toBe(true);
    await answer("page=1&per_page", [makeRun({ id: "1" }), makeRun({ id: "2", number: 2 })]);
    await waitFor(() => expect(result.current.runs).toHaveLength(2));

    rerender({ q: "gen" });
    expect(result.current.loading).toBe(false);
    expect(result.current.refreshing).toBe(true);
    expect(result.current.settled).toBe(false);
    expect(result.current.runs.map((r) => r.id)).toEqual(["1", "2"]);

    await answer("q=gen", [makeRun({ id: "7", number: 7 })]);
    await waitFor(() => expect(result.current.runs.map((r) => r.id)).toEqual(["7"]));
    expect(result.current.refreshing).toBe(false);
    expect(result.current.settled).toBe(true);
    expect(result.current.loading).toBe(false);
  });

  it("ignores a late answer from a superseded query", async () => {
    const { result, rerender } = renderHook(({ q }) => useTaskRuns(q ? { q } : {}), { initialProps: { q: "" } });
    await answer("page=1&per_page", [makeRun({ id: "1" })]);
    await waitFor(() => expect(result.current.loading).toBe(false));

    rerender({ q: "ge" });
    rerender({ q: "general" });
    await answer("q=general", [makeRun({ id: "9", number: 9 })]);
    await waitFor(() => expect(result.current.runs.map((r) => r.id)).toEqual(["9"]));
    // The answer for "ge" arrives last; it must not replace the rows for "general".
    await answer("q=ge&", [makeRun({ id: "5", number: 5 })]);
    expect(result.current.runs.map((r) => r.id)).toEqual(["9"]);
    expect(result.current.settled).toBe(true);
    expect(result.current.refreshing).toBe(false);
  });

  it("useTaskRun ignores a late answer for the run it left", async () => {
    const { result, rerender } = renderHook(({ id }) => useTaskRun(id), { initialProps: { id: "1" } });
    rerender({ id: "2" });
    await answer("/runs/2", [makeRun({ id: "2", number: 2, steps: [] })]);
    await waitFor(() => expect(result.current.run?.id).toBe("2"));
    await answer("/runs/1", [makeRun({ id: "1", steps: [] })]);
    expect(result.current.run?.id).toBe("2");
  });
});
