import { act, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { MemoryRouter, Route, Routes } from "react-router";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { ToastProvider } from "../components/Toast";
import { ConfirmProvider } from "../components/ui/ConfirmDialog";
import type { TaskRun } from "../types/tasks";
import { Tasks } from "./Tasks";
import { BUSY } from "./tasks/TasksSkeleton";

class FakeWs {
  static OPEN = 1;
  readyState = 0;
  onopen: (() => void) | null = null;
  onmessage: (() => void) | null = null;
  onclose: (() => void) | null = null;
  close() {}
  send() {}
}

function run(over: Partial<TaskRun>): TaskRun {
  return {
    id: "1",
    type: "backfill_channels",
    number: 1,
    title: "Backfill #general",
    context: { label: "Main mapping", mapping_id: "m1", guild_id: "g1" },
    status: "success",
    trigger: "manual",
    actor: "admin",
    created_at: "2026-10-14T18:00:00Z",
    started_at: "2026-10-14T18:00:01Z",
    finished_at: "2026-10-14T18:00:49Z",
    error: null,
    summary: null,
    seq: 3,
    ...over,
  };
}

const TYPES = {
  types: [
    { key: "backfill_channels", title: "Backfill channels", description: "Clone message history", inputs_schema: {}, active: 1, total: 2 },
  ],
};

type Handler = (url: string, method: string) => Response | undefined;

function serve(runs: TaskRun[], opts: { total?: number; handle?: Handler } = {}) {
  const fn = vi.fn(async (url: string, init?: RequestInit) => {
      const hit0 = opts.handle?.(url, init?.method ?? "GET");
      if (hit0) return hit0;
      if (url === "/api/tasks/types") return Response.json(TYPES);
      if (url.startsWith("/api/tasks/runs?"))
        return Response.json({
          runs,
          total: opts.total ?? runs.length,
          page: 1,
          actors: ["admin", "bot"],
          contexts: [{ label: "Main mapping", mapping_id: "m1" }],
        });
      const id = url.split("/").pop();
      const hit = runs.find((r) => r.id === id);
      return hit ? Response.json({ run: { ...hit, steps: [] } }) : Response.json({ detail: "not found" }, { status: 404 });
  });
  vi.stubGlobal("fetch", fn);
  return fn;
}

/** Observers the LoadMore sentinel made; scrollToEnd tells the live ones their target came into view. */
let observers: Array<{ cb: IntersectionObserverCallback; el: Element | null; live: boolean }> = [];

class FakeObserver {
  entry: { cb: IntersectionObserverCallback; el: Element | null; live: boolean };
  constructor(cb: IntersectionObserverCallback) {
    this.entry = { cb, el: null, live: true };
    observers.push(this.entry);
  }
  observe(el: Element) {
    this.entry.el = el;
  }
  unobserve() {}
  disconnect() {
    this.entry.live = false;
  }
  takeRecords() {
    return [];
  }
}

function scrollToEnd() {
  for (const o of observers.filter((x) => x.live && x.el))
    o.cb([{ isIntersecting: true, target: o.el } as unknown as IntersectionObserverEntry], {} as IntersectionObserver);
}

/** Waits for the sentinel's observer (its effect runs after the rows paint), then scrolls to it. */
async function scrollWhenReady() {
  await waitFor(() => expect(observers.some((o) => o.live && o.el)).toBe(true));
  await act(async () => scrollToEnd());
}

/** The run rows (the rail is a list too). */
const runRows = () => within(screen.getByRole("list", { name: "Task runs" })).getAllByRole("listitem");

/** The ?page= of a list URL (per_page holds "page=" too). */
const pageOf = (u: string) => new URL(u, "http://x").searchParams.get("page");

/** `count` runs r1..rN served a page at a time by ?page= and ?per_page=; `wait` can hold a page. */
function servePaged(count: number, wait?: (page: number) => Promise<void> | void) {
  const all = Array.from({ length: count }, (_, i) => run({ id: `r${i + 1}`, number: count - i }));
  return serve(all, {
    handle: (url) => {
      if (!url.startsWith("/api/tasks/runs?")) return undefined;
      const qs = new URL(url, "http://x").searchParams;
      const page = Number(qs.get("page"));
      const per = Number(qs.get("per_page"));
      const body = { runs: all.slice((page - 1) * per, page * per), total: count, page, actors: ["admin", "bot"], contexts: [] };
      const w = wait?.(page);
      return (w ? w.then(() => Response.json(body)) : Response.json(body)) as unknown as Response;
    },
  });
}

/** The native select a ToolbarSelect keeps in the DOM, by filter key. */
const sel = (key: string) => document.getElementById(`tasks-filter-${key}`) as HTMLSelectElement;

/** The list URLs fetched so far. */
function listCalls(fn: ReturnType<typeof serve>): string[] {
  return fn.mock.calls.map((c) => String(c[0])).filter((u) => u.startsWith("/api/tasks/runs?"));
}

function renderAt(path: string) {
  return render(
    <ToastProvider>
      <ConfirmProvider>
        <MemoryRouter initialEntries={[path]}>
          <Routes>
            <Route path="/tasks" element={<Tasks />} />
            <Route path="/tasks/runs/:id" element={<Tasks />} />
          </Routes>
        </MemoryRouter>
      </ConfirmProvider>
    </ToastProvider>,
  );
}

async function openMenu(rowTitle: string) {
  const link = await screen.findByRole("link", { name: rowTitle });
  const row = link.closest("li")!;
  fireEvent.click(within(row as HTMLElement).getByRole("button", { name: "Channel menu" }));
}

beforeEach(() => {
  vi.stubGlobal("WebSocket", FakeWs);
  observers = [];
  vi.stubGlobal("IntersectionObserver", FakeObserver);
  localStorage.clear();
});

describe("Tasks page", () => {
  it("lists runs as one link each: the status glyph, the name over the run ID, mapping, trigger and actor, the outcome line, started over duration", async () => {
    serve([
      run({ id: "4f2a9c1e7b3d4e5f8a6b0c2d1e3f5a7b", number: 12, summary: { success: 2, failed: 0, skipped: 0, cancelled: 4, delivered: 92 }, step_count: 6 }),
      run({ id: "9e8d7c6b5a4f4e3d8c2b1a0f9e8d7c6b", number: 11, title: "Backfill 3 channels", status: "failed" }),
    ]);
    renderAt("/tasks");
    const list = await screen.findByRole("list", { name: "Task runs" });
    const rows = within(list).getAllByRole("listitem");
    expect(rows).toHaveLength(2);
    expect(screen.getByText("2 task runs")).toBeInTheDocument();
    const link = await within(rows[0]!).findByRole("link", { name: "Backfill channels #12" });
    expect(link).toHaveAttribute("href", "/tasks/runs/4f2a9c1e7b3d4e5f8a6b0c2d1e3f5a7b");
    // The run's own title is gone from the row; the link's tooltip keeps it with the start date.
    expect(link.getAttribute("title")).toMatch(/^Backfill #general, started /);
    expect(rows[1]).not.toHaveTextContent("Backfill 3 channels");
    // The status glyph alone carries the status, its word in the label and title; no badge on the row.
    expect(within(rows[0]!).getByRole("img", { name: "Success" })).toHaveAttribute("title", "Success");
    expect(within(rows[1]!).getByRole("img", { name: "Failed" })).toHaveAttribute("title", "Failed");
    expect(within(rows[1]!).queryByTitle("Status: Failed")).toBeNull();
    expect(rows[1]).not.toHaveTextContent("Failed");
    // The meta line says only who started the run: no run ID, no mapping, no trigger.
    expect(within(rows[0]!).getByText("Triggered by admin").textContent).toBe("Triggered by admin");
    expect(within(rows[0]!).queryByText("4f2a9c1e")).toBeNull();
    expect(rows[0]).not.toHaveTextContent("Main mapping");
    // No step counts or outcome text on the row, and no bar.
    expect(rows[0]).not.toHaveTextContent(/steps|delivered/);
    expect(within(rows[0]!).queryByTestId("run-outcome")).toBeNull();
    expect(within(list).queryByRole("progressbar")).toBeNull();
    // Started (relative, the date in the title) over the duration.
    expect(within(rows[0]!).getByTitle("Duration")).toHaveTextContent("48s");
    expect(within(rows[0]!).getByTitle(/^Started /)).toBeInTheDocument();

    const rail = screen.getByRole("navigation", { name: "Task types" });
    expect(within(rail).getByRole("link", { name: /All tasks/ })).toHaveAttribute("aria-current", "page");
    expect(within(rail).getByRole("link", { name: /Backfill channels/ })).toHaveAttribute("href", "/tasks?type=backfill_channels");
    // Each entry's run count and a running badge while it has active runs; All tasks adds them up.
    const type = within(rail).getByRole("link", { name: /Backfill channels/ });
    expect(type).toHaveAttribute("title", "2 runs, 1 running");
    expect(within(type).getByTestId("rail-count")).toHaveTextContent("2");
    expect(within(rail).getByRole("link", { name: /All tasks/ })).toHaveAttribute("title", "2 runs, 1 running");
    // All tasks has no Run task: the rail picks a type first.
    expect(screen.queryByRole("button", { name: "Run task" })).toBeNull();
  });

  it("says a run that is gone was deleted or never existed, not the server's text, with a link back to all tasks", async () => {
    serve([], {
      handle: (url) => (url.startsWith("/api/tasks/runs/") ? Response.json({ detail: "Unknown run" }, { status: 404 }) : undefined),
    });
    renderAt("/tasks/runs/8e3b2d853281447fadf99f8db59baed3");
    expect(await screen.findByText("Run not found")).toBeInTheDocument();
    expect(screen.getByText("This run was deleted or never existed.")).toBeInTheDocument();
    expect(screen.queryByText(/Unknown run/)).toBeNull();
    const back = screen.getByRole("link", { name: "Back to all tasks" });
    expect(back).toHaveAttribute("href", "/tasks");
    expect(back.className).toContain("text-accent");
    // The page shell stays: the rail is still there.
    expect(screen.getByRole("navigation", { name: "Task types" })).toBeInTheDocument();
  });

  it("names the type in the header and on the run page while the types are still loading, never a bar", async () => {
    const pending = (url: string) => (url === "/api/tasks/types" ? new Response(new ReadableStream()) : undefined);
    serve([run({ id: "4f2a9c1e7b3d4e5f8a6b0c2d1e3f5a7b", number: 12 })], { handle: pending });
    const first = renderAt("/tasks?type=backfill_channels");
    // A first visit: the id in words, no shimmer in the title or the description line.
    expect(screen.getByRole("heading", { level: 2 })).toHaveTextContent("Backfill channels");
    expect(first.container.querySelector("h2 [data-skeleton], h2 + p [data-skeleton]")).toBeNull();
    await screen.findByRole("link", { name: "Backfill channels #12" });
    first.unmount();

    // A visit after the types loaded once: their own title and description, still before they answer.
    localStorage.setItem("tasks:types", JSON.stringify({ backfill_channels: { title: "Backfill history", description: "Clone message history" } }));
    const again = renderAt("/tasks?type=backfill_channels");
    expect(screen.getByRole("heading", { level: 2 })).toHaveTextContent("Backfill history");
    expect(screen.getByText("Clone message history")).toBeInTheDocument();
    // The list remembered run 12's type, so its page names it while the run loads.
    again.unmount();
    renderAt("/tasks/runs/4f2a9c1e7b3d4e5f8a6b0c2d1e3f5a7b");
    const loading = screen.getByRole("status", { name: "Loading run" });
    expect(within(loading).getByRole("heading", { level: 2, hidden: true })).toHaveTextContent("Backfill history");
    expect(within(loading).getByRole("link", { name: "Backfill history", hidden: true })).toHaveAttribute("href", "/tasks?type=backfill_channels");
    // The run lands with the same title, the types still out.
    await waitFor(() => expect(screen.queryByRole("status", { name: "Loading run" })).toBeNull());
    expect(screen.getByRole("heading", { level: 2, name: "Backfill history" })).toBeInTheDocument();
    expect(screen.getByText("#12")).toBeInTheDocument();
  });

  it("opens a run with its header and the Summary, Steps and Report sections, no log", async () => {
    serve([run({ id: "4f2a9c1e7b3d4e5f8a6b0c2d1e3f5a7b", number: 12 })]);
    renderAt("/tasks/runs/4f2a9c1e7b3d4e5f8a6b0c2d1e3f5a7b");
    expect(await screen.findByRole("heading", { level: 2, name: "Backfill channels" })).toBeInTheDocument();
    expect(screen.getByText("#12")).toBeInTheDocument();
    for (const name of ["Summary", "Steps", "Report"]) expect(screen.getByRole("region", { name })).toBeInTheDocument();
    expect(screen.queryByRole("region", { name: "Log" })).toBeNull();
  });

  it("sends the URL filters with the list fetch and narrows it from the dropdowns", async () => {
    const fn = serve([run({ id: "4f2a9c1e7b3d4e5f8a6b0c2d1e3f5a7b", number: 12 })]);
    renderAt("/tasks?status=failed&actor=admin&q=general&context=m1&page=2");
    await screen.findByRole("link", { name: "Backfill channels #12" });
    const first = new URL(listCalls(fn)[0]!, "http://x").searchParams;
    // A ?page= left in an old link is ignored: the list always starts at the first page.
    expect(Object.fromEntries(first)).toMatchObject({ status: "failed", actor: "admin", q: "general", context: "m1", page: "1" });
    expect(screen.getByRole("searchbox", { name: "Search runs" })).toHaveValue("general");

    fireEvent.change(sel("actor"), { target: { value: "bot" } });
    await waitFor(() => expect(listCalls(fn).at(-1)).toContain("actor=bot"));
    // A filter change goes back to page 1.
    expect(listCalls(fn).at(-1)).toContain("page=1");
    expect(listCalls(fn).at(-1)).toContain("status=failed");
  });

  it("typing a search keeps the rows, with no skeleton and no empty state until the search answers", async () => {
    const held: { answer?: () => void } = {};
    const fn = serve([run({ id: "4f2a9c1e7b3d4e5f8a6b0c2d1e3f5a7b", number: 12 })]);
    const inner = fn.getMockImplementation()!;
    // A search waits until the test answers it, with no match.
    fn.mockImplementation(async (url: string, init?: RequestInit) => {
      if (!url.startsWith("/api/tasks/runs?") || !url.includes("q=")) return inner(url, init);
      await new Promise<void>((r) => (held.answer = r));
      return Response.json({ runs: [], total: 0, page: 1, actors: [], contexts: [] });
    });
    renderAt("/tasks");
    const list = await screen.findByRole("list", { name: "Task runs" });
    const box = screen.getByRole("searchbox", { name: "Search runs" });
    for (const ch of ["n", "no", "nop", "nope"]) fireEvent.change(box, { target: { value: ch } });
    await waitFor(() => expect(held.answer).toBeDefined(), { timeout: 2000 });
    // The search is out: the old row stays, marked busy, and nothing else appears.
    expect(screen.queryByRole("status", { name: "Loading task runs" })).toBeNull();
    expect(screen.queryByText("No runs match these filters")).toBeNull();
    expect(screen.queryByText("No task runs yet")).toBeNull();
    expect(within(list).getByRole("link", { name: "Backfill channels #12" })).toBeInTheDocument();
    expect(list).toHaveAttribute("aria-busy", "true");
    // Quietly: a dim that waits 300 ms before it shows, and fades back when the answer lands.
    expect(list.className).toContain(BUSY);
    // Debounced: one search for the settled text, not one per keystroke.
    expect(listCalls(fn).filter((u) => u.includes("q="))).toEqual([expect.stringContaining("q=nope")]);

    await act(async () => held.answer!());
    expect(await screen.findByText("No runs match these filters")).toBeInTheDocument();
    expect(screen.queryByRole("status", { name: "Loading task runs" })).toBeNull();
  });

  it("loads the next page when the end of the list scrolls into view, appending the rows", async () => {
    const fn = servePaged(60);
    renderAt("/tasks");
    const list = await screen.findByRole("list", { name: "Task runs" });
    expect(within(list).getAllByRole("listitem")).toHaveLength(25);
    expect(screen.queryByRole("navigation", { name: "Pagination" })).toBeNull();
    expect(screen.queryByRole("button", { name: "Next" })).toBeNull();
    await scrollWhenReady();
    await waitFor(() => expect(within(list).getAllByRole("listitem")).toHaveLength(50));
    expect(pageOf(listCalls(fn).at(-1)!)).toBe("2");
    expect(listCalls(fn).at(-1)).toContain("per_page=25");
    // The first page's rows are still there, in order, the second page under them.
    const ids = within(list).getAllByRole("listitem").map((li) => li.getAttribute("data-run-id"));
    expect(ids.slice(0, 2)).toEqual(["r1", "r2"]);
    expect(ids.at(-1)).toBe("r50");
  });

  it("shows Loading more while the next page is out and fetches it once however often the sentinel fires", async () => {
    const held: { answer?: () => void } = {};
    const fn = servePaged(60, async (page) => {
      if (page === 2) await new Promise<void>((r) => (held.answer = r));
    });
    renderAt("/tasks");
    await screen.findByRole("list", { name: "Task runs" });
    await scrollWhenReady();
    await waitFor(() => expect(held.answer).toBeDefined());
    expect(screen.getByText("Loading more")).toBeInTheDocument();
    await act(async () => {
      scrollToEnd();
      scrollToEnd();
    });
    expect(listCalls(fn).filter((u) => pageOf(u) === "2")).toHaveLength(1);
    await act(async () => held.answer!());
    await waitFor(() => expect(runRows()).toHaveLength(50));
    expect(screen.queryByText("Loading more")).toBeNull();
  });

  it("stops at the end of the list: no sentinel and no fetch past the last run", async () => {
    const fn = servePaged(30);
    renderAt("/tasks");
    await screen.findByRole("list", { name: "Task runs" });
    await scrollWhenReady();
    await waitFor(() => expect(runRows()).toHaveLength(30));
    expect(document.querySelector("[data-load-more]")).toBeNull();
    const n = listCalls(fn).length;
    await act(async () => scrollToEnd());
    expect(listCalls(fn)).toHaveLength(n);
    expect(screen.queryByText("Loading more")).toBeNull();
  });

  it("a filter or status tab change starts again from the first page", async () => {
    const fn = servePaged(60);
    renderAt("/tasks");
    await screen.findByRole("list", { name: "Task runs" });
    await scrollWhenReady();
    await waitFor(() => expect(runRows()).toHaveLength(50));
    fireEvent.change(sel("actor"), { target: { value: "bot" } });
    await waitFor(() => expect(listCalls(fn).at(-1)).toContain("actor=bot"));
    expect(pageOf(listCalls(fn).at(-1)!)).toBe("1");
    expect(listCalls(fn).at(-1)).toContain("per_page=25");
    await waitFor(() => expect(runRows()).toHaveLength(25));
    await waitFor(() => expect(screen.getByRole("list", { name: "Task runs" })).not.toHaveAttribute("aria-busy"));
    // The next page after the change is page 2 of the new filters.
    await scrollWhenReady();
    await waitFor(() => expect(listCalls(fn).at(-1)).toMatch(/actor=bot.*&page=2&/));
  });

  it("names every Mapping option: plain string contexts and ones without a label fall back to the mapping id", async () => {
    serve([run({ id: "1", number: 12 })], {
      handle: (url) =>
        url.startsWith("/api/tasks/runs?")
          ? Response.json({
              runs: [run({ id: "1", number: 12 })],
              total: 1,
              page: 1,
              actors: [],
              contexts: ["Main mapping", "", { label: "", mapping_id: "m9" }, { label: "Other", mapping_id: "m2" }],
            })
          : undefined,
    });
    renderAt("/tasks");
    await screen.findByRole("link", { name: "Backfill channels #12" });
    const opts = Array.from(sel("context").options).map((o) => [o.value, o.text]);
    expect(opts).toEqual([
      ["", "All"],
      ["Main mapping", "Main mapping"],
      ["m9", "m9"],
      ["m2", "Other"],
    ]);
    for (const [, text] of opts) expect(String(text).trim()).not.toBe("");
  });

  it("shows No runs match these filters with Clear filters when a filtered list is empty", async () => {
    const fn = serve([]);
    renderAt("/tasks?status=failed");
    expect(await screen.findByText("No runs match these filters")).toBeInTheDocument();
    expect(screen.getByText("Try other filters, or clear them to see every run")).toBeInTheDocument();
    const clears = screen.getAllByRole("button", { name: "Clear filters" });
    fireEvent.click(clears[clears.length - 1]!);
    expect(await screen.findByText("No task runs yet")).toBeInTheDocument();
    expect(listCalls(fn).at(-1)).not.toContain("status=");
    expect(screen.getByText("Runs show up here once a task starts")).toBeInTheDocument();
  });

  describe("a type that needs the bots", () => {
    const NEEDS_BOTS = { types: [{ ...TYPES.types[0]!, requires_bots: true }] };

    function serveBots(up: { value: boolean }) {
      return serve([], {
        handle: (url) => {
          if (url === "/api/tasks/types") return Response.json(NEEDS_BOTS);
          if (url === "/api/status") return Response.json({ both_running: up.value, server: { running: up.value }, client: { running: up.value } });
          return undefined;
        },
      });
    }

    afterEach(() => vi.useRealTimers());

    it("keeps Run task off with Start the bots first beside it while the bots are down", async () => {
      serveBots({ value: false });
      renderAt("/tasks?type=backfill_channels");
      expect(await screen.findAllByText("Start the bots first")).toHaveLength(1);
      for (const b of screen.getAllByRole("button", { name: "Run task" })) expect(b).toBeDisabled();
    });

    it("turns Run task on once the status says both bots run", async () => {
      vi.useFakeTimers({ shouldAdvanceTime: true });
      const up = { value: false };
      serveBots(up);
      renderAt("/tasks?type=backfill_channels");
      expect(await screen.findAllByText("Start the bots first")).toHaveLength(1);
      up.value = true;
      await act(() => vi.advanceTimersByTimeAsync(4000));
      await waitFor(() => expect(screen.queryByText("Start the bots first")).toBeNull());
      for (const b of screen.getAllByRole("button", { name: "Run task" })) expect(b).toBeEnabled();
    });

  });

  it("cancels an active run after confirming and patches the row from the response", async () => {
    const live = run({ id: "4f2a9c1e7b3d4e5f8a6b0c2d1e3f5a7b", number: 12, status: "running", finished_at: null, seq: 4 });
    const calls: string[] = [];
    serve([live], {
      handle: (url, method) => {
        if (method !== "POST") return undefined;
        calls.push(url);
        return Response.json({ run: { ...live, status: "cancelled", seq: 5, finished_at: "2026-10-14T18:01:00Z" } });
      },
    });
    renderAt("/tasks");
    await openMenu("Backfill channels #12");
    expect(screen.queryByRole("menuitem", { name: "Delete" })).toBeNull();
    fireEvent.click(screen.getByRole("menuitem", { name: "Cancel run" }));
    // The run page asks the same, word for word.
    const dialog = await screen.findByRole("dialog");
    expect(within(dialog).getByText("Work already done is kept; the run stops at its next checkpoint.")).toBeInTheDocument();
    expect(within(dialog).getByRole("button", { name: "Keep running" })).toBeInTheDocument();
    fireEvent.click(within(dialog).getByRole("button", { name: "Cancel run" }));
    expect(await screen.findByRole("img", { name: "Cancelled" })).toBeInTheDocument();
    expect(calls).toEqual(["/api/tasks/runs/4f2a9c1e7b3d4e5f8a6b0c2d1e3f5a7b/cancel"]);
  });

  it("toasts the server message when a cancel is refused with 409", async () => {
    const live = run({ id: "4f2a9c1e7b3d4e5f8a6b0c2d1e3f5a7b", number: 12, status: "running", finished_at: null });
    serve([live], {
      handle: (_url, method) => (method === "POST" ? Response.json({ detail: "Run already success" }, { status: 409 }) : undefined),
    });
    renderAt("/tasks");
    await openMenu("Backfill channels #12");
    fireEvent.click(screen.getByRole("menuitem", { name: "Cancel run" }));
    fireEvent.click(await screen.findByRole("button", { name: "Cancel run" }));
    expect(await screen.findByText("Run already success")).toBeInTheDocument();
  });

  it("deletes a finished run after confirming and drops the row without a refetch", async () => {
    const deleted: string[] = [];
    const fn = serve([run({ id: "4f2a9c1e7b3d4e5f8a6b0c2d1e3f5a7b", number: 12 }), run({ id: "9e8d7c6b5a4f4e3d8c2b1a0f9e8d7c6b", number: 11, title: "Backfill 3 channels" })], {
      handle: (url, method) => {
        if (method !== "DELETE") return undefined;
        deleted.push(url);
        return new Response(null, { status: 204 });
      },
    });
    renderAt("/tasks");
    await openMenu("Backfill channels #12");
    expect(screen.queryByRole("menuitem", { name: "Cancel run" })).toBeNull();
    fireEvent.click(screen.getByRole("menuitem", { name: "Delete" }));
    expect(await screen.findByText("The run and its report are deleted. This cannot be undone.")).toBeInTheDocument();
    const before = listCalls(fn).length;
    fireEvent.click(await screen.findByRole("button", { name: "Delete" }));
    await waitFor(() => expect(screen.queryByRole("link", { name: "Backfill channels #12" })).toBeNull());
    expect(deleted).toEqual(["/api/tasks/runs/4f2a9c1e7b3d4e5f8a6b0c2d1e3f5a7b"]);
    expect(screen.getByText("1 task run")).toBeInTheDocument();
    expect(await screen.findByText("Deleted Backfill channels #12")).toBeInTheDocument();
    expect(listCalls(fn).length).toBe(before);
  });
});
