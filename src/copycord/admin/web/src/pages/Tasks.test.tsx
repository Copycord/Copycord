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
  localStorage.clear();
});

describe("Tasks page", () => {
  it("lists runs as one link each: name and status badge over the run ID, mapping, trigger and actor, the outcome and its bar, started over duration", async () => {
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
    // One coloured status badge per row, its word always there.
    expect(within(rows[0]!).getByTitle("Status: Success")).toHaveTextContent("Success");
    expect(within(rows[1]!).getByTitle("Status: Failed")).toHaveTextContent("Failed");
    // The run ID line: the first 8 characters in mono, the full id in its title.
    const id = within(rows[0]!).getByText("4f2a9c1e");
    expect(id).toHaveClass("font-mono");
    expect(id).toHaveAttribute("title", "Run ID 4f2a9c1e7b3d4e5f8a6b0c2d1e3f5a7b");
    // The meta line: the run ID, the mapping chip, the trigger and the actor.
    expect(id.parentElement).toHaveTextContent("4f2a9c1e·Main mapping·Manual·admin");
    // What the run did, from its summary, over the segmented bar; a run with nothing to count says nothing.
    expect(within(rows[0]!).getByTestId("run-outcome")).toHaveTextContent("6 steps · 2 done · 4 cancelled · 92 delivered");
    expect(within(rows[0]!).getByRole("progressbar", { name: "6 of 6 steps finished" })).toBeInTheDocument();
    expect(within(rows[1]!).getByTestId("run-outcome").textContent).toBe("");
    expect(within(rows[1]!).queryByRole("progressbar")).toBeNull();
    // Started (relative, the date in the title) over the duration.
    expect(within(rows[0]!).getByTitle("Duration")).toHaveTextContent("48s");
    expect(within(rows[0]!).getByTitle(/^Started /)).toBeInTheDocument();

    const rail = screen.getByRole("navigation", { name: "Task types" });
    expect(within(rail).getByRole("link", { name: /All tasks/ })).toHaveAttribute("aria-current", "page");
    expect(within(rail).getByRole("link", { name: /Backfill channels/ })).toHaveAttribute("href", "/tasks?type=backfill_channels");
    expect(within(rail).getByLabelText("1 active")).toBeInTheDocument();
    // All tasks has no Run task: the rail picks a type first.
    expect(screen.queryByRole("button", { name: "Run task" })).toBeNull();
  });

  it("shows the empty state when there are no runs: icon, words, Run Backfill channels, and no pager", async () => {
    serve([]);
    renderAt("/tasks");
    // The loading list stands in the empty state's shape with the same words, so wait for it to go.
    await waitFor(() => expect(screen.queryByRole("status", { name: "Loading task runs" })).toBeNull());
    expect(screen.getByText("No task runs yet")).toBeInTheDocument();
    expect(screen.getByText("0 task runs")).toBeInTheDocument();
    expect(screen.getByText("Runs show up here once a task starts")).toBeInTheDocument();
    expect(await screen.findByRole("button", { name: "Run Backfill channels" })).toBeEnabled();
    expect(screen.queryByRole("button", { name: "Run task" })).toBeNull();
    const box = screen.getByText("No task runs yet").parentElement!;
    expect(box.className).not.toMatch(/border-dashed/);
    expect(box.querySelector("svg")).not.toBeNull();
    expect(screen.queryByRole("navigation", { name: "Pagination" })).toBeNull();
  });

  it("opens the backfill form from the empty state's button", async () => {
    serve([]);
    renderAt("/tasks");
    fireEvent.click(await screen.findByRole("button", { name: "Run Backfill channels" }));
    expect(await screen.findByRole("dialog")).toBeInTheDocument();
  });

  it("shows the type title and description when a type is picked", async () => {
    serve([]);
    renderAt("/tasks?type=backfill_channels");
    expect(await screen.findByRole("heading", { level: 2, name: "Backfill channels" })).toBeInTheDocument();
    expect(screen.getByText("Clone message history")).toBeInTheDocument();
    expect(screen.getAllByRole("button", { name: "Run task" }).length).toBeGreaterThan(0);
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
    expect(Object.fromEntries(first)).toMatchObject({ status: "failed", actor: "admin", q: "general", context: "m1", page: "2" });
    expect(screen.getByRole("searchbox", { name: "Search runs" })).toHaveValue("general");

    fireEvent.change(sel("actor"), { target: { value: "bot" } });
    await waitFor(() => expect(listCalls(fn).at(-1)).toContain("actor=bot"));
    // A filter change goes back to page 1.
    expect(listCalls(fn).at(-1)).toContain("page=1");
    expect(listCalls(fn).at(-1)).toContain("status=failed");
  });

  it("names the filter on the closed control only, lists plain options, and opens the list in a card that does not clip it", async () => {
    serve([run({ id: "4f2a9c1e7b3d4e5f8a6b0c2d1e3f5a7b", number: 12 })]);
    renderAt("/tasks");
    await screen.findByRole("link", { name: "Backfill channels #12" });
    expect(screen.getByRole("button", { name: "Auto-refresh: 30s" })).toBeInTheDocument();
    for (const name of ["Task type: All", "Mapping: All", "Actor: All"]) expect(screen.getByRole("button", { name })).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "Status: All" }));
    const listbox = screen.getByRole("listbox", { name: "Status" });
    expect(within(listbox).getAllByRole("option").map((o) => o.textContent)).toEqual(["All", "Queued", "Running", "Success", "Failed", "Cancelled"]);
    // Nothing between the open list and the page clips it, the runs card included.
    for (let el = listbox.parentElement; el; el = el.parentElement) expect(el.getAttribute("class") ?? "").not.toMatch(/overflow-(hidden|clip|auto|y-auto)/);
    fireEvent.click(within(listbox).getByRole("option", { name: "Cancelled" }));
    expect(await screen.findByRole("button", { name: "Status: Cancelled" })).toBeInTheDocument();
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

  it("pages with Previous and Next, 25 per page", async () => {
    const fn = serve([run({ id: "4f2a9c1e7b3d4e5f8a6b0c2d1e3f5a7b", number: 12 })], { total: 60 });
    renderAt("/tasks");
    await screen.findByText("Page 1 of 3");
    expect(screen.getByRole("button", { name: "Previous" })).toBeDisabled();
    fireEvent.click(screen.getByRole("button", { name: "Next" }));
    await screen.findByText("Page 2 of 3");
    expect(listCalls(fn).at(-1)).toContain("page=2");
    expect(listCalls(fn).at(-1)).toContain("per_page=25");
    fireEvent.click(screen.getByRole("button", { name: "Previous" }));
    await screen.findByText("Page 1 of 3");
    expect(listCalls(fn).at(-1)).toContain("page=1");
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

  it("shows No task runs yet with the type's own button on an empty type page with no filter, Run task in the header", async () => {
    serve([]);
    renderAt("/tasks?type=backfill_channels");
    const title = await screen.findByText("No task runs yet");
    expect(screen.getByText("Runs show up here once a task starts")).toBeInTheDocument();
    expect(screen.queryByText("No runs match these filters")).toBeNull();
    expect(screen.queryByRole("button", { name: "Clear filters" })).toBeNull();
    expect(await screen.findAllByRole("button", { name: "Run task" })).toHaveLength(1);
    // Inside the runs frame, not a second dashed card; its one button runs this type.
    const box = title.parentElement!;
    expect(box.className).not.toMatch(/border/);
    expect(within(box).getAllByRole("button").map((b) => b.textContent)).toEqual(["Run Backfill channels"]);
  });

  it("shows No runs match these filters on a type page only when a filter is set, and Clear filters stays on the type", async () => {
    const fn = serve([]);
    renderAt("/tasks?type=backfill_channels&actor=bot");
    expect(await screen.findByText("No runs match these filters")).toBeInTheDocument();
    const clears = screen.getAllByRole("button", { name: "Clear filters" });
    fireEvent.click(clears[clears.length - 1]!);
    expect(await screen.findByText("No task runs yet")).toBeInTheDocument();
    const last = new URL(listCalls(fn).at(-1)!, "http://x").searchParams;
    expect(last.get("type")).toBe("backfill_channels");
    expect(last.get("actor")).toBeNull();
    expect(screen.getByRole("heading", { level: 2, name: "Backfill channels" })).toBeInTheDocument();
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

    it("does not ask for the status on a type that does not need the bots", async () => {
      const fn = serve([]);
      renderAt("/tasks?type=backfill_channels");
      expect(await screen.findAllByRole("button", { name: "Run task" })).toHaveLength(1);
      for (const b of screen.getAllByRole("button", { name: "Run task" })) expect(b).toBeEnabled();
      expect(fn.mock.calls.some((c) => String(c[0]) === "/api/status")).toBe(false);
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
    expect(await screen.findByTitle("Status: Cancelled")).toBeInTheDocument();
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

  it("toasts the server message when a delete is refused with 409", async () => {
    serve([run({ id: "4f2a9c1e7b3d4e5f8a6b0c2d1e3f5a7b", number: 12 })], {
      handle: (_url, method) =>
        method === "DELETE" ? Response.json({ detail: "Run is running; cancel it first" }, { status: 409 }) : undefined,
    });
    renderAt("/tasks");
    await openMenu("Backfill channels #12");
    fireEvent.click(screen.getByRole("menuitem", { name: "Delete" }));
    fireEvent.click(await screen.findByRole("button", { name: "Delete" }));
    expect(await screen.findByText("Run is running; cancel it first")).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "Backfill channels #12" })).toBeInTheDocument();
  });

  it("a finished run's menu holds View and Delete only", async () => {
    serve([run({ id: "4f2a9c1e7b3d4e5f8a6b0c2d1e3f5a7b", number: 12 })]);
    renderAt("/tasks");
    await openMenu("Backfill channels #12");
    expect(screen.getAllByRole("menuitem").map((m) => m.textContent)).toEqual(["View", "Delete"]);
  });
});
