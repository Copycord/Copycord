import { act, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { MemoryRouter, Route, Routes } from "react-router";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { ToastProvider } from "../components/Toast";
import { ConfirmProvider } from "../components/ui/ConfirmDialog";
import type { TaskRun } from "../types/tasks";
import { Tasks } from "./Tasks";

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
  it("lists runs with status, title link, type line and context badge", async () => {
    serve([run({ id: "12", number: 12 }), run({ id: "11", number: 11, title: "Backfill 3 channels", status: "failed" })]);
    renderAt("/tasks");
    const list = await screen.findByRole("list", { name: "Task runs" });
    const rows = within(list).getAllByRole("listitem");
    expect(rows).toHaveLength(2);
    expect(screen.getByText("2 task runs")).toBeInTheDocument();
    expect(within(rows[0]!).getByRole("link", { name: "Backfill #general" })).toHaveAttribute("href", "/tasks/runs/12");
    expect(within(rows[0]!).getByRole("img", { name: "Success" })).toBeInTheDocument();
    expect(within(rows[1]!).getByRole("img", { name: "Failed" })).toBeInTheDocument();
    expect(await within(rows[0]!).findByText("Backfill channels #12: Manual run by admin")).toBeInTheDocument();
    expect(within(rows[0]!).getByText("Main mapping")).toBeInTheDocument();
    expect(within(rows[0]!).getByText("48s")).toBeInTheDocument();

    const rail = screen.getByRole("navigation", { name: "Task types" });
    expect(within(rail).getByRole("link", { name: /All tasks/ })).toHaveAttribute("aria-current", "page");
    expect(within(rail).getByRole("link", { name: /Backfill channels/ })).toHaveAttribute("href", "/tasks?type=backfill_channels");
    expect(within(rail).getByLabelText("1 active")).toBeInTheDocument();
    // All tasks has no Run task: the rail picks a type first.
    expect(screen.queryByRole("button", { name: "Run task" })).toBeNull();
  });

  it("shows the empty state when there are no runs", async () => {
    serve([]);
    renderAt("/tasks");
    expect(await screen.findByText("No task runs yet")).toBeInTheDocument();
    expect(screen.getByText("0 task runs")).toBeInTheDocument();
    expect(screen.getByText("Pick a task type on the left to run one")).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Run task" })).toBeNull();
  });

  it("shows the type title and description when a type is picked", async () => {
    serve([]);
    renderAt("/tasks?type=backfill_channels");
    expect(await screen.findByRole("heading", { level: 2, name: "Backfill channels" })).toBeInTheDocument();
    expect(screen.getByText("Clone message history")).toBeInTheDocument();
    expect(screen.getAllByRole("button", { name: "Run task" }).length).toBeGreaterThan(0);
  });

  it("opens a run with its header and the Steps, Log and Report sections", async () => {
    serve([run({ id: "12", number: 12 })]);
    renderAt("/tasks/runs/12");
    expect(await screen.findByRole("heading", { level: 2, name: "Backfill #general" })).toBeInTheDocument();
    expect(screen.getByText("#12")).toBeInTheDocument();
    for (const name of ["Steps", "Log", "Report"]) expect(screen.getByRole("region", { name })).toBeInTheDocument();
  });

  it("sends the URL filters with the list fetch and narrows it from the dropdowns", async () => {
    const fn = serve([run({ id: "12", number: 12 })]);
    renderAt("/tasks?status=failed&actor=admin&q=general&context=m1&page=2");
    await screen.findByRole("link", { name: "Backfill #general" });
    const first = new URL(listCalls(fn)[0]!, "http://x").searchParams;
    expect(Object.fromEntries(first)).toMatchObject({ status: "failed", actor: "admin", q: "general", context: "m1", page: "2" });
    expect(screen.getByRole("searchbox", { name: "Search runs" })).toHaveValue("general");

    fireEvent.change(sel("actor"), { target: { value: "bot" } });
    await waitFor(() => expect(listCalls(fn).at(-1)).toContain("actor=bot"));
    // A filter change goes back to page 1.
    expect(listCalls(fn).at(-1)).toContain("page=1");
    expect(listCalls(fn).at(-1)).toContain("status=failed");
  });

  it("pages with Previous and Next, 25 per page", async () => {
    const fn = serve([run({ id: "12", number: 12 })], { total: 60 });
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

  it("shows No runs match with Clear filters when a filtered list is empty", async () => {
    const fn = serve([]);
    renderAt("/tasks?status=failed");
    expect(await screen.findByText("No runs match")).toBeInTheDocument();
    const clears = screen.getAllByRole("button", { name: "Clear filters" });
    fireEvent.click(clears[clears.length - 1]!);
    expect(await screen.findByText("No task runs yet")).toBeInTheDocument();
    expect(listCalls(fn).at(-1)).not.toContain("status=");
    expect(screen.getByText("Pick a task type on the left to run one")).toBeInTheDocument();
  });

  it("shows No task runs yet with Run task on an empty type page with no filter", async () => {
    serve([]);
    renderAt("/tasks?type=backfill_channels");
    expect(await screen.findByText("No task runs yet")).toBeInTheDocument();
    expect(screen.queryByText("No runs match")).toBeNull();
    expect(screen.queryByRole("button", { name: "Clear filters" })).toBeNull();
    expect(await screen.findAllByRole("button", { name: "Run task" })).toHaveLength(2);
  });

  it("shows No runs match on a type page only when a filter is set, and Clear filters stays on the type", async () => {
    const fn = serve([]);
    renderAt("/tasks?type=backfill_channels&actor=bot");
    expect(await screen.findByText("No runs match")).toBeInTheDocument();
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
      expect(await screen.findAllByText("Start the bots first")).toHaveLength(2);
      for (const b of screen.getAllByRole("button", { name: "Run task" })) expect(b).toBeDisabled();
    });

    it("turns Run task on once the status says both bots run", async () => {
      vi.useFakeTimers({ shouldAdvanceTime: true });
      const up = { value: false };
      serveBots(up);
      renderAt("/tasks?type=backfill_channels");
      expect(await screen.findAllByText("Start the bots first")).toHaveLength(2);
      up.value = true;
      await act(() => vi.advanceTimersByTimeAsync(4000));
      await waitFor(() => expect(screen.queryByText("Start the bots first")).toBeNull());
      for (const b of screen.getAllByRole("button", { name: "Run task" })) expect(b).toBeEnabled();
    });

    it("does not ask for the status on a type that does not need the bots", async () => {
      const fn = serve([]);
      renderAt("/tasks?type=backfill_channels");
      expect(await screen.findAllByRole("button", { name: "Run task" })).toHaveLength(2);
      for (const b of screen.getAllByRole("button", { name: "Run task" })) expect(b).toBeEnabled();
      expect(fn.mock.calls.some((c) => String(c[0]) === "/api/status")).toBe(false);
    });
  });

  it("cancels an active run after confirming and patches the row from the response", async () => {
    const live = run({ id: "12", number: 12, status: "running", finished_at: null, seq: 4 });
    const calls: string[] = [];
    serve([live], {
      handle: (url, method) => {
        if (method !== "POST") return undefined;
        calls.push(url);
        return Response.json({ run: { ...live, status: "cancelled", seq: 5, finished_at: "2026-10-14T18:01:00Z" } });
      },
    });
    renderAt("/tasks");
    await openMenu("Backfill #general");
    expect(screen.queryByRole("menuitem", { name: "Delete" })).toBeNull();
    fireEvent.click(screen.getByRole("menuitem", { name: "Cancel" }));
    fireEvent.click(await screen.findByRole("button", { name: "Cancel run" }));
    expect(await screen.findByRole("img", { name: "Cancelled" })).toBeInTheDocument();
    expect(calls).toEqual(["/api/tasks/runs/12/cancel"]);
  });

  it("toasts the server message when a cancel is refused with 409", async () => {
    const live = run({ id: "12", number: 12, status: "running", finished_at: null });
    serve([live], {
      handle: (_url, method) => (method === "POST" ? Response.json({ detail: "Run already success" }, { status: 409 }) : undefined),
    });
    renderAt("/tasks");
    await openMenu("Backfill #general");
    fireEvent.click(screen.getByRole("menuitem", { name: "Cancel" }));
    fireEvent.click(await screen.findByRole("button", { name: "Cancel run" }));
    expect(await screen.findByText("Run already success")).toBeInTheDocument();
  });

  it("deletes a finished run after confirming and drops the row without a refetch", async () => {
    const deleted: string[] = [];
    const fn = serve([run({ id: "12", number: 12 }), run({ id: "11", number: 11, title: "Backfill 3 channels" })], {
      handle: (url, method) => {
        if (method !== "DELETE") return undefined;
        deleted.push(url);
        return new Response(null, { status: 204 });
      },
    });
    renderAt("/tasks");
    await openMenu("Backfill #general");
    expect(screen.queryByRole("menuitem", { name: "Cancel" })).toBeNull();
    fireEvent.click(screen.getByRole("menuitem", { name: "Delete" }));
    const before = listCalls(fn).length;
    fireEvent.click(await screen.findByRole("button", { name: "Delete" }));
    await waitFor(() => expect(screen.queryByRole("link", { name: "Backfill #general" })).toBeNull());
    expect(deleted).toEqual(["/api/tasks/runs/12"]);
    expect(screen.getByText("1 task run")).toBeInTheDocument();
    expect(await screen.findByText("Deleted Backfill channels #12")).toBeInTheDocument();
    expect(listCalls(fn).length).toBe(before);
  });

  it("toasts the server message when a delete is refused with 409", async () => {
    serve([run({ id: "12", number: 12 })], {
      handle: (_url, method) =>
        method === "DELETE" ? Response.json({ detail: "Run is running; cancel it first" }, { status: 409 }) : undefined,
    });
    renderAt("/tasks");
    await openMenu("Backfill #general");
    fireEvent.click(screen.getByRole("menuitem", { name: "Delete" }));
    fireEvent.click(await screen.findByRole("button", { name: "Delete" }));
    expect(await screen.findByText("Run is running; cancel it first")).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "Backfill #general" })).toBeInTheDocument();
  });

  it("Re-run goes to the type with the run to copy in the URL", async () => {
    serve([run({ id: "12", number: 12 })]);
    renderAt("/tasks");
    await openMenu("Backfill #general");
    fireEvent.click(screen.getByRole("menuitem", { name: "Re-run" }));
    expect(await screen.findByRole("heading", { level: 2, name: "Backfill channels" })).toBeInTheDocument();
  });
});
