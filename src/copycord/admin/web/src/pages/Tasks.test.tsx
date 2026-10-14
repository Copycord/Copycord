import { render, screen, within } from "@testing-library/react";
import { MemoryRouter, Route, Routes } from "react-router";
import { beforeEach, describe, expect, it, vi } from "vitest";
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

function serve(runs: TaskRun[]) {
  vi.stubGlobal(
    "fetch",
    vi.fn(async (url: string) => {
      if (url === "/api/tasks/types") return Response.json(TYPES);
      if (url.startsWith("/api/tasks/runs?")) return Response.json({ runs, total: runs.length, page: 1, actors: [], contexts: [] });
      const id = url.split("/").pop();
      const hit = runs.find((r) => r.id === id);
      return hit ? Response.json({ run: { ...hit, steps: [] } }) : Response.json({ detail: "not found" }, { status: 404 });
    }),
  );
}

function renderAt(path: string) {
  return render(
    <MemoryRouter initialEntries={[path]}>
      <Routes>
        <Route path="/tasks" element={<Tasks />} />
        <Route path="/tasks/runs/:id" element={<Tasks />} />
      </Routes>
    </MemoryRouter>,
  );
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
    expect(within(rows[0]).getByRole("link", { name: "Backfill #general" })).toHaveAttribute("href", "/tasks/runs/12");
    expect(within(rows[0]).getByRole("img", { name: "Success" })).toBeInTheDocument();
    expect(within(rows[1]).getByRole("img", { name: "Failed" })).toBeInTheDocument();
    expect(await within(rows[0]).findByText("Backfill channels #12: Manual run by admin")).toBeInTheDocument();
    expect(within(rows[0]).getByText("Main mapping")).toBeInTheDocument();
    expect(within(rows[0]).getByText("48s")).toBeInTheDocument();

    const rail = screen.getByRole("navigation", { name: "Task types" });
    expect(within(rail).getByRole("link", { name: /All tasks/ })).toHaveAttribute("aria-current", "page");
    expect(within(rail).getByRole("link", { name: /Backfill channels/ })).toHaveAttribute("href", "/tasks?type=backfill_channels");
    expect(within(rail).getByLabelText("1 active")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Run task" })).toBeDisabled();
  });

  it("shows the empty state when there are no runs", async () => {
    serve([]);
    renderAt("/tasks");
    expect(await screen.findByText("No task runs yet")).toBeInTheDocument();
    expect(screen.getByText("0 task runs")).toBeInTheDocument();
  });

  it("shows the type title and description when a type is picked", async () => {
    serve([]);
    renderAt("/tasks?type=backfill_channels");
    expect(await screen.findByRole("heading", { level: 2, name: "Backfill channels" })).toBeInTheDocument();
    expect(screen.getByText("Clone message history")).toBeInTheDocument();
  });

  it("opens a run with its header and the Steps, Log and Report sections", async () => {
    serve([run({ id: "12", number: 12 })]);
    renderAt("/tasks/runs/12");
    expect(await screen.findByRole("heading", { level: 2, name: "Backfill #general" })).toBeInTheDocument();
    expect(screen.getByText("#12")).toBeInTheDocument();
    for (const name of ["Steps", "Log", "Report"]) expect(screen.getByRole("region", { name })).toBeInTheDocument();
  });
});
