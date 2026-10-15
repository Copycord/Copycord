import { act, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter, Route, Routes, useLocation } from "react-router";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { ToastProvider } from "../../components/Toast";
import type { TaskRun, TaskType } from "../../types/tasks";
import { ReportPanel } from "./ReportPanel";
import { RunDetail, RunHeader, RunSummary, RunView } from "./RunDetail";
import { StepList, stepLabel } from "./StepList";
import { describeRunInputs, rangeWords } from "./types";

const BACKFILL: TaskType = { key: "backfill_channels", title: "Backfill channels", description: "", inputs_schema: {}, active: 0, total: 0 };

function makeRun(over: Partial<TaskRun> = {}): TaskRun {
  return {
    id: "12",
    type: "backfill_channels",
    number: 12,
    title: "Backfill #general",
    context: { label: "Main mapping", mapping_id: "m1", guild_id: "g1" },
    status: "success",
    trigger: "manual",
    actor: "admin",
    inputs: {
      mapping_id: "m1",
      channel_ids: ["1521898853337403511", "1521898853337403512"],
      category_ids: [],
      range: { mode: "last_n", last_n: 500 },
      ignore_cloned: false,
      mode: "resume",
    },
    created_at: "2026-10-14T18:00:00Z",
    started_at: "2026-10-14T18:00:01Z",
    finished_at: "2026-10-14T18:00:49Z",
    error: null,
    summary: { success: 2, failed: 0, delivered: 1234 },
    seq: 3,
    steps: [
      { key: "c1", title: "#general", state: "running", done: 1234, total: 5000 },
      { key: "c2", title: "#news", state: "queued", queue_position: 3 },
    ],
    log_lines: [{ seq: 1, ts: "2026-10-14T18:00:01Z", level: "info", step: null, msg: "Starting" }],
    report: null,
    ...over,
  };
}

function Where() {
  const loc = useLocation();
  return <div data-testid="where">{loc.pathname + loc.search}</div>;
}

function renderHeader(run: TaskRun) {
  return render(
    <ToastProvider>
      <MemoryRouter initialEntries={["/tasks/runs/12"]}>
        <Routes>
          <Route path="*" element={<><RunHeader run={run} typeTitle="Backfill channels" /><Where /></>} />
        </Routes>
      </MemoryRouter>
    </ToastProvider>,
  );
}

function renderView(run: TaskRun, type: TaskType | undefined = BACKFILL) {
  return render(
    <MemoryRouter initialEntries={["/tasks/runs/12"]}>
      <RunView run={run} type={type} />
    </MemoryRouter>,
  );
}

const text = (el: Element | null | undefined) => (el?.textContent ?? "").trim();
const pairs = (dl: Element | null) =>
  dl ? Array.from(dl.querySelectorAll("dt")).map((dt) => [text(dt), text(dt.nextElementSibling)]) : [];

/** The page as a reader sees it: the header, then each card in order with what it holds. */
function outline(root: HTMLElement) {
  const header = root.querySelector("header")!;
  return {
    header: {
      crumbs: Array.from(header.querySelectorAll("nav a")).map(text),
      glyph: header.querySelector("[role=img]")?.getAttribute("aria-label"),
      title: text(header.querySelector("h2")),
      number: text(header.querySelector("h2 + span")),
      status: text(within(header).getByTestId("run-status")),
      actions: within(header).getAllByRole("button").map(text),
    },
    cards: Array.from(root.querySelectorAll("section[aria-label]")).map((s) => {
      const name = s.getAttribute("aria-label");
      if (name === "Summary") {
        return { name, counts: pairs(s.querySelector("dl[aria-label=Counts]")), inputs: pairs(s.querySelector("dl[aria-label=Inputs]")) };
      }
      if (name === "Steps") {
        return {
          name,
          title: text(s.querySelector("h3")),
          rows: Array.from(s.querySelectorAll("li")).map((li) => [
            li.querySelector("[role=img]")?.getAttribute("aria-label"),
            text(li.querySelector(".font-semibold")),
            text(li.querySelector("[title]:not([role])")),
            li.querySelector("[role=progressbar]") ? "bar" : "",
          ]),
        };
      }
      const details = s.querySelector("details");
      return { name, summary: text(s.querySelector("summary")), open: details?.open };
    }),
  };
}

afterEach(() => {
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

describe("RunHeader", () => {
  it("shows the breadcrumb, title, number and meta, with Re-run and Delete when finished", () => {
    renderHeader(makeRun());
    expect(screen.getByRole("link", { name: "All tasks" })).toHaveAttribute("href", "/tasks");
    expect(screen.getByRole("link", { name: "Backfill channels" })).toHaveAttribute("href", "/tasks?type=backfill_channels");
    expect(screen.getByRole("heading", { level: 2, name: "Backfill channels" })).toBeInTheDocument();
    expect(screen.getByText("#12")).toBeInTheDocument();
    expect(screen.getByText("Manual run by admin")).toBeInTheDocument();
    expect(screen.getByText("Main mapping")).toBeInTheDocument();
    expect(screen.getByTitle("Duration")).toHaveTextContent("48s");
    expect(screen.getByTestId("run-status")).toHaveTextContent("Success");
    expect(screen.queryByRole("button", { name: "Cancel run" })).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Re-run" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Delete" })).toBeInTheDocument();
  });

  it("shows the error text when failed", () => {
    renderHeader(makeRun({ status: "failed", error: "1 of 2 channels failed" }));
    expect(screen.getByText("1 of 2 channels failed")).toBeInTheDocument();
    expect(screen.getByTestId("run-status")).toHaveTextContent("Failed");
  });

  it("re-run navigates to the type form with the run id", async () => {
    const user = userEvent.setup();
    renderHeader(makeRun());
    await user.click(screen.getByRole("button", { name: "Re-run" }));
    expect(screen.getByTestId("where")).toHaveTextContent("/tasks?type=backfill_channels&rerun=12");
  });

  it("cancels an active run after the confirm", async () => {
    const user = userEvent.setup();
    const fetchMock = vi.fn(async () => Response.json({ ok: true }));
    vi.stubGlobal("fetch", fetchMock);
    renderHeader(makeRun({ status: "running", finished_at: null }));
    expect(screen.queryByRole("button", { name: "Delete" })).not.toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Cancel run" }));
    const dialog = await screen.findByRole("dialog");
    expect(within(dialog).getByText("Cancel Backfill channels #12?")).toBeInTheDocument();
    await user.click(within(dialog).getByRole("button", { name: "Cancel run" }));
    await waitFor(() => expect(fetchMock).toHaveBeenCalled());
    const [url, init] = fetchMock.mock.calls[0] as unknown as [string, RequestInit];
    expect(url).toBe("/api/tasks/runs/12/cancel");
    expect(init.method).toBe("POST");
    expect(await screen.findByText("Cancelling Backfill channels #12")).toBeInTheDocument();
  });

  it("does nothing when the cancel confirm is dismissed", async () => {
    const user = userEvent.setup();
    const fetchMock = vi.fn(async () => Response.json({ ok: true }));
    vi.stubGlobal("fetch", fetchMock);
    renderHeader(makeRun({ status: "running", finished_at: null }));
    await user.click(screen.getByRole("button", { name: "Cancel run" }));
    await user.click(within(await screen.findByRole("dialog")).getByRole("button", { name: "Keep running" }));
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it("deletes a finished run after the confirm and goes back to /tasks", async () => {
    const user = userEvent.setup();
    const fetchMock = vi.fn(async () => new Response(null, { status: 204 }));
    vi.stubGlobal("fetch", fetchMock);
    renderHeader(makeRun());
    await user.click(screen.getByRole("button", { name: "Delete" }));
    const dialog = await screen.findByRole("dialog");
    await user.click(within(dialog).getByRole("button", { name: "Delete" }));
    await waitFor(() => expect(screen.getByTestId("where")).toHaveTextContent(/^\/tasks$/));
    const [url, init] = fetchMock.mock.calls[0] as unknown as [string, RequestInit];
    expect(url).toBe("/api/tasks/runs/12");
    expect(init.method).toBe("DELETE");
    expect(await screen.findByText("Deleted Backfill channels #12")).toBeInTheDocument();
  });
});

describe("Run page structure", () => {
  it("a two step run: header, then Summary, Steps and a collapsed Report in one column, no log", () => {
    const { container } = renderView(makeRun({ status: "running", finished_at: null }));
    expect(outline(container)).toEqual({
      header: {
        crumbs: ["All tasks", "Backfill channels"],
        glyph: "Running",
        title: "Backfill channels",
        number: "#12",
        status: "Running",
        actions: ["Cancel run"],
      },
      cards: [
        {
          name: "Summary",
          counts: [
            ["Succeeded", "2"],
            ["Failed", "0"],
            ["Messages", "1,234"],
          ],
          inputs: [
            ["Mapping", "Main mapping"],
            ["Channels", "2 channels"],
            ["Categories", "0 categories"],
            ["Range", "Last 500 messages"],
            ["Ignore cloned", "No"],
            ["Mode", "Resume previous"],
          ],
        },
        {
          name: "Steps",
          title: "Steps",
          rows: [
            ["Running", "#general", "1,234 / 5,000", "bar"],
            ["Queued", "#news", "Queued #3", ""],
          ],
        },
        { name: "Report", summary: "›Report (JSON)", open: false },
      ],
    });
    // No log anywhere, and the cards stack in one column with no sidebar.
    expect(screen.queryByRole("log")).not.toBeInTheDocument();
    expect(screen.queryByRole("region", { name: "Log" })).not.toBeInTheDocument();
    expect(screen.queryByText("Starting")).not.toBeInTheDocument();
    const body = screen.getByRole("region", { name: "Summary" }).parentElement!;
    expect(body.className).not.toMatch(/grid/);
    expect(Array.from(body.children).map((c) => c.getAttribute("aria-label"))).toEqual(["Summary", "Steps", "Report"]);
  });
});

describe("StepList", () => {
  it("one row per step with its note, a bar only under a running step, nothing clickable", () => {
    const run = makeRun({
      steps: [
        { key: "c1", title: "#general", state: "running", done: 1234, total: 5000 },
        { key: "c2", title: "#news", state: "success", done: 40, total: 40, note: "40 messages" },
        { key: "c3", title: "#old", state: "failed", note: "Missing access" },
      ],
    });
    render(<StepList run={run} />);
    expect(screen.getAllByRole("listitem")).toHaveLength(3);
    expect(screen.getByText("1,234 / 5,000")).toBeInTheDocument();
    expect(screen.getByText("40 messages")).toBeInTheDocument();
    expect(screen.getByText("Missing access")).toHaveClass("text-danger");
    expect(screen.getAllByRole("progressbar")).toHaveLength(1);
    expect(screen.getByRole("progressbar", { name: "#general progress" })).toHaveAttribute("aria-valuenow", "1234");
    expect(screen.queryByRole("button")).not.toBeInTheDocument();
  });

  it("says when there are no steps yet", () => {
    render(<StepList run={makeRun({ steps: [] })} />);
    expect(screen.getByText("No steps yet")).toBeInTheDocument();
  });

  it("prefers the server label and the note", () => {
    expect(stepLabel({ key: "a", title: "#a", state: "running", label: "Cleaning up", done: 1, total: 2 })).toBe("Cleaning up");
    expect(stepLabel({ key: "a", title: "#a", state: "skipped", note: "Already running in #12" })).toBe("Already running in #12");
    expect(stepLabel({ key: "a", title: "#a", state: "queued", queue_position: 2 })).toBe("Queued #2");
    expect(stepLabel({ key: "a", title: "#a", state: "cancelled" })).toBe("Cancelled");
  });
});

describe("RunSummary", () => {
  const summaryOf = (run: TaskRun, schema?: Record<string, unknown>) => {
    const { container } = render(<RunSummary run={run} schema={schema} />);
    return {
      counts: pairs(container.querySelector("dl[aria-label=Counts]")),
      inputs: pairs(container.querySelector("dl[aria-label=Inputs]")),
    };
  };

  it("has no tiles until the run reports counts", () => {
    expect(summaryOf(makeRun({ status: "running", summary: null })).counts).toEqual([]);
    expect(screen.getByRole("heading", { name: "Inputs" })).toBeInTheDocument();
  });

  it("falls back to the report's summary", () => {
    expect(summaryOf(makeRun({ summary: null, report: { summary: { delivered: 7, note: "x" } } })).counts).toEqual([["Messages", "7"]]);
  });

  it("a type without a registry entry: counts in words, inputs by schema title in schema order", () => {
    const run = makeRun({ type: "prune", summary: { pruned: 3, kept_rows: 1200 }, inputs: { extra: "x", dry_run: true, limit: 5 } });
    const schema = { properties: { limit: { type: "integer", title: "Row limit" }, dry_run: { type: "boolean" } } };
    expect(summaryOf(run, schema)).toEqual({
      counts: [
        ["Pruned", "3"],
        ["Kept rows", "1,200"],
      ],
      inputs: [
        ["Row limit", "5"],
        ["Dry run", "Yes"],
        ["Extra", "x"],
      ],
    });
  });

  it("says when a run has no inputs", () => {
    render(<RunSummary run={makeRun({ inputs: {} })} />);
    expect(screen.getByText("No inputs")).toBeInTheDocument();
  });
});

describe("Inputs formatter", () => {
  it("formats backfill keys in words and falls back to the key in words", () => {
    const rows = describeRunInputs({
      type: "backfill_channels",
      context: { label: "Main mapping" },
      inputs: {
        mapping_id: "m1",
        channel_ids: ["a"],
        category_ids: ["k"],
        range: { mode: "last_n", last_n: 500 },
        ignore_cloned: true,
        mode: "new",
        extra: { a: 1 },
      },
    });
    expect(rows.map((r) => [r.label, r.value])).toEqual([
      ["Mapping", "Main mapping"],
      ["Channels", "1 channel"],
      ["Categories", "1 category"],
      ["Range", "Last 500 messages"],
      ["Ignore cloned", "Yes"],
      ["Mode", "Start new"],
      ["Extra", '{"a":1}'],
    ]);
    expect(rangeWords({ mode: "all" })).toBe("All history");
    expect(describeRunInputs({ type: "backfill_channels", context: null, inputs: { mapping_id: "m1" } })).toEqual([{ label: "Mapping", value: "m1" }]);
  });
});

describe("ReportPanel", () => {
  it("is collapsed by default and copies and downloads the pretty JSON", async () => {
    const user = userEvent.setup();
    const writeText = vi.fn(async () => undefined);
    Object.defineProperty(navigator, "clipboard", { configurable: true, value: { writeText } });
    const report = { channels: [{ id: "c1", delivered: 10 }] };
    render(
      <ToastProvider>
        <ReportPanel run={makeRun({ report })} />
      </ToastProvider>,
    );
    const details = screen.getByRole("region", { name: "Report" }).querySelector("details")!;
    expect(details.open).toBe(false);
    await user.click(screen.getByText("Report (JSON)"));
    expect(details.open).toBe(true);
    const json = JSON.stringify(report, null, 2);
    expect(screen.getByLabelText("Report JSON").textContent).toBe(json);
    expect(screen.getByRole("link", { name: "Download" })).toHaveAttribute("href", "/api/tasks/runs/12/report.json");
    expect(screen.getByRole("link", { name: "Download" })).toHaveAttribute("download");
    await user.click(screen.getByRole("button", { name: "Copy" }));
    expect(writeText).toHaveBeenCalledWith(json);
    expect(await screen.findByText("Report copied")).toBeInTheDocument();
  });

  it("says the report comes at the end while the run is active", () => {
    render(<ReportPanel run={makeRun({ status: "running", report: null })} />);
    expect(screen.getByText("The report appears when the run finishes")).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Copy" })).not.toBeInTheDocument();
  });
});

class FakeWs {
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

describe("RunDetail live", () => {
  beforeEach(() => {
    FakeWs.all = [];
    vi.stubGlobal("WebSocket", FakeWs);
  });

  it("shows the run skeleton first, then follows task.run.updated frames without going back to it", async () => {
    const first = makeRun({ status: "running", finished_at: null, summary: null, seq: 1 });
    vi.stubGlobal("fetch", vi.fn(async () => Response.json({ run: first })));
    render(
      <MemoryRouter initialEntries={["/tasks/runs/12"]}>
        <RunDetail id="12" typeOf={(k) => (k === BACKFILL.key ? BACKFILL : undefined)} />
      </MemoryRouter>,
    );
    expect(screen.getByRole("status", { name: "Loading run" })).toBeInTheDocument();
    await screen.findByText("1,234 / 5,000");
    expect(screen.queryByRole("status", { name: "Loading run" })).not.toBeInTheDocument();
    expect(screen.getByRole("region", { name: "Summary" }).querySelector("dl[aria-label=Counts]")).toBeNull();

    const ws = FakeWs.all[0]!;
    act(() => ws.open());
    const { inputs: _inputs, steps: _steps, ...bare } = first;
    act(() =>
      ws.emit({ kind: "task.run.updated", role: "admin", payload: { run: { ...bare, seq: 2 }, steps: [{ key: "c1", title: "#general", state: "running", done: 4000, total: 5000 }] } }),
    );
    expect(screen.getByText("4,000 / 5,000")).toBeInTheDocument();
    expect(screen.getByRole("progressbar", { name: "#general progress" })).toHaveAttribute("aria-valuenow", "4000");

    act(() =>
      ws.emit({
        kind: "task.run.updated",
        role: "admin",
        payload: {
          run: { ...bare, seq: 3, status: "success", finished_at: "2026-10-14T18:00:49Z", summary: { success: 1, failed: 0, delivered: 36 } },
          steps: [
            { key: "c1", title: "#general", state: "success", done: 36, total: 5000, note: "36 messages" },
            { key: "c2", title: "#news", state: "skipped", note: "Already running in #11" },
          ],
        },
      }),
    );
    expect(screen.getByTestId("run-status")).toHaveTextContent("Success");
    expect(screen.getByText("36 messages")).toBeInTheDocument();
    expect(screen.queryByRole("progressbar")).not.toBeInTheDocument();
    const counts = screen.getByRole("region", { name: "Summary" }).querySelector("dl[aria-label=Counts]");
    expect(pairs(counts)).toEqual([
      ["Succeeded", "1"],
      ["Failed", "0"],
      ["Messages", "36"],
    ]);
    // The inputs are not in frames; the held ones stay.
    expect(screen.getByText("Last 500 messages")).toBeInTheDocument();
    expect(screen.queryByRole("status", { name: "Loading run" })).not.toBeInTheDocument();
  });
});
