import { act, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter, Route, Routes, useLocation } from "react-router";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { ToastProvider } from "../../components/Toast";
import type { TaskRun, TaskStep, TaskType } from "../../types/tasks";
import { ReportPanel } from "./ReportPanel";
import { RunDetail, RunHeader, RunSummary, RunView } from "./RunDetail";
import { StepList, orderSteps, overallProgress, stepLabel, stepSummary } from "./StepList";
import { BUSY, FADE_IN, RUN_HEAD_MAIN, STEP_LIST, STEP_ROW, STEP_ROWS_MAX } from "./TasksSkeleton";
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
          summary: text(s.querySelector("[data-testid=steps-summary]")),
          filter: text(s.querySelector("button[aria-haspopup=listbox]")?.firstElementChild),
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
          summary: "1 running, 1 queued",
          filter: "Status: All",
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

  it("fades in only what stood as bars in the skeleton, and dims the held run while a refetch is out", () => {
    const run = makeRun({ status: "running", finished_at: null });
    const { container, rerender } = renderView(run);
    const fades = (el: Element | null) => (el?.getAttribute("class") ?? "").includes(FADE_IN);
    expect(fades(screen.getByRole("link", { name: "Backfill channels" }))).toBe(true);
    expect(fades(container.querySelector(`[class^="${RUN_HEAD_MAIN} "]`))).toBe(true);
    expect(fades(container.querySelector("dl[aria-label=Counts]"))).toBe(true);
    expect(fades(container.querySelector("dl[aria-label=Inputs]"))).toBe(true);
    expect(fades(within(screen.getByRole("region", { name: "Steps" })).getByRole("list"))).toBe(true);
    expect(fades(screen.getByTestId("steps-summary").firstElementChild)).toBe(true);
    expect(fades(screen.getByRole("progressbar", { name: "Overall progress" }))).toBe(true);
    // The static text the skeleton already showed does not blink.
    expect(fades(screen.getByRole("link", { name: "All tasks" }))).toBe(false);
    expect(fades(screen.getByRole("heading", { name: "Steps" }))).toBe(false);
    const view = container.firstElementChild!;
    expect(view.className).not.toContain(BUSY);
    rerender(
      <MemoryRouter initialEntries={["/tasks/runs/12"]}>
        <RunView run={run} type={BACKFILL} refreshing />
      </MemoryRouter>,
    );
    expect(view).toHaveAttribute("aria-busy", "true");
    expect(view.className).toContain(BUSY);
  });
});

describe("StepList", () => {
  it("one line per step with its note, a thin bar only on a running step, nothing clickable but the filter", () => {
    const run = makeRun({
      steps: [
        { key: "c1", title: "#general", state: "running", done: 1234, total: 5000 },
        { key: "c2", title: "#news", state: "success", done: 40, total: 40, note: "40 messages" },
        { key: "c3", title: "#old", state: "failed", note: "Missing access" },
      ],
    });
    render(<StepList run={run} />);
    const rows = screen.getAllByRole("listitem");
    expect(rows).toHaveLength(3);
    for (const li of rows) expect(li.className).toBe(STEP_ROW);
    expect(screen.getByText("1,234 / 5,000")).toBeInTheDocument();
    expect(screen.getByText("40 messages")).toBeInTheDocument();
    expect(screen.getByText("Missing access")).toHaveClass("text-danger");
    expect(screen.getAllByRole("progressbar").map((b) => b.getAttribute("aria-label"))).toEqual(["Overall progress", "#general progress"]);
    expect(screen.getByRole("progressbar", { name: "#general progress" })).toHaveAttribute("aria-valuenow", "1234");
    // The row bar sits on the step's own line, not under it.
    expect(screen.getByRole("progressbar", { name: "#general progress" }).parentElement).toBe(rows[0]);
    expect(screen.getAllByRole("button").map((b) => b.textContent)).toEqual(["Status: All▾"]);
  });

  it("counts the steps by state in the head, and fills the overall bar by the share done", () => {
    const steps: TaskStep[] = [
      ...Array.from({ length: 94 }, (_, i): TaskStep => ({ key: `d${i}`, title: `#done-${i}`, state: "success", note: "3 messages" })),
      { key: "r1", title: "#r1", state: "running", done: 25, total: 100 },
      { key: "r2", title: "#r2", state: "running", done: 0, total: 10 },
      { key: "r3", title: "#r3", state: "running" },
      { key: "q1", title: "#q1", state: "queued", queue_position: 1 },
      { key: "q2", title: "#q2", state: "queued", queue_position: 2 },
      { key: "f1", title: "#f1", state: "failed", note: "delivery timed out" },
    ];
    expect(stepSummary(steps)).toBe("3 running, 2 queued, 94 done, 1 failed");
    expect(stepSummary([{ key: "s", title: "#s", state: "skipped" }, { key: "c", title: "#c", state: "cancelled" }])).toBe("1 skipped, 1 cancelled");
    expect(stepSummary([])).toBe("");
    // 95 finished whole, a quarter of #r1, nothing for the rest: 95.25 of 100.
    expect(overallProgress(steps)).toBeCloseTo(0.9525);
    expect(overallProgress([])).toBeNull();
    render(<StepList run={makeRun({ steps })} />);
    expect(screen.getByTestId("steps-summary")).toHaveTextContent("3 running, 2 queued, 94 done, 1 failed");
    expect(screen.getByRole("progressbar", { name: "Overall progress" })).toHaveAttribute("aria-valuenow", "95");
  });

  it("puts running and failed steps first, then queued in queue order, then the finished ones in run order", () => {
    const steps: TaskStep[] = [
      { key: "a", title: "#a", state: "success" },
      { key: "b", title: "#b", state: "queued", queue_position: 3 },
      { key: "c", title: "#c", state: "failed" },
      { key: "d", title: "#d", state: "queued", queue_position: 2 },
      { key: "e", title: "#e", state: "running", done: 1, total: 2 },
      { key: "f", title: "#f", state: "skipped" },
      { key: "g", title: "#g", state: "running" },
    ];
    expect(orderSteps(steps).map((s) => s.key)).toEqual(["e", "g", "c", "d", "b", "a", "f"]);
    render(<StepList run={makeRun({ steps })} />);
    expect(screen.getAllByRole("listitem").map((li) => li.querySelector(".font-semibold")?.textContent)).toEqual(["#e", "#g", "#c", "#d", "#b", "#a", "#f"]);
  });

  it("filters by state from the head, All by default, and says when a state has none", async () => {
    const user = userEvent.setup();
    const steps: TaskStep[] = [
      { key: "a", title: "#a", state: "success" },
      { key: "b", title: "#b", state: "failed", note: "delivery timed out" },
      { key: "c", title: "#c", state: "success" },
    ];
    render(<StepList run={makeRun({ steps })} />);
    const titles = () => screen.queryAllByRole("listitem").map((li) => li.querySelector(".font-semibold")?.textContent);
    expect(titles()).toEqual(["#b", "#a", "#c"]);
    const pick = async (label: string) => {
      await user.click(screen.getByRole("button", { name: /^Status: / }));
      await user.click(screen.getByRole("option", { name: label }));
    };
    await pick("Status: Done");
    expect(titles()).toEqual(["#a", "#c"]);
    await pick("Status: Failed");
    expect(titles()).toEqual(["#b"]);
    await pick("Status: Running");
    expect(titles()).toEqual([]);
    expect(screen.getByText("No running steps")).toBeInTheDocument();
    // The head still counts every step.
    expect(screen.getByTestId("steps-summary")).toHaveTextContent("2 done, 1 failed");
    await pick("Status: All");
    expect(titles()).toEqual(["#b", "#a", "#c"]);
  });

  it("caps the list at twelve rows and scrolls past them", () => {
    const steps = Array.from({ length: 100 }, (_, i): TaskStep => ({ key: `s${i}`, title: `#s${i}`, state: "success" }));
    render(<StepList run={makeRun({ steps })} />);
    const list = within(screen.getByRole("region", { name: "Steps" })).getByRole("list");
    expect(list.className).toContain(STEP_LIST);
    expect(STEP_LIST).toContain("overflow-y-auto");
    // Rows are 32 px (h-8), so the cap is twelve of them.
    expect(STEP_ROW).toContain("h-8");
    expect(STEP_LIST).toContain(`max-h-[${STEP_ROWS_MAX * 32}px]`);
    expect(screen.getAllByRole("listitem")).toHaveLength(100);
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
    expect(screen.queryByRole("progressbar", { name: "#general progress" })).not.toBeInTheDocument();
    expect(screen.getByRole("progressbar", { name: "Overall progress" })).toHaveAttribute("aria-valuenow", "100");
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
