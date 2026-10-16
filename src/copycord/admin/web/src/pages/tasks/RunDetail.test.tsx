import { act, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter, Route, Routes, useLocation } from "react-router";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { ToastProvider } from "../../components/Toast";
import type { TaskRun, TaskStep, TaskType } from "../../types/tasks";
import { ReportPanel } from "./ReportPanel";
import { RunDetail, RunHeader, RunSummary, RunView } from "./RunDetail";
import { formatAgo, formatWhen, runConfirm } from "./RunList";
import { finishedText, outcomeText, stepCounts } from "./Outcome";
import { StepList, orderSteps, stepLabel, stepSummary } from "./StepList";
import {
  BUSY,
  FADE_IN,
  INPUTS_LIST,
  OVERVIEW,
  RUN_HEAD_ROW,
  RUN_TITLE_ROW,
  RunDetailSkeleton,
  STATS_LIST,
  STAT_ZERO,
  STEP_LIST,
  STEP_ROW,
  STEP_ROWS_MAX,
} from "./TasksSkeleton";
import { backfillStepNote, channelNames, describeRunInputs, rangeWords } from "./types";

const BACKFILL: TaskType = { key: "backfill_channels", title: "Backfill channels", description: "", inputs_schema: {}, active: 0, total: 0 };

function makeRun(over: Partial<TaskRun> = {}): TaskRun {
  return {
    id: "4f2a9c1e7b3d4e5f8a6b0c2d1e3f5a7b",
    type: "backfill_channels",
    number: 12,
    title: "Backfill #general",
    context: { label: "Main mapping", mapping_id: "m1", guild_id: "g1" },
    status: "success",
    trigger: "manual",
    actor: "admin",
    inputs: {
      mapping_id: "m1",
      channel_ids: ["c1", "c2"],
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
      <MemoryRouter initialEntries={["/tasks/runs/4f2a9c1e7b3d4e5f8a6b0c2d1e3f5a7b"]}>
        <Routes>
          <Route path="*" element={<><RunHeader run={run} typeTitle="Backfill channels" /><Where /></>} />
        </Routes>
      </MemoryRouter>
    </ToastProvider>,
  );
}

function renderView(run: TaskRun, type: TaskType | undefined = BACKFILL) {
  return render(
    <MemoryRouter initialEntries={["/tasks/runs/4f2a9c1e7b3d4e5f8a6b0c2d1e3f5a7b"]}>
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
      title: text(header.querySelector("h2")),
      number: text(header.querySelector("h2 + span")),
      status: text(within(header).getByTestId("run-status")),
      meta: text(within(header).getByTestId("run-meta")),
      actions: within(header).getAllByRole("button").map((b) => b.getAttribute("aria-label") ?? text(b)),
    },
    cards: Array.from(root.querySelectorAll("section[aria-label]")).map((s) => {
      const name = s.getAttribute("aria-label");
      if (name === "Summary") {
        return {
          name,
          hero: text(s.querySelector("[data-testid=run-hero]")),
          finished: text(s.querySelector("[data-testid=run-finished]")),
          bar: s.querySelector("[role=progressbar]")?.getAttribute("aria-label") ?? null,
          counts: pairs(s.querySelector("dl[aria-label=Counts]")),
          inputs: pairs(s.querySelector("dl[aria-label=Inputs]")),
        };
      }
      if (name === "Steps") {
        return {
          name,
          title: text(s.querySelector("h3")),
          summary: text(s.querySelector("[data-testid=steps-summary]")),
          filter: text(s.querySelector("button[aria-haspopup=listbox]")?.firstElementChild),
          rows: Array.from(s.querySelectorAll("li")).map((li) => [
            li.querySelector("[role=img]")?.getAttribute("aria-label"),
            text(li.querySelector(".font-medium")),
            text(li.querySelector("[title]:not([role])")),
            li.querySelector("[role=progressbar]") ? "bar" : "",
            text(li.lastElementChild),
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
  const menuItems = async (user: ReturnType<typeof userEvent.setup>) => {
    await user.click(screen.getByRole("button", { name: "Run actions" }));
    return within(screen.getByRole("menu")).getAllByRole("menuitem").map((b) => b.textContent);
  };

  it("shows the breadcrumb, title, number, the one status badge and the actions menu on one line, the meta line under it", () => {
    renderHeader(makeRun());
    expect(screen.getByRole("link", { name: "All tasks" })).toHaveAttribute("href", "/tasks");
    expect(screen.getByRole("link", { name: "Backfill channels" })).toHaveAttribute("href", "/tasks?type=backfill_channels");
    expect(screen.getByRole("heading", { level: 2, name: "Backfill channels" })).toBeInTheDocument();
    expect(screen.getByText("#12")).toBeInTheDocument();
    expect(screen.getByTestId("run-status")).toHaveTextContent("Success");
    // The badge sits beside the number; no glyph of the header's own.
    expect(screen.getByTestId("run-status").parentElement!.className).toBe(RUN_TITLE_ROW);
    expect(screen.getAllByRole("img", { hidden: true }).filter((g) => g.hasAttribute("data-status"))).toHaveLength(1);
    // The menu button sits on the title's row; a finished run has no Cancel run.
    const row = screen.getByRole("heading", { level: 2 }).closest(`[class^="${RUN_HEAD_ROW}"]`)!;
    expect(within(row as HTMLElement).getAllByRole("button").map((b) => b.getAttribute("aria-label") ?? text(b))).toEqual(["Run actions"]);
  });

  it("says how, when, how long, the mapping and the ID on one muted line under the title", () => {
    renderHeader(makeRun());
    const started = "2026-10-14T18:00:01Z";
    const meta = screen.getByTestId("run-meta");
    expect(text(meta)).toBe(
      `Triggered manually by admin·started ${formatWhen(started)} (${formatAgo(started, Date.now())})·ran 48s·mapping Main mapping·ID 4f2a9c1e`,
    );
    expect(meta.tagName).toBe("P");
    // 12 px glyphs before trigger, started, duration and mapping, and on the copy button.
    expect(Array.from(meta.querySelectorAll("svg")).map((s) => s.getAttribute("width"))).toEqual(["12", "12", "12", "12", "12"]);
    // The mapping is a quiet outlined chip; the ID is mono with the whole id in its title and a copy button.
    expect(within(meta).getByText("Main mapping")).toHaveClass("rounded-full", "border");
    const id = screen.getByTestId("run-id");
    expect(id).toHaveTextContent("4f2a9c1e");
    expect(id).toHaveClass("font-mono");
    expect(id).toHaveAttribute("title", "Run ID 4f2a9c1e7b3d4e5f8a6b0c2d1e3f5a7b");
    expect(within(meta).getByRole("button", { name: "Copy run ID" })).toBeInTheDocument();
    // Other triggers, no actor, and a run still going.
    const { unmount } = renderHeader(makeRun({ trigger: "api", actor: "", status: "running", finished_at: null }));
    const live = screen.getAllByTestId("run-meta")[1]!;
    expect(text(live)).toMatch(/^Triggered via the API·started /);
    expect(within(live).getByTestId("run-duration")).toHaveTextContent(/^running \d/);
    unmount();
  });

  it("a finished run's menu: Copy run ID, Download report when there is one, then Delete apart, never Cancel run", async () => {
    const user = userEvent.setup();
    renderHeader(makeRun({ report: { summary: { delivered: 1 } } }));
    expect(await menuItems(user)).toEqual(["Copy run ID", "Download report", "Delete"]);
    const del = within(screen.getByRole("menu")).getByRole("menuitem", { name: "Delete" });
    expect(del).toHaveClass("text-danger");
    expect(del.previousElementSibling).toHaveAttribute("role", "separator");
  });

  it("an active run shows Cancel run as a button beside the menu, and the menu has no Delete; no report, no Download report", async () => {
    for (const status of ["running", "queued"] as const) {
      const user = userEvent.setup();
      const view = renderHeader(makeRun({ status, finished_at: null }));
      const row = screen.getByRole("heading", { level: 2 }).closest(`[class^="${RUN_HEAD_ROW}"]`) as HTMLElement;
      expect(within(row).getAllByRole("button").map((b) => b.getAttribute("aria-label") ?? text(b))).toEqual(["Cancel run", "Run actions"]);
      expect(await menuItems(user)).toEqual(["Copy run ID"]);
      view.unmount();
    }
  });

  it("never offers Re-run", async () => {
    const user = userEvent.setup();
    renderHeader(makeRun({ status: "cancelled" }));
    expect(screen.queryByRole("button", { name: /re-run/i })).toBeNull();
    expect(await menuItems(user)).toEqual(["Copy run ID", "Delete"]);
  });

  it("copies the full run id from the menu and from the Run ID cell", async () => {
    const user = userEvent.setup();
    const writeText = vi.fn(async () => undefined);
    Object.defineProperty(navigator, "clipboard", { configurable: true, value: { writeText } });
    render(
      <ToastProvider>
        <MemoryRouter>
          <RunView run={makeRun()} type={BACKFILL} />
        </MemoryRouter>
      </ToastProvider>,
    );
    await user.click(screen.getByRole("button", { name: "Run actions" }));
    await user.click(screen.getByRole("menuitem", { name: "Copy run ID" }));
    expect(writeText).toHaveBeenLastCalledWith("4f2a9c1e7b3d4e5f8a6b0c2d1e3f5a7b");
    expect(await screen.findByText("Run ID copied")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Copy run ID" }));
    expect(writeText).toHaveBeenCalledTimes(2);
    expect(writeText).toHaveBeenLastCalledWith("4f2a9c1e7b3d4e5f8a6b0c2d1e3f5a7b");
  });

  it("shows the error text when failed", () => {
    renderHeader(makeRun({ status: "failed", error: "1 of 2 channels failed" }));
    expect(screen.getByText("1 of 2 channels failed")).toBeInTheDocument();
    expect(screen.getByTestId("run-status")).toHaveTextContent("Failed");
  });

  it("cancels an active run after the confirm", async () => {
    const user = userEvent.setup();
    const fetchMock = vi.fn(async () => Response.json({ ok: true }));
    vi.stubGlobal("fetch", fetchMock);
    renderHeader(makeRun({ status: "running", finished_at: null }));
    await user.click(screen.getByRole("button", { name: "Cancel run" }));
    const dialog = await screen.findByRole("dialog");
    expect(within(dialog).getByText("Cancel Backfill channels #12?")).toBeInTheDocument();
    // The list's menu asks the same, word for word.
    expect(within(dialog).getByText(runConfirm("cancel", "x").body as string)).toBeInTheDocument();
    expect(within(dialog).getByRole("button", { name: "Keep running" })).toBeInTheDocument();
    await user.click(within(dialog).getByRole("button", { name: "Cancel run" }));
    await waitFor(() => expect(fetchMock).toHaveBeenCalled());
    const [url, init] = fetchMock.mock.calls[0] as unknown as [string, RequestInit];
    expect(url).toBe("/api/tasks/runs/4f2a9c1e7b3d4e5f8a6b0c2d1e3f5a7b/cancel");
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
    await user.click(screen.getByRole("button", { name: "Run actions" }));
    await user.click(screen.getByRole("menuitem", { name: "Delete" }));
    const dialog = await screen.findByRole("dialog");
    expect(within(dialog).getByText("Delete Backfill channels #12?")).toBeInTheDocument();
    expect(within(dialog).getByText(runConfirm("delete", "x").body as string)).toBeInTheDocument();
    expect(within(dialog).getByRole("button", { name: "Keep" })).toBeInTheDocument();
    await user.click(within(dialog).getByRole("button", { name: "Delete" }));
    await waitFor(() => expect(screen.getByTestId("where")).toHaveTextContent(/^\/tasks$/));
    const [url, init] = fetchMock.mock.calls[0] as unknown as [string, RequestInit];
    expect(url).toBe("/api/tasks/runs/4f2a9c1e7b3d4e5f8a6b0c2d1e3f5a7b");
    expect(init.method).toBe("DELETE");
    expect(await screen.findByText("Deleted Backfill channels #12")).toBeInTheDocument();
  });

  it("keeps Delete when the confirm is dismissed", async () => {
    const user = userEvent.setup();
    const fetchMock = vi.fn(async () => new Response(null, { status: 204 }));
    vi.stubGlobal("fetch", fetchMock);
    renderHeader(makeRun());
    await user.click(screen.getByRole("button", { name: "Run actions" }));
    await user.click(screen.getByRole("menuitem", { name: "Delete" }));
    await user.click(within(await screen.findByRole("dialog")).getByRole("button", { name: "Keep" }));
    expect(fetchMock).not.toHaveBeenCalled();
    expect(screen.getByTestId("where")).toHaveTextContent("/tasks/runs/4f2a9c1e7b3d4e5f8a6b0c2d1e3f5a7b");
  });
});

describe("Run page structure", () => {
  it("a two step run: header, then Summary, Steps and a collapsed Report in one column, no log", () => {
    const { container } = renderView(makeRun({ status: "running", finished_at: null }));
    expect(outline(container)).toEqual({
      header: {
        crumbs: ["All tasks", "Backfill channels"],
        title: "Backfill channels",
        number: "#12",
        status: "Running",
        meta: expect.stringMatching(/^Triggered manually by admin·started .+·running .+·mapping Main mapping·ID 4f2a9c1e$/),
        actions: ["Cancel run", "Run actions", "Copy run ID"],
      },
      cards: [
        {
          name: "Summary",
          hero: "1,234messages delivered",
          finished: "0/2 steps finished",
          bar: "0 of 2 steps finished",
          counts: [
            ["Steps done", "2"],
            ["Steps failed", "0"],
          ],
          inputs: [
            ["Mapping", "Main mapping"],
            ["Channels", "#general#news"],
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
            ["Running", "#general", "1,234 / 5,000", "bar", "Running"],
            ["Queued", "#news", "Queued #3", "", "Queued"],
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
    const header = container.querySelector("header")!;
    expect(fades(screen.getByTestId("run-status"))).toBe(true);
    expect(fades(header.querySelector("h2 + span"))).toBe(true);
    expect(fades(screen.getByTestId("run-started"))).toBe(true);
    expect(fades(screen.getByTestId("run-duration"))).toBe(true);
    expect(fades(screen.getByTestId("run-hero").firstElementChild)).toBe(true);
    expect(fades(screen.getByTestId("run-finished").firstElementChild)).toBe(true);
    expect(fades(screen.getByRole("progressbar", { name: "0 of 2 steps finished" }))).toBe(true);
    expect(fades(container.querySelector("dl[aria-label=Counts]"))).toBe(true);
    expect(fades(container.querySelector("dl[aria-label=Inputs]"))).toBe(true);
    expect(fades(within(screen.getByRole("region", { name: "Steps" })).getByRole("list"))).toBe(true);
    expect(fades(screen.getByTestId("steps-summary").firstElementChild)).toBe(true);
    // The static text the skeleton already showed does not blink, the type's title and the meta words included.
    expect(fades(screen.getByRole("link", { name: "All tasks" }))).toBe(false);
    expect(fades(screen.getByRole("link", { name: "Backfill channels" }))).toBe(false);
    expect(fades(header.querySelector(`[class="${RUN_TITLE_ROW}"]`))).toBe(false);
    expect(fades(screen.getByTestId("run-meta"))).toBe(false);
    expect(fades(screen.getByRole("heading", { level: 2 }))).toBe(false);
    expect(fades(screen.getByRole("heading", { name: "Steps" }))).toBe(false);
    const view = container.firstElementChild!;
    expect(view.className).not.toContain(BUSY);
    rerender(
      <MemoryRouter initialEntries={["/tasks/runs/4f2a9c1e7b3d4e5f8a6b0c2d1e3f5a7b"]}>
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
    // Each row ends on its state in words, coloured like its glyph.
    expect(rows.map((li) => [text(li.lastElementChild), li.lastElementChild!.className.split(" ").at(-1)])).toEqual([
      ["Running", "text-info"],
      ["Failed", "text-danger"],
      ["Done", "text-good"],
    ]);
    expect(screen.getAllByRole("progressbar").map((b) => b.getAttribute("aria-label"))).toEqual(["#general progress"]);
    expect(screen.getByRole("progressbar", { name: "#general progress" })).toHaveAttribute("aria-valuenow", "1234");
    // The row bar sits on the step's own line, not under it.
    expect(screen.getByRole("progressbar", { name: "#general progress" }).parentElement).toBe(rows[0]);
    expect(screen.getAllByRole("button").map((b) => b.textContent)).toEqual(["Status: All▾"]);
  });

  it("counts the steps by state in the head, with no percent bar of its own", () => {
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
    render(<StepList run={makeRun({ steps })} />);
    expect(screen.getByTestId("steps-summary")).toHaveTextContent("3 running, 2 queued, 94 done, 1 failed");
    expect(screen.getByTestId("steps-summary").parentElement!.querySelector("[role=progressbar]")).toBeNull();
  });

  it("leaves out a note that only repeats the state", () => {
    const steps: TaskStep[] = [
      { key: "a", title: "#a", state: "cancelled" },
      { key: "b", title: "#b", state: "skipped", note: "Already running in #12" },
    ];
    render(<StepList run={makeRun({ steps })} />);
    const [a, b] = screen.getAllByRole("listitem");
    expect(Array.from(a!.children).map(text)).toEqual(["", "#a", "", "Cancelled"]);
    expect(a!.lastElementChild).toHaveClass("text-custom-ink");
    expect(Array.from(b!.children).map(text)).toEqual(["", "#b", "Already running in #12", "Skipped"]);
    expect(b!.lastElementChild).toHaveClass("text-text-2");
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
    expect(screen.getAllByRole("listitem").map((li) => li.querySelector(".font-medium")?.textContent)).toEqual(["#e", "#g", "#c", "#d", "#b", "#a", "#f"]);
  });

  it("filters by state from the head, All by default, and says when a state has none", async () => {
    const user = userEvent.setup();
    const steps: TaskStep[] = [
      { key: "a", title: "#a", state: "success" },
      { key: "b", title: "#b", state: "failed", note: "delivery timed out" },
      { key: "c", title: "#c", state: "success" },
    ];
    render(<StepList run={makeRun({ steps })} />);
    const titles = () => screen.queryAllByRole("listitem").map((li) => li.querySelector(".font-medium")?.textContent);
    expect(titles()).toEqual(["#b", "#a", "#c"]);
    const pick = async (label: string) => {
      await user.click(screen.getByRole("button", { name: /^Status: / }));
      await user.click(screen.getByRole("option", { name: label }));
    };
    await pick("Done");
    expect(titles()).toEqual(["#a", "#c"]);
    await pick("Failed");
    expect(titles()).toEqual(["#b"]);
    await pick("Running");
    expect(titles()).toEqual([]);
    expect(screen.getByText("No running steps")).toBeInTheDocument();
    // The head still counts every step.
    expect(screen.getByTestId("steps-summary")).toHaveTextContent("2 done, 1 failed");
    await pick("All");
    expect(titles()).toEqual(["#b", "#a", "#c"]);
  });

  it("caps the list at twelve rows and scrolls past them", () => {
    const steps = Array.from({ length: 100 }, (_, i): TaskStep => ({ key: `s${i}`, title: `#s${i}`, state: "success" }));
    render(<StepList run={makeRun({ steps })} />);
    const list = within(screen.getByRole("region", { name: "Steps" })).getByRole("list");
    expect(list.className).toContain(STEP_LIST);
    expect(STEP_LIST).toContain("overflow-y-auto");
    // Rows are 36 px (h-9), so the cap is twelve of them.
    expect(STEP_ROW).toContain("h-9");
    expect(STEP_LIST).toContain(`max-h-[${STEP_ROWS_MAX * 36}px]`);
    expect(screen.getAllByRole("listitem")).toHaveLength(100);
  });

  it("words a finished backfill channel like its running label", () => {
    const note = (step: TaskStep) => stepLabel(step, backfillStepNote);
    expect(note({ key: "a", title: "#a", state: "running", done: 9, total: 36, label: "9 of 36 delivered, 36 pulled" })).toBe("9 of 36 delivered, 36 pulled");
    expect(note({ key: "a", title: "#a", state: "success", done: 1234, total: 5000, note: "1,234 messages" })).toBe("1,234 delivered");
    expect(note({ key: "a", title: "#a", state: "cancelled", done: 9, note: "Cancelled after 9 messages" })).toBe("9 delivered, then cancelled");
    expect(note({ key: "a", title: "#a", state: "cancelled", done: 0, note: "Cancelled" })).toBe("Cancelled");
    expect(note({ key: "a", title: "#a", state: "failed", done: 3, note: "delivery timed out" })).toBe("delivery timed out");
    expect(note({ key: "a", title: "#a", state: "skipped", note: "Already running in #12" })).toBe("Already running in #12");
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
    expect(summaryOf(makeRun({ summary: null, report: { summary: { delivered: 7, failed: 1, note: "x" } } })).counts).toEqual([["Steps failed", "1"]]);
    expect(screen.getByTestId("run-hero")).toHaveTextContent("7messages delivered");
  });

  it("makes messages delivered the hero, the step counts tiles with their unit, coloured by state and dimmed at zero", () => {
    const run = makeRun({
      summary: { success: 2, failed: 0, skipped: 0, cancelled: 4, delivered: 92 },
      steps: [
        ...Array.from({ length: 2 }, (_, i): TaskStep => ({ key: `d${i}`, title: `#d${i}`, state: "success" })),
        ...Array.from({ length: 4 }, (_, i): TaskStep => ({ key: `c${i}`, title: `#c${i}`, state: "cancelled" })),
      ],
    });
    const { container } = render(<RunSummary run={run} />);
    const overview = container.querySelector(`[class="${OVERVIEW}"]`)!;
    // The Summary card: head, the overview, then the tiles.
    expect(container.querySelector("section")!.children[1]).toBe(overview);
    const hero = screen.getByTestId("run-hero");
    expect(text(hero)).toBe("92messages delivered");
    expect(hero.firstElementChild).toHaveClass("text-accent", "text-[32px]");
    // A cancelled run is never a full bar: 6 of 6 finished, green for the 2 done, amber for the 4 cancelled.
    expect(screen.getByTestId("run-finished")).toHaveTextContent("6/6 steps finished");
    const bar = within(overview as HTMLElement).getByRole("progressbar", { name: "6 of 6 steps finished" });
    expect(Array.from(bar.children).map((s) => [s.getAttribute("data-segment"), (s as HTMLElement).style.width])).toEqual([
      ["success", `${(2 / 6) * 100}%`],
      ["cancelled", `${(4 / 6) * 100}%`],
    ]);
    const tiles = Array.from(container.querySelectorAll("dl[aria-label=Counts] dd"));
    expect(pairs(container.querySelector("dl[aria-label=Counts]"))).toEqual([
      ["Steps done", "2"],
      ["Steps failed", "0"],
      ["Steps skipped", "0"],
      ["Steps cancelled", "4"],
    ]);
    expect(tiles[0]).toHaveClass("text-good");
    expect(tiles[3]).toHaveClass("text-custom-ink");
    for (const zero of [tiles[1], tiles[2]]) expect(zero!.className).toContain(STAT_ZERO);
  });

  it("words the outcome the same for a list row: steps by outcome, then the messages delivered", () => {
    const counts = stepCounts({ summary: { success: 2, failed: 0, skipped: 0, cancelled: 4, delivered: 92 }, step_count: 6 });
    expect(counts).toEqual({ total: 6, success: 2, failed: 0, skipped: 0, cancelled: 4 });
    expect(outcomeText(counts, 92)).toBe("6 steps · 2 done · 4 cancelled · 92 delivered");
    expect(finishedText(counts!)).toBe("6/6 steps finished");
    // A running run: steps still to go leave the track bare.
    const live = stepCounts({ summary: { success: 1, failed: 0, skipped: 0, cancelled: 0, delivered: 9 }, step_count: 3 })!;
    expect(finishedText(live)).toBe("1/3 steps finished");
    expect(outcomeText(live, 9)).toBe("3 steps · 1 done · 9 delivered");
    // No steps and no counts: nothing to say.
    expect(stepCounts({ summary: null })).toBeNull();
    expect(outcomeText(null, null)).toBe("");
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

  it("shows the channels as chips by their step names, the first four and +N more, the count in the title", () => {
    const steps: TaskStep[] = Array.from({ length: 7 }, (_, i) => ({ key: `c${i}`, title: `#ch${i}`, state: "success" }));
    const run = makeRun({ steps, inputs: { channel_ids: steps.map((s) => s.key) } });
    render(<RunSummary run={run} />);
    const dd = screen.getByText("Channels").nextElementSibling!;
    const chips = dd.firstElementChild!;
    expect(chips).toHaveAttribute("title", "7 channels");
    expect(Array.from(chips.children).map(text)).toEqual(["#ch0", "#ch1", "#ch2", "#ch3", "+3 more"]);
    // A channel no step names shows its id; with no names at all the count stands alone.
    expect(channelNames(["c0", "x9"], steps)).toEqual(["#ch0", "x9"]);
    expect(channelNames(["x9"], steps)).toBeUndefined();
  });

  it("says when a run has no inputs", () => {
    render(<RunSummary run={makeRun({ inputs: {} })} />);
    expect(screen.getByText("No inputs")).toBeInTheDocument();
  });
});

describe("Run skeleton after a finished run", () => {
  it("prints the last page's tile and Inputs labels in the same boxes, bars only for the values", () => {
    localStorage.clear();
    const shape = (root: Element) => {
      const summary = root.querySelector("section[aria-label=Summary]")!;
      const tiles = summary.querySelector(`dl[class^="${STATS_LIST}"]`);
      const inputs = summary.querySelector(`dl[class^="${INPUTS_LIST}"]`);
      return {
        tiles: tiles ? Array.from(tiles.querySelectorAll("dt")).map((dt) => [dt.className, dt.textContent, dt.nextElementSibling!.className]) : [],
        inputs: inputs ? Array.from(inputs.querySelectorAll("dt")).map((dt) => [dt.className, dt.textContent]) : [],
      };
    };
    // A first visit: a backfill run's five tiles and six inputs, bars for every label.
    const cold = render(
      <MemoryRouter>
        <RunDetailSkeleton />
      </MemoryRouter>,
    );
    expect(shape(cold.container).tiles).toHaveLength(4);
    expect(shape(cold.container).inputs).toHaveLength(6);
    expect(cold.container.querySelectorAll("section[aria-label=Summary] dt [data-skeleton]")).toHaveLength(4 + 6);
    cold.unmount();

    const loaded = renderView(makeRun({ summary: { success: 1, failed: 0, skipped: 0, cancelled: 0, delivered: 36 } }));
    const real = shape(loaded.container);
    loaded.unmount();
    const skel = render(
      <MemoryRouter>
        <RunDetailSkeleton />
      </MemoryRouter>,
    );
    expect(real.tiles.map((t) => t[1])).toEqual(["Steps done", "Steps failed", "Steps skipped", "Steps cancelled"]);
    // The tiles' value class without the state colour: the skeleton bars them in the same box.
    const plain = (s: ReturnType<typeof shape>) => ({ ...s, tiles: s.tiles.map(([dt, label, dd]) => [dt, label, dd!.split(" ").filter((c) => c.startsWith("m") || c.startsWith("text-xl") || c.startsWith("leading") || c === "tabular-nums").join(" ")]) });
    expect(plain(shape(skel.container))).toEqual(plain(real));
    expect(skel.container.querySelectorAll("section[aria-label=Summary] dt [data-skeleton]")).toHaveLength(0);
    expect(skel.container.querySelectorAll("section[aria-label=Summary] dl dd [data-skeleton]")).toHaveLength(4 + 6);
    // The overview: bars for the hero count and its words, the finished line and the segmented bar.
    expect(skel.container.querySelectorAll(`section[aria-label=Summary] [class="${OVERVIEW}"] [data-skeleton]`)).toHaveLength(4);
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
    expect(screen.getByRole("link", { name: "Download" })).toHaveAttribute("href", "/api/tasks/runs/4f2a9c1e7b3d4e5f8a6b0c2d1e3f5a7b/report.json");
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
      <MemoryRouter initialEntries={["/tasks/runs/4f2a9c1e7b3d4e5f8a6b0c2d1e3f5a7b"]}>
        <RunDetail id="4f2a9c1e7b3d4e5f8a6b0c2d1e3f5a7b" typeOf={(k) => (k === BACKFILL.key ? BACKFILL : undefined)} />
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
    // The server's "36 messages" reads in the running label's words once the channel ends.
    expect(screen.getByText("36 delivered")).toBeInTheDocument();
    expect(screen.queryByText("36 messages")).not.toBeInTheDocument();
    expect(screen.queryByRole("progressbar", { name: "#general progress" })).not.toBeInTheDocument();
    expect(screen.getByRole("progressbar", { name: "2 of 2 steps finished" })).toHaveAttribute("aria-valuenow", "2");
    expect(screen.getByTestId("run-finished")).toHaveTextContent("2/2 steps finished");
    expect(screen.getByTestId("run-hero")).toHaveTextContent("36messages delivered");
    const counts = screen.getByRole("region", { name: "Summary" }).querySelector("dl[aria-label=Counts]");
    expect(pairs(counts)).toEqual([
      ["Steps done", "1"],
      ["Steps failed", "0"],
    ]);
    // The inputs are not in frames; the held ones stay.
    expect(screen.getByText("Last 500 messages")).toBeInTheDocument();
    expect(screen.queryByRole("status", { name: "Loading run" })).not.toBeInTheDocument();
  });
});
