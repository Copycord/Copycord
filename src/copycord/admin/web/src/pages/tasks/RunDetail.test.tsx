import { fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { useState } from "react";
import { MemoryRouter, Route, Routes, useLocation } from "react-router";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ToastProvider } from "../../components/Toast";
import type { TaskRun } from "../../types/tasks";
import { LogView } from "./LogView";
import { ReportPanel } from "./ReportPanel";
import { RunHeader, inputRows, rangeWords } from "./RunDetail";
import { StepList, stepLabel } from "./StepList";

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
    log_lines: [
      { seq: 1, ts: "2026-10-14T18:00:01Z", level: "info", step: null, msg: "Starting" },
      { seq: 2, ts: "2026-10-14T18:00:02Z", level: "warn", step: "c1", msg: "General slow" },
      { seq: 3, ts: "2026-10-14T18:00:03Z", level: "error", step: "c2", msg: "News failed" },
    ],
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

afterEach(() => {
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

describe("RunHeader", () => {
  it("shows the breadcrumb, title, number and meta, with Re-run and Delete when finished", () => {
    renderHeader(makeRun());
    expect(screen.getByRole("link", { name: "All tasks" })).toHaveAttribute("href", "/tasks");
    expect(screen.getByRole("link", { name: "Backfill channels" })).toHaveAttribute("href", "/tasks?type=backfill_channels");
    expect(screen.getByRole("heading", { level: 2, name: "Backfill #general" })).toBeInTheDocument();
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

describe("StepList and LogView", () => {
  function Detail({ run }: { run: TaskRun }) {
    const [step, setStep] = useState<string | null>(null);
    return (
      <>
        <StepList run={run} selected={step} onSelect={setStep} />
        <LogView run={run} step={step} />
      </>
    );
  }

  it("labels steps and draws a bar when done and total are known", () => {
    render(<Detail run={makeRun()} />);
    expect(screen.getByText("Cloning 1,234 / 5,000")).toBeInTheDocument();
    expect(screen.getByText("Queued #3")).toBeInTheDocument();
    expect(screen.getByRole("progressbar", { name: "#general progress" })).toHaveAttribute("aria-valuenow", "1234");
    expect(screen.getAllByRole("progressbar")).toHaveLength(1);
    const summary = screen.getByRole("region", { name: "Summary" });
    expect(within(summary).getByText("Delivered")).toBeInTheDocument();
    expect(within(summary).getByText("1,234")).toBeInTheDocument();
  });

  it("prefers the server label and the note", () => {
    expect(stepLabel({ key: "a", title: "#a", state: "running", label: "Cleaning up", done: 1, total: 2 })).toBe("Cleaning up");
    expect(stepLabel({ key: "a", title: "#a", state: "skipped", note: "Already running in #12" })).toBe("Already running in #12");
  });

  it("a step click narrows the log and All steps resets it", async () => {
    const user = userEvent.setup();
    render(<Detail run={makeRun()} />);
    const log = screen.getByRole("log", { name: "Run log" });
    expect(within(log).getByText("Starting")).toBeInTheDocument();
    expect(within(log).getByText("General slow").closest("[data-level]")).toHaveAttribute("data-level", "warn");
    await user.click(screen.getByRole("button", { name: /#general/ }));
    expect(within(log).queryByText("Starting")).not.toBeInTheDocument();
    expect(within(log).queryByText("News failed")).not.toBeInTheDocument();
    expect(within(log).getByText("General slow")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "All steps" }));
    expect(within(log).getByText("News failed")).toBeInTheDocument();
  });

  it("shows Jump to latest when scrolled up and follows again on click", async () => {
    const user = userEvent.setup();
    render(<LogView run={makeRun()} step={null} />);
    const log = screen.getByRole("log", { name: "Run log" });
    Object.defineProperty(log, "scrollHeight", { configurable: true, value: 1000 });
    Object.defineProperty(log, "clientHeight", { configurable: true, value: 200 });
    expect(screen.queryByRole("button", { name: "Jump to latest" })).not.toBeInTheDocument();
    log.scrollTop = 100;
    fireEvent.scroll(log);
    await user.click(await screen.findByRole("button", { name: "Jump to latest" }));
    expect(log.scrollTop).toBe(1000);
    expect(screen.queryByRole("button", { name: "Jump to latest" })).not.toBeInTheDocument();
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
    const text = JSON.stringify(report, null, 2);
    expect(screen.getByLabelText("Report JSON").textContent).toBe(text);
    expect(screen.getByRole("link", { name: "Download" })).toHaveAttribute("href", "/api/tasks/runs/12/report.json");
    expect(screen.getByRole("link", { name: "Download" })).toHaveAttribute("download");
    await user.click(screen.getByRole("button", { name: "Copy" }));
    expect(writeText).toHaveBeenCalledWith(text);
    expect(await screen.findByText("Report copied")).toBeInTheDocument();
  });

  it("says the report comes at the end while the run is active", () => {
    render(<ReportPanel run={makeRun({ status: "running", report: null })} />);
    expect(screen.getByText("The report appears when the run finishes")).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Copy" })).not.toBeInTheDocument();
  });
});

describe("Inputs", () => {
  it("formats known keys in words and falls back to the raw key and value", () => {
    const rows = inputRows({
      context: { label: "Main mapping" },
      inputs: {
        mapping_id: "m1",
        channel_ids: ["a", "b"],
        category_ids: ["k"],
        range: { mode: "last_n", last_n: 500 },
        ignore_cloned: true,
        mode: "resume",
        extra: { a: 1 },
      },
    });
    expect(rows).toEqual([
      ["Mapping", "Main mapping"],
      ["Channels", "2 channels"],
      ["Categories", "1 category"],
      ["Range", "Last 500 messages"],
      ["Ignore cloned", "Yes"],
      ["Mode", "Resume previous"],
      ["extra", '{"a":1}'],
    ]);
    expect(rangeWords({ mode: "all" })).toBe("All history");
  });
});
