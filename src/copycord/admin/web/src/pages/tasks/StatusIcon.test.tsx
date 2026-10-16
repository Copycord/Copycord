import { render, screen, within } from "@testing-library/react";
import { MemoryRouter } from "react-router";
import { describe, expect, it } from "vitest";
import type { RunStatus, StepState, TaskRun } from "../../types/tasks";
import { RunHeader } from "./RunDetail";
import { RunRow } from "./RunList";
import { COLOUR, RunStatusPill, StatusIcon } from "./StatusIcon";
import { StepList } from "./StepList";
import { RUN_TITLE_ROW, RunDetailSkeleton, RunRowsLoading, STATUS_BOX, STEP_ROW, STEP_STATE } from "./TasksSkeleton";

const RUN: TaskRun = {
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
  seq: 1,
  steps: [
    { key: "a", title: "#general", state: "success", note: "35 messages" },
    { key: "b", title: "#random", state: "running", done: 3, total: 10 },
  ],
};

const classes = (el: Element) => (el.getAttribute("class") ?? "").split(/\s+/).filter(Boolean);
const BOX = STATUS_BOX.split(" ");
// Nothing but self-center may move the glyph: no margins, offsets or fixed line box.
const NUDGE = /^-?(m[tby]?|self|align|translate-y|top|relative|h)(-|$)/;

describe("status colours", () => {
  it("gives each state one colour on every surface: blue active, green done, red failed, amber cancelled, neutral skipped", () => {
    expect(COLOUR).toEqual({
      queued: "text-info",
      running: "text-info",
      success: "text-good",
      failed: "text-danger",
      cancelled: "text-custom-ink",
      skipped: "text-text-2",
    });
    for (const status of Object.keys(COLOUR) as (RunStatus | StepState)[]) {
      const { container, unmount } = render(<StatusIcon status={status} />);
      const glyph = container.querySelector("[data-status]")!;
      expect(glyph).toHaveClass(COLOUR[status]);
      expect(glyph).toHaveAttribute("role", "img");
      expect(glyph.getAttribute("aria-label")).toBeTruthy();
      unmount();
    }
  });

  it("tints the status badge by state, always with its glyph and its word", () => {
    const tone: Record<RunStatus, string> = {
      queued: "text-info",
      running: "text-info",
      success: "text-good",
      failed: "text-danger",
      cancelled: "text-custom-ink",
    };
    for (const [status, text] of Object.entries(tone) as [RunStatus, string][]) {
      const { unmount } = render(<RunStatusPill status={status} testId="pill" />);
      const pill = screen.getByTestId("pill");
      const badge = pill.firstElementChild!;
      expect(badge).toHaveClass(text);
      expect(pill).toHaveAttribute("title", `Status: ${pill.textContent}`);
      expect(badge.querySelector("svg")).not.toBeNull();
      expect(badge.querySelector("svg")).toHaveAttribute("width", "12");
      // Queued is the blue outline alone.
      expect(badge.classList.contains("bg-transparent")).toBe(status === "queued");
      unmount();
    }
  });
});

describe("status glyph alignment", () => {
  it("centres itself on its row with the glyph centred in the box, and nothing else nudges it", () => {
    const { container } = render(<StatusIcon status="success" />);
    const glyph = container.querySelector("[data-status]")!;
    expect(classes(glyph)).toEqual(expect.arrayContaining(BOX));
    expect(BOX).toEqual(expect.arrayContaining(["self-center", "items-center"]));
    expect(classes(glyph).filter((c) => c !== "self-center" && NUDGE.test(c))).toEqual([]);
  });

  it("is the badge's glyph on a run row and the run header, a bare glyph only on a step row", () => {
    const { container } = render(
      <MemoryRouter>
        <ul>
          <RunRow run={RUN} typeTitle="Backfill channels" />
        </ul>
        <StepList run={RUN} />
        <RunHeader run={RUN} typeTitle="Backfill channels" />
      </MemoryRouter>,
    );
    const glyphs = Array.from(container.querySelectorAll("[data-status]"));
    // The row's badge, the two step rows (running sorts first), the header's badge.
    expect(glyphs.map((g) => g.getAttribute("data-status"))).toEqual(["success", "running", "success", "success"]);
    const boxes = glyphs.map((g) => classes(g).filter((c) => !c.startsWith("text-")).sort().join(" "));
    expect(new Set(boxes)).toEqual(new Set([[...BOX].sort().join(" ")]));
    // The row's only glyph is inside its status badge, next to the name.
    const row = container.querySelector("li")!;
    expect(row.querySelectorAll("[data-status]")).toHaveLength(1);
    expect(row.querySelector("[data-status]")!.closest("[title^='Status:']")).not.toBeNull();
    // A step row is the glyph's parent, and says its state in words in the same colour.
    expect(glyphs[1]!.parentElement!.className).toBe(STEP_ROW);
    const state = glyphs[1]!.parentElement!.lastElementChild!;
    expect(state.className).toBe(`${STEP_STATE} text-info`);
    expect(state).toHaveTextContent("Running");
    // The header has no glyph of its own: the badge beside the title carries it.
    const header = container.querySelector("header")!;
    expect(header.querySelectorAll("[data-status]")).toHaveLength(1);
    expect(within(header.querySelector(`[class="${RUN_TITLE_ROW}"]`) as HTMLElement).getByTestId("run-status")).toHaveTextContent("Success");
  });

  it("keeps the step glyph box, in the same place, in the loading rows", () => {
    const { container } = render(
      <MemoryRouter>
        <RunRowsLoading rows={1} />
        <RunDetailSkeleton steps={2} />
      </MemoryRouter>,
    );
    const boxes = Array.from(container.querySelectorAll(`span[class="${STATUS_BOX}"]`));
    // The two step rows; the run row and the header draw the badge's box instead.
    expect(boxes).toHaveLength(2);
    for (const box of boxes) expect(box.querySelector("[data-skeleton]")).not.toBeNull();
    expect(boxes.map((b) => b.parentElement!.className)).toEqual([STEP_ROW, STEP_ROW]);
    expect(container.querySelectorAll("[data-testid=run-status]")).toHaveLength(1);
  });
});
