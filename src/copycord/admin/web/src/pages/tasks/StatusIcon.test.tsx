import { render, screen, within } from "@testing-library/react";
import { MemoryRouter } from "react-router";
import { describe, expect, it } from "vitest";
import type { RunStatus, StepState, TaskRun } from "../../types/tasks";
import { RunHeader } from "./RunDetail";
import { RunRow } from "./RunList";
import { COLOUR, StatusIcon } from "./StatusIcon";
import { StepList } from "./StepList";
import { FADE_IN, ROW_BOX, RUN_TITLE_ROW, RunDetailSkeleton, RunRowsLoading, STATUS_BOX, STEP_ROW } from "./TasksSkeleton";

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

  it("says the status in its title and label, the word the filters use", () => {
    render(<StatusIcon status="cancelled" testId="glyph" />);
    expect(screen.getByTestId("glyph")).toHaveAttribute("title", "Cancelled");
    expect(screen.getByRole("img", { name: "Cancelled" })).toBe(screen.getByTestId("glyph"));
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

  it("leads a run row, the run title and a step row, alone carrying the status on each", () => {
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
    // The row's glyph, the two step rows (running sorts first), the header's glyph.
    expect(glyphs.map((g) => g.getAttribute("data-status"))).toEqual(["success", "running", "success", "success"]);
    // The header's glyph fades in with the run's other parts; the box itself is the same everywhere.
    const boxes = glyphs.map((g) => classes(g).filter((c) => !c.startsWith("text-") && !FADE_IN.split(" ").includes(c)).sort().join(" "));
    expect(new Set(boxes)).toEqual(new Set([[...BOX].sort().join(" ")]));
    // The row's only glyph is its first child, centred on the whole row; it alone carries the status, no badge.
    const row = container.querySelector("li")!;
    expect(row.querySelectorAll("[data-status]")).toHaveLength(1);
    expect(row.firstElementChild).toBe(row.querySelector("[data-status]"));
    expect(row.querySelector("[title^='Status:']")).toBeNull();
    // A step row is the glyph's parent and has no state word: the glyph's label says it.
    const step = glyphs[1]!.parentElement!;
    expect(step.className).toBe(STEP_ROW);
    expect(glyphs[1]).toHaveAccessibleName("Running");
    expect(within(step as HTMLElement).queryByText("Running")).toBeNull();
    // The header's one glyph stands right before the title, and it is the run's status; no badge.
    const header = container.querySelector("header")!;
    expect(header.querySelectorAll("[data-status]")).toHaveLength(1);
    expect(header.querySelector("[data-status]")!.nextElementSibling!.tagName).toBe("H2");
    const status = within(header.querySelector(`[class="${RUN_TITLE_ROW}"]`) as HTMLElement).getByTestId("run-status");
    expect(status).toBe(header.querySelector("[data-status]"));
    expect(status).toHaveAttribute("title", "Success");
    expect(header.querySelector("[title^='Status:']")).toBeNull();
  });

  it("keeps every glyph box, in the same place, in the loading rows and header", () => {
    const { container } = render(
      <MemoryRouter>
        <RunRowsLoading rows={1} />
        <RunDetailSkeleton steps={2} />
      </MemoryRouter>,
    );
    const boxes = Array.from(container.querySelectorAll(`span[class="${STATUS_BOX}"]`));
    // The run row, the header before the title, the two step rows.
    expect(boxes).toHaveLength(4);
    for (const box of boxes) expect(box.querySelector("[data-skeleton]")).not.toBeNull();
    expect(boxes.map((b) => b.parentElement!.className)).toEqual([ROW_BOX, RUN_TITLE_ROW, STEP_ROW, STEP_ROW]);
    expect(boxes[1]!.nextElementSibling!.tagName).toBe("H2");
    expect(container.querySelectorAll("[data-testid=run-status]")).toHaveLength(0);
  });
});
