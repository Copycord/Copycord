import { render, within } from "@testing-library/react";
import { MemoryRouter } from "react-router";
import { describe, expect, it } from "vitest";
import type { TaskRun } from "../../types/tasks";
import { RunHeader } from "./RunDetail";
import { RunRow } from "./RunList";
import { StatusIcon } from "./StatusIcon";
import { StepList } from "./StepList";
import { FADE_IN, ROW_BOX, RUN_HEAD_MAIN, RUN_META, RunDetailSkeleton, RunRowsLoading, STATUS_BOX, STEP_ROW } from "./TasksSkeleton";

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

describe("status glyph alignment", () => {
  it("centres itself on its row with the glyph centred in the box, and nothing else nudges it", () => {
    const { container } = render(<StatusIcon status="success" />);
    const glyph = container.querySelector("[data-status]")!;
    expect(classes(glyph)).toEqual(expect.arrayContaining(BOX));
    expect(BOX).toEqual(expect.arrayContaining(["self-center", "items-center"]));
    expect(classes(glyph).filter((c) => c !== "self-center" && NUDGE.test(c))).toEqual([]);
  });

  it("sits mid-row the same way in the run list, the step list and the run header", () => {
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
    // The run row's glyph, then the small glyph in its status pill. Running steps sort first in the step list.
    // The last is the small glyph inside the header's status pill.
    expect(glyphs.map((g) => g.getAttribute("data-status"))).toEqual(["success", "success", "running", "success", "success", "success"]);
    // Colours and the header glyph's fade in aside.
    const fade = FADE_IN.split(" ");
    const boxes = glyphs.map((g) => classes(g).filter((c) => !c.startsWith("text-") && !fade.includes(c)).sort().join(" "));
    expect(new Set(boxes)).toEqual(new Set([[...BOX].sort().join(" ")]));
    const [row, , , , header] = glyphs.map((g) => g.parentElement!);
    // The run row centres its columns; the glyph, a direct child, centres on the name and run ID lines.
    expect(row!.tagName).toBe("LI");
    expect(row!.className).toContain("items-center");
    expect(Array.from(row!.children[1]!.children).map((c) => c.textContent)).toEqual(["Backfill channels #1", "1"]);
    // The pill's glyph sits inside the row's status pill, not on the row.
    expect(glyphs[1]!.closest("li")).toBe(row);
    expect(glyphs[1]!.parentElement).not.toBe(row);
    // A step row is the glyph's parent.
    expect(glyphs[2]!.parentElement!.className).toBe(STEP_ROW);
    // The header centres the glyph beside its title line (title, number, status pill).
    expect(header!.className).toBe(RUN_HEAD_MAIN);
    expect(header!.children[1]!.querySelector("h2")).not.toBeNull();
    expect(within(header!.children[1] as HTMLElement).getByTestId("run-status")).toBeInTheDocument();
  });

  it("keeps the same box, in the same place, in the loading rows", () => {
    const { container } = render(
      <MemoryRouter>
        <RunRowsLoading rows={1} />
        <RunDetailSkeleton steps={2} />
      </MemoryRouter>,
    );
    const boxes = Array.from(container.querySelectorAll(`span[class="${STATUS_BOX}"]`));
    // The run row, the run header and the two step rows.
    expect(boxes).toHaveLength(4);
    for (const box of boxes) expect(box.querySelector("[data-skeleton]")).not.toBeNull();
    expect(boxes.map((b) => b.parentElement!.className)).toEqual([ROW_BOX, RUN_HEAD_MAIN, STEP_ROW, STEP_ROW]);
    // The header's glyph sits beside the title line there too; the meta chips row is under it, on the header's left edge.
    expect(boxes[1]!.nextElementSibling!.querySelector("h2")).not.toBeNull();
    expect(boxes[1]!.closest("header")!.querySelector(`:scope > [class="${RUN_META}"]`)).not.toBeNull();
  });
});
