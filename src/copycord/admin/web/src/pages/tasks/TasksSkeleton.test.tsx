import { render } from "@testing-library/react";
import { MemoryRouter } from "react-router";
import { describe, expect, it } from "vitest";
import type { TaskRun } from "../../types/tasks";
import { RunRow } from "./RunList";
import { ROW_BOX, RunRowsLoading } from "./TasksSkeleton";

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
};

/** The layout classes of each column, without colours, hover and margins on the glyph. */
const layout = (el: Element) =>
  (el.getAttribute("class") ?? "")
    .split(/\s+/)
    .filter((c) => /^(w-|shrink-|flex-1$|min-w-0$|self-|max-md:)/.test(c))
    .sort()
    .join(" ");

describe("run row skeleton", () => {
  it("has the box and the columns of a real run row", () => {
    const { container } = render(
      <MemoryRouter>
        <ul>
          <RunRow run={RUN} typeTitle="Backfill channels" actions={<div className="shrink-0 self-start">menu</div>} />
        </ul>
        <RunRowsLoading rows={1} />
      </MemoryRouter>,
    );
    const [real, skeleton] = Array.from(container.querySelectorAll("li"));
    expect(skeleton!.className).toBe(ROW_BOX);
    expect(real!.className.startsWith(ROW_BOX)).toBe(true);
    const cols = (li: Element) => Array.from(li.children).map(layout);
    expect(cols(skeleton!)).toEqual(cols(real!));
    // Title and type lines: a title line and a text-xs line with the same gap.
    expect(skeleton!.children[1]!.children).toHaveLength(2);
    expect(skeleton!.children[1]!.children[1]!.className).toContain("mt-0.5 text-xs");
  });
});
