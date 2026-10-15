import { render, screen, within } from "@testing-library/react";
import { MemoryRouter } from "react-router";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { ToastProvider } from "../../components/Toast";
import { ConfirmProvider } from "../../components/ui/ConfirmDialog";
import type { TaskRun } from "../../types/tasks";
import { Tasks } from "../Tasks";
import { RunRow } from "./RunList";
import { FRAME, FRAME_HEAD, FRAME_HEAD_FILTERS, FRAME_HEAD_LEFT, RAIL_LIST, ROW_BOX, RunRowsLoading, TasksSkeleton } from "./TasksSkeleton";

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

class FakeWs {
  readyState = 0;
  onopen: (() => void) | null = null;
  onmessage: (() => void) | null = null;
  onclose: (() => void) | null = null;
  close() {}
  send() {}
}

const TYPES = {
  types: [{ key: "backfill_channels", title: "Backfill channels", description: "Clone message history", inputs_schema: {}, active: 0, total: 0 }],
};

/** The page chrome a skeleton has to match: title, subtitle, search, rail entries and the frame head controls. */
function chrome(root: HTMLElement) {
  const byClass = (cls: string) => root.querySelector(`[class="${cls}"]`) as HTMLElement;
  const head = byClass(FRAME_HEAD);
  // The chosen label, without the caret.
  const label = (b: Element) => b.firstElementChild?.textContent;
  return {
    title: root.querySelector("h2")?.textContent,
    subtitle: root.querySelector("h2 + p")?.textContent,
    search: within(root).queryAllByRole("searchbox", { hidden: true }).length,
    searchButton: within(root).queryAllByRole("button", { name: "Search", hidden: true }).length,
    railEntries: byClass(RAIL_LIST).children.length,
    frames: root.querySelectorAll(`[class="${FRAME}"]`).length,
    headGroups: Array.from(head.children).map((c) => c.className),
    headLeft: byClass(FRAME_HEAD_LEFT).children.length,
    dropdowns: Array.from(byClass(FRAME_HEAD_FILTERS).querySelectorAll("button[aria-haspopup=listbox]")).map(label),
    autoRefresh: label(byClass(FRAME_HEAD_LEFT).querySelector("button[aria-haspopup=listbox]")!),
  };
}

describe("page skeleton", () => {
  beforeEach(() => {
    vi.stubGlobal("WebSocket", FakeWs);
    localStorage.clear();
    localStorage.setItem("tasks:autorefresh", "0");
    vi.stubGlobal(
      "fetch",
      vi.fn(async (url: string) =>
        url === "/api/tasks/types"
          ? Response.json(TYPES)
          : Response.json({ runs: [], total: 0, page: 1, actors: [], contexts: [] }),
      ),
    );
  });
  afterEach(() => vi.unstubAllGlobals());

  it("has the loaded page's structure with zero runs: header text, search, one rail entry, the toolbar controls", async () => {
    const skel = render(
      <MemoryRouter initialEntries={["/tasks"]}>
        <TasksSkeleton />
      </MemoryRouter>,
    );
    const skeleton = chrome(skel.container);
    expect(within(skel.container).getAllByRole("listitem", { hidden: true }).filter((li) => li.className === ROW_BOX)).toHaveLength(3);
    skel.unmount();

    const page = render(
      <ToastProvider>
        <ConfirmProvider>
          <MemoryRouter initialEntries={["/tasks"]}>
            <Tasks />
          </MemoryRouter>
        </ConfirmProvider>
      </ToastProvider>,
    );
    await screen.findByText("No task runs yet");
    await screen.findByRole("link", { name: "Backfill channels" });
    const loaded = chrome(page.container);

    expect(skeleton).toEqual(loaded);
    expect(loaded).toMatchObject({
      title: "All tasks",
      subtitle: "Showing runs from all task types",
      search: 1,
      searchButton: 1,
      railEntries: 1,
      headLeft: 2,
      autoRefresh: "Auto-refresh: Off",
      dropdowns: ["Task type: All", "Status: All", "Mapping: All", "Actor: All"],
    });
  });

  it("on a run url shows the run page's header and cards, not the list", () => {
    const { container } = render(
      <MemoryRouter initialEntries={["/tasks/runs/12"]}>
        <TasksSkeleton />
      </MemoryRouter>,
    );
    const run = within(container).getByRole("status", { name: "Loading run", hidden: true });
    expect(within(run).getByRole("link", { name: "All tasks", hidden: true })).toHaveAttribute("href", "/tasks");
    expect(Array.from(run.querySelectorAll("section[aria-label]")).map((s) => s.getAttribute("aria-label"))).toEqual(["Summary", "Steps", "Report"]);
    expect(within(run).getByRole("heading", { name: "Inputs", hidden: true })).toBeInTheDocument();
    expect(within(run).getByRole("heading", { name: "Steps", hidden: true })).toBeInTheDocument();
    expect(run.querySelector("summary")).toHaveTextContent("Report (JSON)");
    // No buttons: the actions depend on the run's status.
    expect(within(run).queryAllByRole("button", { hidden: true })).toHaveLength(0);
    expect(within(container).queryAllByRole("searchbox", { hidden: true })).toHaveLength(0);
  });

  it("leaves Task type out and bars the header on a type page, as the page does", () => {
    const { container } = render(
      <MemoryRouter initialEntries={["/tasks?type=backfill_channels"]}>
        <TasksSkeleton />
      </MemoryRouter>,
    );
    const c = chrome(container);
    expect(c.dropdowns).toEqual(["Status: All", "Mapping: All", "Actor: All"]);
    expect(c.title).toBe("");
  });
});
