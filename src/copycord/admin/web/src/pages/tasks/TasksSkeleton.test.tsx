import { render, screen, waitFor, within } from "@testing-library/react";
import { MemoryRouter } from "react-router";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { ToastProvider } from "../../components/Toast";
import { ConfirmProvider } from "../../components/ui/ConfirmDialog";
import type { TaskRun, TaskStep } from "../../types/tasks";
import { Tasks } from "../Tasks";
import { RunList, RunRow } from "./RunList";
import { REVEAL } from "../../components/ui/Skeleton";
import userEvent from "@testing-library/user-event";
import { countsText, RunView } from "./RunDetail";
import { StepList } from "./StepList";
import { rememberRunTypes, rememberTypes } from "./typeLabels";
import {
  ALL_TASKS_DESCRIPTION,
  CARD_HEAD,
  OVERVIEW,
  PAGE,
  STEP_EMPTY,
  STEP_ROW,
  STEP_ROW_PX,
  RunDetailSkeleton,
  STEPS_COUNT_KEY,
  STEPS_HEAD,
  STEP_LIST,
  BUSY,
  FADE_IN,
  LIST_FRAME,
  META_ITEM,
  FRAME_HEAD,
  FRAME_HEAD_FILTERS,
  FRAME_HEAD_LEFT,
  PER_PAGE,
  RAIL_COUNTS,
  RAIL_LIST,
  ROW_BOX,
  ROW_HEAD,
  ROW_LINK,
  ROW_META,
  ROW_TIME,
  ROW_MAIN,
  ROW_MENU,
  ROW_NAME,
  ROW_TIMING,
  STATUS_BOX,
  RUN_PAGES_KEY,
  RUN_TITLE_ROW,
  RUNS_COUNT_KEY,
  RunRowsLoading,
  StatusTabs,
  TasksHeader,
  TasksSkeleton,
} from "./TasksSkeleton";

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
    const run: TaskRun = { ...RUN, status: "cancelled", summary: { success: 2, failed: 0, skipped: 0, cancelled: 4, delivered: 92 }, step_count: 6 };
    const { container } = render(
      <MemoryRouter>
        <ul>
          <RunRow run={run} typeTitle="Backfill channels" actions={<div className={ROW_MENU}>menu</div>} />
        </ul>
        <RunRowsLoading rows={1} />
      </MemoryRouter>,
    );
    const [real, skeleton] = Array.from(container.querySelectorAll("li"));
    expect(skeleton!.className).toBe(ROW_BOX);
    expect(real!.className.startsWith(ROW_BOX)).toBe(true);
    const cols = (li: Element) => Array.from(li.children).map(layout);
    expect(cols(skeleton!)).toEqual(cols(real!));
    // The status glyph centred on the row, name and meta line, timing, menu.
    expect(Array.from(real!.children).map((c) => c.className)).toEqual([`${STATUS_BOX} text-custom-ink`, ROW_MAIN, ROW_TIMING, ROW_MENU]);
    expect(real!.children[0]).toHaveAttribute("data-status", "cancelled");
    // The glyph alone carries the status: no badge on the row, the word in the glyph's label and title.
    expect(real!.children[0]).toHaveAttribute("role", "img");
    expect(real!.children[0]).toHaveAttribute("aria-label", "Cancelled");
    expect(real!.children[0]).toHaveAttribute("title", "Cancelled");
    expect(real!.querySelector('[title^="Status:"]')).toBeNull();
    expect(Array.from(skeleton!.children).map((c) => c.className)).toEqual([STATUS_BOX, ROW_MAIN, ROW_TIMING, ROW_MENU]);
    expect(skeleton!.children[0]!.querySelector("[data-skeleton]")).not.toBeNull();
    // The name line (the name alone) over the meta line; the skeleton bars both.
    const lines = (li: Element) => Array.from(li.children[1]!.children).map((c) => c.className);
    expect(lines(real!)).toEqual([ROW_HEAD, ROW_META]);
    expect(lines(skeleton!)).toEqual(lines(real!));
    expect(real!.children[1]!.children[0]!.children).toHaveLength(1);
    expect(skeleton!.children[1]!.children[0]!.children).toHaveLength(1);
    expect(real!.querySelector(`[class="${ROW_LINK}"]`)).toHaveTextContent("Backfill channels #1");
    expect(skeleton!.querySelector(`[class="${ROW_NAME}"], [class="block ${ROW_NAME}"]`)).not.toBeNull();
    // The meta line: plain muted text naming who started the run, no glyphs, no run ID, no mapping.
    const meta = real!.children[1]!.children[1]!;
    expect(meta.textContent).toBe("Triggered by admin");
    expect(meta.children).toHaveLength(0);
    expect(ROW_META.split(" ")).toContain("truncate");
    // No step counts or outcome text on the row, and no bar; the skeleton holds no place for them.
    expect(real!.textContent).not.toMatch(/steps|delivered/);
    expect(within(real!).queryByTestId("run-outcome")).toBeNull();
    expect(real!.querySelector("[role=progressbar]")).toBeNull();
    expect(skeleton!.querySelector("[role=progressbar]")).toBeNull();
    expect(ROW_MAIN.split(" ")).toEqual(expect.arrayContaining(["flex-1", "lg:min-w-[220px]"]));
    // Started over duration, each a glyph and its value; the skeleton draws the glyphs and bars the values.
    const timing = (li: Element) => Array.from(li.children[2]!.children).map((c) => `${c.className} ${c.querySelectorAll("svg").length}`);
    expect(timing(real!)).toEqual([`${ROW_TIME} 1`, `${ROW_TIME} 1`]);
    expect(timing(skeleton!)).toEqual(timing(real!));
    expect(real!.children[2]!.children[1]!.textContent).toBe("48s");
    expect(skeleton!.children[2]!.querySelectorAll("[data-skeleton]")).toHaveLength(2);
  });

  it("makes the whole row one link with a focus ring and a hover tint, the menu above it", () => {
    expect(ROW_LINK).toMatch(/before:absolute before:inset-0/);
    expect(ROW_LINK).toMatch(/focus-visible:before:outline-2/);
    expect(ROW_BOX.split(" ")).toContain("relative");
    expect(ROW_MENU.split(" ")).toEqual(expect.arrayContaining(["relative", "z-10"]));
  });

  it("keeps the runs card unclipped with no empty minimum body under one row, the page at most 1280 px and centred", () => {
    expect(LIST_FRAME.split(" ")).not.toContain("overflow-hidden");
    expect(LIST_FRAME).not.toMatch(/min-h-/);
    expect(ROW_BOX).toMatch(/min-h-16/);
    expect(PAGE.split(" ")).toEqual(expect.arrayContaining(["mx-auto", "max-w-[1280px]"]));
  });

  it("draws as many rows as the list had last time, at most a page, one on a first visit, and the empty state with none", () => {
    const rows = () => {
      const { container, unmount } = render(<RunRowsLoading />);
      const out = { li: container.querySelectorAll("li").length, empty: container.textContent };
      unmount();
      return out;
    };
    expect(rows().li).toBe(1);
    localStorage.setItem(`skel:${RUNS_COUNT_KEY}`, "0");
    expect(rows()).toEqual({ li: 0, empty: "No task runs yetRuns show up here once a task starts" });
    localStorage.setItem(`skel:${RUNS_COUNT_KEY}`, "4");
    expect(rows().li).toBe(4);
    localStorage.setItem(`skel:${RUNS_COUNT_KEY}`, "90");
    expect(rows().li).toBe(PER_PAGE);
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
  // The loaded parts may carry the fade in (useReveal) after their own classes.
  const byClass = (cls: string) => root.querySelector(`[class="${cls}"], [class^="${cls} "]`) as HTMLElement;
  const head = byClass(FRAME_HEAD);
  // The chosen label, without the caret.
  const label = (b: Element) => b.firstElementChild?.textContent;
  return {
    title: root.querySelector("h2")?.textContent,
    subtitle: root.querySelector("h2 + p")?.textContent,
    search: within(root).queryAllByRole("searchbox", { hidden: true }).length,
    searchButton: within(root).queryAllByRole("button", { name: "Search", hidden: true }).length,
    railEntries: byClass(RAIL_LIST).children.length,
    // A count on All tasks and on each entry: a bar while loading, the number once loaded.
    railCounts: root.querySelectorAll(`[class="${RAIL_COUNTS}"]`).length,
    // The quick status tabs: their words as text in both, the counts after them.
    tabs: Array.from(root.querySelectorAll("[role=group][aria-label='Runs by status'] button")).map((b) => `${b.firstChild?.textContent} ${b.childNodes.length}`),
    frames: root.querySelectorAll(`[class="${LIST_FRAME}"]`).length,
    headGroups: Array.from(head.children).map((c) => c.className),
    headLeft: byClass(FRAME_HEAD_LEFT).children.length,
    dropdowns: Array.from(byClass(FRAME_HEAD_FILTERS).querySelectorAll("button[aria-haspopup=listbox]")).map(label),
    headLeftControls: byClass(FRAME_HEAD_LEFT).querySelectorAll("button").length,
    pager: Array.from(root.querySelectorAll("nav[aria-label=Pagination] button")).map((b) => `${b.textContent} ${(b as HTMLButtonElement).disabled}`),
  };
}

describe("page skeleton", () => {
  beforeEach(() => {
    vi.stubGlobal("WebSocket", FakeWs);
    localStorage.clear();
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

  it("has the loaded page's structure with zero runs: header text, search, one rail entry, the toolbar controls, the pager", async () => {
    // The list had no runs last time.
    localStorage.setItem(`skel:${RUNS_COUNT_KEY}`, "0");
    const skel = render(
      <MemoryRouter initialEntries={["/tasks"]}>
        <TasksSkeleton />
      </MemoryRouter>,
    );
    const skeleton = chrome(skel.container);
    // No runs last time: the loading list is the empty state's shape, no rows.
    const skelEmpty = within(skel.container).getByText("No task runs yet").closest(".flex-col")!.className;
    expect(within(skel.container).queryAllByRole("listitem", { hidden: true }).filter((li) => li.className === ROW_BOX)).toHaveLength(0);
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
    await waitFor(() => expect(screen.queryByRole("status", { name: "Loading task runs" })).toBeNull());
    await screen.findByRole("link", { name: "Backfill channels" });
    const loaded = chrome(page.container);
    expect(screen.getByText("No task runs yet").closest(".flex-col")!.className).toBe(skelEmpty);
    // The empty state sits inside the runs frame, not a second dashed card: icon, title and a line, no
    // button, in the skeleton's shape.
    expect(page.container.querySelector(".border-dashed")).toBeNull();
    expect(within(screen.getByText("No task runs yet").parentElement!).queryAllByRole("button")).toHaveLength(0);
    expect(screen.getByText("Runs show up here once a task starts")).toBeInTheDocument();

    expect(skeleton).toEqual(loaded);
    expect(loaded).toMatchObject({
      title: "All tasks",
      subtitle: "Showing runs from all task types",
      search: 1,
      searchButton: 1,
      railEntries: 1,
      railCounts: 2,
      tabs: ["All 2", "Running 2", "Done 2", "Failed 2", "Cancelled 2"],
      // The runs card does not clip, so an open filter list hangs past it.
      frames: 1,
      // The run count alone: no Auto-refresh control, the list is live.
      headLeft: 1,
      headLeftControls: 0,
      dropdowns: ["Task type: All", "Status: All", "Mapping: All", "Actor: All"],
      // One page: no pager in either.
      pager: [],
    });
  });

  it("draws the pager while loading only when the last list had more than one page", () => {
    const pager = () => {
      const { container, unmount } = render(
        <MemoryRouter initialEntries={["/tasks"]}>
          <TasksSkeleton />
        </MemoryRouter>,
      );
      const n = container.querySelectorAll("nav[aria-label=Pagination]").length;
      unmount();
      return n;
    };
    expect(pager()).toBe(0);
    localStorage.setItem(`skel:${RUN_PAGES_KEY}`, "1");
    expect(pager()).toBe(0);
    localStorage.setItem(`skel:${RUN_PAGES_KEY}`, "3");
    expect(pager()).toBe(1);
  });

  it("on a run url shows the run page's header and cards, not the list", () => {
    const { container } = render(
      <MemoryRouter initialEntries={["/tasks/runs/4f2a9c1e7b3d4e5f8a6b0c2d1e3f5a7b"]}>
        <TasksSkeleton />
      </MemoryRouter>,
    );
    const run = within(container).getByRole("status", { name: "Loading run", hidden: true });
    expect(within(run).getByRole("link", { name: "All tasks", hidden: true })).toHaveAttribute("href", "/tasks");
    expect(Array.from(run.querySelectorAll("section[aria-label]")).map((s) => s.getAttribute("aria-label"))).toEqual(["Summary", "Steps", "Report"]);
    expect(within(run).getByRole("heading", { name: "Steps", hidden: true })).toBeInTheDocument();
    // The Inputs and the Report stand as their closed disclosure rows.
    expect(Array.from(run.querySelectorAll("details")).map((d) => [d.open, d.querySelector("summary")?.textContent])).toEqual([
      [false, "›Inputs"],
      [false, "›Report (JSON)"],
    ]);
    // The controls render as themselves, inert: the Run actions menu button, the run ID chip's copy button
    // (both disabled until the run lands) and the Steps filter.
    const buttons = within(run).queryAllByRole("button", { hidden: true });
    expect(buttons.map((b) => b.getAttribute("aria-label") ?? "")).toEqual(["Run actions", "Copy run ID", expect.any(String)]);
    expect(buttons[0]).toBeDisabled();
    expect(buttons[0]).toHaveTextContent("⋯");
    expect(buttons[1]).toBeDisabled();
    expect(buttons[2]!.firstElementChild?.textContent).toBe("Status: All");
    expect(within(container).queryAllByRole("searchbox", { hidden: true })).toHaveLength(0);
  });

  it("draws the Steps card with the loaded card's head and rows, bars only for the rows", () => {
    localStorage.setItem(`skel:${STEPS_COUNT_KEY}`, "3");
    const steps: TaskStep[] = [
      { key: "a", title: "#a", state: "running", done: 1, total: 4 },
      { key: "b", title: "#b", state: "success", note: "3 messages" },
      { key: "c", title: "#c", state: "queued", queue_position: 1 },
    ];
    const { container } = render(
      <MemoryRouter>
        <RunDetailSkeleton />
        <StepList run={{ ...RUN, steps }} />
      </MemoryRouter>,
    );
    const [skel, real] = Array.from(container.querySelectorAll("section[aria-label=Steps]"));
    const head = (s: Element) => {
      const h = s.querySelector(`[class="${STEPS_HEAD}"]`)!;
      return {
        parts: Array.from(h.children).map((c) => `${c.tagName} ${c.getAttribute("class")}`),
        title: h.querySelector("h3")?.textContent,
        filter: h.querySelector("button[aria-haspopup=listbox]")?.firstElementChild?.textContent,
      };
    };
    const skelHead = head(skel!);
    const realHead = head(real!);
    // The title and the filter, in the same boxes; no counts (the Summary has them) and no percent bar.
    expect(skelHead.parts).toHaveLength(2);
    expect(skelHead.parts).toEqual(realHead.parts);
    expect(real!.querySelector(`[class="${STEPS_HEAD}"] [role=progressbar]`)).toBeNull();
    expect(skelHead).toMatchObject({ title: "Steps", filter: "Status: All" });
    expect(realHead).toMatchObject({ title: "Steps", filter: "Status: All" });
    expect(skel!.querySelector(`[class="${STEPS_HEAD}"] [data-skeleton]`)).toBeNull();
    // Each row as the loaded one: the glyph's box, the title and the note, no state word at the edge.
    const rows = (s: Element) => Array.from(s.querySelectorAll("li")).map((li) => [li.className, li.children.length]);
    expect(rows(skel!)).toEqual([
      [STEP_ROW, 3],
      [STEP_ROW, 3],
      [STEP_ROW, 3],
    ]);
    // The running row also holds its thin bar.
    expect(rows(real!)).toEqual([
      [STEP_ROW, 4],
      [STEP_ROW, 3],
      [STEP_ROW, 3],
    ]);
    expect(skel!.querySelector("ul")!.className).toBe(STEP_LIST);
    expect(real!.querySelector("ul")!.className.startsWith(STEP_LIST)).toBe(true);
  });

  it("leaves Task type out on a type page and names the type there, never a bar", () => {
    const at = (path: string) => {
      const { container, unmount } = render(
        <MemoryRouter initialEntries={[path]}>
          <TasksSkeleton />
        </MemoryRouter>,
      );
      const out = { ...chrome(container), bars: container.querySelectorAll("h2 [data-skeleton], h2 + p [data-skeleton]").length };
      unmount();
      return out;
    };
    // A first visit: the id in words, the description line held empty.
    const cold = at("/tasks?type=backfill_channels");
    expect(cold.dropdowns).toEqual(["Status: All", "Mapping: All", "Actor: All"]);
    expect(cold).toMatchObject({ title: "Backfill channels", subtitle: " ", bars: 0 });
    // Once the page has fetched the types, their own words.
    rememberTypes([{ key: "prune_rows", title: "Prune old rows", description: "Drop rows past the limit", inputs_schema: {}, active: 0, total: 0 }]);
    expect(at("/tasks?type=prune_rows")).toMatchObject({ title: "Prune old rows", subtitle: "Drop rows past the limit", bars: 0 });
  });

  it("names a run's type in the run skeleton's breadcrumb and title once it is known", () => {
    const header = () => {
      const { container, unmount } = render(
        <MemoryRouter initialEntries={["/tasks/runs/4f2a9c1e7b3d4e5f8a6b0c2d1e3f5a7b"]}>
          <TasksSkeleton />
        </MemoryRouter>,
      );
      const h = container.querySelector("header")!;
      const out = {
        crumbs: Array.from(h.querySelectorAll("nav a")).map((a) => [a.textContent, a.getAttribute("href")]),
        title: h.querySelector("h2")?.textContent?.trim(),
        titleBar: h.querySelector("h2 [data-skeleton]") !== null,
        crumbBar: h.querySelector("nav [data-skeleton]") !== null,
      };
      unmount();
      return out;
    };
    // Nothing known about run 12 or the types: bars, the one case left.
    expect(header()).toEqual({ crumbs: [["All tasks", "/tasks"]], title: "", titleBar: true, crumbBar: true });
    rememberTypes([
      { key: "backfill_channels", title: "Backfill channels", description: "", inputs_schema: {}, active: 0, total: 0 },
      { key: "prune_rows", title: "Prune old rows", description: "", inputs_schema: {}, active: 0, total: 0 },
    ]);
    rememberRunTypes([{ id: "4f2a9c1e7b3d4e5f8a6b0c2d1e3f5a7b", type: "prune_rows" }]);
    expect(header()).toEqual({
      crumbs: [
        ["All tasks", "/tasks"],
        ["Prune old rows", "/tasks?type=prune_rows"],
      ],
      title: "Prune old rows",
      titleBar: false,
      crumbBar: false,
    });
  });
});

describe("placeholder to content", () => {
  const list = (loading: boolean, refreshing = false) => (
    <MemoryRouter>
      <RunList runs={[RUN]} total={1} loading={loading} refreshing={refreshing} typeTitle={() => "Backfill channels"} />
    </MemoryRouter>
  );

  it("fades in over 150 to 200 ms, and not at all under reduced motion", () => {
    const ms = Number(/duration-\[(\d+)ms\]/.exec(FADE_IN)?.[1]);
    expect(ms).toBeGreaterThanOrEqual(150);
    expect(ms).toBeLessThanOrEqual(200);
    expect(FADE_IN.split(" ")).toEqual(expect.arrayContaining(["transition-opacity", "starting:opacity-0", "motion-reduce:transition-none"]));
  });

  it("fades the count and the rows in where the bars stood, and dims them only while a refetch is out", () => {
    const { container, rerender } = render(list(true));
    expect(container.querySelector(`[class~="starting:opacity-0"]`)).toBeNull();
    rerender(list(false));
    const rows = screen.getByRole("list", { name: "Task runs" });
    expect(screen.getByText("1 task run").className).toContain(FADE_IN);
    expect(rows.className).toContain(FADE_IN);
    expect(rows.className).not.toContain(BUSY);
    rerender(list(false, true));
    expect(screen.getByRole("list", { name: "Task runs" })).toBe(rows);
    expect(rows.className).toContain(BUSY);
  });

  it("shows a type's title as text from the first paint, and fades in only a description that arrives later", () => {
    const { unmount } = render(<TasksHeader title="Backfill channels" description="Clone message history" action={null} />);
    // Remembered from an earlier visit: nothing fades, nothing shimmers.
    expect(screen.getByRole("heading", { name: "Backfill channels" }).children).toHaveLength(0);
    expect(screen.getByText("Clone message history")).not.toHaveClass(REVEAL);
    unmount();
    // A first visit: the id in words over an empty line, then the fetched description fades in.
    const { container, rerender } = render(<TasksHeader title="Backfill channels" description="" action={null} />);
    expect(container.querySelector("[data-skeleton]")).toBeNull();
    expect(container.querySelector("h2 + p")?.textContent).toBe(" ");
    rerender(<TasksHeader title="Backfill channels" description="Clone message history" action={null} />);
    expect(screen.getByText("Clone message history")).toHaveClass(REVEAL);
    expect(screen.getByRole("heading", { name: "Backfill channels" }).children).toHaveLength(0);
    rerender(<TasksHeader title="All tasks" description={ALL_TASKS_DESCRIPTION} action={null} />);
    expect(screen.getByRole("heading", { name: "All tasks" }).querySelector("span")).toBeNull();
  });
});

describe("run page skeleton structure", () => {
  beforeEach(() => localStorage.clear());

  it("draws as many blocks as the loaded page in the header, the summary and the steps", () => {
    const run: TaskRun = {
      ...RUN,
      status: "failed",
      error: "1 of 6 channels failed",
      summary: { channels: 6, delivered: 224, failed: 1 },
      inputs: { since: "2026-10-01", limit: 100 },
      steps: [
        { key: "a", title: "#a", state: "success" },
        { key: "b", title: "#b", state: "success" },
        { key: "c", title: "#c", state: "success" },
        { key: "d", title: "#d", state: "success" },
        { key: "e", title: "#e", state: "failed", note: "Missing access" },
        { key: "f", title: "#f", state: "cancelled" },
      ],
    };
    const blocks = (root: Element) => ({
      // The meta line under the title: values between dots, no static words, the copy button its one glyph; the actions button.
      meta: Array.from(root.querySelectorAll(`header [data-testid=run-meta] > [class="${META_ITEM}"]`)).map((s) =>
        Array.from(s.childNodes)
          .filter((n) => n.nodeType === 3)
          .map((n) => n.textContent!.trim())
          .join(""),
      ),
      metaGlyphs: root.querySelectorAll("header [data-testid=run-meta] svg").length,
      copy: root.querySelectorAll("header button[aria-label='Copy run ID']").length,
      titleParts: root.querySelectorAll(`header [class="${RUN_TITLE_ROW}"] > *`).length,
      menu: root.querySelectorAll("header button[aria-label='Run actions']").length,
      headerLines: root.querySelectorAll("header p").length,
      cardHeads: root.querySelectorAll(`[class="${CARD_HEAD}"]`).length,
      overview: Array.from(root.querySelectorAll(`section[aria-label=Summary] > [class="${OVERVIEW}"] > *`)).map((c) => c.getAttribute("data-testid") ?? "bar"),
      details: Array.from(root.querySelectorAll<HTMLDetailsElement>("section details")).map((d) => [d.open, d.querySelector("summary")?.textContent]),
      steps: root.querySelectorAll("section[aria-label=Steps] li").length,
    });
    const loaded = render(
      <MemoryRouter>
        <RunView run={run} />
      </MemoryRouter>,
    );
    const real = blocks(loaded.container);
    loaded.unmount();
    const { container } = render(
      <MemoryRouter>
        <RunDetailSkeleton runType="backfill_channels" />
      </MemoryRouter>,
    );
    const skel = blocks(container);
    expect(real).toMatchObject({
      meta: ["", "", "", "", ""],
      metaGlyphs: 1,
      copy: 1,
      menu: 1,
      headerLines: 1,
      // The Steps card's head; the Summary card opens on its hero.
      cardHeads: 1,
      // The hero, the segmented bar, the counts line.
      overview: ["run-hero", "bar", "run-counts"],
      details: [
        [false, "›Inputs"],
        [false, "›Report (JSON)"],
      ],
      steps: 6,
    });
    // The glyph, title, number and the failed run's error on the title line; the skeleton has no error.
    expect(real.titleParts).toBe(4);
    expect(skel.titleParts).toBe(3);
    // The error sits on the title line, so the header has no line the skeleton lacks.
    expect({ ...skel, titleParts: 0 }).toEqual({ ...real, titleParts: 0 });
    // The counts line: the steps by state, then the summary's other counts but the hero.
    expect(countsText(run, "delivered")).toBe("4 done · 1 failed · 1 cancelled · 6 channels");
  });

  it("keeps the Steps list's height when the filter matches nothing", async () => {
    const user = userEvent.setup();
    const steps: TaskStep[] = Array.from({ length: 6 }, (_, i) => ({ key: `s${i}`, title: `#s${i}`, state: i === 4 ? "failed" : "success" }));
    render(<StepList run={{ ...RUN, steps }} />);
    await user.click(screen.getByRole("button", { name: /^Status: / }));
    expect(screen.getAllByRole("option").map((o) => o.textContent)).toEqual(["All", "Running", "Queued", "Failed", "Done", "Cancelled", "Skipped"]);
    await user.click(screen.getByRole("option", { name: "Queued" }));
    expect(screen.getByRole("button", { name: /^Status: / })).toHaveTextContent("Status: Queued");
    const empty = screen.getByText("No queued steps");
    expect(empty.className).toBe(STEP_EMPTY);
    expect(empty.style.minHeight).toBe(`${6 * STEP_ROW_PX}px`);
  });
});

describe("status tabs", () => {
  it("labels each tab as text with its count, presses the current status, and selects a status on click", async () => {
    const onSelect = vi.fn();
    const { rerender } = render(
      <StatusTabs counts={{ all: 7, queued: 1, running: 2, success: 3, failed: 1, cancelled: 0 }} value="running" onSelect={onSelect} />,
    );
    const tabs = screen.getAllByRole("button");
    expect(tabs.map((t) => t.textContent)).toEqual(["All7", "Running2", "Done3", "Failed1", "Cancelled0"]);
    expect(tabs.map((t) => t.getAttribute("aria-pressed"))).toEqual(["false", "true", "false", "false", "false"]);
    await userEvent.setup().click(screen.getByRole("button", { name: /^Failed/ }));
    expect(onSelect).toHaveBeenCalledWith("failed");
    // While the first page loads the words stay, only the counts shimmer.
    rerender(<StatusTabs counts={null} value="" />);
    for (const tab of screen.getAllByRole("button")) expect(tab.querySelector("[data-skeleton]")).not.toBeNull();
    expect(screen.getAllByRole("button").map((t) => t.firstChild?.textContent)).toEqual(["All", "Running", "Done", "Failed", "Cancelled"]);
  });
});
