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
import { RunView } from "./RunDetail";
import { StepList } from "./StepList";
import { rememberRunTypes, rememberTypes } from "./typeLabels";
import {
  ALL_TASKS_DESCRIPTION,
  CARD_HEAD,
  STAT_TILE,
  STEP_EMPTY,
  STEP_ROW_PX,
  RunDetailSkeleton,
  STEPS_BAR,
  STEPS_COUNT_KEY,
  STEPS_HEAD,
  STEPS_PCT,
  STEPS_PROGRESS,
  STEP_LIST,
  BUSY,
  FADE_IN,
  LIST_FRAME,
  FRAME_HEAD,
  FRAME_HEAD_FILTERS,
  FRAME_HEAD_LEFT,
  PER_PAGE,
  RAIL_LIST,
  ROW_BOX,
  ROW_ID,
  ROW_META,
  ROW_TIME,
  RUN_FACTS,
  ROW_MAIN,
  ROW_MENU,
  ROW_NAME,
  ROW_TIMING,
  RUNS_COUNT_KEY,
  RunRowsLoading,
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
    const { container } = render(
      <MemoryRouter>
        <ul>
          <RunRow run={RUN} typeTitle="Backfill channels" actions={<div className={ROW_MENU}>menu</div>} />
        </ul>
        <RunRowsLoading rows={1} />
      </MemoryRouter>,
    );
    const [real, skeleton] = Array.from(container.querySelectorAll("li"));
    expect(skeleton!.className).toBe(ROW_BOX);
    expect(real!.className.startsWith(ROW_BOX)).toBe(true);
    const cols = (li: Element) => Array.from(li.children).map(layout);
    expect(cols(skeleton!)).toEqual(cols(real!));
    // Glyph, name and meta line, timing, menu; no status pill or chips in the row.
    expect(real!.children).toHaveLength(4);
    expect(Array.from(real!.children).slice(1).map((c) => c.className)).toEqual([ROW_MAIN, ROW_TIMING, ROW_MENU]);
    expect(Array.from(skeleton!.children).slice(1).map((c) => c.className)).toEqual([ROW_MAIN, ROW_TIMING, ROW_MENU]);
    expect(real!.querySelector("[class*=rounded-full]")).toBeNull();
    // The name line over the plain meta line, in the same type and gap; the skeleton has bars in both.
    const lines = (li: Element) => Array.from(li.children[1]!.children).map((c) => c.className.replace(/^block /, ""));
    expect(lines(real!)).toEqual([ROW_NAME, ROW_META]);
    expect(lines(skeleton!)).toEqual(lines(real!));
    expect(skeleton!.children[1]!.querySelectorAll("[data-skeleton]")).toHaveLength(2);
    expect(real!.children[1]!.children[1]!.textContent).toBe("1 · Manual run by admin · Main mapping");
    expect(real!.querySelector(`[class="${ROW_ID}"]`)).toHaveAttribute("title", "Run ID 1");
    // Started over duration, each a glyph and its value; the skeleton draws the glyphs and bars the values.
    const timing = (li: Element) => Array.from(li.children[2]!.children).map((c) => `${c.className} ${c.querySelectorAll("svg").length}`);
    expect(timing(real!)).toEqual([`${ROW_TIME} 1`, `${ROW_TIME} 1`]);
    expect(timing(skeleton!)).toEqual(timing(real!));
    expect(real!.children[2]!.children[1]!.textContent).toBe("48s");
    expect(skeleton!.children[2]!.querySelectorAll("[data-skeleton]")).toHaveLength(2);
  });

  it("keeps the runs card unclipped with no empty minimum body under one row", () => {
    expect(LIST_FRAME.split(" ")).not.toContain("overflow-hidden");
    expect(LIST_FRAME).not.toMatch(/min-h-/);
    expect(ROW_BOX).toMatch(/min-h-14/);
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
    expect(rows()).toEqual({ li: 0, empty: "No task runs yetPick a task type on the left to run one" });
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
    frames: root.querySelectorAll(`[class="${LIST_FRAME}"]`).length,
    headGroups: Array.from(head.children).map((c) => c.className),
    headLeft: byClass(FRAME_HEAD_LEFT).children.length,
    dropdowns: Array.from(byClass(FRAME_HEAD_FILTERS).querySelectorAll("button[aria-haspopup=listbox]")).map(label),
    autoRefresh: label(byClass(FRAME_HEAD_LEFT).querySelector("button[aria-haspopup=listbox]")!),
    pager: Array.from(root.querySelectorAll("nav[aria-label=Pagination] button")).map((b) => `${b.textContent} ${(b as HTMLButtonElement).disabled}`),
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
    // The empty state is plain text inside the runs frame, not a second dashed card, and has no Run task.
    expect(page.container.querySelector(".border-dashed")).toBeNull();
    expect(screen.queryByRole("button", { name: "Run task" })).toBeNull();

    expect(skeleton).toEqual(loaded);
    expect(loaded).toMatchObject({
      title: "All tasks",
      subtitle: "Showing runs from all task types",
      search: 1,
      searchButton: 1,
      railEntries: 1,
      // The runs card does not clip, so an open filter list hangs past it.
      frames: 1,
      headLeft: 2,
      autoRefresh: "Auto-refresh: Off",
      dropdowns: ["Task type: All", "Status: All", "Mapping: All", "Actor: All"],
      pager: ["Previous true", "Next true"],
    });
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
    expect(within(run).getByRole("heading", { name: "Inputs", hidden: true })).toBeInTheDocument();
    expect(within(run).getByRole("heading", { name: "Steps", hidden: true })).toBeInTheDocument();
    expect(run.querySelector("summary")).toHaveTextContent("Report (JSON)");
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

  it("draws the Steps card with the loaded card's head and rows, bars only for the counts, the bar and the rows", () => {
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
    // Title, counts line, the overall bar with its percent and the filter, in the same boxes.
    expect(skelHead.parts).toHaveLength(4);
    expect(skelHead.parts).toEqual(realHead.parts);
    const progress = (s: Element) => s.querySelector(`[class="${STEPS_HEAD}"]`)!.children[2] as HTMLElement;
    expect(progress(skel!).className).toBe(STEPS_PROGRESS);
    expect(progress(skel!).firstElementChild!.className).toBe(STEPS_PCT);
    expect(progress(real!).firstElementChild).toHaveTextContent("42%");
    // The skeleton's bar takes the track's size (w-28 h-1, 112 by 4 px); the pair hides on small screens.
    const skelBar = progress(skel!).children[1] as HTMLElement;
    expect(skelBar.style.width).toBe("112px");
    expect(skelBar.style.height).toBe("4px");
    expect(STEPS_BAR.split(" ")).toEqual(expect.arrayContaining(["w-28", "h-1", "shrink-0"]));
    expect(STEPS_PROGRESS.split(" ")).toContain("max-sm:hidden");
    expect(progress(real!).children[1]!.className.startsWith(STEPS_BAR)).toBe(true);
    expect(skelHead).toMatchObject({ title: "Steps", filter: "Status: All" });
    expect(realHead).toMatchObject({ title: "Steps", filter: "Status: All" });
    // Bars only where the run's data lands: the counts line and the overall bar, not the title or the filter.
    expect(skel!.querySelector("h3 [data-skeleton]")).toBeNull();
    expect(skel!.querySelector(`[data-testid=steps-summary] [data-skeleton]`)).not.toBeNull();
    const rows = (s: Element) => Array.from(s.querySelectorAll("li")).map((li) => li.className);
    expect(rows(skel!)).toEqual(rows(real!));
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
      <RunList runs={[RUN]} total={1} loading={loading} refreshing={refreshing} typeTitle={() => "Backfill channels"} autoRefresh="0" onAutoRefresh={() => {}} />
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

  it("draws as many blocks as the loaded page in the header, tiles, inputs and steps", () => {
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
      // No chip strip under the title; the facts grid in the Summary card, labels as text; the badge beside the number; the actions button.
      chipRows: root.querySelectorAll("header .flex-wrap").length,
      facts: Array.from(root.querySelectorAll(`section[aria-label=Summary] [class="${RUN_FACTS}"] > div > dt`)).map((d) => d.textContent),
      factValues: root.querySelectorAll(`section[aria-label=Summary] [class="${RUN_FACTS}"] > div > dd`).length,
      copy: root.querySelectorAll("section[aria-label=Summary] button[aria-label='Copy run ID']").length,
      pill: root.querySelectorAll("header [data-testid=run-status] > span").length,
      menu: root.querySelectorAll("header button[aria-label='Run actions']").length,
      headerLines: root.querySelectorAll("header p").length,
      cardHeads: root.querySelectorAll(`[class="${CARD_HEAD}"]`).length,
      tiles: root.querySelectorAll(`section[aria-label=Summary] dl > [class="${STAT_TILE}"]`).length,
      inputs: root.querySelectorAll("section[aria-label=Summary] dl > .contents").length,
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
    expect(real).toMatchObject({ chipRows: 0, facts: ["Triggered", "Mapping", "Started", "Duration", "Run ID"], factValues: 5, copy: 1, pill: 1, menu: 1, cardHeads: 2, tiles: 3, steps: 6 });
    expect(real.inputs).toBeGreaterThan(0);
    // The error sits on the title line, so the header has no line the skeleton lacks.
    expect(skel).toEqual(real);
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
