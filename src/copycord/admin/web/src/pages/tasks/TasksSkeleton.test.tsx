import { render, screen, waitFor, within } from "@testing-library/react";
import { MemoryRouter } from "react-router";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { ToastProvider } from "../../components/Toast";
import { ConfirmProvider } from "../../components/ui/ConfirmDialog";
import type { TaskRun, TaskStep } from "../../types/tasks";
import { Tasks } from "../Tasks";
import { RunList, RunRow } from "./RunList";
import { StepList } from "./StepList";
import {
  ALL_TASKS_DESCRIPTION,
  RunDetailSkeleton,
  STEPS_BAR,
  STEPS_COUNT_KEY,
  STEPS_HEAD,
  STEP_LIST,
  BUSY,
  FADE_IN,
  FRAME,
  FRAME_HEAD,
  FRAME_HEAD_FILTERS,
  FRAME_HEAD_LEFT,
  PER_PAGE,
  RAIL_LIST,
  ROW_BOX,
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

  it("draws as many rows as the list had last time, at most a page, and the empty state with none", () => {
    const rows = () => {
      const { container, unmount } = render(<RunRowsLoading />);
      const out = { li: container.querySelectorAll("li").length, empty: container.textContent };
      unmount();
      return out;
    };
    expect(rows()).toEqual({ li: 0, empty: "No task runs yetPick a task type on the left to run one" });
    localStorage.setItem(`skel:${RUNS_COUNT_KEY}`, "0");
    expect(rows().li).toBe(0);
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
    frames: root.querySelectorAll(`[class="${FRAME}"]`).length,
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
    const skel = render(
      <MemoryRouter initialEntries={["/tasks"]}>
        <TasksSkeleton />
      </MemoryRouter>,
    );
    const skeleton = chrome(skel.container);
    // A first visit has no stored run count: the loading list is the empty state's shape, no rows.
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
      headLeft: 2,
      autoRefresh: "Auto-refresh: Off",
      dropdowns: ["Task type: All", "Status: All", "Mapping: All", "Actor: All"],
      pager: ["Previous true", "Next true"],
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
    // The only control is the Steps filter, real and inert; the header actions depend on the run's status.
    expect(within(run).queryAllByRole("button", { hidden: true }).map((b) => b.firstElementChild?.textContent)).toEqual(["Status: All"]);
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
    // Title, counts line and filter in the same boxes; the third part is the overall bar.
    expect(skelHead.parts).toHaveLength(4);
    expect(skelHead.parts.filter((_, i) => i !== 2)).toEqual(realHead.parts.filter((_, i) => i !== 2));
    // The skeleton's bar takes the track's size (w-28 h-1, 112 by 4 px) and hides with it on small screens.
    const skelBar = skel!.querySelector(`[class="${STEPS_HEAD}"]`)!.children[2] as HTMLElement;
    expect(skelBar.style.width).toBe("112px");
    expect(skelBar.style.height).toBe("4px");
    expect(STEPS_BAR.split(" ")).toEqual(expect.arrayContaining(["w-28", "h-1", "shrink-0", "max-sm:hidden"]));
    expect(skelBar.className).toContain("shrink-0 max-sm:hidden");
    expect(real!.querySelector(`[class="${STEPS_HEAD}"]`)!.children[2]!.className.startsWith(STEPS_BAR)).toBe(true);
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

  it("leaves Task type out and bars the header on a type page, as the page does", () => {
    const { container } = render(
      <MemoryRouter initialEntries={["/tasks?type=backfill_channels"]}>
        <TasksSkeleton />
      </MemoryRouter>,
    );
    const c = chrome(container);
    expect(c.dropdowns).toEqual(["Status: All", "Mapping: All", "Actor: All"]);
    expect(c.title?.trim()).toBe("");
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

  it("fades a type page's title in, but leaves the static All tasks header alone", () => {
    const { rerender } = render(<TasksHeader title="All tasks" description={ALL_TASKS_DESCRIPTION} action={null} />);
    expect(screen.getByRole("heading", { name: "All tasks" }).querySelector("span")).toBeNull();
    rerender(<TasksHeader title="Backfill channels" description="Clone message history" action={null} fade />);
    expect(screen.getByText("Backfill channels")).toHaveClass(...FADE_IN.split(" "));
    expect(screen.getByText("Clone message history")).toHaveClass(...FADE_IN.split(" "));
  });
});
