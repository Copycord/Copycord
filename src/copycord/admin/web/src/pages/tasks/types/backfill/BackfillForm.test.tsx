import { fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { MemoryRouter, Route, Routes, useParams } from "react-router";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { ToastProvider } from "../../../../components/Toast";
import type { BackfillOptionsResponse, TaskRun, TaskType } from "../../../../types/tasks";
import { BackfillForm } from "./BackfillForm";

const TYPE: TaskType = {
  key: "backfill_channels",
  title: "Backfill channels",
  description: "Clone message history",
  inputs_schema: {},
  active: 0,
  total: 0,
};

const MAPPINGS = {
  ok: true,
  mappings: [
    { mapping_id: "m1", mapping_name: "Main mapping", original_guild_id: "1", original_guild_name: "Src", cloned_guild_id: "2", cloned_guild_name: "Dst" },
    { mapping_id: "m2", mapping_name: "Second", original_guild_id: "3", original_guild_name: "S2", cloned_guild_id: "4", cloned_guild_name: "D2" },
  ],
};

const OPTIONS: BackfillOptionsResponse = {
  mapping_id: "m1",
  categories: [
    {
      id: "c1",
      name: "Announcements",
      channels: [
        { id: "11", name: "news", orphan: false, busy: null, resumable: null },
        { id: "12", name: "updates", orphan: false, busy: null, resumable: null },
        { id: "13", name: "gone", orphan: true, busy: null, resumable: null },
      ],
    },
    {
      id: "c2",
      name: "Chat",
      channels: [
        { id: "21", name: "general", orphan: false, busy: null, resumable: { checkpoint_at: "2026-10-03T12:00:00Z" } },
        { id: "22", name: "memes", orphan: false, busy: { run_id: "r12", number: 12 }, resumable: null },
        { id: "23", name: "help", orphan: false, busy: null, resumable: null },
      ],
    },
  ],
};

type Post = { url: string; body: Record<string, unknown> };

function serve(post?: (body: Record<string, unknown>) => Response) {
  const posts: Post[] = [];
  const fn = vi.fn(async (url: string, init?: RequestInit) => {
    if (url === "/api/guild-mappings") return Response.json(MAPPINGS);
    if (url.startsWith("/api/tasks/types/backfill_channels/options?")) {
      const m = new URL(url, "http://x").searchParams.get("mapping_id");
      return Response.json(m === "m1" ? OPTIONS : { mapping_id: m, categories: [] });
    }
    if (url === "/api/tasks/runs" && init?.method === "POST") {
      const body = JSON.parse(String(init.body)) as Record<string, unknown>;
      posts.push({ url, body });
      return post ? post(body) : Response.json({ run: { id: "r13", number: 13 } }, { status: 201 });
    }
    return Response.json({ detail: "not found" }, { status: 404 });
  });
  vi.stubGlobal("fetch", fn);
  return { fn, posts };
}

function RunPage() {
  const { id } = useParams();
  return <p>Run page {id}</p>;
}

function renderForm(prefill: TaskRun | null = null, onClose = vi.fn()) {
  render(
    <ToastProvider>
      <MemoryRouter initialEntries={["/tasks"]}>
        <Routes>
          <Route path="/tasks" element={<BackfillForm type={TYPE} open onClose={onClose} prefill={prefill} />} />
          <Route path="/tasks/runs/:id" element={<RunPage />} />
        </Routes>
      </MemoryRouter>
    </ToastProvider>,
  );
  return onClose;
}

const box = (name: string) => screen.getByRole("checkbox", { name }) as HTMLInputElement;

beforeEach(() => {
  localStorage.clear();
});

describe("BackfillForm", () => {
  it("loads the mappings, defaults to the first and lists its channels without orphans", async () => {
    const { fn } = serve();
    renderForm();
    expect(await screen.findByRole("checkbox", { name: "#news" })).toBeInTheDocument();
    expect(screen.getByRole("combobox", { name: "Mapping" })).toHaveValue("m1");
    expect(screen.queryByRole("checkbox", { name: "#gone" })).toBeNull();
    expect(fn.mock.calls.some((c) => String(c[0]).includes("options?mapping_id=m1"))).toBe(true);

    fireEvent.change(screen.getByRole("combobox", { name: "Mapping" }), { target: { value: "m2" } });
    expect(await screen.findByText("No channels in this mapping")).toBeInTheDocument();
  });

  it("selects a category's channels with a tri-state box", async () => {
    serve();
    renderForm();
    await screen.findByRole("checkbox", { name: "#news" });
    fireEvent.click(box("#news"));
    expect(box("Category Announcements").indeterminate).toBe(true);
    expect(box("Category Announcements")).toHaveAttribute("aria-checked", "mixed");
    fireEvent.click(box("Category Announcements"));
    expect(box("#updates").checked).toBe(true);
    expect(box("Category Announcements").checked).toBe(true);
    expect(box("Category Announcements").indeterminate).toBe(false);
    fireEvent.click(box("Category Announcements"));
    expect(box("#news").checked).toBe(false);
    expect(box("#updates").checked).toBe(false);
  });

  it("disables busy channels and notes resumable ones; Mode shows only for a resumable pick", async () => {
    serve();
    renderForm();
    await screen.findByRole("checkbox", { name: "#memes" });
    expect(box("#memes")).toBeDisabled();
    expect(screen.getByText("Running in #12")).toBeInTheDocument();
    expect(screen.getByText(/resume from Oct 3/)).toBeInTheDocument();
    expect(screen.queryByRole("radiogroup", { name: "Mode" })).toBeNull();
    fireEvent.click(box("#general"));
    const mode = screen.getByRole("radiogroup", { name: "Mode" });
    expect(within(mode).getByRole("radio", { name: "Resume previous" })).toBeChecked();
    // The category box covers only the free channels.
    fireEvent.click(box("Category Chat"));
    expect(box("#memes").checked).toBe(false);
    expect(box("#help").checked).toBe(true);
  });

  it("shows the v3 range errors and does not post", async () => {
    const { posts } = serve();
    renderForm();
    await screen.findByRole("checkbox", { name: "#news" });
    fireEvent.click(box("#news"));

    fireEvent.click(screen.getByRole("radio", { name: "Since date/time" }));
    fireEvent.click(screen.getByRole("button", { name: "Start" }));
    expect(await screen.findByText("Pick a date.")).toBeInTheDocument();

    fireEvent.click(screen.getByRole("radio", { name: "Last N messages" }));
    fireEvent.change(screen.getByLabelText("Last N messages", { selector: "input[type=number]" }), { target: { value: "0" } });
    fireEvent.click(screen.getByRole("button", { name: "Start" }));
    expect(await screen.findByText("Enter a valid number.")).toBeInTheDocument();

    fireEvent.click(screen.getByRole("radio", { name: "Between dates" }));
    fireEvent.change(screen.getByLabelText("From"), { target: { value: "2026-10-05" } });
    fireEvent.change(screen.getByLabelText("To"), { target: { value: "2026-10-01" } });
    fireEvent.click(screen.getByRole("button", { name: "Start" }));
    expect(await screen.findByText("End must be after the start.")).toBeInTheDocument();
    expect(posts).toHaveLength(0);
  });

  it("asks for a channel before posting", async () => {
    const { posts } = serve();
    renderForm();
    await screen.findByRole("checkbox", { name: "#news" });
    fireEvent.click(screen.getByRole("button", { name: "Start" }));
    expect(await screen.findByText("Select at least one channel or category.")).toBeInTheDocument();
    expect(posts).toHaveLength(0);
  });

  it("starts a category plus two channels, navigates to the run and toasts", async () => {
    const { posts } = serve();
    const onClose = renderForm();
    await screen.findByRole("checkbox", { name: "#news" });
    fireEvent.click(box("Category Announcements"));
    fireEvent.click(box("#general"));
    fireEvent.click(box("#help"));
    fireEvent.click(screen.getByRole("checkbox", { name: "Ignore cloned messages" }));
    fireEvent.click(screen.getByRole("radio", { name: "Last N messages" }));
    fireEvent.change(screen.getByLabelText("Last N messages", { selector: "input[type=number]" }), { target: { value: "50" } });
    fireEvent.click(screen.getByRole("button", { name: "Start" }));

    expect(await screen.findByText("Run page r13")).toBeInTheDocument();
    expect(await screen.findByText("Started Backfill channels #13")).toBeInTheDocument();
    expect(onClose).toHaveBeenCalled();
    expect(posts[0]!.body).toEqual({
      type: "backfill_channels",
      inputs: {
        mapping_id: "m1",
        category_ids: ["c1"],
        channel_ids: ["21", "23"],
        range: { mode: "last_n", last_n: 50 },
        ignore_cloned: true,
        mode: "resume",
      },
    });
  });

  it("shows 422 errors on their fields", async () => {
    serve(() =>
      Response.json({ errors: { range: "Pick a start date", channel_ids: "Unknown channel 11" } }, { status: 422 }),
    );
    renderForm();
    await screen.findByRole("checkbox", { name: "#news" });
    fireEvent.click(box("#news"));
    fireEvent.click(screen.getByRole("button", { name: "Start" }));
    expect(await screen.findByText("Pick a start date")).toBeInTheDocument();
    expect(screen.getByText("Unknown channel 11")).toBeInTheDocument();
    expect(screen.queryByText(/Run page/)).toBeNull();
  });

  it("prefills a Re-run with Resume after a failed run", async () => {
    const { posts } = serve();
    const prefill = {
      id: "r9",
      type: "backfill_channels",
      number: 9,
      status: "failed",
      inputs: {
        mapping_id: "m1",
        channel_ids: ["21"],
        category_ids: ["c1"],
        range: { mode: "since", since: "2026-10-01T08:30" },
        ignore_cloned: false,
        mode: "new",
      },
    } as unknown as TaskRun;
    renderForm(prefill);
    await screen.findByRole("checkbox", { name: "#news" });
    await waitFor(() => expect(box("#updates").checked).toBe(true));
    expect(box("#general").checked).toBe(true);
    expect(screen.getByRole("radio", { name: "Since date/time" })).toBeChecked();
    expect(screen.getByLabelText("Since date")).toHaveValue("2026-10-01");
    expect(screen.getByRole("radio", { name: "Resume previous" })).toBeChecked();
    fireEvent.click(screen.getByRole("button", { name: "Start" }));
    await screen.findByText("Run page r13");
    expect(posts[0]!.body.trigger).toBe("rerun");
  });
});
