import { fireEvent, render, screen, within } from "@testing-library/react";
import { MemoryRouter, Route, Routes, useParams } from "react-router";
import { describe, expect, it, vi } from "vitest";
import { ToastProvider } from "../../../../components/Toast";
import { ConfirmProvider } from "../../../../components/ui/ConfirmDialog";
import type { TaskType } from "../../../../types/tasks";
import { describeRunInputs, entryFor, heroStat } from "..";
import { PURGE_ASSETS_EXPLAINER, PurgeAssetsForm, countTotal, countWords } from "./PurgeAssetsForm";
import { kindsWords } from "./entry";

const TYPE: TaskType = {
  key: "purge_assets",
  title: "Purge assets",
  description: "",
  inputs_schema: {},
  active: 0,
  total: 0,
};

const MAPPINGS = [
  { mapping_id: "m1", name: "Main mapping", cloned_guild_id: "2", cloned_guild_name: "Dst" },
  { mapping_id: "m2", name: "Second", cloned_guild_id: "4", cloned_guild_name: "D2" },
];

function serve(counts = { emojis: 12, stickers: 1, roles: 3 }) {
  const posts: Record<string, unknown>[] = [];
  const gets: string[] = [];
  vi.stubGlobal(
    "fetch",
    vi.fn(async (url: string, init?: RequestInit) => {
      if (url === "/api/tasks/types/purge_assets/options") {
        return Response.json({ mappings: MAPPINGS, counts: { emojis: null, stickers: null, roles: null }, live: false });
      }
      if (url.startsWith("/api/tasks/types/purge_assets/options?")) {
        gets.push(url);
        return Response.json({ mappings: MAPPINGS, counts, live: true, notes: {}, error: null });
      }
      if (url === "/api/tasks/runs" && init?.method === "POST") {
        posts.push(JSON.parse(String(init.body)) as Record<string, unknown>);
        return Response.json({ run: { id: "r9", number: 9 } }, { status: 201 });
      }
      return Response.json({ detail: "not found" }, { status: 404 });
    }),
  );
  return { posts, gets };
}

function RunPage() {
  const { id } = useParams();
  return <p>Run page {id}</p>;
}

function renderForm() {
  render(
    <ToastProvider>
      <ConfirmProvider>
        <MemoryRouter initialEntries={["/tasks"]}>
          <Routes>
            <Route path="/tasks" element={<PurgeAssetsForm type={TYPE} open onClose={() => {}} />} />
            <Route path="/tasks/runs/:id" element={<RunPage />} />
          </Routes>
        </MemoryRouter>
      </ConfirmProvider>
    </ToastProvider>,
  );
}

describe("PurgeAssetsForm", () => {
  it("explains the job, picks the first mapping and shows the counts beside the kinds", async () => {
    const { gets } = serve();
    renderForm();
    expect(screen.getByText(PURGE_ASSETS_EXPLAINER)).toBeInTheDocument();
    expect(await screen.findByRole("combobox", { name: "Mapping" })).toHaveTextContent("Main mapping");
    expect(await screen.findByText("12")).toBeInTheDocument();
    expect(screen.getByTestId("count-roles")).toHaveTextContent("3");
    expect(gets[0]).toContain("mapping_id=m1");
    expect(gets[0]).toContain("scope=cloned");
    expect(screen.getByRole("button", { name: "Purge assets" })).toBeInTheDocument();
  });

  it("needs at least one kind", async () => {
    const { posts } = serve();
    renderForm();
    await screen.findByText("12");
    for (const name of ["Emojis", "Stickers", "Roles"]) fireEvent.click(screen.getByRole("checkbox", { name: new RegExp(name) }));
    fireEvent.click(screen.getByRole("button", { name: "Purge assets" }));
    expect(await screen.findByRole("alert")).toHaveTextContent("Pick at least one kind of asset.");
    expect(posts).toEqual([]);
  });

  it("previews the count in words", async () => {
    serve();
    renderForm();
    await screen.findByText("12");
    fireEvent.click(screen.getByRole("button", { name: "Preview count" }));
    const status = await screen.findByRole("status");
    expect(status).toHaveTextContent("12 emojis, 1 sticker and 3 roles cloned by Copycord in Dst.");
  });

  it("confirms with the count before deleting, and Keep posts nothing", async () => {
    const { posts } = serve();
    renderForm();
    await screen.findByText("12");
    fireEvent.click(screen.getByRole("button", { name: "Purge assets" }));
    const dialog = await screen.findByRole("dialog", { name: "Delete 16 assets in Dst?" });
    fireEvent.click(within(dialog).getByRole("button", { name: "Keep" }));
    expect(posts).toEqual([]);

    fireEvent.click(screen.getByRole("button", { name: "Purge assets" }));
    const again = await screen.findByRole("dialog", { name: "Delete 16 assets in Dst?" });
    fireEvent.click(within(again).getByRole("button", { name: "Delete" }));
    expect(await screen.findByText("Run page r9")).toBeInTheDocument();
    expect(posts[0]).toEqual({
      type: "purge_assets",
      inputs: { mapping_id: "m1", kinds: ["emojis", "stickers", "roles"], scope: "cloned", dry_run: false },
    });
  });

  it("starts a dry run without a confirm", async () => {
    const { posts } = serve();
    renderForm();
    await screen.findByText("12");
    fireEvent.click(screen.getByRole("checkbox", { name: /Stickers/ }));
    fireEvent.click(screen.getByRole("checkbox", { name: /Dry run/ }));
    fireEvent.click(screen.getByRole("button", { name: "Start dry run" }));
    expect(await screen.findByText("Run page r9")).toBeInTheDocument();
    expect(posts[0]!.inputs).toEqual({ mapping_id: "m1", kinds: ["emojis", "roles"], scope: "cloned", dry_run: true });
  });

  it("words and totals the counts", () => {
    expect(countWords(["roles"], { roles: 1 })).toBe("1 role");
    expect(countWords(["emojis", "roles"], { emojis: 2, roles: null })).toBeNull();
    expect(countTotal(["emojis", "stickers"], { emojis: 2, stickers: 5 })).toBe(7);
    expect(kindsWords(["roles", "emojis"])).toBe("Emojis and roles");
  });
});

describe("purge_assets registration", () => {
  it("has its own form, inputs rows and hero", () => {
    expect(entryFor("purge_assets").Form).toBe(PurgeAssetsForm);
    const run = {
      type: "purge_assets",
      inputs: { mapping_id: "m2", kinds: ["emojis", "stickers", "roles"], scope: "all", dry_run: false },
      context: { label: "Second" },
      summary: { matched: 5, deleted: 4 },
      report: null,
    } as unknown as Parameters<typeof describeRunInputs>[0] & Parameters<typeof heroStat>[0];
    expect(describeRunInputs(run)).toEqual([
      { label: "Mapping", value: "Second" },
      { label: "Assets", value: "Emojis, stickers and roles" },
      { label: "Which assets", value: "Everything" },
      { label: "Dry run", value: "No" },
    ]);
    expect(heroStat(run)).toEqual({ key: "deleted", label: "assets deleted", value: 4 });
  });
});
