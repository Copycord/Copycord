import { fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { MemoryRouter, Route, Routes, useParams } from "react-router";
import { describe, expect, it, vi } from "vitest";
import { ToastProvider } from "../../../../components/Toast";
import { ConfirmProvider } from "../../../../components/ui/ConfirmDialog";
import type { TaskType } from "../../../../types/tasks";
import { describeRunInputs, entryFor, heroStat, summaryStats } from "..";
import { SYNC_ASSETS_EXPLAINER, SyncAssetsForm } from "./SyncAssetsForm";
import { syncKindsWords } from "./entry";

const TYPE: TaskType = {
  key: "sync_assets",
  title: "Sync assets",
  description: "",
  inputs_schema: {},
  active: 0,
  total: 0,
};

const MAPPINGS = [
  { mapping_id: "m1", mapping_name: "Alpha", original_guild_id: "1", original_guild_name: "Src", cloned_guild_id: "2", cloned_guild_name: "Dst" },
  { mapping_id: "m2", mapping_name: "Second", original_guild_id: "3", original_guild_name: "S2", cloned_guild_id: "4", cloned_guild_name: "D2" },
];

function serve() {
  const posts: Record<string, unknown>[] = [];
  vi.stubGlobal(
    "fetch",
    vi.fn(async (url: string, init?: RequestInit) => {
      if (url === "/api/guild-mappings") return Response.json({ ok: true, mappings: MAPPINGS });
      if (url === "/api/tasks/runs" && init?.method === "POST") {
        posts.push(JSON.parse(String(init.body)) as Record<string, unknown>);
        return Response.json({ run: { id: "r7", number: 7 } }, { status: 201 });
      }
      return Response.json({ detail: "not found" }, { status: 404 });
    }),
  );
  return { posts };
}

// The pending bar says "Main mapping", so the first mapping's own name shows the list has loaded.
async function loaded() {
  await waitFor(() => expect(screen.getByRole("combobox", { name: "Mapping" })).toHaveTextContent("Alpha"));
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
            <Route path="/tasks" element={<SyncAssetsForm type={TYPE} open onClose={() => {}} />} />
            <Route path="/tasks/runs/:id" element={<RunPage />} />
          </Routes>
        </MemoryRouter>
      </ConfirmProvider>
    </ToastProvider>,
  );
}

describe("SyncAssetsForm", () => {
  it("explains the job, picks the first mapping and starts with every kind and Delete extras off", async () => {
    serve();
    renderForm();
    expect(screen.getByText(SYNC_ASSETS_EXPLAINER)).toBeInTheDocument();
    await loaded();
    for (const name of ["Roles", "Emojis", "Stickers"]) expect(screen.getByRole("checkbox", { name })).toBeChecked();
    expect(screen.getByRole("checkbox", { name: /Delete extras/ })).not.toBeChecked();
    expect(screen.getByRole("button", { name: "Sync assets" })).toBeInTheDocument();
  });

  it("needs at least one kind", async () => {
    const { posts } = serve();
    renderForm();
    await loaded();
    for (const name of ["Roles", "Emojis", "Stickers"]) fireEvent.click(screen.getByRole("checkbox", { name }));
    fireEvent.click(screen.getByRole("button", { name: "Sync assets" }));
    expect(await screen.findByRole("alert")).toHaveTextContent("Pick at least one kind of asset.");
    expect(posts).toEqual([]);
  });

  it("starts without a confirm while Delete extras is off", async () => {
    const { posts } = serve();
    renderForm();
    await loaded();
    fireEvent.click(screen.getByRole("checkbox", { name: "Emojis" }));
    fireEvent.click(screen.getByRole("button", { name: "Sync assets" }));
    expect(await screen.findByText("Run page r7")).toBeInTheDocument();
    expect(screen.queryByRole("dialog")).toBeNull();
    expect(posts[0]).toEqual({ type: "sync_assets", inputs: { mapping_id: "m1", kinds: ["roles", "stickers"], delete_extras: false } });
  });

  it("confirms before a sync that deletes extras, and Keep posts nothing", async () => {
    const { posts } = serve();
    renderForm();
    await loaded();
    fireEvent.click(screen.getByRole("checkbox", { name: /Delete extras/ }));
    fireEvent.click(screen.getByRole("button", { name: "Sync assets" }));
    const dialog = await screen.findByRole("dialog", { name: "Delete extra roles, emojis and stickers in Dst?" });
    fireEvent.click(within(dialog).getByRole("button", { name: "Keep" }));
    expect(posts).toEqual([]);

    fireEvent.click(screen.getByRole("button", { name: "Sync assets" }));
    const again = await screen.findByRole("dialog", { name: "Delete extra roles, emojis and stickers in Dst?" });
    fireEvent.click(within(again).getByRole("button", { name: "Sync and delete" }));
    expect(await screen.findByText("Run page r7")).toBeInTheDocument();
    expect(posts[0]!.inputs).toEqual({ mapping_id: "m1", kinds: ["roles", "emojis", "stickers"], delete_extras: true });
  });
});

describe("sync_assets registration", () => {
  it("has its own form, inputs rows, summary words and hero", () => {
    expect(entryFor("sync_assets").Form).toBe(SyncAssetsForm);
    expect(syncKindsWords(["stickers", "roles"])).toBe("Roles and stickers");
    const run = {
      type: "sync_assets",
      inputs: { mapping_id: "m2", kinds: ["roles", "emojis"], delete_extras: false },
      context: { label: "Second" },
      summary: { created: 3, updated: 2, deleted: 0, reordered: 5, skipped: 1, failed: 0 },
      report: null,
    } as unknown as Parameters<typeof describeRunInputs>[0] & Parameters<typeof heroStat>[0];
    expect(describeRunInputs(run)).toEqual([
      { label: "Mapping", value: "Second" },
      { label: "Assets", value: "Roles and emojis" },
      { label: "Delete extras", value: "No" },
    ]);
    expect(summaryStats(run).map((s) => s.label)).toEqual([
      "Assets created",
      "Assets updated",
      "Assets deleted",
      "Roles reordered",
      "Assets skipped",
      "Assets failed",
    ]);
    expect(heroStat(run)).toEqual({ key: "created", label: "assets created", value: 3 });
  });
});
