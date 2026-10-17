import { fireEvent, render, screen, within, waitFor } from "@testing-library/react";
import { MemoryRouter, Route, Routes, useParams } from "react-router";
import { describe, expect, it, vi } from "vitest";
import { ToastProvider } from "../../../../components/Toast";
import { ConfirmProvider } from "../../../../components/ui/ConfirmDialog";
import type { TaskType } from "../../../../types/tasks";
import { ageWords, describeRunInputs, entryFor, heroStat } from "..";
import { PURGE_EXPLAINER, PurgeForm, olderThanError, recordWords } from "./PurgeForm";

const TYPE: TaskType = {
  key: "purge_messages",
  title: "Purge messages",
  description: "",
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

function serve(matched = 1234) {
  const posts: Record<string, unknown>[] = [];
  const gets: string[] = [];
  vi.stubGlobal(
    "fetch",
    vi.fn(async (url: string, init?: RequestInit) => {
      if (url === "/api/guild-mappings") return Response.json(MAPPINGS);
      if (url.startsWith("/api/tasks/types/purge_messages/options?")) {
        gets.push(url);
        return Response.json({
          matched,
          mappings: [
            { mapping_id: "m1", name: "Main mapping", matched: matched - 4 },
            { mapping_id: "m2", name: "Second", matched: 4 },
          ],
        });
      }
      if (url === "/api/tasks/runs" && init?.method === "POST") {
        posts.push(JSON.parse(String(init.body)) as Record<string, unknown>);
        return Response.json({ run: { id: "r7", number: 7 } }, { status: 201 });
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
            <Route path="/tasks" element={<PurgeForm type={TYPE} open onClose={() => {}} />} />
            <Route path="/tasks/runs/:id" element={<RunPage />} />
          </Routes>
        </MemoryRouter>
      </ConfirmProvider>
    </ToastProvider>,
  );
}

describe("PurgeForm", () => {
  it("explains what is deleted and defaults to 30 days over all mappings", async () => {
    serve();
    renderForm();
    expect(screen.getByText(PURGE_EXPLAINER)).toBeInTheDocument();
    expect(PURGE_EXPLAINER).toContain("Messages on Discord are not touched");
    expect(screen.getByRole("spinbutton", { name: "Older than" })).toHaveValue(30);
    expect(screen.getByRole("combobox", { name: "Unit" })).toHaveTextContent("days");
    await waitFor(() => expect(screen.getByRole("combobox", { name: "Mapping" })).toHaveDisplayValue(/All mappings/));
    expect(screen.getByRole("button", { name: "Delete records" })).toBeInTheDocument();
  });

  it("rejects 0, negatives and fractions without a request", async () => {
    const { gets, posts } = serve();
    renderForm();
    for (const bad of ["0", "-2", "1.5", ""]) {
      fireEvent.change(screen.getByRole("spinbutton", { name: "Older than" }), { target: { value: bad } });
      fireEvent.click(screen.getByRole("button", { name: "Delete records" }));
      expect(await screen.findByRole("alert")).toHaveTextContent("Enter a whole number above 0.");
    }
    expect(gets).toEqual([]);
    expect(posts).toEqual([]);
  });

  it("previews the count per mapping", async () => {
    const { gets } = serve();
    renderForm();
    fireEvent.click(screen.getByRole("button", { name: "Preview count" }));
    const status = await screen.findByRole("status");
    expect(status).toHaveTextContent("1,234 message records older than 30 days in all mappings.");
    expect(within(status).getByText("Second")).toBeInTheDocument();
    expect(gets[0]).toContain("older_than=30");
    expect(gets[0]).toContain("unit=days");
    expect(gets[0]).not.toContain("mapping_id");
  });

  it("confirms with the count before deleting, and Keep posts nothing", async () => {
    const { posts } = serve();
    renderForm();
    fireEvent.click(screen.getByRole("button", { name: "Delete records" }));
    const dialog = await screen.findByRole("dialog", { name: "Delete 1,234 message records?" });
    fireEvent.click(within(dialog).getByRole("button", { name: "Keep" }));
    expect(posts).toEqual([]);

    fireEvent.click(screen.getByRole("button", { name: "Delete records" }));
    const again = await screen.findByRole("dialog", { name: "Delete 1,234 message records?" });
    fireEvent.click(within(again).getByRole("button", { name: "Delete" }));
    expect(await screen.findByText("Run page r7")).toBeInTheDocument();
    expect(posts[0]).toEqual({
      type: "purge_messages",
      inputs: { older_than: 30, unit: "days", mapping_id: null, dry_run: false },
    });
  });

  it("starts a dry run without a confirm", async () => {
    const { posts } = serve();
    renderForm();
    fireEvent.click(screen.getByRole("checkbox", { name: /Dry run/ }));
    fireEvent.change(screen.getByRole("spinbutton", { name: "Older than" }), { target: { value: "12" } });
    fireEvent.click(screen.getByRole("button", { name: "Start dry run" }));
    expect(await screen.findByText("Run page r7")).toBeInTheDocument();
    expect(posts[0]!.inputs).toEqual({ older_than: 12, unit: "days", mapping_id: null, dry_run: true });
  });

  it("All records disables the age, previews and confirms every record", async () => {
    const { gets, posts } = serve(241);
    renderForm();
    expect(screen.getByRole("radio", { name: "Older than" })).toBeChecked();
    fireEvent.change(screen.getByRole("spinbutton", { name: "Older than" }), { target: { value: "" } });
    fireEvent.click(screen.getByRole("radio", { name: "All records" }));
    expect(screen.getByRole("spinbutton", { name: "Older than" })).toBeDisabled();
    expect(screen.getByRole("combobox", { name: "Unit" })).toBeDisabled();

    fireEvent.click(screen.getByRole("button", { name: "Preview count" }));
    expect(await screen.findByRole("status")).toHaveTextContent(/^241 message records in all mappings\./);
    expect(gets[0]).toContain("all_records=true");
    expect(gets[0]).not.toContain("older_than");

    fireEvent.click(screen.getByRole("button", { name: "Delete records" }));
    const dialog = await screen.findByRole("dialog", { name: "Delete all 241 message records?" });
    fireEvent.click(within(dialog).getByRole("button", { name: "Delete" }));
    expect(await screen.findByText("Run page r7")).toBeInTheDocument();
    expect(posts[0]!.inputs).toEqual({ all_records: true, mapping_id: null, dry_run: false });
  });

  it("switching back to Older than validates the age again", async () => {
    const { gets } = serve();
    renderForm();
    fireEvent.click(screen.getByRole("radio", { name: "All records" }));
    fireEvent.click(screen.getByRole("radio", { name: "Older than" }));
    expect(screen.getByRole("spinbutton", { name: "Older than" })).toBeEnabled();
    fireEvent.change(screen.getByRole("spinbutton", { name: "Older than" }), { target: { value: "0" } });
    fireEvent.click(screen.getByRole("button", { name: "Preview count" }));
    expect(await screen.findByRole("alert")).toHaveTextContent("Enter a whole number above 0.");
    expect(gets).toEqual([]);
  });

  it("words the count and validates the age", () => {
    expect(recordWords(1)).toBe("1 message record");
    expect(recordWords(1234)).toBe("1,234 message records");
    expect(olderThanError("7")).toBeUndefined();
    expect(olderThanError("0")).toBeTruthy();
  });
});

describe("purge_messages registration", () => {
  it("has its own form, inputs rows and hero", () => {
    expect(entryFor("purge_messages").Form).toBe(PurgeForm);
    expect(ageWords(1, "weeks")).toBe("1 week");
    expect(ageWords(30, "days")).toBe("30 days");
    const run = {
      type: "purge_messages",
      inputs: { older_than: 6, unit: "hours", mapping_id: "m2", dry_run: true },
      context: { label: "Second" },
      summary: { matched: 40, deleted: 0 },
      report: null,
    } as unknown as Parameters<typeof describeRunInputs>[0] & Parameters<typeof heroStat>[0];
    expect(describeRunInputs(run)).toEqual([
      { label: "Older than", value: "6 hours" },
      { label: "Mapping", value: "Second" },
      { label: "Dry run", value: "Yes, nothing deleted" },
    ]);
    expect(heroStat(run)).toEqual({ key: "deleted", label: "message records deleted", value: 0 });
    const all = { ...run, inputs: { older_than: 30, unit: "days", mapping_id: null, dry_run: false, all_records: true } };
    expect(describeRunInputs(all)[0]).toEqual({ label: "Records", value: "All records" });
    expect(describeRunInputs(all)[1]).toEqual({ label: "Mapping", value: "All mappings" });
  });
});
