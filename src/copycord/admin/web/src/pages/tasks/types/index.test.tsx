import { fireEvent, render, screen } from "@testing-library/react";
import { MemoryRouter, Route, Routes } from "react-router";
import { describe, expect, it, vi } from "vitest";
import { ToastProvider } from "../../../components/Toast";
import type { TaskType } from "../../../types/tasks";
import { BackfillForm } from "./backfill/BackfillForm";
import { describeGeneric, entryFor, SchemaForm, schemaFields, TaskTypeForm } from ".";

const SCHEMA = {
  properties: {
    name: { type: "string", title: "Name" },
    limit: { type: "integer" },
    dry_run: { type: "boolean", default: true },
    level: { enum: ["low", "high"], type: "string" },
    on: { anyOf: [{ type: "string", format: "date" }, { type: "null" }] },
  },
  required: ["name", "level"],
};

const TYPE: TaskType = { key: "prune", title: "Prune", description: "", inputs_schema: SCHEMA, active: 0, total: 0 };

describe("task type registry", () => {
  it("has a Backfill entry and falls back to the schema form", () => {
    expect(entryFor("backfill_channels").Form).toBe(BackfillForm);
    expect(entryFor("prune").Form).toBe(SchemaForm);
  });

  it("reads string, number, boolean, enum and date fields", () => {
    expect(schemaFields(SCHEMA).map((f) => [f.key, f.kind, f.required])).toEqual([
      ["name", "string", true],
      ["limit", "number", false],
      ["dry_run", "boolean", false],
      ["level", "enum", true],
      ["on", "date", false],
    ]);
  });

  it("renders the fallback form from inputs_schema and posts its inputs", async () => {
    const posts: unknown[] = [];
    vi.stubGlobal(
      "fetch",
      vi.fn(async (_url: string, init?: RequestInit) => {
        posts.push(JSON.parse(String(init?.body)));
        return Response.json({ run: { id: "r1", number: 1 } }, { status: 201 });
      }),
    );
    render(
      <ToastProvider>
        <MemoryRouter initialEntries={["/tasks"]}>
          <Routes>
            <Route path="/tasks" element={<TaskTypeForm type={TYPE} open onClose={() => {}} />} />
            <Route path="/tasks/runs/:id" element={<p>Opened</p>} />
          </Routes>
        </MemoryRouter>
      </ToastProvider>,
    );
    expect(screen.getByRole("dialog", { name: "Run Prune" })).toBeInTheDocument();
    expect(screen.getByLabelText("Limit")).toHaveAttribute("type", "number");
    expect(screen.getByLabelText("On")).toHaveAttribute("type", "date");
    expect(screen.getByRole("checkbox", { name: "Dry run" })).toBeChecked();

    fireEvent.click(screen.getByRole("button", { name: "Start" }));
    expect(await screen.findAllByText("Required.")).toHaveLength(1);

    fireEvent.change(screen.getByLabelText("Name"), { target: { value: "x" } });
    fireEvent.change(screen.getByLabelText("Limit"), { target: { value: "5" } });
    fireEvent.click(screen.getByRole("button", { name: "Start" }));
    expect(await screen.findByText("Opened")).toBeInTheDocument();
    expect(posts[0]).toEqual({ type: "prune", inputs: { name: "x", limit: 5, dry_run: true, level: "low" } });
  });

  it("formats inputs as key/value rows", () => {
    expect(describeGeneric({ mapping_id: "m1", dry_run: false, ids: ["1", "2"] })).toEqual([
      { label: "Mapping id", value: "m1" },
      { label: "Dry run", value: "No" },
      { label: "Ids", value: "1, 2" },
    ]);
  });
});
