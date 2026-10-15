import { beforeEach, describe, expect, it } from "vitest";
import type { TaskType } from "../../types/tasks";
import { humanizeTypeKey, rememberRunTypes, rememberTypes, runTypeOf, typeLabel } from "./typeLabels";

const type = (key: string, title: string, description = ""): TaskType => ({ key, title, description, inputs_schema: {}, active: 0, total: 0 });

beforeEach(() => localStorage.clear());

describe("task type labels", () => {
  it("says an unknown type's id in words", () => {
    expect(humanizeTypeKey("backfill_channels")).toBe("Backfill channels");
    expect(humanizeTypeKey("prune-old_rows")).toBe("Prune old rows");
    expect(humanizeTypeKey("")).toBe("");
    expect(typeLabel("backfill_channels")).toEqual({ title: "Backfill channels", description: "" });
  });

  it("keeps the fetched types across visits and follows storage", () => {
    rememberTypes([type("backfill_channels", "Backfill channels", "Clone message history"), type("prune", "Prune rows")]);
    expect(typeLabel("backfill_channels")).toEqual({ title: "Backfill channels", description: "Clone message history" });
    expect(typeLabel("prune")).toEqual({ title: "Prune rows", description: "" });
    expect(JSON.parse(localStorage.getItem("tasks:types")!)).toHaveProperty("prune");
    // A cleared storage is a first visit again.
    localStorage.clear();
    expect(typeLabel("prune")).toEqual({ title: "Prune", description: "" });
    localStorage.setItem("tasks:types", "not json");
    expect(typeLabel("prune").title).toBe("Prune");
  });

  it("remembers each run's type, and knows it without asking when there is only one type", () => {
    expect(runTypeOf("12")).toBeNull();
    rememberTypes([type("backfill_channels", "Backfill channels")]);
    expect(runTypeOf("12")).toBe("backfill_channels");
    rememberTypes([type("backfill_channels", "Backfill channels"), type("prune", "Prune rows")]);
    expect(runTypeOf("12")).toBeNull();
    rememberRunTypes([{ id: "12", type: "prune" }, { id: "13", type: "backfill_channels" }]);
    expect(runTypeOf("12")).toBe("prune");
    expect(runTypeOf("13")).toBe("backfill_channels");
  });

  it("keeps at most 500 runs, dropping the lowest ids", () => {
    rememberRunTypes(Array.from({ length: 520 }, (_, i) => ({ id: String(i + 1), type: "prune" })));
    const held = JSON.parse(localStorage.getItem("tasks:run-types")!) as Record<string, string>;
    expect(Object.keys(held)).toHaveLength(500);
    expect(runTypeOf("20")).toBeNull();
    expect(runTypeOf("21")).toBe("prune");
    expect(runTypeOf("520")).toBe("prune");
  });
});
