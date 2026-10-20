import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { DashboardMapping } from "../../../../types/dashboard-mappings";
import { MappingModal } from "./MappingModal";
import type { MappingMode } from "./model";

const celebrate = vi.hoisted(() => vi.fn(async () => undefined));
vi.mock("../../../../lib/celebrate", () => ({ celebrate }));

const json = (body: unknown, status = 200) =>
  new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json" } });

const MAPPING: DashboardMapping = {
  mapping_id: "m1",
  mapping_name: "Main",
  original_guild_id: "111",
  cloned_guild_id: "222",
  settings: {},
};

/** Answers the save with `save` and every other call (the user tokens list) with an empty ok. */
function mockApi(save: () => Response = () => json({ ok: true })) {
  const fn = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
    const url = String(input);
    const method = init?.method ?? "GET";
    if (url.startsWith("/api/guild-mappings") && !url.includes("/user-tokens") && method !== "GET") return save();
    return json({ ok: true, tokens: [] });
  });
  vi.stubGlobal("fetch", fn);
  return fn;
}

async function save(mode: MappingMode, mapping: DashboardMapping | null) {
  const user = userEvent.setup();
  const onClose = vi.fn();
  const onSaved = vi.fn();
  render(<MappingModal mapping={mapping} mode={mode} onClose={onClose} onSaved={onSaved} />);
  // The labels hold their tip text too, so the boxes are found by id.
  const box = (id: string) => document.getElementById(id) as HTMLInputElement;
  if (mode !== "edit") {
    await user.type(box("map_mapping_name"), "New");
    await user.type(box("map_original_guild_id"), "333");
    await user.type(box("map_cloned_guild_id"), "444");
  }
  await user.click(screen.getByRole("button", { name: "Save Mapping" }));
  return { onClose, onSaved };
}

describe("MappingModal confetti", () => {
  beforeEach(() => celebrate.mockClear());

  it.each([
    ["create", null],
    ["clone", MAPPING],
  ] as const)("a successful %s throws confetti once", async (mode, mapping) => {
    const fetch = mockApi();
    const { onClose } = await save(mode, mapping);
    await waitFor(() => expect(onClose).toHaveBeenCalled());
    expect(fetch).toHaveBeenCalledWith("/api/guild-mappings", expect.objectContaining({ method: "POST" }));
    expect(celebrate).toHaveBeenCalledTimes(1);
  });

  it("an edit throws none", async () => {
    const fetch = mockApi();
    const { onClose } = await save("edit", MAPPING);
    await waitFor(() => expect(onClose).toHaveBeenCalled());
    expect(fetch).toHaveBeenCalledWith("/api/guild-mappings/m1", expect.objectContaining({ method: "PATCH" }));
    expect(celebrate).not.toHaveBeenCalled();
  });

  it("a failed create throws none", async () => {
    mockApi(() => json({ detail: "Clone guild already mapped" }, 400));
    const { onClose, onSaved } = await save("create", null);
    await waitFor(() => expect(screen.getByRole("button", { name: "Save Mapping" })).toBeEnabled());
    expect(onSaved).not.toHaveBeenCalled();
    expect(onClose).not.toHaveBeenCalled();
    expect(celebrate).not.toHaveBeenCalled();
  });
});
