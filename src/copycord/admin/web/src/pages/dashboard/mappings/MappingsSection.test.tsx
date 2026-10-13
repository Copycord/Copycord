import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ToastProvider } from "../../../components/Toast";
import { ConfirmProvider } from "../../../components/ui/ConfirmDialog";
import { CSRF_HEADER } from "../../../lib/api";
import type { DashboardMapping } from "../../../types/dashboard-mappings";
import { MappingsSection } from "../MappingsSection";
import { deleteBody, filterMappings, isPaused, statusToast } from "./model";
import * as openers from "./openers";

const A: DashboardMapping = {
  mapping_id: "m1",
  mapping_name: "Alpha",
  original_guild_id: "111",
  original_guild_name: "Host One",
  cloned_guild_id: "222",
  cloned_guild_name: "Clone One",
  status: "active",
};
const B: DashboardMapping = {
  mapping_id: "m2",
  mapping_name: "Beta",
  original_guild_id: "333",
  original_guild_name: "Host Two",
  cloned_guild_id: "444",
  cloned_guild_name: "Clone Two",
  status: "paused",
};

const json = (body: unknown, status = 200) =>
  new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json" } });

function server(handlers: Record<string, (init: RequestInit) => Response> = {}) {
  const mock = vi.fn(async (input: RequestInfo | URL, init: RequestInit = {}) => {
    const key = `${init.method ?? "GET"} ${String(input)}`;
    const h = key === "GET /api/guild-mappings" ? handlers[key] ?? (() => json({ ok: true, mappings: [A, B] })) : handlers[key];
    return h ? h(init) : new Response("not found", { status: 404 });
  });
  vi.stubGlobal("fetch", mock);
  return mock;
}

const callsTo = (mock: ReturnType<typeof server>, method: string, url: string) =>
  mock.mock.calls.filter(([u, i]) => String(u) === url && (i?.method ?? "GET") === method);

function renderIt(props: { locked?: boolean } = {}) {
  return render(
    <ToastProvider>
      <ConfirmProvider>
        <MappingsSection {...props} />
      </ConfirmProvider>
    </ToastProvider>,
  );
}

afterEach(() => vi.restoreAllMocks());

describe("mapping model", () => {
  it("searches names and ids case insensitively", () => {
    expect(filterMappings([A, B], "")).toEqual([A, B]);
    expect(filterMappings([A, B], "  ALPHA ")).toEqual([A]);
    expect(filterMappings([A, B], "clone two")).toEqual([B]);
    expect(filterMappings([A, B], "333")).toEqual([B]);
    expect(filterMappings([A, B], "zzz")).toEqual([]);
  });

  it("treats a missing status as active", () => {
    expect(isPaused({ ...A, status: undefined })).toBe(false);
    expect(isPaused({ ...A, status: "PAUSED" })).toBe(true);
  });

  it("words the delete body and status toast like the old page", () => {
    expect(deleteBody(A)).toBe("This will remove the mapping “Alpha”. This cannot be undone.");
    expect(deleteBody({ ...A, mapping_name: " " })).toBe("This will remove the mapping ID m1. This cannot be undone.");
    expect(statusToast("paused")).toMatch(/paused/);
    expect(statusToast("active")).toMatch(/resumed/);
  });
});

describe("MappingsSection", () => {
  it("lists the mappings, filters by search and clears on Escape", async () => {
    server();
    renderIt();
    expect(await screen.findByText("Alpha")).toBeTruthy();
    expect(screen.getByText("Beta")).toBeTruthy();
    const search = screen.getByLabelText("Search mappings");
    await userEvent.type(search, "beta");
    expect(screen.queryByText("Alpha")).toBeNull();
    await userEvent.keyboard("{Escape}");
    expect(await screen.findByText("Alpha")).toBeTruthy();
  });

  it("toggles status through POST toggle-status with the CSRF header", async () => {
    const mock = server({ "POST /api/guild-mappings/m1/toggle-status": () => json({ ok: true, status: "paused" }) });
    renderIt();
    await screen.findByText("Alpha");
    await userEvent.click(screen.getByRole("button", { name: "Pause cloning for this mapping" }));
    await waitFor(() => expect(document.querySelector('[data-id="m1"]')?.getAttribute("data-status")).toBe("paused"));
    const [[, init]] = callsTo(mock, "POST", "/api/guild-mappings/m1/toggle-status");
    expect(new Headers(init?.headers).get(CSRF_HEADER)).toBeTruthy();
  });

  it("deletes only after the confirm", async () => {
    const mock = server({ "DELETE /api/guild-mappings/m1": () => json({ ok: true }) });
    renderIt();
    await screen.findByText("Alpha");
    const del = () => document.querySelector('[data-id="m1"] .delete-mapping-btn') as HTMLElement;
    await userEvent.click(del());
    await userEvent.click(await screen.findByRole("button", { name: "Cancel" }));
    expect(callsTo(mock, "DELETE", "/api/guild-mappings/m1")).toHaveLength(0);
    await userEvent.click(del());
    await userEvent.click(await screen.findByRole("button", { name: "Delete mapping" }));
    await waitFor(() => expect(callsTo(mock, "DELETE", "/api/guild-mappings/m1")).toHaveLength(1));
    await waitFor(() => expect(callsTo(mock, "GET", "/api/guild-mappings")).toHaveLength(2));
  });

  it("hands Settings, Filters, Clone and Add to the openers", async () => {
    server();
    const modal = vi.spyOn(openers, "openMappingModal");
    const filters = vi.spyOn(openers, "openFilters");
    renderIt();
    await screen.findByText("Alpha");
    const card = document.querySelector('[data-id="m1"]') as HTMLElement;
    await userEvent.click(card.querySelector(".edit-mapping-btn") as HTMLElement);
    expect(modal).toHaveBeenLastCalledWith(A);
    await userEvent.click(card.querySelector(".mapping-filters-btn") as HTMLElement);
    expect(filters).toHaveBeenCalledWith(A);
    await userEvent.click(card.querySelector(".clone-mapping-btn") as HTMLElement);
    expect(modal).toHaveBeenLastCalledWith(A, { mode: "clone" });
    await userEvent.click(screen.getByRole("button", { name: "Add new mapping" }));
    expect(modal).toHaveBeenLastCalledWith();
  });

  it("locks the cards while a bot runs", async () => {
    server();
    renderIt({ locked: true });
    await screen.findByText("Alpha");
    expect(document.querySelectorAll("#guild-mapping-list .guild-card.locked").length).toBeGreaterThan(1);
  });
});
