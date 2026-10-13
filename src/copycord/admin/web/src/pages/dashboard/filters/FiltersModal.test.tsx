import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ToastProvider } from "../../../components/Toast";
import { CSRF_HEADER } from "../../../lib/api";
import { FiltersModal } from "./FiltersModal";

const json = (body: unknown, status = 200) =>
  new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json" } });

const MAPPING = {
  mapping_id: "m1",
  mapping_name: "Alpha",
  original_guild_id: "1",
  cloned_guild_id: "2",
};

function setup(opts: { failLoad?: boolean } = {}) {
  const calls: Array<{ url: string; method: string; body: unknown; csrf: string | null }> = [];
  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: RequestInfo | URL, init: RequestInit = {}) => {
      const url = String(input);
      calls.push({ url, method: init.method ?? "GET", body: init.body, csrf: new Headers(init.headers).get(CSRF_HEADER) });
      if (url === "/filters/m1/save") return json({ ok: true });
      if (url === "/filters/m1") return opts.failLoad ? json({}, 500) : json({ wl_channels: ["5"], blocked_role_ids: ["30"] });
      if (url === "/api/mappings/m1/roles")
        return json({ roles: [{ id: "30", name: "Mod" }, { id: "31", name: "Admin", color_hex: "#FF0000" }] });
      if (url === "/api/mappings/m1/channels")
        return json({ categories: [{ id: "1", name: "Cat" }], channels: [{ id: "5", name: "gen", parent_id: "1" }, { id: "6", name: "<b>x</b>", parent_id: "1" }] });
      return json({}, 404);
    }),
  );
  const onClosed = vi.fn();
  render(
    <ToastProvider>
      <FiltersModal mapping={MAPPING} onClosed={onClosed} />
    </ToastProvider>,
  );
  return { calls, onClosed };
}

afterEach(() => vi.unstubAllGlobals());

describe("FiltersModal", () => {
  it("loads the lists, saves them as a form with the CSRF header and closes", async () => {
    const user = userEvent.setup();
    const { calls, onClosed } = setup();
    expect(screen.getByRole("heading")).toHaveTextContent("Filters – Alpha");
    await screen.findByRole("button", { name: "Remove 5" });
    await user.type(document.getElementById("blocked_words_input")!, "spoiler{Enter}");
    await user.click(screen.getByRole("button", { name: "Save Filters" }));
    await waitFor(() => expect(onClosed).toHaveBeenCalled());
    const save = calls.find((c) => c.url === "/filters/m1/save")!;
    expect(save.method).toBe("POST");
    expect(save.csrf).toBeTruthy();
    const fd = save.body as FormData;
    expect(fd.get("wl_channels")).toBe("5");
    expect(fd.get("blocked_role_ids")).toBe("30");
    expect(fd.get("blocked_words")).toBe("spoiler");
  });

  it("disables Save when the load failed so nothing is overwritten", async () => {
    setup({ failLoad: true });
    expect(await screen.findByRole("alert")).toHaveTextContent("Failed to load filters");
    expect(screen.getByRole("button", { name: "Save Filters" })).toBeDisabled();
  });

  it("asks before discarding an edit and keeps the modal on Cancel", async () => {
    const user = userEvent.setup();
    const { onClosed } = setup();
    await screen.findByRole("button", { name: "Remove 5" });
    await user.type(document.getElementById("wl_users_input")!, "9{Enter}");
    await user.click(document.getElementById("filters-close")!);
    const dialog = await screen.findByRole("dialog", { name: "Discard unsaved filter changes?" });
    await user.click(within(dialog).getByRole("button", { name: "Cancel" }));
    expect(onClosed).not.toHaveBeenCalled();
    await user.click(document.getElementById("filters-close")!);
    await user.click(await screen.findByRole("button", { name: "Discard changes" }));
    expect(onClosed).toHaveBeenCalled();
  });

  it("applies the role picker selection and shows names as text", async () => {
    const user = userEvent.setup();
    setup();
    await screen.findByRole("button", { name: "Remove 30" });
    await user.click(document.getElementById("filters-role-refresh")!);
    const box = await screen.findByRole("checkbox", { name: "Block Admin" });
    expect(screen.getByRole("checkbox", { name: "Block Mod" })).toBeChecked();
    await user.click(box);
    await user.click(document.getElementById("roleBlocksApply")!);
    expect(await screen.findByRole("button", { name: "Remove 31" })).toBeInTheDocument();
    expect(document.getElementById("blocked_role_ids")).toHaveValue("30,31");
  });

  it("applies the channel picker selection and renders hostile names as text", async () => {
    const user = userEvent.setup();
    setup();
    await screen.findByRole("button", { name: "Remove 5" });
    await user.click(document.querySelector<HTMLElement>('[data-filter-target="wl_channels"]')!);
    expect(await screen.findByText("<b>x</b>")).toBeInTheDocument();
    expect(document.querySelector("#filterObjectsTableBody b")).toBeNull();
    await user.click(screen.getByRole("checkbox", { name: "Select <b>x</b>" }));
    await user.click(document.getElementById("filterObjectsApply")!);
    expect(document.getElementById("wl_channels")).toHaveValue("5,6");
    expect(document.getElementById("filterObjectsModal")).toBeNull();
  });
});
