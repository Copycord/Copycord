import { fireEvent, render, screen, within } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ToastProvider } from "../../../../components/Toast";
import { ConfirmProvider } from "../../../../components/ui/ConfirmDialog";
import { UserTokensBlock } from "./UserTokensBlock";

function renderBlock(strategy: string) {
  const setSetting = vi.fn();
  render(
    <ToastProvider>
      <ConfirmProvider>
        <UserTokensBlock mappingId={null} settings={{ USER_TOKEN_STRATEGY: strategy }} setSetting={setSetting} search="" />
      </ConfirmProvider>
    </ToastProvider>,
  );
  // The section starts collapsed, as in the mapping modal.
  document.getElementById("user-tokens-section")!.setAttribute("open", "");
  return setSetting;
}

describe("UserTokensBlock account selection", () => {
  it("is the app dropdown named by its label, keeps the id ut_strategy and saves the picked strategy", () => {
    const setSetting = renderBlock("round_robin");
    const select = screen.getByRole("combobox", { name: "Account selection" });
    expect(select).toHaveAttribute("id", "ut_strategy");
    expect(select).toHaveTextContent("Rotate evenly");
    expect(document.querySelector("select")).toBeNull();

    fireEvent.click(select);
    const list = screen.getByRole("listbox");
    expect(within(list).getAllByRole("option").map((o) => o.textContent)).toEqual(["Rotate evenly", "Sticky per author"]);
    fireEvent.click(within(list).getByRole("option", { name: "Sticky per author" }));
    expect(setSetting).toHaveBeenCalledWith("USER_TOKEN_STRATEGY", "sticky_author");
    expect(select).toHaveAttribute("aria-expanded", "false");
  });

  it("re-picking the current strategy changes nothing, like a native select", () => {
    const setSetting = renderBlock("sticky_author");
    const select = screen.getByRole("combobox", { name: "Account selection" });
    expect(select).toHaveTextContent("Sticky per author");
    fireEvent.click(select);
    fireEvent.click(within(screen.getByRole("listbox")).getByRole("option", { name: "Sticky per author" }));
    expect(setSetting).not.toHaveBeenCalled();
  });
});

const json = (body: unknown) => new Response(JSON.stringify(body), { status: 200, headers: { "content-type": "application/json" } });

const TOKENS = [
  { id: "t1", label: "", username: "alpha", user_id: "1", enabled: true, token_masked: "aaa***", use_count: 0, last_used: null },
  { id: "t2", label: "", username: "bravo", user_id: "2", enabled: true, token_masked: "bbb***", use_count: 0, last_used: null },
  { id: "t3", label: "", username: "charlie", user_id: "3", enabled: true, token_masked: "ccc***", use_count: 0, last_used: null },
  { id: "t4", label: "", username: "delta", user_id: "4", enabled: false, token_masked: "ddd***", use_count: 0, last_used: null },
];

const entry = (state: string, presence: string, dead = false) => ({ state, presence, since: 1, dead, session: "0123456789abcdef" });

const POOL = {
  keep_online: 3,
  live: 2,
  max_loop_lag_ms: 1.5,
  tokens: {
    t1: entry("online", "online"),
    t2: entry("online", "dnd"),
    t3: entry("connecting", "invisible"),
    t4: entry("online", "invisible", true),
  },
};

function mockApi(pool: unknown) {
  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input);
      if (url.includes("/user-tokens")) return json({ ok: true, tokens: TOKENS });
      if (url.includes("/api/status")) return json({ server: { running: true, token_pool: pool }, client: { running: true } });
      return json({ ok: false });
    }),
  );
}

function renderPool(settings: Record<string, string | number | boolean> = {}) {
  const setSetting = vi.fn();
  render(
    <ToastProvider>
      <ConfirmProvider>
        <UserTokensBlock mappingId="m1" settings={settings} setSetting={setSetting} search="" />
      </ConfirmProvider>
    </ToastProvider>,
  );
  return setSetting;
}

describe("UserTokensBlock warm pool", () => {
  afterEach(() => vi.unstubAllGlobals());

  it("keep online writes a whole number and clamps to the enabled token count", async () => {
    mockApi(POOL);
    const setSetting = renderPool({ USER_TOKEN_KEEP_ONLINE: 2 });
    await screen.findByText("alpha");
    const input = screen.getByLabelText("Keep online") as HTMLInputElement;
    expect(input.value).toBe("2");
    expect(input).toHaveAttribute("max", "3");
    fireEvent.change(input, { target: { value: "1" } });
    expect(setSetting).toHaveBeenLastCalledWith("USER_TOKEN_KEEP_ONLINE", 1);
    fireEvent.change(input, { target: { value: "9" } });
    expect(setSetting).toHaveBeenLastCalledWith("USER_TOKEN_KEEP_ONLINE", 3);
    fireEvent.change(input, { target: { value: "" } });
    expect(setSetting).toHaveBeenLastCalledWith("USER_TOKEN_KEEP_ONLINE", 0);
  });

  it("token status is the app dropdown and writes USER_TOKEN_PRESENCE", async () => {
    mockApi(POOL);
    const setSetting = renderPool();
    const select = screen.getByRole("combobox", { name: "Token status" });
    expect(select).toHaveTextContent("Random per session");
    fireEvent.click(select);
    const list = screen.getByRole("listbox");
    expect(within(list).getAllByRole("option").map((o) => o.textContent)).toEqual(["Random per session", "Online", "Busy", "Invisible"]);
    fireEvent.click(within(list).getByRole("option", { name: "Busy" }));
    expect(setSetting).toHaveBeenCalledWith("USER_TOKEN_PRESENCE", "busy");
    await screen.findByText("alpha");
  });

  it("counts this mapping's online tokens against min(keep online, enabled) and dots each chip", async () => {
    mockApi(POOL);
    renderPool({ USER_TOKEN_KEEP_ONLINE: 5 });
    expect(await screen.findByText("2/3 online")).toBeInTheDocument();
    const dotOf = (name: string) => screen.getByText(name).closest(".user-token-row")!.querySelector("[data-dot]")?.getAttribute("data-dot") ?? null;
    expect(dotOf("alpha")).toBe("online");
    expect(dotOf("bravo")).toBe("busy");
    expect(dotOf("charlie")).toBe("connecting");
    // Dead tokens get no dot but keep the slot.
    expect(dotOf("delta")).toBeNull();
    for (const row of document.querySelectorAll(".user-token-row")) expect(row.querySelectorAll(".user-token-dot").length).toBe(1);
    expect(screen.getByRole("img", { name: "Connecting" }).className).toContain("motion-safe:animate-pulse");
    expect(screen.getByRole("img", { name: "Online" }).className).not.toContain("animate");
  });

  it("shows off and no dots while the pool has not started, in the same reserved box", async () => {
    mockApi(null);
    renderPool({ USER_TOKEN_KEEP_ONLINE: 5 });
    await screen.findByText("alpha");
    const count = document.getElementById("ut_keep_online_count")!;
    await vi.waitFor(() => expect(count).toHaveTextContent("off"));
    expect(count.className).toContain("w-[76px]");
    expect(document.querySelectorAll("[data-dot]").length).toBe(0);
    expect(document.querySelectorAll(".user-token-dot").length).toBe(4);
  });
});
