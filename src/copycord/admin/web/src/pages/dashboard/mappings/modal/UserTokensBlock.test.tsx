import { fireEvent, render, screen, within } from "@testing-library/react";
import { useState } from "react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ToastProvider } from "../../../../components/Toast";
import { ConfirmProvider } from "../../../../components/ui/ConfirmDialog";
import type { MappingSettings } from "../../../../types/dashboard-mappings";
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

describe("UserTokensBlock state chip", () => {
  const chipFor = (settings: MappingSettings) => {
    const { unmount } = render(
      <ToastProvider>
        <ConfirmProvider>
          <UserTokensBlock mappingId={null} settings={settings} setSetting={vi.fn()} search="" />
        </ConfirmProvider>
      </ToastProvider>,
    );
    const chip = document.querySelector<HTMLElement>("#user-tokens-section summary [data-section-chip]")!;
    const out = { text: chip.textContent, state: chip.getAttribute("data-section-chip"), cls: chip.className };
    unmount();
    return out;
  };

  it("shows Disabled on the far right of the section header when user tokens are off, and Enabled when on", () => {
    const off = chipFor({ USE_USER_TOKENS: false });
    expect(off).toMatchObject({ text: "Disabled", state: "off" });
    expect(off.cls).toContain("ml-auto");
    expect(off.cls).not.toContain("border");
    expect(chipFor({ USE_USER_TOKENS: true })).toMatchObject({ text: "Enabled", state: "on" });
  });
});

const json = (body: unknown) => new Response(JSON.stringify(body), { status: 200, headers: { "content-type": "application/json" } });

const TOKENS = [
  { id: "t1", label: "", username: "alpha", user_id: "1", enabled: true, token_masked: "aaa***", use_count: 0, last_used: null },
  { id: "t2", label: "", username: "bravo", user_id: "2", enabled: true, token_masked: "bbb***", use_count: 0, last_used: null },
  { id: "t3", label: "", username: "charlie", user_id: "3", enabled: true, token_masked: "ccc***", use_count: 0, last_used: null },
  { id: "t4", label: "", username: "delta", user_id: "4", enabled: false, token_masked: "ddd***", use_count: 0, last_used: null },
];

function mockApi() {
  const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
    const url = String(input);
    if (url.includes("/user-tokens")) return json({ ok: true, tokens: TOKENS });
    return json({ ok: false });
  });
  vi.stubGlobal("fetch", fetchMock);
  return fetchMock;
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

  it("has no keep online field: Token status then Rotate status follow the token list", async () => {
    mockApi();
    const setSetting = renderPool({ USER_TOKEN_KEEP_ONLINE: 2 });
    await screen.findByText("alpha");
    expect(document.getElementById("ut_keep_online")).toBeNull();
    expect(screen.queryByText("Keep online")).toBeNull();
    const pool = document.getElementById("user-token-pool")!;
    expect(pool.children).toHaveLength(2);
    expect(pool.children[0]).toHaveTextContent(/^Token status/);
    expect(pool.children[1]).toHaveAttribute("data-setting-key", "user_token_presence_rotate");
    expect(setSetting.mock.calls.some(([k]) => k === "USER_TOKEN_KEEP_ONLINE")).toBe(false);
  });

  it("token status is the app dropdown and writes USER_TOKEN_PRESENCE", async () => {
    mockApi();
    const setSetting = renderPool();
    const select = screen.getByRole("combobox", { name: "Token status" });
    expect(select).toHaveTextContent("Random");
    expect(document.getElementById("ut_presence_desc")).toHaveTextContent("Picks a random status for each account.");
    fireEvent.click(select);
    const list = screen.getByRole("listbox");
    expect(within(list).getAllByRole("option").map((o) => o.textContent)).toEqual(["Random", "Online", "Idle", "Busy"]);
    fireEvent.click(within(list).getByRole("option", { name: "Idle" }));
    expect(setSetting).toHaveBeenCalledWith("USER_TOKEN_PRESENCE", "idle");
    await screen.findByText("alpha");
  });

  it("a saved invisible status shows as Online and Invisible is not offered", async () => {
    mockApi();
    renderPool({ USER_TOKEN_PRESENCE: "invisible" });
    const select = screen.getByRole("combobox", { name: "Token status" });
    expect(select).toHaveTextContent("Online");
    expect(document.getElementById("ut_presence_desc")).toHaveTextContent("Every kept online account shows as Online.");
    expect(screen.getByRole("checkbox", { name: "Rotate status" })).toBeDisabled();
    fireEvent.click(select);
    expect(within(screen.getByRole("listbox")).queryByRole("option", { name: "Invisible" })).toBeNull();
    expect(screen.queryByText(/appears offline/)).toBeNull();
    await screen.findByText("alpha");
  });

  it("token status Idle shows Idle with its description", async () => {
    mockApi();
    renderPool({ USER_TOKEN_PRESENCE: "idle" });
    const select = screen.getByRole("combobox", { name: "Token status" });
    expect(select).toHaveTextContent("Idle");
    expect(document.getElementById("ut_presence_desc")).toHaveTextContent("Every kept online account shows as Idle.");
    await screen.findByText("alpha");
  });

  it("rotate status sits under token status with its description and writes USER_TOKEN_PRESENCE_ROTATE", async () => {
    mockApi();
    const setSetting = renderPool({ USER_TOKEN_PRESENCE: "random" });
    const toggle = screen.getByRole("checkbox", { name: "Rotate status" });
    expect(toggle).toHaveAttribute("id", "map_USER_TOKEN_PRESENCE_ROTATE");
    expect(toggle).toBeEnabled();
    expect(toggle).not.toBeChecked();
    const row = document.querySelector('[data-setting-key="user_token_presence_rotate"]')!;
    expect(row).toHaveTextContent("Change each account's status every 20 to 90 minutes.");
    expect(row.previousElementSibling).toHaveTextContent(/^Token status/);
    fireEvent.click(toggle);
    expect(setSetting).toHaveBeenLastCalledWith("USER_TOKEN_PRESENCE_ROTATE", true);
    await screen.findByText("alpha");
  });

  it("rotate status is on and enabled for Random when saved on", async () => {
    mockApi();
    const setSetting = renderPool({ USER_TOKEN_PRESENCE: "random", USER_TOKEN_PRESENCE_ROTATE: true });
    const toggle = screen.getByRole("checkbox", { name: "Rotate status" });
    expect(toggle).toBeEnabled();
    expect(toggle).toBeChecked();
    fireEvent.click(toggle);
    expect(setSetting).toHaveBeenLastCalledWith("USER_TOKEN_PRESENCE_ROTATE", false);
    await screen.findByText("alpha");
  });

  it.each(["online", "idle", "busy"])("rotate status is shown but disabled and off for %s", async (presence) => {
    mockApi();
    renderPool({ USER_TOKEN_PRESENCE: presence, USER_TOKEN_PRESENCE_ROTATE: true });
    const toggle = screen.getByRole("checkbox", { name: "Rotate status" });
    expect(toggle).toBeInTheDocument();
    expect(toggle).toBeDisabled();
    expect(toggle).not.toBeChecked();
    expect(document.querySelector('[data-setting-key="user_token_presence_rotate"]')).toHaveClass("is-disabled");
    await screen.findByText("alpha");
  });

  it("the saved rotate value survives a round trip through a fixed status", async () => {
    mockApi();
    const spy = vi.fn();
    function Harness() {
      const [settings, setSettings] = useState<MappingSettings>({ USER_TOKEN_PRESENCE: "random", USER_TOKEN_PRESENCE_ROTATE: true });
      return (
        <UserTokensBlock
          mappingId="m1"
          settings={settings}
          setSetting={(k, v) => {
            spy(k, v);
            setSettings((s) => ({ ...s, [k]: v }));
          }}
          search=""
        />
      );
    }
    render(
      <ToastProvider>
        <ConfirmProvider>
          <Harness />
        </ConfirmProvider>
      </ToastProvider>,
    );
    const toggle = () => screen.getByRole("checkbox", { name: "Rotate status" });
    const pick = (name: string) => {
      fireEvent.click(screen.getByRole("combobox", { name: "Token status" }));
      fireEvent.click(within(screen.getByRole("listbox")).getByRole("option", { name }));
    };
    expect(toggle()).toBeChecked();
    pick("Busy");
    expect(toggle()).toBeDisabled();
    expect(toggle()).not.toBeChecked();
    expect(document.getElementById("ut_presence_desc")).toHaveTextContent("Every kept online account shows as Busy (Do Not Disturb).");
    pick("Random");
    expect(toggle()).toBeEnabled();
    expect(toggle()).toBeChecked();
    expect(spy.mock.calls.map(([k]) => k)).toEqual(["USER_TOKEN_PRESENCE", "USER_TOKEN_PRESENCE"]);
    await screen.findByText("alpha");
  });

  it("shows no status dots or online count and never polls the pool; live status is on the System page", async () => {
    const fetchMock = mockApi();
    renderPool();
    await screen.findByText("alpha");
    expect(document.querySelectorAll("[data-dot], .user-token-dot").length).toBe(0);
    expect(screen.queryByText(/\d+\/\d+ online|^off$/)).toBeNull();
    expect(fetchMock.mock.calls.some(([u]) => String(u).includes("/api/status"))).toBe(false);
  });

  it("has no proxy switch: token cloning always sends through proxies", async () => {
    mockApi();
    renderPool({ USER_TOKEN_USE_PROXIES: true });
    await screen.findByText("alpha");
    expect(document.getElementById("map_USER_TOKEN_USE_PROXIES")).toBeNull();
    expect(document.querySelector('[data-setting-key="user_token_use_proxies"]')).toBeNull();
    expect(screen.queryByText(/prox(y|ies)/i)).toBeNull();
    // Attachments as links is followed straight by the send delay row, no gap or label between.
    const links = document.querySelector('[data-setting-key="user_token_links_only"]')!;
    expect(links.nextElementSibling).toHaveTextContent(/^Send delay/);
  });
});
