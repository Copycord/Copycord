import { fireEvent, render, screen, within } from "@testing-library/react";
import { useState } from "react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ToastProvider } from "../../../../components/Toast";
import { ConfirmProvider } from "../../../../components/ui/ConfirmDialog";
import type { MappingSettings } from "../../../../types/dashboard-mappings";
import { UserTokensBlock } from "./UserTokensBlock";

function renderBlock(settings: MappingSettings) {
  const setSetting = vi.fn();
  render(
    <ToastProvider>
      <ConfirmProvider>
        <UserTokensBlock mappingId={null} settings={settings} setSetting={setSetting} search="" />
      </ConfirmProvider>
    </ToastProvider>,
  );
  // The section starts collapsed, as in the mapping modal.
  document.getElementById("user-tokens-section")!.setAttribute("open", "");
  return setSetting;
}

const STICKY_KEYS = ["user_token_sticky_nickname", "user_token_sticky_avatar", "user_token_sticky_roles"];
const stickyRows = () =>
  STICKY_KEYS.map((k) => document.querySelector(`#user-token-sticky-options [data-setting-key="${k}"]`));

describe("UserTokensBlock notices", () => {
  it("shows the exact user token warning", () => {
    renderBlock({});
    const warning = document.querySelector(".user-token-disclaimer")!;
    expect(warning.querySelector("strong")).toHaveTextContent("Warning:");
    expect(warning.textContent!.replace(/\s+/g, " ").trim()).toBe(
      "Warning: This feature uses user tokens. Discord may revoke your tokens or disable them. Only use accounts you are willing to lose.",
    );
  });

  it("has no proxies notice", () => {
    renderBlock({});
    expect(document.querySelector(".user-token-proxy-notice")).toBeNull();
    expect(screen.queryByText("Proxies:")).toBeNull();
  });

  it("points to the System page table in a card right after the warning, opening a new tab", () => {
    renderBlock({});
    const card = document.querySelector(".user-token-disclaimer")!.nextElementSibling as HTMLElement;
    expect(card).toHaveClass("user-token-system-notice");
    expect(card).toHaveTextContent("Token Message Cloning table");
    const link = within(card).getByRole("link", { name: "System page" });
    expect(link).toHaveAttribute("href", "/system");
    expect(link).toHaveAttribute("target", "_blank");
  });
});

describe("UserTokensBlock sticky per author", () => {
  it("has no account selection dropdown and always shows the three sticky options", () => {
    renderBlock({});
    expect(document.getElementById("ut_strategy")).toBeNull();
    expect(screen.queryByRole("combobox", { name: "Account selection" })).toBeNull();
    expect(screen.queryByText("Rotate evenly")).toBeNull();
    expect(screen.queryByText("Sticky per author")).toBeNull();
    expect(stickyRows().every((r) => r !== null)).toBe(true);
  });

  it("a stored round_robin value from an older version still shows the sticky options and is never written", () => {
    const setSetting = renderBlock({ USER_TOKEN_STRATEGY: "round_robin" });
    expect(stickyRows().every((r) => r !== null)).toBe(true);
    expect(screen.queryByText("Rotate evenly")).toBeNull();
    expect(setSetting.mock.calls.some(([k]) => k === "USER_TOKEN_STRATEGY")).toBe(false);
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

describe("UserTokensBlock Verify tokens", () => {
  afterEach(() => vi.unstubAllGlobals());

  const VERIFY_OK = { ok: true, total: 1, working: 1, broken: 0, results: [{ id: "t1", masked: "aaa***", username: "alpha", ok: true }] };

  function mockVerify() {
    const fetchMock = vi.fn(async (input: RequestInfo | URL, _init?: RequestInit) => {
      const url = String(input);
      if (url.includes("/verify")) return json(VERIFY_OK);
      if (url.includes("/user-tokens")) return json({ ok: true, tokens: TOKENS });
      return json({ ok: false });
    });
    vi.stubGlobal("fetch", fetchMock);
    return fetchMock;
  }

  const verifyCalls = (fetchMock: ReturnType<typeof mockVerify>) =>
    fetchMock.mock.calls.filter(([u]) => String(u).includes("/verify"));

  async function openVerify() {
    await screen.findByText("alpha");
    fireEvent.click(screen.getByRole("button", { name: "Token options" }));
    fireEvent.click(document.getElementById("user-token-verify")!);
    return screen.findByRole("dialog", { name: "Verify tokens" });
  }

  const radio = (dialog: HTMLElement, name: RegExp) => within(dialog).getByRole("radio", { name });

  it("asks first, preselecting Use proxies when the setting is on", async () => {
    const fetchMock = mockVerify();
    renderPool({ USER_TOKEN_USE_PROXIES: true });
    const dialog = await openVerify();
    expect(radio(dialog, /^Use proxies/)).toBeChecked();
    expect(radio(dialog, /^Use proxies/)).toHaveAccessibleName(/Recommended/);
    expect(radio(dialog, /^Don't use proxies/)).not.toBeChecked();
    expect(dialog).toHaveTextContent(
      "Checking without proxies sends the checks from this server's own IP address, which Discord can link across accounts, and is slower.",
    );
    expect(verifyCalls(fetchMock)).toHaveLength(0);
  });

  it("preselects Don't use proxies when the mapping's setting is off", async () => {
    mockVerify();
    renderPool({ USER_TOKEN_USE_PROXIES: false });
    const dialog = await openVerify();
    expect(radio(dialog, /^Don't use proxies/)).toBeChecked();
    expect(radio(dialog, /^Use proxies/)).not.toBeChecked();
  });

  it("preselects Don't use proxies when the mapping has no stored setting, still recommending proxies", async () => {
    mockVerify();
    renderPool({});
    const dialog = await openVerify();
    expect(radio(dialog, /^Don't use proxies/)).toBeChecked();
    expect(radio(dialog, /^Use proxies/)).not.toBeChecked();
    expect(radio(dialog, /^Use proxies/)).toHaveAccessibleName(/Recommended/);
    expect(dialog).toHaveTextContent("this server's own IP address, which Discord can link across accounts");
  });

  it("Cancel sends nothing", async () => {
    const fetchMock = mockVerify();
    renderPool();
    const dialog = await openVerify();
    fireEvent.click(within(dialog).getByRole("button", { name: "Cancel" }));
    expect(screen.queryByRole("dialog", { name: "Verify tokens" })).toBeNull();
    await new Promise((r) => setTimeout(r, 0));
    expect(verifyCalls(fetchMock)).toHaveLength(0);
  });

  it.each([
    [true, /^Use proxies/, false],
    [false, /^Don't use proxies/, true],
  ])("posts use_proxies %s for that choice", async (expected, choice, settingOn) => {
    const fetchMock = mockVerify();
    renderPool({ USER_TOKEN_USE_PROXIES: settingOn });
    const dialog = await openVerify();
    fireEvent.click(radio(dialog, choice));
    fireEvent.click(within(dialog).getByRole("button", { name: "Verify" }));
    await screen.findByRole("dialog", { name: "Token verification" });
    const calls = verifyCalls(fetchMock);
    expect(calls).toHaveLength(1);
    const init = calls[0]![1] as RequestInit;
    expect(init.method).toBe("POST");
    expect(JSON.parse(String(init.body))).toEqual({ use_proxies: expected });
  });
});

describe("UserTokensBlock Release all tokens", () => {
  afterEach(() => vi.unstubAllGlobals());

  function mockRelease(release: () => Response | Promise<Response>, tokens = TOKENS) {
    const fetchMock = vi.fn(async (input: RequestInfo | URL, _init?: RequestInit) => {
      const url = String(input);
      if (url.includes("/release")) return release();
      if (url.includes("/user-tokens")) return json({ ok: true, tokens });
      return json({ ok: false });
    });
    vi.stubGlobal("fetch", fetchMock);
    return fetchMock;
  }

  const releaseCalls = (fetchMock: ReturnType<typeof mockRelease>) =>
    fetchMock.mock.calls.filter(([u]) => String(u).includes("/release"));

  async function openRelease() {
    await screen.findByText("alpha");
    fireEvent.click(screen.getByRole("button", { name: "Token options" }));
    fireEvent.click(screen.getByRole("menuitem", { name: "Release all tokens" }));
    return screen.findByRole("dialog", { name: "Release all tokens?" });
  }

  it("lists Release all tokens between Verify tokens and Delete all tokens", async () => {
    mockRelease(() => json({ ok: true, released: 0, reset_applied: true }));
    renderPool();
    await screen.findByText("alpha");
    fireEvent.click(screen.getByRole("button", { name: "Token options" }));
    const items = screen.getAllByRole("menuitem").map((b) => b.textContent);
    expect(items).toEqual(["Verify tokens", "Release all tokens", "Delete all tokens"]);
    expect(screen.getByRole("menuitem", { name: "Release all tokens" })).toBeEnabled();
  });

  it("is not available when the mapping has no tokens", async () => {
    mockRelease(() => json({ ok: true, released: 0 }), []);
    renderPool();
    await new Promise((r) => setTimeout(r, 0));
    expect(screen.queryByRole("button", { name: "Token options" })).toBeNull();
    expect(screen.queryByRole("menuitem", { name: "Release all tokens" })).toBeNull();
  });

  it("asks first and Cancel sends nothing", async () => {
    const fetchMock = mockRelease(() => json({ ok: true, released: 2, reset_applied: true }));
    renderPool();
    const dialog = await openRelease();
    expect(dialog).toHaveTextContent("Every token is released from the author it is assigned to");
    expect(dialog).toHaveTextContent("nicknames and roles it applied in the clone server are reset");
    expect(dialog).toHaveTextContent("Tokens stay in this mapping and keep sending");
    fireEvent.click(within(dialog).getByRole("button", { name: "Cancel" }));
    await new Promise((r) => setTimeout(r, 0));
    expect(releaseCalls(fetchMock)).toHaveLength(0);
  });

  it("posts to the release path and shows the count", async () => {
    const fetchMock = mockRelease(() => json({ ok: true, released: 3, reset_applied: true }));
    renderPool();
    const dialog = await openRelease();
    fireEvent.click(within(dialog).getByRole("button", { name: "Release all" }));
    expect(await screen.findByText("Released 3 tokens.")).toBeInTheDocument();
    const calls = releaseCalls(fetchMock);
    expect(calls).toHaveLength(1);
    expect(String(calls[0]![0])).toContain("/api/guild-mappings/m1/user-tokens/release");
    expect((calls[0]![1] as RequestInit).method).toBe("POST");
  });

  it("says so when no tokens were assigned", async () => {
    mockRelease(() => json({ ok: true, released: 0, reset_applied: true }));
    renderPool();
    const dialog = await openRelease();
    fireEvent.click(within(dialog).getByRole("button", { name: "Release all" }));
    expect(await screen.findByText("No tokens were assigned.")).toBeInTheDocument();
  });

  it("explains that nicknames and roles were not reset when the server is not running", async () => {
    mockRelease(() => json({ ok: true, released: 1, reset_applied: false }));
    renderPool();
    const dialog = await openRelease();
    fireEvent.click(within(dialog).getByRole("button", { name: "Release all" }));
    expect(
      await screen.findByText(
        "Released 1 token. Nicknames and roles were not reset because the server is not running.",
      ),
    ).toBeInTheDocument();
  });

  it("shows an error toast when the release fails", async () => {
    mockRelease(() => json({ ok: false, error: "Mapping not found." }));
    renderPool();
    const dialog = await openRelease();
    fireEvent.click(within(dialog).getByRole("button", { name: "Release all" }));
    expect(await screen.findByText("Mapping not found.")).toBeInTheDocument();
  });
});

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

  it("shows the Send through proxies switch after Attachments as links, with no label under it", async () => {
    mockApi();
    renderPool({ USER_TOKEN_USE_PROXIES: true });
    await screen.findByText("alpha");
    const row = document.querySelector('[data-setting-key="user_token_use_proxies"]')!;
    expect(row).toHaveTextContent("Send through proxies");
    expect(row).toHaveTextContent("Give each account its own proxy from your proxy list.");
    const links = document.querySelector('[data-setting-key="user_token_links_only"]')!;
    expect(links.nextElementSibling).toBe(row);
    expect(document.getElementById("user-token-proxy-off-warning")).toBeNull();
    expect(row.nextElementSibling).toHaveTextContent(/^Send delay/);
    expect(document.getElementById("user-tokens-section")).not.toHaveTextContent(
      "Turning this off sends token traffic",
    );
  });

  it("the proxy switch is off when the mapping has no stored value", async () => {
    mockApi();
    renderPool({});
    await screen.findByText("alpha");
    expect(document.getElementById("map_USER_TOKEN_USE_PROXIES")).not.toBeChecked();
  });

  it("the proxy switch is on when the mapping stored it on", async () => {
    mockApi();
    renderPool({ USER_TOKEN_USE_PROXIES: true });
    await screen.findByText("alpha");
    expect(document.getElementById("map_USER_TOKEN_USE_PROXIES")).toBeChecked();
  });

  it("the proxy switch shows a stored off and posts the new value", async () => {
    mockApi();
    const setSetting = renderPool({ USER_TOKEN_USE_PROXIES: false });
    await screen.findByText("alpha");
    const toggle = document.getElementById("map_USER_TOKEN_USE_PROXIES")!;
    expect(toggle).not.toBeChecked();
    fireEvent.click(toggle);
    expect(setSetting).toHaveBeenCalledWith("USER_TOKEN_USE_PROXIES", true);
  });
});
