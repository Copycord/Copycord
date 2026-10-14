import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { ToastProvider } from "../../components/Toast";
import { ConfirmProvider } from "../../components/ui/ConfirmDialog";
import { CSRF_HEADER } from "../../lib/api";
import type { ForwardingRule, ForwardingRulePayload } from "../../types/forwarding";
import { Forwarding } from "../Forwarding";
import { buildPayload, EMPTY_FORM, formFromRequest, formFromRule, localProblem } from "./ruleForm";

// Made up secrets; every request below is answered by the fetch stub.
const PUSH_RULE: ForwardingRule = {
  rule_id: "r1",
  label: "Phone alerts",
  guild_id: "222",
  provider: "pushover",
  enabled: true,
  config: { app_token: "FAKE-app-token-123", user_key: "FAKE-user-key-456" },
  filters: { keywords_any: ["urgent", "down"], channel_ids: "10, 11", include_bots: true },
};
const HOOK = "https://discord.com/api/webhooks/123456789012345678/FAKE-webhook-token-abcdefghijklmnopqrstuvwxyz-0123456789-abcdefghijklmnopqrstuvwxyz";

function jsonResponse(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json" } });
}

type Handler = (init: RequestInit) => Response;

function server(extra: Record<string, Handler> = {}) {
  const routes: Record<string, Handler> = {
    "GET /api/forwarding": () => jsonResponse({ ok: true, items: [PUSH_RULE] }),
    "GET /api/forwarding/count": () => jsonResponse({ ok: true, count: 3 }),
    "GET /api/forwarding/count/by-rule": () => jsonResponse({ ok: true, by_rule: { r1: 3 } }),
    "GET /api/client-guilds": () => jsonResponse({ items: [{ id: "111", name: "Alpha" }, { id: "222", name: "Beta" }] }),
    "POST /api/forwarding": (init) => {
      const body = JSON.parse(String(init.body)) as ForwardingRulePayload;
      return jsonResponse({ ok: true, rule_id: body.rule_id ?? "new1", guild_id: body.guild_id ?? "" });
    },
    "POST /api/forwarding/validate": () => jsonResponse({ ok: true }),
    ...extra,
  };
  const fetchMock = vi.fn(async (input: RequestInfo | URL, init: RequestInit = {}) => {
    const handler = routes[`${init.method ?? "GET"} ${String(input)}`];
    return handler ? handler(init) : new Response("not found", { status: 404 });
  });
  vi.stubGlobal("fetch", fetchMock);
  return fetchMock;
}

function calls(fetchMock: ReturnType<typeof server>, key: string) {
  return fetchMock.mock.calls.filter(([url, init]) => `${init?.method ?? "GET"} ${String(url)}` === key);
}

function posted(fetchMock: ReturnType<typeof server>, key: string): ForwardingRulePayload {
  const list = calls(fetchMock, key);
  expect(list).toHaveLength(1);
  const init = list[0]![1]!;
  expect(new Headers(init.headers).get(CSRF_HEADER)).toBe("1");
  return JSON.parse(String(init.body)) as ForwardingRulePayload;
}

function renderPage() {
  return render(
    <ToastProvider>
      <ConfirmProvider>
        <Forwarding />
      </ConfirmProvider>
    </ToastProvider>,
  );
}

function findCard(id: string): Promise<HTMLElement> {
  return waitFor(() => {
    const el = document.querySelector<HTMLElement>(`.fwd-card[data-id="${id}"]`);
    if (!el) throw new Error(`card ${id} not rendered yet`);
    return el;
  });
}

function input(id: string): HTMLInputElement {
  return document.getElementById(id) as HTMLInputElement;
}

async function openNew(user: ReturnType<typeof userEvent.setup>) {
  renderPage();
  await user.click(await screen.findByRole("button", { name: "New Forward" }));
  const dialog = await screen.findByRole("dialog", { name: "New Forwarding Rule" });
  expect(document.getElementById("fwdModal")).toHaveClass("show");
  return dialog;
}

async function pickProvider(user: ReturnType<typeof userEvent.setup>, name: string) {
  await user.click(document.getElementById("providerSelectBtn")!);
  await user.click(screen.getByRole("option", { name }));
}

describe("rule payload", () => {
  const base = { ...EMPTY_FORM, label: " Rule ", keywords_any: ["a", " "], channel_ids: ["5"] };

  it("builds the Pushover config", () => {
    const p = buildPayload({ ...base, provider: "pushover", pushover_app_token: " tok ", pushover_user_key: "key", telegram_bot_token: "ignored" });
    expect(p).toMatchObject({ rule_id: null, guild_id: null, label: "Rule", provider: "pushover", enabled: true });
    expect(p.config).toEqual({ app_token: "tok", user_key: "key" });
    expect(p.filters).toEqual({
      keywords_any: ["a"],
      keywords_all: [],
      channel_ids: ["5"],
      user_ids: [],
      case_sensitive: false,
      include_embeds: false,
      include_bots: false,
      include_system: false,
      has_attachments: false,
    });
  });

  it("builds the Discord config, username and avatar only when set", () => {
    expect(buildPayload({ ...base, provider: "discord", discord_urls: [HOOK] }).config).toEqual({ urls: [HOOK] });
    expect(
      buildPayload({ ...base, provider: "discord", discord_urls: [HOOK], discord_username: "Bot", discord_avatar_url: "https://x/a.png" }).config,
    ).toEqual({ urls: [HOOK], username: "Bot", avatar_url: "https://x/a.png" });
  });

  it("builds the Telegram config", () => {
    expect(buildPayload({ ...base, provider: "telegram", telegram_bot_token: "1:AB", telegram_chat_id: "-100" }).config).toEqual({
      bot_token: "1:AB",
      chat_id: "-100",
    });
  });

  it("runs the old local checks", () => {
    expect(localProblem(buildPayload({ ...base, provider: "" }))).toBe("Please choose a provider.");
    expect(localProblem(buildPayload({ ...base, provider: "telegram", telegram_bot_token: "x" }))).toMatch(/Telegram requires/);
    expect(localProblem(buildPayload({ ...base, provider: "pushover", pushover_app_token: "x" }))).toMatch(/Pushover requires/);
    expect(localProblem(buildPayload({ ...base, provider: "discord" }))).toMatch(/at least one webhook/);
    expect(localProblem(buildPayload({ ...base, provider: "discord", discord_urls: ["https://example.com/x"] }))).toMatch(/Invalid Discord/);
    expect(localProblem(buildPayload({ ...base, provider: "discord", discord_urls: [HOOK] }))).toBeNull();
  });

  it("prefills edit as stored and duplicates without the id", () => {
    const form = formFromRule({ ...PUSH_RULE, provider: "discord", config: { url: HOOK, bot_username: "Old" } });
    expect(form.discord_urls).toEqual([HOOK]);
    expect(form.discord_username).toBe("Old");
    expect(form.channel_ids).toEqual(["10", "11"]);
    const dup = formFromRequest({ mode: "duplicate", rule: PUSH_RULE });
    expect(dup.rule_id).toBe("");
    expect(dup.label).toBe("Phone alerts (Copy)");
    expect(buildPayload(dup).rule_id).toBeNull();
  });
});

describe("rule modal", () => {
  it("lists client guilds and creates a Discord rule without the validate call", async () => {
    const user = userEvent.setup();
    const fetchMock = server();
    const dialog = await openNew(user);

    const guild = input("fwd_guild_id") as unknown as HTMLSelectElement;
    await waitFor(() => expect(within(guild).getByRole("option", { name: "Beta (222)" })).toBeInTheDocument());
    expect(within(guild).getAllByRole("option").map((o) => o.textContent)).toEqual(["All guilds", "Alpha (111)", "Beta (222)"]);

    await user.type(input("fwd_label"), "Support <b>pings</b>");
    await user.selectOptions(guild, "111");
    expect(document.getElementById("provider_discord")).toBeNull();
    await pickProvider(user, "Discord");
    expect(input("fwd_provider").value).toBe("discord");
    await user.type(input("discord_webhook_urls"), `${HOOK}{Enter}`);
    expect(within(dialog).getByRole("button", { name: `Remove ${HOOK}` })).toBeInTheDocument();
    await user.type(input("fwd_keywords_any"), "urgent{Enter}");
    await user.type(input("fwd_users"), "42{Enter}");
    await user.click(screen.getByText("More filters"));
    await user.click(input("fwd_bot_messages"));
    await user.click(input("fwd_enabled"));
    await user.click(screen.getByRole("button", { name: "Save Rule" }));

    await waitFor(() => expect(calls(fetchMock, "POST /api/forwarding")).toHaveLength(1));
    const body = posted(fetchMock, "POST /api/forwarding");
    expect(body).toEqual({
      rule_id: null,
      guild_id: "111",
      label: "Support <b>pings</b>",
      provider: "discord",
      enabled: false,
      config: { urls: [HOOK] },
      filters: {
        keywords_any: ["urgent"],
        keywords_all: [],
        channel_ids: [],
        user_ids: ["42"],
        case_sensitive: false,
        include_embeds: false,
        include_bots: true,
        include_system: false,
        has_attachments: false,
      },
    });
    expect(calls(fetchMock, "POST /api/forwarding/validate")).toHaveLength(0);
    expect(await screen.findByText("Forwarding rule saved.")).toBeInTheDocument();
    await waitFor(() => expect(screen.queryByRole("dialog", { name: "New Forwarding Rule" })).toBeNull());
    expect(document.getElementById("fwdModal")).not.toHaveClass("show");
    // Only the id came back, so the list is fetched again.
    expect(calls(fetchMock, "GET /api/forwarding").length).toBeGreaterThan(1);
  });

  it("validates Telegram with the server before saving", async () => {
    const user = userEvent.setup();
    const fetchMock = server();
    await openNew(user);
    await user.type(input("fwd_label"), "TG");
    await pickProvider(user, "Telegram");
    await user.type(input("telegram_bot_token"), "123:FAKE");
    await user.type(input("telegram_chat_id"), "-100");
    await user.click(screen.getByRole("button", { name: "Save Rule" }));

    await waitFor(() => expect(calls(fetchMock, "POST /api/forwarding")).toHaveLength(1));
    expect(posted(fetchMock, "POST /api/forwarding/validate")).toEqual({
      provider: "telegram",
      config: { bot_token: "123:FAKE", chat_id: "-100" },
    });
    const order = fetchMock.mock.calls.map(([url, init]) => `${init?.method ?? "GET"} ${String(url)}`);
    expect(order.indexOf("POST /api/forwarding/validate")).toBeLessThan(order.indexOf("POST /api/forwarding"));
    expect(posted(fetchMock, "POST /api/forwarding").config).toEqual({ bot_token: "123:FAKE", chat_id: "-100" });
  });

  it("shows the validate errors and keeps the modal open without saving", async () => {
    const user = userEvent.setup();
    const fetchMock = server({
      "POST /api/forwarding/validate": () => jsonResponse({ ok: false, errors: ["Pushover rejected the app token."] }, 400),
    });
    await openNew(user);
    await user.type(input("fwd_label"), "Push");
    await pickProvider(user, "Pushover");
    await user.type(input("pushover_app_token"), "bad");
    await user.type(input("pushover_user_key"), "key");
    await user.click(screen.getByRole("button", { name: "Save Rule" }));

    expect(await screen.findByText("Pushover rejected the app token.")).toBeInTheDocument();
    expect(calls(fetchMock, "POST /api/forwarding")).toHaveLength(0);
    expect(screen.getByRole("dialog", { name: "New Forwarding Rule" })).toBeInTheDocument();
  });

  it("stops on the local checks before any request", async () => {
    const user = userEvent.setup();
    const fetchMock = server();
    await openNew(user);
    await user.type(input("fwd_label"), "Bad hook");
    await pickProvider(user, "Discord");
    await user.type(input("discord_webhook_urls"), "https://example.com/hook{Enter}");
    await user.click(screen.getByRole("button", { name: "Save Rule" }));
    expect(await screen.findByText(/Invalid Discord webhook URL/)).toBeInTheDocument();
    expect(calls(fetchMock, "POST /api/forwarding")).toHaveLength(0);
    expect(calls(fetchMock, "POST /api/forwarding/validate")).toHaveLength(0);
  });

  it("prefills edit with the stored secrets as the old page did and saves under the same id", async () => {
    const user = userEvent.setup();
    const fetchMock = server({
      "POST /api/forwarding": (init) => {
        const body = JSON.parse(String(init.body)) as ForwardingRulePayload;
        return jsonResponse({ ok: true, rule_id: "r1", guild_id: "222", item: { ...PUSH_RULE, label: body.label } });
      },
    });
    renderPage();
    const card = await findCard("r1");
    await user.click(within(card).getByRole("button", { name: /edit/i }));
    const dialog = await screen.findByRole("dialog", { name: "Edit Forwarding Rule" });

    expect(input("fwd_id").value).toBe("r1");
    expect(input("fwd_label").value).toBe("Phone alerts");
    await waitFor(() => expect((input("fwd_guild_id") as unknown as HTMLSelectElement).value).toBe("222"));
    expect(input("fwd_provider").value).toBe("pushover");
    // The old modal shows provider secrets in plain text inputs, unmasked.
    expect(input("pushover_app_token")).toHaveAttribute("type", "text");
    expect(input("pushover_app_token").value).toBe("FAKE-app-token-123");
    expect(input("pushover_user_key").value).toBe("FAKE-user-key-456");
    expect(within(dialog).getByRole("button", { name: "Remove urgent" })).toBeInTheDocument();
    expect(within(dialog).getByRole("button", { name: "Remove 11" })).toBeInTheDocument();
    expect(document.getElementById("fwdFiltersMore")).toHaveAttribute("open");
    expect(input("fwd_bot_messages")).toBeChecked();
    expect(input("fwd_enabled")).toBeChecked();

    await user.clear(input("fwd_label"));
    await user.type(input("fwd_label"), "Renamed");
    await user.click(screen.getByRole("button", { name: "Save Rule" }));
    await waitFor(() => expect(calls(fetchMock, "POST /api/forwarding")).toHaveLength(1));
    const body = posted(fetchMock, "POST /api/forwarding");
    expect(body).toMatchObject({
      rule_id: "r1",
      guild_id: "222",
      label: "Renamed",
      provider: "pushover",
      config: { app_token: "FAKE-app-token-123", user_key: "FAKE-user-key-456" },
    });
    expect(body.filters).toMatchObject({ keywords_any: ["urgent", "down"], channel_ids: ["10", "11"], include_bots: true });
    expect(posted(fetchMock, "POST /api/forwarding/validate").provider).toBe("pushover");
    expect(await screen.findByText("Renamed")).toBeInTheDocument();
  });

  it("opens duplicate with the copy label and closes on Cancel and Escape", async () => {
    const user = userEvent.setup();
    server();
    renderPage();
    const card = await findCard("r1");
    await user.click(within(card).getByRole("button", { name: /duplicate/i }));
    await screen.findByRole("dialog", { name: "Duplicate Forwarding Rule" });
    expect(input("fwd_label").value).toBe("Phone alerts (Copy)");
    expect(input("fwd_id").value).toBe("");
    await user.click(screen.getByRole("button", { name: "Cancel" }));
    expect(screen.queryByRole("dialog")).toBeNull();

    await user.click(screen.getByRole("button", { name: "New Forward" }));
    await screen.findByRole("dialog", { name: "New Forwarding Rule" });
    expect(input("fwd_label").value).toBe("");
    expect(input("pushover_app_token")).toBeNull();
    await user.keyboard("{Escape}");
    expect(screen.queryByRole("dialog")).toBeNull();
  });
});
