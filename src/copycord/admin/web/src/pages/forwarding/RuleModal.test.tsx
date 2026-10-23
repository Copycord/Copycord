import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { ToastProvider } from "../../components/Toast";
import { ConfirmProvider } from "../../components/ui/ConfirmDialog";
import { CSRF_HEADER } from "../../lib/api";
import type { ForwardingRule, ForwardingRulePayload } from "../../types/forwarding";
import { Forwarding } from "../Forwarding";
import {
  buildPayload,
  EMPTY_FORM,
  formFromRequest,
  formFromRule,
  GUILDS_RATE_LIMITED_MESSAGE,
  HEADER_MASK,
  localProblem,
  MAX_HTTP_HEADERS,
} from "./ruleForm";
import type { HeaderRow } from "./ruleForm";

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

describe("rule modal", () => {
  it("says Discord is rate limiting when the guild list is rate limited", async () => {
    const user = userEvent.setup();
    server({ "GET /api/client-guilds": () => jsonResponse({ detail: "discord-rate-limited" }, 429) });
    const dialog = await openNew(user);
    expect(await within(dialog).findByRole("alert")).toHaveTextContent(GUILDS_RATE_LIMITED_MESSAGE);
  });

  it("lists client guilds and creates a Discord rule without the validate call", async () => {
    const user = userEvent.setup();
    const fetchMock = server();
    const dialog = await openNew(user);

    // The guild scope is the app dropdown: the Field label names the combobox, which keeps the id.
    const guild = within(dialog).getByRole("combobox", { name: "Guild scope" });
    expect(guild).toHaveAttribute("id", "fwd_guild_id");
    expect(guild).toHaveAttribute("aria-describedby", "fwd_guild_id-help");
    expect(guild).toHaveDisplayValue(/All guilds/);
    expect(document.querySelector("select")).toBeNull();
    await user.click(guild);
    // The open list floats in the body, outside the dialog box, so the dialog never scrolls for it.
    await waitFor(() => expect(screen.getByRole("option", { name: "Beta (222)" })).toBeInTheDocument());
    expect(dialog).not.toContainElement(screen.getByRole("listbox"));
    expect(screen.getAllByRole("option").map((o) => o.textContent)).toEqual(["All guilds", "Alpha (111)", "Beta (222)"]);
    await user.click(screen.getByRole("option", { name: "Alpha (111)" }));
    expect(guild).toHaveDisplayValue(/Alpha \(111\)/);
    expect(dialog).toBeInTheDocument();

    await user.type(input("fwd_label"), "Support <b>pings</b>");
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
    await waitFor(() => expect(within(dialog).getByRole("combobox", { name: "Guild scope" })).toHaveDisplayValue(/Beta \(222\)/));
    expect(input("fwd_provider").value).toBe("pushover");
    // Provider secrets show in plain text inputs, unmasked.
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

// The list endpoint masks every header value; the made up secret below only ever lives in the URL.
const HTTP_RULE: ForwardingRule = {
  rule_id: "h1",
  label: "Webhook relay",
  guild_id: null,
  provider: "http",
  enabled: true,
  config: {
    url: "https://bot:FAKE-pass@hooks.example.com:8443/in/copycord?token=FAKE-query-secret#frag",
    headers: { Authorization: HEADER_MASK, "X-Api-Key": HEADER_MASK },
  },
  filters: {},
};

describe("HTTP / API rules", () => {
  it("creates an HTTP rule with headers, drops blank rows and skips the validate call", async () => {
    const user = userEvent.setup();
    const fetchMock = server();
    const dialog = await openNew(user);
    await user.type(input("fwd_label"), "Relay");
    await pickProvider(user, "HTTP / API");
    expect(input("fwd_provider").value).toBe("http");
    expect(within(dialog).getByText(/POSTs a JSON payload \(version 1\) to this URL/)).toBeInTheDocument();
    await user.type(input("http_url"), " https://api.example.com/hook ");
    await user.click(within(dialog).getByRole("button", { name: "Add header" }));
    await user.click(within(dialog).getByRole("button", { name: "Add header" }));
    await user.type(input("http_header_name_0"), "Authorization");
    await user.type(input("http_header_value_0"), "Bearer FAKE-token");
    expect(input("http_header_value_0")).toHaveAttribute("type", "password");
    await user.click(screen.getByRole("button", { name: "Save Rule" }));

    await waitFor(() => expect(calls(fetchMock, "POST /api/forwarding")).toHaveLength(1));
    expect(posted(fetchMock, "POST /api/forwarding")).toMatchObject({
      rule_id: null,
      provider: "http",
      config: { url: "https://api.example.com/hook", headers: { Authorization: "Bearer FAKE-token" } },
    });
    expect(calls(fetchMock, "POST /api/forwarding/validate")).toHaveLength(0);
  });

  it("keeps masked header values on edit unless one is retyped", async () => {
    const user = userEvent.setup();
    const fetchMock = server({ "GET /api/forwarding": () => jsonResponse({ ok: true, items: [HTTP_RULE] }) });
    renderPage();
    const card = await findCard("h1");
    await user.click(within(card).getByRole("button", { name: /edit/i }));
    await screen.findByRole("dialog", { name: "Edit Forwarding Rule" });

    expect(input("http_url").value).toBe(HTTP_RULE.config!.url);
    expect(input("http_header_name_0").value).toBe("Authorization");
    expect(input("http_header_name_1").value).toBe("X-Api-Key");
    // The mask never shows as a value; the input says the secret is saved.
    for (const id of ["http_header_value_0", "http_header_value_1"]) {
      expect(input(id).value).toBe("");
      expect(input(id)).toHaveAttribute("placeholder", "Saved (type to replace)");
    }
    await user.type(input("http_header_value_1"), "FAKE-new-key");
    await user.click(screen.getByRole("button", { name: "Save Rule" }));

    await waitFor(() => expect(calls(fetchMock, "POST /api/forwarding")).toHaveLength(1));
    const body = posted(fetchMock, "POST /api/forwarding");
    expect(body.rule_id).toBe("h1");
    expect(body.config).toEqual({
      url: HTTP_RULE.config!.url,
      headers: { Authorization: HEADER_MASK, "X-Api-Key": "FAKE-new-key" },
    });
  });

  it("stops on bad URLs and headers before any request", async () => {
    const user = userEvent.setup();
    const fetchMock = server();
    await openNew(user);
    await user.type(input("fwd_label"), "Relay");
    await pickProvider(user, "HTTP / API");
    const save = () => user.click(screen.getByRole("button", { name: "Save Rule" }));

    await save();
    expect(await screen.findByText("HTTP / API requires an endpoint URL.")).toBeInTheDocument();
    await user.type(input("http_url"), "ftp://example.com/x");
    await save();
    expect(await screen.findByText("The endpoint URL must start with http:// or https://.")).toBeInTheDocument();
    await user.clear(input("http_url"));
    await user.type(input("http_url"), "https://example.com/x");
    await user.click(screen.getByRole("button", { name: "Add header" }));
    await user.type(input("http_header_name_0"), "content-type");
    await user.type(input("http_header_value_0"), "text/plain");
    await save();
    expect(await screen.findByText("Content-Type is always application/json and cannot be set.")).toBeInTheDocument();
    expect(calls(fetchMock, "POST /api/forwarding")).toHaveLength(0);
    expect(calls(fetchMock, "POST /api/forwarding/validate")).toHaveLength(0);
  });

  it("shows only the endpoint host on the card", async () => {
    server({ "GET /api/forwarding": () => jsonResponse({ ok: true, items: [HTTP_RULE] }) });
    renderPage();
    const card = await findCard("h1");
    expect(within(card).getByText("HTTP / API")).toBeInTheDocument();
    expect(card.querySelector(".fwd-card-summary")).toHaveTextContent(/^hooks\.example\.com:8443$/);
    for (const hidden of ["FAKE-pass", "FAKE-query-secret", "/in/copycord", "frag", "Authorization", HEADER_MASK]) {
      expect(card.textContent).not.toContain(hidden);
    }
  });
});

describe("HTTP rule form", () => {
  const base = { ...EMPTY_FORM, label: "Relay", provider: "http", http_url: "https://example.com/hook" };

  it("round trips masked headers through the form and drops blank rows", () => {
    const form = formFromRule(HTTP_RULE);
    expect(form.http_headers).toEqual([
      { name: "Authorization", value: HEADER_MASK },
      { name: "X-Api-Key", value: HEADER_MASK },
    ]);
    const payload = buildPayload({ ...form, http_headers: [...form.http_headers, { name: " ", value: "" }] });
    expect(payload.config).toEqual({ url: HTTP_RULE.config!.url, headers: HTTP_RULE.config!.headers });
    expect(localProblem(payload, form.http_headers)).toBeNull();
    expect(buildPayload(base).config).toEqual({ url: "https://example.com/hook", headers: {} });
  });

  it("clears masked values on duplicate, since a new rule has nothing stored", () => {
    const form = formFromRequest({ mode: "duplicate", rule: HTTP_RULE });
    expect(form.rule_id).toBe("");
    expect(form.http_headers).toEqual([
      { name: "Authorization", value: "" },
      { name: "X-Api-Key", value: "" },
    ]);
    expect(localProblem(buildPayload(form), form.http_headers)).toBe("Header Authorization needs a value.");
  });

  it("checks header names", () => {
    const problem = (rows: HeaderRow[]) => localProblem(buildPayload({ ...base, http_headers: rows }), rows);
    expect(problem([{ name: "", value: "x" }])).toBe("Every header needs a name.");
    expect(problem([{ name: "Bad Name", value: "x" }])).toBe("Invalid header name: Bad Name");
    expect(problem([{ name: "Content-Type", value: "x" }])).toMatch(/Content-Type/);
    expect(
      problem([
        { name: "X-Key", value: "a" },
        { name: "x-key", value: "b" },
      ]),
    ).toBe("Header x-key is listed more than once.");
    const many = Array.from({ length: MAX_HTTP_HEADERS + 1 }, (_, i) => ({ name: `X-H${i}`, value: "v" }));
    expect(problem(many)).toBe(`At most ${MAX_HTTP_HEADERS} headers are allowed.`);
    expect(problem(many.slice(0, MAX_HTTP_HEADERS))).toBeNull();
  });
});
