import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ToastProvider } from "../../components/Toast";
import { ConfirmProvider } from "../../components/ui/ConfirmDialog";
import { CSRF_HEADER } from "../../lib/api";
import type { GlobalConfig } from "../../types/dashboard-config";
import { Dashboard } from "../Dashboard";
import { holdFetch } from "../../test/holdFetch";
import { GlobalConfigSection } from "./GlobalConfigSection";
import {
  buildAddForm,
  buildSaveForm,
  inviteUrl,
  isDirty,
  loadConfig,
  missingTokens,
  tokensFromErrors,
} from "./config/model";

const CONFIG: GlobalConfig = {
  SERVER_TOKEN: "srv-tok",
  CLIENT_TOKEN: "cli-tok",
  COMMAND_USERS: ["111", "222"],
  COPYCORD_AUTOSTART: true,
  LOG_LEVEL: "DEBUG",
  LOG_MAX_SIZE_MB: "25",
};

type Handler = (init: RequestInit) => Response;

const json = (body: unknown, status = 200) =>
  new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json" } });

function server(routes: Record<string, Handler>) {
  const fetchMock = vi.fn(async (input: RequestInfo | URL, init: RequestInit = {}) => {
    const handler = routes[String(input)];
    return handler ? handler(init) : new Response("not found", { status: 404 });
  });
  vi.stubGlobal("fetch", fetchMock);
  return fetchMock;
}

const callsTo = (m: ReturnType<typeof server>, url: string) =>
  m.mock.calls.filter(([u]) => String(u) === url).map(([, init]) => init ?? {});

const baseRoutes = (): Record<string, Handler> => ({
  "/api/config": () =>
    json({
      SERVER_TOKEN: "srv-tok",
      CLIENT_TOKEN: "cli-tok",
      COMMAND_USERS: "111,222",
      COPYCORD_AUTOSTART: "true",
      LOG_LEVEL: "DEBUG",
      LOG_MAX_SIZE_MB: "25",
    }),
  "/api/validate-tokens": () => json({ ok: true, has_tokens: true, errors: [], bot_client_id: "42" }),
  "/api/status": () => json({ server: { running: false }, client: { running: false } }),
});

function renderSection() {
  return render(
    <ToastProvider>
      <ConfirmProvider>
        <GlobalConfigSection />
      </ConfirmProvider>
    </ToastProvider>,
  );
}

afterEach(() => vi.unstubAllGlobals());

describe("config model", () => {
  it("reads the saved values from GET /api/config", async () => {
    server(baseRoutes());
    expect(await loadConfig()).toEqual(CONFIG);
    server({ "/api/config": () => json({}) });
    expect(await loadConfig()).toEqual({
      SERVER_TOKEN: "",
      CLIENT_TOKEN: "",
      COMMAND_USERS: [],
      COPYCORD_AUTOSTART: false,
      LOG_LEVEL: "INFO",
      LOG_MAX_SIZE_MB: "10",
    });
  });

  it("builds the six form fields /save expects", () => {
    const fd = buildSaveForm(CONFIG);
    expect(Object.fromEntries(fd.entries())).toEqual({
      SERVER_TOKEN: "srv-tok",
      CLIENT_TOKEN: "cli-tok",
      COMMAND_USERS: "111,222",
      COPYCORD_AUTOSTART: "true",
      LOG_LEVEL: "DEBUG",
      LOG_MAX_SIZE_MB: "25",
    });
  });

  it("tracks dirty state, missing tokens and token errors", () => {
    expect(isDirty(CONFIG, { ...CONFIG })).toBe(false);
    expect(isDirty(CONFIG, { ...CONFIG, COMMAND_USERS: ["111"] })).toBe(true);
    expect(isDirty(CONFIG, { ...CONFIG, LOG_LEVEL: "INFO" })).toBe(true);
    expect(missingTokens({ ...CONFIG, CLIENT_TOKEN: "  " })).toEqual(["CLIENT_TOKEN"]);
    expect([...tokensFromErrors(["Invalid CLIENT_TOKEN", "bad SERVER_TOKEN"])].sort()).toEqual(["CLIENT_TOKEN", "SERVER_TOKEN"]);
    expect(inviteUrl("42")).toBe("https://discord.com/oauth2/authorize?client_id=42&permissions=8&integration_type=0&scope=bot");
    expect(inviteUrl(null)).toBeNull();
    expect(Object.fromEntries(buildAddForm("t", "n").entries())).toEqual({ token_value: "t", note: "n" });
  });
});

describe("Global Configuration card", () => {
  it("loads the saved values, hides Save until dirty and enables the invite link", async () => {
    server(baseRoutes());
    renderSection();
    await waitFor(() => expect(screen.getByLabelText("Server Token")).toHaveValue("srv-tok"));
    expect(screen.getByLabelText("Client Token")).toHaveValue("cli-tok");
    expect(screen.getByLabelText("Client Token")).toHaveAttribute("type", "password");
    expect(screen.getByRole("button", { name: "Remove 111" })).toBeInTheDocument();
    expect(screen.getByRole("switch", { name: "Auto Start" })).toBeChecked();
    expect(screen.getByLabelText("Log Level")).toHaveValue("DEBUG");
    expect(screen.getByLabelText("Max Log Size")).toHaveValue(25);
    expect(document.getElementById("cfg-save-btn")).toBeDisabled();
    expect(document.getElementById("cfg-cancel-btn")).toBeDisabled();
    await waitFor(() =>
      expect(document.getElementById("bot-invite-btn")).toHaveAttribute("href", expect.stringContaining("client_id=42")),
    );
  });

  it("shows and hides a token", async () => {
    server(baseRoutes());
    const user = userEvent.setup();
    renderSection();
    const input = await screen.findByLabelText("Server Token");
    await user.click(screen.getByRole("button", { name: "Show SERVER_TOKEN" }));
    expect(input).toHaveAttribute("type", "text");
    await user.click(screen.getByRole("button", { name: "Hide SERVER_TOKEN" }));
    expect(input).toHaveAttribute("type", "password");
  });

  it("edits chips and saves a CSRF form POST to /save, then goes clean", async () => {
    const fetchMock = server({ ...baseRoutes(), "/save": () => new Response("", { status: 200 }) });
    const user = userEvent.setup();
    renderSection();
    await screen.findByLabelText("Server Token");
    await user.type(screen.getByLabelText("Command Users"), "333{Enter}");
    await user.click(screen.getByRole("button", { name: "Remove 111" }));
    await user.click(screen.getByRole("switch", { name: "Auto Start" }));
    const save = document.getElementById("cfg-save-btn")!;
    await waitFor(() => expect(save).toBeEnabled());
    await user.click(save);
    await waitFor(() => expect(callsTo(fetchMock, "/save")).toHaveLength(1));
    const [post] = callsTo(fetchMock, "/save");
    expect(post!.method).toBe("POST");
    expect(new Headers(post!.headers).get(CSRF_HEADER)).toBe("1");
    expect(Object.fromEntries((post!.body as FormData).entries())).toMatchObject({
      COMMAND_USERS: "222,333",
      COPYCORD_AUTOSTART: "false",
      LOG_LEVEL: "DEBUG",
    });
    await waitFor(() => expect(save).toBeDisabled());
  });

  it("Cancel restores the loaded values", async () => {
    server(baseRoutes());
    const user = userEvent.setup();
    renderSection();
    const token = await screen.findByLabelText("Server Token");
    await user.clear(token);
    await user.type(token, "changed");
    await user.click(document.getElementById("cfg-cancel-btn")!);
    expect(token).toHaveValue("srv-tok");
    expect(document.getElementById("cfg-save-btn")).toBeDisabled();
  });

  it("keeps the form dirty and shows the server message when /save fails", async () => {
    server({ ...baseRoutes(), "/save": () => new Response("Invalid config:\n- bad CLIENT_TOKEN", { status: 400 }) });
    const user = userEvent.setup();
    renderSection();
    const token = await screen.findByLabelText("Client Token");
    await user.type(token, "x");
    const save = document.getElementById("cfg-save-btn")!;
    await waitFor(() => expect(save).toBeEnabled());
    await user.click(save);
    expect(await screen.findByText(/bad CLIENT_TOKEN/)).toBeInTheDocument();
    expect(save).toBeEnabled();
  });

  it("locks Save and Cancel while a bot is running", async () => {
    server({ ...baseRoutes(), "/api/status": () => json({ server: { running: true }, client: { running: false } }) });
    const user = userEvent.setup();
    renderSection();
    await user.type(await screen.findByLabelText("Server Token"), "x");
    const save = document.getElementById("cfg-save-btn")!;
    await waitFor(() => expect(save).toHaveAttribute("title", "Stop the bot to edit global configuration"));
    expect(save).toBeDisabled();
    expect(document.getElementById("cfg-cancel-btn")).toBeDisabled();
  });

  it("marks tokens the validation endpoint rejects and keeps the invite link off", async () => {
    server({
      ...baseRoutes(),
      "/api/validate-tokens": () => json({ ok: false, has_tokens: true, errors: ["CLIENT_TOKEN is invalid"] }),
    });
    renderSection();
    await waitFor(() => expect(screen.getByLabelText("Client Token")).toHaveAttribute("aria-invalid", "true"));
    expect(screen.getByLabelText("Server Token")).not.toHaveAttribute("aria-invalid");
    expect(document.getElementById("bot-invite-btn")).not.toHaveAttribute("href");
  });
});

describe("Backup tokens modal", () => {
  const LIST = {
    ok: true,
    tokens: [{ token_id: "t1", masked: "abcd…wxyz", note: "alt account", added_at: 1700000000, last_used: null }],
  };

  it("lists, adds with a note and removes after a confirm", async () => {
    let list = LIST;
    const fetchMock = server({
      ...baseRoutes(),
      "/api/backup-tokens": () => json(list),
      "/api/backup-tokens/add": () => {
        list = { ok: true, tokens: [...LIST.tokens, { token_id: "t2", masked: "••••", note: "", added_at: 1, last_used: null }] };
        return json({ ok: true, token_id: "t2" });
      },
      "/api/backup-tokens/delete": () => json({ ok: true }),
    });
    const user = userEvent.setup();
    render(
      <ToastProvider>
        <ConfirmProvider>
          <GlobalConfigSection />
        </ConfirmProvider>
      </ToastProvider>,
    );
    await screen.findByLabelText("Server Token");
    await user.click(document.getElementById("backupTokensBtn")!);
    const dialog = await screen.findByRole("dialog", { name: "Backup Tokens" });
    expect(await within(dialog).findByText("abcd…wxyz")).toBeInTheDocument();
    expect(within(dialog).getByText("alt account")).toBeInTheDocument();
    expect(within(dialog).getByText(/Last used: Never/)).toBeInTheDocument();

    await user.type(within(dialog).getByPlaceholderText("Paste a CLIENT_TOKEN"), "new-token");
    await user.type(within(dialog).getByLabelText("Backup token note"), "spare");
    await user.click(document.getElementById("backup-token-add")!);
    await waitFor(() => expect(callsTo(fetchMock, "/api/backup-tokens/add")).toHaveLength(1));
    const [add] = callsTo(fetchMock, "/api/backup-tokens/add");
    expect(new Headers(add!.headers).get(CSRF_HEADER)).toBe("1");
    expect(Object.fromEntries((add!.body as FormData).entries())).toEqual({ token_value: "new-token", note: "spare" });
    await waitFor(() => expect(within(dialog).getAllByRole("button", { name: "Remove backup token" })).toHaveLength(2));

    await user.click(within(dialog).getAllByRole("button", { name: "Remove backup token" })[0]!);
    await user.click(await screen.findByRole("button", { name: "Remove" }));
    await waitFor(() => expect(callsTo(fetchMock, "/api/backup-tokens/delete")).toHaveLength(1));
    expect(Object.fromEntries((callsTo(fetchMock, "/api/backup-tokens/delete")[0]!.body as FormData).entries())).toEqual({ token_id: "t1" });
  });

  it("does not post an empty token and shows the server's reason on a rejected one", async () => {
    const fetchMock = server({
      ...baseRoutes(),
      "/api/backup-tokens": () => json({ ok: true, tokens: [] }),
      "/api/backup-tokens/add": () => json({ detail: "Invalid backup token. It must be a valid Discord user token." }, 400),
    });
    const user = userEvent.setup();
    renderSection();
    await screen.findByLabelText("Server Token");
    await user.click(document.getElementById("backupTokensBtn")!);
    const dialog = await screen.findByRole("dialog", { name: "Backup Tokens" });
    expect(await within(dialog).findByText("No backup tokens saved yet.")).toBeInTheDocument();
    await user.click(document.getElementById("backup-token-add")!);
    expect(await screen.findByText("Paste a token first.")).toBeInTheDocument();
    expect(callsTo(fetchMock, "/api/backup-tokens/add")).toHaveLength(0);
    await user.type(within(dialog).getByPlaceholderText("Paste a CLIENT_TOKEN"), "bad");
    await user.click(document.getElementById("backup-token-add")!);
    expect(await screen.findByText(/must be a valid Discord user token/)).toBeInTheDocument();
  });

  it("closes on Escape", async () => {
    server({ ...baseRoutes(), "/api/backup-tokens": () => json({ ok: true, tokens: [] }) });
    const user = userEvent.setup();
    renderSection();
    await screen.findByLabelText("Server Token");
    await user.click(document.getElementById("backupTokensBtn")!);
    await screen.findByRole("dialog", { name: "Backup Tokens" });
    await user.keyboard("{Escape}");
    await waitFor(() => expect(screen.queryByRole("dialog")).toBeNull());
  });
});

describe("Dashboard link cards", () => {
  it("shows Star on GitHub, Join the Discord and Donate above the Bots card", async () => {
    server({ ...baseRoutes(), "/version": () => json({ current: "v4", latest: "v4", url: "", update_available: false }) });
    render(
      <ToastProvider>
        <ConfirmProvider>
          <Dashboard />
        </ConfirmProvider>
      </ToastProvider>,
    );
    const cards = screen.getByRole("region", { name: "Quick actions" });
    const links = within(cards).getAllByRole("link");
    expect(links.map((a) => a.getAttribute("href"))).toEqual([
      "https://github.com/Copycord/Copycord",
      "https://discord.gg/ArFdqrJHBj",
      "https://ko-fi.com/xmacj",
    ]);
    expect(within(cards).getByText("Star on GitHub")).toBeInTheDocument();
    expect(within(cards).getByText("Join the Discord")).toBeInTheDocument();
    expect(within(cards).getByText("Donate")).toBeInTheDocument();
    for (const a of links) expect(a).toHaveAttribute("target", "_blank");
    expect(cards.compareDocumentPosition(screen.getByRole("heading", { name: "Bots" })) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
    await screen.findByLabelText("Server Token");
  });
});

describe("Global Configuration loading", () => {
  it("shows a skeleton until the saved config arrives, then the form", async () => {
    server(baseRoutes());
    const release = holdFetch("/api/config");
    renderSection();
    expect(await screen.findByRole("status", { name: "Loading configuration" })).toBeInTheDocument();
    expect(document.getElementById("cfg-form")).toBeNull();
    release();
    await waitFor(() => expect(document.getElementById("cfg-form")).not.toBeNull());
    expect(screen.queryByRole("status", { name: "Loading configuration" })).toBeNull();
  });
});
