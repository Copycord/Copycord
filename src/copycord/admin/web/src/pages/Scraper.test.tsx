import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { ToastProvider } from "../components/Toast";
import { ConfirmProvider } from "../components/ui/ConfirmDialog";
import { CSRF_HEADER } from "../lib/api";
import type { ScraperToken } from "../types/scraper";
import { holdFetch } from "../test/holdFetch";
import { Scraper } from "./Scraper";
import { displayMask, maskToken } from "./scraper/mask";
import { FakeSource, FakeWs } from "./scraper/testFakes";

// Made up test values, not Discord tokens.
const RAW = "FAKE.test-token-value.abcdefghijklmnop.WXYZ";
const MASKED = maskToken(RAW);

const TOKENS: ScraperToken[] = [
  { token_id: "t1", label: "Main", masked: "MTAxMj…ab12", is_valid: true, username: "scrapey" },
  { token_id: "t2", label: "Spare", masked: "NzQ5ND…cd34", is_valid: false, username: null },
];

function jsonResponse(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json" } });
}

type Handler = (init: RequestInit) => Response;

function server(routes: Record<string, Handler>) {
  const fetchMock = vi.fn(async (input: RequestInfo | URL, init: RequestInit = {}) => {
    const method = (init.method ?? "GET").toUpperCase();
    const handler = routes[`${method} ${String(input)}`];
    return handler ? handler(init) : new Response("not found", { status: 404 });
  });
  vi.stubGlobal("fetch", fetchMock);
  return fetchMock;
}

function callsTo(fetchMock: ReturnType<typeof server>, method: string, url: string): RequestInit[] {
  return fetchMock.mock.calls
    .filter(([u, init]) => String(u) === url && ((init?.method ?? "GET").toUpperCase() === method))
    .map(([, init]) => init ?? {});
}

function baseRoutes(tokens: ScraperToken[] = TOKENS, proxies: string[] = []): Record<string, Handler> {
  return {
    "GET /api/scraper/tokens": () => jsonResponse({ ok: true, tokens }),
    "GET /api/scraper/status": () => jsonResponse({ running: false, queue: [] }),
    "GET /api/scraper/proxies": () => jsonResponse({ ok: true, proxies }),
  };
}

function renderPage() {
  return render(
    <ToastProvider>
      <ConfirmProvider>
        <Scraper />
      </ConfirmProvider>
    </ToastProvider>,
  );
}

/** The token row once the list has loaded. */
function findRow(id: string): Promise<HTMLElement> {
  return waitFor(() => {
    const row = document.querySelector<HTMLElement>(`.sc-token-item[data-id="${id}"]`);
    if (!row) throw new Error(`no row ${id}`);
    return row;
  });
}

// The live cards open /ws/out and /logs/stream/scraper; keep them off the network.
beforeEach(() => {
  vi.stubGlobal("WebSocket", FakeWs);
  vi.stubGlobal("EventSource", FakeSource);
});

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("token masking", () => {
  it("matches app.py _mask_token", () => {
    expect(maskToken("")).toBe("");
    expect(maskToken("abcdefghij")).toBe("ab…");
    expect(maskToken("abcdefghijk")).toBe("abcdef…hijk");
    expect(MASKED).toBe("FAKE.t…WXYZ");
  });

  it("keeps a server mask and masks anything that is not one", () => {
    expect(displayMask("MTAxMj…ab12")).toBe("MTAxMj…ab12");
    expect(displayMask(RAW)).toBe(MASKED);
  });
});

describe("Scraper tokens card", () => {
  it("lists tokens by name with their masked values and the count", async () => {
    server(baseRoutes());
    renderPage();
    const list = document.getElementById("sc-token-list")!;
    await waitFor(() => expect(within(list).getByText("scrapey")).toBeInTheDocument());
    expect(within(list).getByText("Spare")).toBeInTheDocument();
    expect(within(list).getByText("MTAxMj…ab12")).toBeInTheDocument();
    expect(document.getElementById("sc-token-count")).toHaveTextContent("2 tokens");
  });

  it("shows the empty state with no tokens", async () => {
    server(baseRoutes([]));
    renderPage();
    await waitFor(() => expect(document.getElementById("sc-token-empty")).toHaveTextContent("No tokens configured"));
    expect(document.getElementById("sc-token-count")).toHaveTextContent("0 tokens");
  });

  it("adds a token as a multipart form with the CSRF header and never shows it in full", async () => {
    const user = userEvent.setup();
    let saved: ScraperToken[] = [];
    const fetchMock = server({
      ...baseRoutes(),
      "GET /api/scraper/tokens": () => jsonResponse({ ok: true, tokens: saved }),
      "POST /api/scraper/tokens/add": () => {
        saved = [{ token_id: "t9", label: "Alt", masked: MASKED, is_valid: true, username: null }];
        return jsonResponse({ ok: true, token_id: "t9", is_valid: true });
      },
    });
    renderPage();
    const value = document.getElementById("sc-token-value") as HTMLInputElement;
    expect(value.type).toBe("text");
    await user.type(value, RAW);
    await user.type(document.getElementById("sc-token-label")!, "  Alt ");
    await user.click(screen.getByRole("button", { name: "Add" }));

    await waitFor(() => expect(callsTo(fetchMock, "POST", "/api/scraper/tokens/add")).toHaveLength(1));
    const init = callsTo(fetchMock, "POST", "/api/scraper/tokens/add")[0]!;
    expect(new Headers(init.headers).get(CSRF_HEADER)).toBe("1");
    const body = init.body as FormData;
    expect(body.get("token_value")).toBe(RAW);
    expect(body.get("label")).toBe("Alt");

    await waitFor(() => expect(screen.getByText(MASKED)).toBeInTheDocument());
    expect(value.value).toBe("");
    expect(document.body.textContent).not.toContain(RAW);
    expect(document.body.innerHTML).not.toContain(RAW);
  });

  it("does not post an empty token", async () => {
    const user = userEvent.setup();
    const fetchMock = server(baseRoutes());
    renderPage();
    await user.click(screen.getByRole("button", { name: "Add" }));
    expect(callsTo(fetchMock, "POST", "/api/scraper/tokens/add")).toHaveLength(0);
  });

  it("re-validates one token and reloads the list", async () => {
    const user = userEvent.setup();
    const fetchMock = server({
      ...baseRoutes(),
      "POST /api/scraper/tokens/t2/validate": () => jsonResponse({ ok: true, is_valid: false }),
    });
    renderPage();
    const row = await findRow("t2");
    await user.click(within(row).getByRole("button", { name: "Re-validate" }));
    await waitFor(() => expect(callsTo(fetchMock, "POST", "/api/scraper/tokens/t2/validate")).toHaveLength(1));
    const init = callsTo(fetchMock, "POST", "/api/scraper/tokens/t2/validate")[0]!;
    expect(new Headers(init.headers).get(CSRF_HEADER)).toBe("1");
    await waitFor(() => expect(callsTo(fetchMock, "GET", "/api/scraper/tokens").length).toBeGreaterThanOrEqual(2));
  });

  it("deletes a token only after the confirm", async () => {
    const user = userEvent.setup();
    const fetchMock = server({
      ...baseRoutes(),
      "DELETE /api/scraper/tokens/t1": () => jsonResponse({ ok: true }),
    });
    renderPage();
    const row = await findRow("t1");

    await user.click(within(row).getByRole("button", { name: "Delete" }));
    let dialog = await screen.findByRole("dialog", { name: "Delete token?" });
    await user.click(within(dialog).getByRole("button", { name: "Cancel" }));
    await waitFor(() => expect(screen.queryByRole("dialog")).not.toBeInTheDocument());
    expect(callsTo(fetchMock, "DELETE", "/api/scraper/tokens/t1")).toHaveLength(0);

    await user.click(within(row).getByRole("button", { name: "Delete" }));
    dialog = await screen.findByRole("dialog", { name: "Delete token?" });
    await user.click(within(dialog).getByRole("button", { name: "Delete" }));
    await waitFor(() => expect(callsTo(fetchMock, "DELETE", "/api/scraper/tokens/t1")).toHaveLength(1));
  });
});

describe("Scraper configuration card", () => {
  it("validates the guild as a multipart form and shows the result", async () => {
    const user = userEvent.setup();
    const fetchMock = server({
      ...baseRoutes(),
      "POST /api/scraper/validate-setup": () =>
        jsonResponse({ ok: true, tokens_in_guild: 1, tokens_missing: 1, results: [] }),
    });
    renderPage();
    await user.type(document.getElementById("sc-guild-id")!, " 123456789012345678 ");
    await user.click(screen.getByRole("button", { name: "Validate" }));
    await waitFor(() => expect(callsTo(fetchMock, "POST", "/api/scraper/validate-setup")).toHaveLength(1));
    const body = callsTo(fetchMock, "POST", "/api/scraper/validate-setup")[0]!.body as FormData;
    expect(body.get("guild_id")).toBe("123456789012345678");
    const result = await waitFor(() => document.getElementById("sc-validation-result")!);
    expect(result).toHaveTextContent("1 token have access · 1 missing access");
  });

  it("shows the server error when validation fails", async () => {
    const user = userEvent.setup();
    server({
      ...baseRoutes(),
      "POST /api/scraper/validate-setup": () => jsonResponse({ ok: false, error: "No valid tokens configured" }, 400),
    });
    renderPage();
    await user.type(document.getElementById("sc-guild-id")!, "42");
    await user.click(screen.getByRole("button", { name: "Validate" }));
    await waitFor(() =>
      expect(document.getElementById("sc-validation-result")).toHaveTextContent("No valid tokens configured"),
    );
  });

  it("starts with the include flags and saved proxies as JSON, then shows Cancel", async () => {
    const user = userEvent.setup();
    const fetchMock = server({
      ...baseRoutes(TOKENS, ["127.0.0.1:8080"]),
      "POST /api/scraper/start": () => jsonResponse({ ok: true, queue_position: 1, tokens_used: 1 }),
      "POST /api/scraper/cancel": () => jsonResponse({ ok: true }),
    });
    renderPage();
    await waitFor(() => expect(screen.getByText("scrapey")).toBeInTheDocument());
    await user.type(document.getElementById("sc-guild-id")!, "42");
    await user.click(document.getElementById("sc-opt-avatar")!);
    await user.click(document.getElementById("sc-opt-roles")!);
    await user.click(screen.getByRole("button", { name: "Start Scrape" }));

    await waitFor(() => expect(callsTo(fetchMock, "POST", "/api/scraper/start")).toHaveLength(1));
    const init = callsTo(fetchMock, "POST", "/api/scraper/start")[0]!;
    expect(new Headers(init.headers).get("Content-Type")).toBe("application/json");
    expect(JSON.parse(init.body as string)).toEqual({
      guild_id: "42",
      include_username: true,
      include_avatar_url: false,
      include_bio: false,
      include_roles: true,
      proxies: ["127.0.0.1:8080"],
    });

    const cancel = await screen.findByRole("button", { name: "Cancel" });
    expect(document.getElementById("sc-status-label")).toHaveTextContent("Running");
    await user.click(cancel);
    await waitFor(() => expect(callsTo(fetchMock, "POST", "/api/scraper/cancel")).toHaveLength(1));
  });

  it("adds to the queue without proxies when none are saved", async () => {
    const user = userEvent.setup();
    const fetchMock = server({
      ...baseRoutes(),
      "POST /api/scraper/queue/add": () => jsonResponse({ ok: true, queue_position: 2 }),
    });
    renderPage();
    await waitFor(() => expect(screen.getByText("scrapey")).toBeInTheDocument());
    await user.type(document.getElementById("sc-guild-id")!, "77");
    await user.click(screen.getByRole("button", { name: "Add to Queue" }));
    await waitFor(() => expect(callsTo(fetchMock, "POST", "/api/scraper/queue/add")).toHaveLength(1));
    const payload = JSON.parse(callsTo(fetchMock, "POST", "/api/scraper/queue/add")[0]!.body as string) as Record<
      string,
      unknown
    >;
    expect(payload).toEqual({
      guild_id: "77",
      include_username: true,
      include_avatar_url: true,
      include_bio: false,
      include_roles: false,
    });
    await waitFor(() => expect((document.getElementById("sc-guild-id") as HTMLInputElement).value).toBe(""));
  });

  it("does not start without tokens", async () => {
    const user = userEvent.setup();
    const fetchMock = server(baseRoutes([]));
    renderPage();
    await user.type(document.getElementById("sc-guild-id")!, "42");
    await user.click(screen.getByRole("button", { name: "Start Scrape" }));
    expect(callsTo(fetchMock, "POST", "/api/scraper/start")).toHaveLength(0);
  });
});

describe("Scraper proxies card", () => {
  it("loads the saved list and saves the edited list with PUT", async () => {
    const user = userEvent.setup();
    const fetchMock = server({
      ...baseRoutes(TOKENS, ["10.0.0.1:3128"]),
      "PUT /api/scraper/proxies": () => jsonResponse({ ok: true, count: 2 }),
    });
    renderPage();
    await user.click(screen.getByRole("button", { name: "Expand proxies" }));
    await waitFor(() => expect(screen.getByText("10.0.0.1:3128")).toBeInTheDocument());
    const save = screen.getByRole("button", { name: "Save proxies" });
    expect(save).toBeDisabled();

    await user.type(document.getElementById("sc-proxy-input")!, "http://u:p@10.0.0.2:8080{Enter}");
    await user.click(save);
    await waitFor(() => expect(callsTo(fetchMock, "PUT", "/api/scraper/proxies")).toHaveLength(1));
    const init = callsTo(fetchMock, "PUT", "/api/scraper/proxies")[0]!;
    expect(new Headers(init.headers).get(CSRF_HEADER)).toBe("1");
    expect(JSON.parse(init.body as string)).toEqual({ proxies: ["10.0.0.1:3128", "http://u:p@10.0.0.2:8080"] });
    await waitFor(() => expect(save).toBeDisabled());
  });
});

describe("Scraper loading", () => {
  it("shows token skeleton rows, not the empty state, until the tokens arrive", async () => {
    server(baseRoutes());
    const release = holdFetch("/api/scraper/tokens");
    renderPage();
    expect(await screen.findByRole("status", { name: "Loading tokens" })).toBeInTheDocument();
    expect(document.getElementById("sc-token-empty")).toBeNull();
    release();
    await waitFor(() => expect(screen.queryByRole("status", { name: "Loading tokens" })).toBeNull());
    expect(document.getElementById("sc-token-list")!.textContent).not.toBe("");
  });
});
