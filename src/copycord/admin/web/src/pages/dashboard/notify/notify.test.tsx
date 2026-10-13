import { act, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { ToastProvider } from "../../../components/Toast";
import { CSRF_HEADER } from "../../../lib/api";
import { NotificationsSection } from "../NotificationsSection";
import { SyncSettingsSection } from "../SyncSettingsSection";
import { DEFAULT_EVENTS, DEFAULT_SYNC, mergeEvents, notifPayload, syncFormFrom, syncPayload } from "./model";

describe("notification model", () => {
  it("keeps defaults for events the server omits and ignores unknown keys", () => {
    expect(mergeEvents({ CLIENT_OFFLINE: false, OTHER: false })).toEqual({ ...DEFAULT_EVENTS, CLIENT_OFFLINE: false });
    expect(mergeEvents(undefined)).toEqual(DEFAULT_EVENTS);
  });

  it("trims the webhook URL in the payload", () => {
    expect(notifPayload("  https://x.test/w  ", DEFAULT_EVENTS)).toEqual({
      webhook_url: "https://x.test/w",
      events: DEFAULT_EVENTS,
    });
  });
});

describe("sync model", () => {
  it("falls back to 15, 3 and randomize on when values are missing or zero", () => {
    expect(syncFormFrom({})).toEqual(DEFAULT_SYNC);
    expect(syncFormFrom({ SYNC_STARTUP_DELAY: 0, SYNC_INTER_GUILD_DELAY: null })).toEqual(DEFAULT_SYNC);
  });

  it("reads randomize 0 as off", () => {
    expect(syncFormFrom({ SYNC_RANDOMIZE_ORDER: 0 }).randomize).toBe(false);
  });

  it("builds integer settings with the old fallbacks", () => {
    expect(syncPayload({ startupDelay: "30", guildDelay: "x", randomize: false })).toEqual({
      settings: { SYNC_STARTUP_DELAY: 30, SYNC_INTER_GUILD_DELAY: 3, SYNC_RANDOMIZE_ORDER: 0 },
    });
  });
});

function json(body: unknown) {
  return new Response(JSON.stringify(body), { status: 200, headers: { "content-type": "application/json" } });
}

function server(routes: Record<string, (init: RequestInit) => Response>) {
  const m = vi.fn(async (input: RequestInfo | URL, init: RequestInit = {}) => {
    const h = routes[`${init.method ?? "GET"} ${String(input)}`];
    return h ? h(init) : new Response("not found", { status: 404 });
  });
  vi.stubGlobal("fetch", m);
  return m;
}

const calls = (m: ReturnType<typeof server>, key: string) =>
  m.mock.calls.filter(([u, i]) => `${(i as RequestInit | undefined)?.method ?? "GET"} ${String(u)}` === key).map(([, i]) => (i ?? {}) as RequestInit);

beforeEach(() => localStorage.clear());
afterEach(() => {
  vi.unstubAllGlobals();
  vi.useRealTimers();
});

describe("NotificationsSection", () => {
  const GET = "GET /api/notifications/settings";
  const PUT = "PUT /api/notifications/settings";

  function renderIt() {
    return render(
      <ToastProvider>
        <NotificationsSection />
      </ToastProvider>,
    );
  }

  it("loads the webhook and switches, then saves one debounced PUT with the CSRF header", async () => {
    const m = server({
      [GET]: () => json({ ok: true, webhook_url: "https://hook.test/a", events: { TOKEN_INVALID: false } }),
      [PUT]: () => json({ ok: true }),
    });
    renderIt();
    const input = screen.getByLabelText("Webhook URL");
    await waitFor(() => expect(input).toHaveValue("https://hook.test/a"));
    expect(document.getElementById("notif-toggle-TOKEN_INVALID")).toHaveAttribute("aria-checked", "false");
    expect(calls(m, PUT)).toHaveLength(0);

    vi.useFakeTimers({ shouldAdvanceTime: true });
    const user = userEvent.setup({ advanceTimers: vi.advanceTimersByTime });
    await user.click(document.getElementById("notif-toggle-CLIENT_OFFLINE")!);
    await user.type(input, "b ");
    expect(calls(m, PUT)).toHaveLength(0);
    await act(async () => {
      await vi.advanceTimersByTimeAsync(700);
    });
    const puts = calls(m, PUT);
    expect(puts).toHaveLength(1);
    expect(new Headers(puts[0]!.headers).get(CSRF_HEADER)).toBe("1");
    expect(JSON.parse(String(puts[0]!.body))).toEqual({
      webhook_url: "https://hook.test/ab",
      events: { CLIENT_OFFLINE: false, SERVER_OFFLINE: true, PROXIES_DEAD: true, TOKEN_INVALID: false },
    });
  });

  it("Test posts once and toasts success", async () => {
    const m = server({
      [GET]: () => json({ ok: true, webhook_url: "", events: {} }),
      "POST /api/notifications/test": () => json({ ok: true, error: null }),
    });
    renderIt();
    await userEvent.click(screen.getByRole("button", { name: "Test" }));
    expect(await screen.findByText("Test notification sent!")).toBeInTheDocument();
    expect(calls(m, "POST /api/notifications/test")).toHaveLength(1);
  });

  it("Test toasts the server error as text", async () => {
    server({
      [GET]: () => json({ ok: true, webhook_url: "", events: {} }),
      "POST /api/notifications/test": () => json({ ok: false, error: "No webhook URL configured" }),
    });
    renderIt();
    await userEvent.click(screen.getByRole("button", { name: "Test" }));
    expect(await screen.findByText("No webhook URL configured")).toBeInTheDocument();
  });

  it("remembers the open state under the old key", async () => {
    server({ [GET]: () => json({ ok: true, events: {} }) });
    const { container } = renderIt();
    const card = container.querySelector<HTMLDetailsElement>("#notifications-card")!;
    expect(card.open).toBe(false);
    await act(async () => {
      card.open = true;
      card.dispatchEvent(new Event("toggle"));
    });
    expect(localStorage.getItem("cpc.notifications-card-open")).toBe("1");
  });
});

describe("SyncSettingsSection", () => {
  const GET = "GET /api/sync/settings";
  const PUT = "PUT /api/sync/settings";

  it("loads the values and saves only after a change", async () => {
    const m = server({
      [GET]: () => json({ ok: true, settings: { SYNC_STARTUP_DELAY: 20, SYNC_INTER_GUILD_DELAY: 5, SYNC_RANDOMIZE_ORDER: 0 } }),
      [PUT]: () => json({ ok: true }),
    });
    render(<SyncSettingsSection />);
    const startup = document.getElementById("sync-startup-delay") as HTMLInputElement;
    await waitFor(() => expect(startup).toHaveValue(20));
    expect(document.getElementById("sync-guild-delay")).toHaveValue(5);
    expect(document.getElementById("sync-randomize")).toHaveAttribute("aria-checked", "false");
    expect(calls(m, PUT)).toHaveLength(0);

    vi.useFakeTimers({ shouldAdvanceTime: true });
    const user = userEvent.setup({ advanceTimers: vi.advanceTimersByTime });
    await user.click(document.getElementById("sync-randomize")!);
    await act(async () => {
      await vi.advanceTimersByTimeAsync(700);
    });
    const puts = calls(m, PUT);
    expect(puts).toHaveLength(1);
    expect(new Headers(puts[0]!.headers).get(CSRF_HEADER)).toBe("1");
    expect(JSON.parse(String(puts[0]!.body))).toEqual({
      settings: { SYNC_STARTUP_DELAY: 20, SYNC_INTER_GUILD_DELAY: 5, SYNC_RANDOMIZE_ORDER: 1 },
    });
  });
});
