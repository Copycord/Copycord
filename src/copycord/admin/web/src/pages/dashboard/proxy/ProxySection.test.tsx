import { act, render, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ToastProvider } from "../../../components/Toast";
import { ConfirmProvider } from "../../../components/ui/ConfirmDialog";
import { CSRF_HEADER } from "../../../lib/api";
import { ProxySection } from "../ProxySection";

class FakeSocket {
  static instances: FakeSocket[] = [];
  onopen: (() => void) | null = null;
  onclose: (() => void) | null = null;
  onmessage: ((ev: MessageEvent) => void) | null = null;
  constructor(public url: string) {
    FakeSocket.instances.push(this);
  }
  send() {}
  close() {}
  push(msg: unknown) {
    this.onmessage?.({ data: JSON.stringify(msg) } as MessageEvent);
  }
}
const WsImpl = FakeSocket as unknown as typeof WebSocket;

const json = (body: unknown) =>
  new Response(JSON.stringify(body), { status: 200, headers: { "content-type": "application/json" } });

const CONFIG = {
  ok: true,
  proxies: ["a.test:1", "b.test:2"],
  enabled: true,
  rotation_interval: 7200,
  favorites: ["a.test:1"],
  favorite_recheck: false,
  favorite_recheck_interval: 300,
};

function setup(config = CONFIG) {
  const calls: Array<{ url: string; method: string; body: unknown; csrf: string | null }> = [];
  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: RequestInfo | URL, init: RequestInit = {}) => {
      const url = String(input);
      const method = init.method ?? "GET";
      calls.push({
        url,
        method,
        body: init.body ? JSON.parse(String(init.body)) : undefined,
        csrf: new Headers(init.headers).get(CSRF_HEADER),
      });
      if (url === "/api/server/proxies" && method === "GET") return json(config);
      if (url.endsWith("/test/status")) return json({ ok: true, running: false });
      if (url.endsWith("/favorites")) return json({ ok: true, favorites: (calls.at(-1)!.body as { favorites: string[] }).favorites });
      if (url.endsWith("/settings")) return json({ ok: true, settings: { PROXY_SUSPEND_DURATION: 60, PROXY_TEST_BATCH_SIZE: 20, PROXY_SLOW_THRESHOLD: 2 } });
      return json({ ok: true });
    }),
  );
  FakeSocket.instances = [];
  render(
    <ToastProvider>
      <ConfirmProvider>
        <ProxySection WebSocketImpl={WsImpl} replayMs={0} />
      </ConfirmProvider>
    </ToastProvider>,
  );
  return calls;
}

const el = (id: string) => document.getElementById(id) as HTMLInputElement;
const to = (calls: ReturnType<typeof setup>, url: string, method: string) =>
  calls.filter((c) => c.url === url && c.method === method);

afterEach(() => {
  localStorage.clear();
  vi.unstubAllGlobals();
});

describe("ProxySection", () => {
  it("loads the list, status chip, rotation and favorites", async () => {
    setup();
    await waitFor(() => expect(document.querySelectorAll(".srv-proxy-chip-item")).toHaveLength(2));
    expect(el("srv-proxy-status")).toHaveTextContent("Enabled · 2 proxies");
    expect(el("srv-proxy-toggle").checked).toBe(true);
    expect(el("srv-proxy-rotation-toggle").checked).toBe(true);
    expect(el("srv-proxy-interval").value).toBe("2");
    expect(el("srv-proxy-interval-unit").value).toBe("3600");
    expect(document.querySelector('[data-proxy="a.test:1"] .srv-proxy-chip-star')).toHaveAttribute("aria-pressed", "true");
  });

  it("saves the list and rotation after a short delay, and the toggle at once", async () => {
    const calls = setup();
    const user = userEvent.setup();
    await waitFor(() => expect(document.querySelectorAll(".srv-proxy-chip-item")).toHaveLength(2));
    await user.type(el("srv-proxy-chips-input"), "c.test:3{Enter}");
    await waitFor(() => expect(to(calls, "/api/server/proxies", "PUT")).toHaveLength(1));
    const put = to(calls, "/api/server/proxies", "PUT")[0]!;
    expect(put.body).toEqual({ proxies: ["a.test:1", "b.test:2", "c.test:3"] });
    expect(put.csrf).toBe("1");
    await waitFor(() => expect(to(calls, "/api/server/proxies/rotation-interval", "PUT")[0]?.body).toEqual({ interval: 7200 }));
    expect(el("srv-proxy-status")).toHaveTextContent("3 proxies");

    await user.click(el("srv-proxy-toggle"));
    await waitFor(() => expect(to(calls, "/api/server/proxies/toggle", "PUT")[0]?.body).toEqual({ enabled: false }));
    expect(el("srv-proxy-status")).toHaveTextContent("Disabled");
    expect(el("srv-proxy-interval")).toBeDisabled();
  });

  it("stars a chip through PUT favorites", async () => {
    const calls = setup();
    const user = userEvent.setup();
    await waitFor(() => expect(document.querySelectorAll(".srv-proxy-chip-item")).toHaveLength(2));
    await user.click(document.querySelector('[data-proxy="b.test:2"] .srv-proxy-chip-star')!);
    await waitFor(() =>
      expect(to(calls, "/api/server/proxies/favorites", "PUT")[0]?.body).toEqual({ favorites: ["a.test:1", "b.test:2"] }),
    );
  });

  it("starts a test, shows live progress, and removes failed proxies from the results", async () => {
    const calls = setup();
    const user = userEvent.setup();
    await waitFor(() => expect(document.querySelectorAll(".srv-proxy-chip-item")).toHaveLength(2));
    await user.click(el("srv-proxy-test"));
    await waitFor(() => expect(to(calls, "/api/server/proxies/test", "POST")[0]?.body).toEqual({ proxies: ["a.test:1", "b.test:2"] }));
    const ws = FakeSocket.instances[0]!;
    const send = (payload: unknown) => act(() => ws.push({ kind: "proxy_test", payload }));
    send({ type: "started", total: 2 });
    await waitFor(() => expect(el("srv-proxy-test")).toHaveTextContent("Stop"));
    send({ type: "progress", current: 2, total: 2, results: [{ proxy: "a.test:1", ok: true, ms: 120 }, { proxy: "b.test:2", ok: false, error: "timeout" }] });
    expect(el("srv-proxy-progress-text")).toHaveTextContent("2 / 2");
    expect(document.querySelector('[data-proxy="a.test:1"]')).toHaveTextContent("120ms");
    send({ type: "complete", results: [{ proxy: "a.test:1", ok: true, ms: 120 }, { proxy: "b.test:2", ok: false, error: "timeout" }] });
    await waitFor(() => expect(el("proxy-results-modal")).toBeTruthy());
    await user.click(el("ptr-confirm"));
    await waitFor(() => expect(to(calls, "/api/server/proxies", "PUT").at(-1)?.body).toEqual({ proxies: ["a.test:1"] }));
    expect(el("srv-proxy-test")).toHaveTextContent("Test All");
  });

  it("Stop posts /test/stop", async () => {
    const calls = setup();
    const user = userEvent.setup();
    await waitFor(() => expect(document.querySelectorAll(".srv-proxy-chip-item")).toHaveLength(2));
    act(() => FakeSocket.instances[0]!.push({ kind: "proxy_test", payload: { type: "started", total: 2 } }));
    await waitFor(() => expect(el("srv-proxy-test")).toHaveTextContent("Stop"));
    await user.click(el("srv-proxy-test"));
    await waitFor(() => expect(to(calls, "/api/server/proxies/test/stop", "POST")).toHaveLength(1));
  });

  it("opens the settings modal, loads values and saves them", async () => {
    const calls = setup();
    const user = userEvent.setup();
    await user.click(el("srv-proxy-settings-btn"));
    await waitFor(() => expect(el("ps-suspend-duration").value).toBe("60"));
    expect(el("ps-slow-threshold").value).toBe("2");
    await user.click(el("ps-reset-defaults"));
    await waitFor(() =>
      expect(to(calls, "/api/server/proxies/settings", "PUT")[0]?.body).toEqual({
        settings: { PROXY_SUSPEND_DURATION: 300, PROXY_TEST_BATCH_SIZE: 50, PROXY_SLOW_THRESHOLD: 3 },
      }),
    );
    await user.click(el("proxy-settings-close"));
    expect(document.getElementById("proxy-settings-modal")).toBeNull();
  });

  it("opens and closes the favorites help without toggling the card", async () => {
    setup();
    const user = userEvent.setup();
    await user.click(el("srv-proxy-fav-help-btn"));
    expect(document.getElementById("proxy-fav-help-modal")).not.toBeNull();
    await user.click(el("proxy-fav-help-close"));
    expect(document.getElementById("proxy-fav-help-modal")).toBeNull();
    expect((document.getElementById("srv-proxy-card") as HTMLDetailsElement).open).toBe(false);
  });
});
