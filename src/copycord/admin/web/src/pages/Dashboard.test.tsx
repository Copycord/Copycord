import { act, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { ToastProvider } from "../components/Toast";
import { ConfirmProvider } from "../components/ui/ConfirmDialog";
import { CSRF_HEADER, resetLoginRedirect } from "../lib/api";
import type { StatusResponse, VersionInfo } from "../types/dashboard";
import { holdFetch } from "../test/holdFetch";
import { Dashboard, DashboardSkeleton } from "./Dashboard";
import { appendCapped, mergeStatus, statusFromWs } from "./dashboard/model";
import { mappingSkeletons } from "./dashboard/DashboardSkeleton";

class FakeSocket {
  static OPEN = 1;
  static instances: FakeSocket[] = [];
  readyState = 0;
  onopen: (() => void) | null = null;
  onclose: (() => void) | null = null;
  onmessage: ((ev: MessageEvent) => void) | null = null;
  constructor(public url: string) {
    FakeSocket.instances.push(this);
  }
  send() {}
  close() {
    this.readyState = 3;
    this.onclose?.();
  }
  push(msg: unknown) {
    this.onmessage?.({ data: JSON.stringify(msg) } as MessageEvent);
  }
}

class FakeSource {
  static instances: FakeSource[] = [];
  closed = false;
  onmessage: ((ev: MessageEvent) => void) | null = null;
  onerror: ((ev: Event) => void) | null = null;
  constructor(public url: string) {
    FakeSource.instances.push(this);
  }
  close() {
    this.closed = true;
  }
  emit(data: string) {
    this.onmessage?.({ data } as MessageEvent);
  }
}

const WsImpl = FakeSocket as unknown as typeof WebSocket;
const EsImpl = FakeSource as unknown as typeof EventSource;

const STOPPED: StatusResponse = { server: { running: false }, client: { running: false } };
const RUNNING: StatusResponse = {
  server: { running: true, uptime_sec: 65, status: "active" },
  client: { running: true, uptime_sec: 3700, status: "Syncing guild 1 of 2" },
};
const NO_UPDATE: VersionInfo = { current: "v4.0.0", latest: "v4.0.0", url: "", update_available: false };

function json(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json" } });
}

type Handler = (init: RequestInit) => Response;

function server(routes: Record<string, Handler>) {
  const fetchMock = vi.fn(async (input: RequestInfo | URL, init: RequestInit = {}) => {
    const handler = routes[String(input)];
    return handler ? handler(init) : new Response("not found", { status: 404 });
  });
  vi.stubGlobal("fetch", fetchMock);
  return fetchMock;
}

function callsTo(fetchMock: ReturnType<typeof server>, url: string): RequestInit[] {
  return fetchMock.mock.calls.filter(([u]) => String(u) === url).map(([, init]) => init ?? {});
}

function renderPage() {
  FakeSocket.instances = [];
  FakeSource.instances = [];
  return render(
    <ToastProvider>
      <ConfirmProvider>
        <Dashboard WebSocketImpl={WsImpl} EventSourceImpl={EsImpl} />
      </ConfirmProvider>
    </ToastProvider>,
  );
}

const csrf = (init: RequestInit) => new Headers(init.headers).get(CSRF_HEADER);

describe("Dashboard bots panel", () => {
  it("shows stopped bots and Starts them with a CSRF POST to /start", async () => {
    let status = STOPPED;
    const fetchMock = server({
      "/api/status": () => json(status),
      "/version": () => json(NO_UPDATE),
      "/start": () => {
        status = RUNNING;
        return new Response("ok", { status: 200 });
      },
    });
    const user = userEvent.setup();
    renderPage();
    const btn = document.getElementById("toggle-btn")!;
    await waitFor(() => expect(callsTo(fetchMock, "/api/status")).toHaveLength(1));
    expect(btn).toHaveTextContent("Start");
    expect(document.getElementById("server-state")).toHaveTextContent("stopped");
    expect(document.getElementById("toggle-form")).toHaveAttribute("action", "/start");

    await user.click(btn);
    await waitFor(() => expect(btn).toHaveTextContent("Stop"));
    const [post] = callsTo(fetchMock, "/start");
    expect(post?.method).toBe("POST");
    expect(csrf(post!)).toBe("1");
    expect(document.getElementById("client-uptime")).toHaveTextContent("1h 1m 40s");
    expect(document.getElementById("client-status")).toHaveTextContent("Syncing guild 1 of 2");
    // The plain "active" note shows in the Status column, capitalized.
    expect(document.getElementById("server-status")).toHaveTextContent(/^Active$/);
    expect(document.getElementById("toggle-form")).toHaveAttribute("action", "/stop");
  });

  it("Stops running bots with a CSRF POST to /stop", async () => {
    let status = RUNNING;
    const fetchMock = server({
      "/api/status": () => json(status),
      "/version": () => json(NO_UPDATE),
      "/stop": () => {
        status = STOPPED;
        return new Response("ok", { status: 200 });
      },
    });
    const user = userEvent.setup();
    renderPage();
    const btn = document.getElementById("toggle-btn")!;
    await waitFor(() => expect(btn).toHaveTextContent("Stop"));
    await user.click(btn);
    await waitFor(() => expect(btn).toHaveTextContent("Start"));
    expect(callsTo(fetchMock, "/start")).toHaveLength(0);
    const [post] = callsTo(fetchMock, "/stop");
    expect(post?.method).toBe("POST");
    expect(csrf(post!)).toBe("1");
  });

  it("toggles in place without navigating, and Start then Stop refetch the status", async () => {
    const go = vi.fn();
    resetLoginRedirect(go);
    try {
      let status = STOPPED;
      const fetchMock = server({
        "/api/status": () => json(status),
        "/version": () => json(NO_UPDATE),
        "/start": () => {
          status = RUNNING;
          return json({ ok: true });
        },
        "/stop": () => {
          status = STOPPED;
          return json({ ok: true });
        },
      });
      const user = userEvent.setup();
      renderPage();
      const btn = document.getElementById("toggle-btn")!;
      await waitFor(() => expect(btn).toHaveTextContent("Start"));
      await user.click(btn);
      await waitFor(() => expect(btn).toHaveTextContent("Stop"));
      expect(document.getElementById("server-state")).toHaveTextContent("running");
      expect(document.getElementById("client-uptime")).toHaveTextContent("1h 1m 40s");
      await user.click(btn);
      await waitFor(() => expect(btn).toHaveTextContent("Start"));
      expect(document.getElementById("server-state")).toHaveTextContent("stopped");
      expect(document.getElementById("client-uptime")).toHaveTextContent("");
      expect(callsTo(fetchMock, "/api/status")).toHaveLength(3);
      expect(go).not.toHaveBeenCalled();
    } finally {
      resetLoginRedirect();
    }
  });

  it("toasts the server reason when Start is refused", async () => {
    server({
      "/api/status": () => json(STOPPED),
      "/version": () => json(NO_UPDATE),
      "/start": () => new Response("Cannot start: SERVER_TOKEN missing", { status: 400 }),
    });
    const user = userEvent.setup();
    renderPage();
    const btn = document.getElementById("toggle-btn")!;
    await waitFor(() => expect(btn).toHaveTextContent("Start"));
    await user.click(btn);
    expect(await screen.findByText("Cannot start: SERVER_TOKEN missing")).toBeInTheDocument();
    expect(btn).toHaveTextContent("Start");
  });

  it("takes running state from status messages on /ws/ui", async () => {
    server({ "/api/status": () => json(STOPPED), "/version": () => json(NO_UPDATE) });
    renderPage();
    const btn = document.getElementById("toggle-btn")!;
    await waitFor(() => expect(btn).toHaveTextContent("Start"));
    const ws = FakeSocket.instances.at(-1)!;
    expect(ws.url).toMatch(/\/ws\/ui$/);

    act(() => ws.push({ type: "status", source: "server", data: { running: true, uptime_sec: 5, note: "Logged in" } }));
    expect(document.getElementById("server-state")).toHaveTextContent("running");
    expect(document.getElementById("server-uptime")).toHaveTextContent("5s");
    expect(document.getElementById("server-status")).toHaveTextContent("Logged in");
    expect(btn).toHaveTextContent("Stop");

    act(() => ws.push({ kind: "agent", type: "status", role: "client", data: { running: true, uptime_sec: 61 } }));
    expect(document.getElementById("client-uptime")).toHaveTextContent("1m 1s");

    act(() => ws.push({ type: "status", source: "server", data: { running: false } }));
    act(() => ws.push({ kind: "agent", type: "status", role: "client", data: { running: false } }));
    expect(btn).toHaveTextContent("Start");
    expect(document.getElementById("server-uptime")).toHaveTextContent("");
    // Other message types leave the table alone.
    act(() => ws.push({ type: "clone_progress", source: "server", data: { running: true } }));
    expect(document.getElementById("server-state")).toHaveTextContent("stopped");
  });

  it("shows the update notice only when /version reports a newer release", async () => {
    server({
      "/api/status": () => json(STOPPED),
      "/version": () => json({ current: "v4.0.0", latest: "v4.1.0", url: "https://example.test/r", update_available: true }),
    });
    renderPage();
    const notice = await screen.findByText("New Update Available");
    expect(notice).toHaveAttribute("id", "update-notice");
    expect(notice).toHaveAttribute("href", "https://example.test/r");
  });
});

describe("Dashboard log modal", () => {
  async function openLogs(which: "server" | "client") {
    const user = userEvent.setup();
    await user.click(screen.getByRole("button", { name: `View ${which} logs` }));
    const modal = document.getElementById("log-modal")!;
    expect(modal).toHaveClass("show");
    return { user, modal, src: FakeSource.instances.at(-1)! };
  }

  it("locks the page scroll only while the log modal is open", async () => {
    server({ "/api/status": () => json(STOPPED), "/version": () => json(NO_UPDATE) });
    renderPage();
    expect(document.getElementById("log-modal")).toBeNull();
    expect(document.body.style.overflow).not.toBe("hidden");
    const { user, modal } = await openLogs("server");
    expect(document.body.style.overflow).toBe("hidden");
    await user.click(within(modal).getByRole("button", { name: /close/i }));
    await waitFor(() => expect(document.getElementById("log-modal")).toBeNull());
    expect(document.body.style.overflow).not.toBe("hidden");
  });

  it("tails /logs/stream/{which} and renders lines as text", async () => {
    server({ "/api/status": () => json(STOPPED), "/version": () => json(NO_UPDATE) });
    renderPage();
    expect(document.getElementById("log-modal")).toBeNull();
    const { modal, src } = await openLogs("client");
    expect(src.url).toBe("/logs/stream/client");
    expect(within(modal).getByText("Client logs")).toBeInTheDocument();

    act(() => src.emit(JSON.stringify({ lines: ["first line", "<b>not bold</b>"] })));
    act(() => src.emit(JSON.stringify({ line: "third line" })));
    act(() => src.emit("plain text line"));
    const body = document.getElementById("log-body")!;
    expect(body.textContent).toBe("first line\n<b>not bold</b>\nthird line\nplain text line\n");
    expect(body.querySelector("b")).toBeNull();
  });

  it("filters lines by the search box, case insensitive", async () => {
    server({ "/api/status": () => json(STOPPED), "/version": () => json(NO_UPDATE) });
    renderPage();
    const { user, src } = await openLogs("server");
    act(() => src.emit(JSON.stringify({ lines: ["INFO ready", "ERROR boom", "info done"] })));
    const search = document.getElementById("log-search-input") as HTMLInputElement;
    await user.type(search, "info");
    expect(document.getElementById("log-body")!.textContent).toBe("INFO ready\ninfo done\n");
    await user.clear(search);
    await user.type(search, "zzz");
    expect(document.getElementById("log-body")!.textContent).toBe("");
    await user.clear(search);
    expect(document.getElementById("log-body")!.textContent).toBe("INFO ready\nERROR boom\ninfo done\n");
  });

  it("clear asks first, Cancel sends nothing, confirm POSTs /logs/clear/{which} and empties the view", async () => {
    const fetchMock = server({
      "/api/status": () => json(STOPPED),
      "/version": () => json(NO_UPDATE),
      "/logs/clear/server": () => new Response("ok", { status: 200 }),
    });
    renderPage();
    const { user, src } = await openLogs("server");
    act(() => src.emit(JSON.stringify({ lines: ["one", "two"] })));

    await user.click(document.getElementById("log-clear-btn")!);
    let dialog = await screen.findByRole("dialog", { name: "Clear logs?" });
    await user.click(within(dialog).getByRole("button", { name: "Cancel" }));
    expect(callsTo(fetchMock, "/logs/clear/server")).toHaveLength(0);
    expect(document.getElementById("log-body")!.textContent).toBe("one\ntwo\n");

    await user.click(document.getElementById("log-clear-btn")!);
    dialog = await screen.findByRole("dialog", { name: "Clear logs?" });
    await user.click(within(dialog).getByRole("button", { name: "Clear logs" }));
    await waitFor(() => expect(callsTo(fetchMock, "/logs/clear/server")).toHaveLength(1));
    const [post] = callsTo(fetchMock, "/logs/clear/server");
    expect(post?.method).toBe("POST");
    expect(csrf(post!)).toBe("1");
    await waitFor(() => expect(document.getElementById("log-body")!.textContent).toBe(""));
    expect(await screen.findByText("Server logs cleared")).toBeInTheDocument();
  });

  it("closes with X, backdrop and Escape, closing the stream each time", async () => {
    server({ "/api/status": () => json(STOPPED), "/version": () => json(NO_UPDATE) });
    renderPage();
    let { user, src } = await openLogs("server");
    await user.click(document.getElementById("log-close")!);
    expect(document.getElementById("log-modal")).toBeNull();
    expect(src.closed).toBe(true);

    ({ user, src } = await openLogs("client"));
    await user.click(screen.getByTestId("log-backdrop"));
    expect(document.getElementById("log-modal")).toBeNull();
    expect(src.closed).toBe(true);

    ({ user, src } = await openLogs("server"));
    // A fresh open starts empty.
    expect(document.getElementById("log-body")!.textContent).toBe("");
    await user.keyboard("{Escape}");
    expect(document.getElementById("log-modal")).toBeNull();
    expect(src.closed).toBe(true);
  });
});

describe("dashboard model", () => {

  it("caps the buffer at the newest lines", () => {
    expect(appendCapped(["a", "b"], ["c", "d"], 3)).toEqual(["b", "c", "d"]);
  });

  it("reads the status frames the hub sends on /ws/ui", () => {
    // AdminBus.status() from the bots and the hub's replay on connect.
    expect(
      statusFromWs({ kind: "status", role: "client", payload: { running: true, status: "Logged in as cc", discord: { ready: true } } }),
    ).toEqual({ role: "client", status: { running: true, status: "Logged in as cc" } });
    // hub.broadcast({type: "status", source, data}) after _normalize.
    expect(
      statusFromWs({ kind: "status", role: "unknown", payload: { type: "status", source: "server", data: { running: true } } }),
    ).toEqual({ role: "server", status: { running: true } });
    // An agent frame from /bus after _normalize.
    expect(
      statusFromWs({ kind: "agent", role: "server", payload: { type: "status", ts: 1, data: { running: false, status: "Stopped" } } }),
    ).toEqual({ role: "server", status: { running: false, status: "Stopped" } });
  });

  it("merges a partial status into the last one", () => {
    const up = { running: true, uptime_sec: 65, status: "Starting" };
    // A note only push keeps running and the uptime.
    expect(mergeStatus(up, { running: true, status: "Logged in as cc" })).toEqual({ running: true, uptime_sec: 65, status: "Logged in as cc" });
    expect(mergeStatus(up, {})).toEqual(up);
    expect(mergeStatus(up, { uptime_sec: 70 })).toEqual({ ...up, uptime_sec: 70 });
    // A stop drops the uptime and the old note.
    expect(mergeStatus(up, { running: false })).toEqual({ running: false, uptime_sec: null, status: "" });
    // A start without an uptime has none yet, not the one from before the stop.
    expect(mergeStatus({ running: false, uptime_sec: 30, status: "" }, { running: true })).toEqual({ running: true, uptime_sec: null, status: "" });
  });
});

describe("Dashboard loading", () => {

  it("the chunk skeleton has the loaded page's cards, titles and order", async () => {
    const shape = (root: Element) =>
      Array.from(root.children).map((el) => `${el.tagName} ${el.querySelector("h2, h3")?.textContent ?? ""}`);
    const skel = render(<DashboardSkeleton />);
    expect(screen.getByRole("heading", { level: 1, name: "Configuration" })).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: "Bots" })).toBeInTheDocument();
    expect(screen.getAllByTestId("card-skeleton")).toHaveLength(6);
    expect(screen.getAllByTestId("mapping-skeleton")).toHaveLength(mappingSkeletons());
    const skelShape = shape(screen.getByRole("heading", { level: 1, name: "Configuration" }).parentElement!);
    skel.unmount();
    server({});
    renderPage();
    await screen.findByRole("heading", { name: "Bots" });
    expect(shape(screen.getByRole("heading", { level: 1, name: "Configuration" }).parentElement!)).toEqual(skelShape);
  });
});
