import { act, fireEvent, render } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import type { StatusResponse } from "../../types/dashboard";
import { BotsPanel, STOP_HOLD_MS } from "./BotsPanel";

class FakeSocket {
  static OPEN = 1;
  readyState = 0;
  onopen: (() => void) | null = null;
  onclose: (() => void) | null = null;
  onmessage: ((ev: MessageEvent) => void) | null = null;
  static last: FakeSocket | null = null;
  constructor(public url: string) {
    FakeSocket.last = this;
  }
  emit(data: unknown) {
    this.onmessage?.({ data: JSON.stringify(data) } as MessageEvent);
  }
  send() {}
  close() {
    this.readyState = 3;
  }
}
const WsImpl = FakeSocket as unknown as typeof WebSocket;

const STOPPED: StatusResponse = { server: { running: false }, client: { running: false } };
const running = (status: string): StatusResponse => ({
  server: { running: true, uptime_sec: 65, status },
  client: { running: true, uptime_sec: 3700, status: "Syncing" },
});

function json(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json" } });
}

/** Stubs fetch: /api/status answers from `status()`, /start and /stop answer ok. */
function stubServer(status: () => StatusResponse | Error) {
  const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
    const url = String(input);
    if (url === "/api/status") {
      const s = status();
      if (s instanceof Error) throw s;
      return json(s);
    }
    if (url === "/start" || url === "/stop") return json({ ok: true });
    return new Response("not found", { status: 404 });
  });
  vi.stubGlobal("fetch", fetchMock);
  return () => fetchMock.mock.calls.filter(([u]) => String(u) === "/api/status").length;
}

const advance = (ms: number) =>
  act(async () => {
    await vi.advanceTimersByTimeAsync(ms);
  });

function mount() {
  render(<BotsPanel onViewLogs={() => {}} WebSocketImpl={WsImpl} />);
  return advance(0);
}

describe("BotsPanel status poll", () => {
  it("polls /api/status every 4 s and re-renders the state, uptime and Status text", async () => {
    vi.useFakeTimers();
    let reply: StatusResponse = STOPPED;
    const calls = stubServer(() => reply);
    await mount();
    expect(calls()).toBe(1);
    expect(document.getElementById("server-state")).toHaveTextContent("stopped");

    reply = running("Logged in");
    await advance(3999);
    expect(calls()).toBe(1);
    await advance(1);
    expect(calls()).toBe(2);
    expect(document.getElementById("server-state")).toHaveTextContent("running");
    expect(document.getElementById("server-uptime")).toHaveTextContent("1m 5s");
    expect(document.getElementById("server-status")).toHaveTextContent("Logged in");

    reply = running("Cloning guild");
    await advance(4000);
    expect(calls()).toBe(3);
    expect(document.getElementById("server-status")).toHaveTextContent("Cloning guild");
    expect(document.getElementById("server-uptime")).toHaveTextContent("1m 5s");
  });

  it("backs off on fetch failure, doubling up to 15 s, and returns to 4 s once it answers", async () => {
    vi.useFakeTimers();
    let fail = true;
    const calls = stubServer(() => (fail ? new TypeError("Failed to fetch") : STOPPED));
    await mount();
    await advance(4000);
    expect(calls()).toBe(2);
    await advance(7999);
    expect(calls()).toBe(2);
    await advance(1);
    expect(calls()).toBe(3);
    await advance(14999);
    expect(calls()).toBe(3);
    await advance(1);
    expect(calls()).toBe(4);
    fail = false;
    await advance(15000);
    expect(calls()).toBe(5);
    await advance(4000);
    expect(calls()).toBe(6);
  });

  it("polls every 800 ms for 15 s after Start, then every 4 s", async () => {
    vi.useFakeTimers();
    let reply: StatusResponse = STOPPED;
    const calls = stubServer(() => reply);
    await mount();
    reply = running("Starting");
    fireEvent.click(document.getElementById("toggle-btn")!);
    await advance(0);
    expect(calls()).toBe(2);
    expect(document.getElementById("toggle-btn")).toHaveTextContent("Stop");

    await advance(800);
    expect(calls()).toBe(3);
    await advance(800);
    expect(calls()).toBe(4);
    // 800 ms ticks while inside the 15 s window: 800 through 15200, then the 4 s pace again.
    await advance(15000 - 1600);
    const afterBurst = calls();
    expect(afterBurst).toBe(2 + 18);
    await advance(200);
    expect(calls()).toBe(afterBurst + 1);
    await advance(3999);
    expect(calls()).toBe(afterBurst + 1);
    await advance(1);
    expect(calls()).toBe(afterBurst + 2);
  });

  it("pauses while the tab is hidden and polls at once when it shows again", async () => {
    vi.useFakeTimers();
    let hidden = false;
    Object.defineProperty(document, "hidden", { configurable: true, get: () => hidden });
    try {
      const calls = stubServer(() => STOPPED);
      await mount();
      hidden = true;
      act(() => {
        document.dispatchEvent(new Event("visibilitychange"));
      });
      await advance(60000);
      expect(calls()).toBe(1);
      hidden = false;
      act(() => {
        document.dispatchEvent(new Event("visibilitychange"));
      });
      await advance(0);
      expect(calls()).toBe(2);
      await advance(4000);
      expect(calls()).toBe(3);
    } finally {
      delete (document as { hidden?: boolean }).hidden;
    }
  });

  it("stops polling on unmount", async () => {
    vi.useFakeTimers();
    const calls = stubServer(() => STOPPED);
    const view = render(<BotsPanel onViewLogs={() => {}} WebSocketImpl={WsImpl} />);
    await advance(0);
    view.unmount();
    await advance(60000);
    expect(calls()).toBe(1);
  });
});

describe("BotsPanel stop", () => {
  const STOPPING: StatusResponse = {
    server: { running: true, state: "stopping", status: "stopping" },
    client: { running: true, state: "stopping", status: "stopping" },
  };
  const chips = () => ["server", "client"].map((r) => document.getElementById(`${r}-state`)!.textContent).join("/");
  const btn = () => document.getElementById("toggle-btn") as HTMLButtonElement;

  /** Answers /api/status from `replies` in order, the last one for good. */
  function scripted(first: StatusResponse) {
    const replies: Array<StatusResponse | Error> = [first];
    const calls = stubServer(() => (replies.length > 1 ? replies.shift()! : replies[0]!));
    return { calls, then: (...next: Array<StatusResponse | Error>) => replies.splice(0, replies.length, ...next) };
  }

  it("says stopping from the click until /api/status reports stopped, never stopped early and never running again", async () => {
    vi.useFakeTimers();
    const srv = scripted(running("Logged in"));
    await mount();
    expect(chips()).toBe("running/running");

    // The answers while the bots shut down: two read before the stop reached them, a failed poll,
    // one more stale running, the server's own stopping, then stopped.
    srv.then(running("Logged in"), running("Logged in"), new TypeError("Failed to fetch"), running("Logged in"), STOPPING, STOPPED);
    const seen: string[] = [];
    fireEvent.click(btn());
    seen.push(chips());
    // The bots' AdminBus "Stopped" pushes come at the start of their shutdown.
    act(() => FakeSocket.last!.emit({ kind: "status", role: "server", payload: { running: false, status: "Stopped" } }));
    act(() => FakeSocket.last!.emit({ kind: "status", role: "client", payload: { running: false, status: "Stopped" } }));
    seen.push(chips());
    await advance(0);
    seen.push(chips());
    expect(btn()).toBeDisabled();
    expect(btn()).toHaveTextContent("Stopping...");
    expect(document.getElementById("server-uptime")).toHaveTextContent("");
    expect(document.getElementById("server-state")!.className).toContain("bg-stopping-bg");
    for (let i = 0; i < 4; i++) {
      await advance(800);
      seen.push(chips());
      expect(btn()).toBeDisabled();
    }
    expect(seen.every((c) => c === "stopping/stopping")).toBe(true);
    expect(srv.calls()).toBe(6);

    await advance(800);
    expect(chips()).toBe("stopped/stopped");
    expect(btn()).not.toBeDisabled();
    expect(btn()).toHaveTextContent("Start");
    expect(document.getElementById("server-state")!.className).toContain("bg-stop-bg");
    vi.useRealTimers();
  });

  it("shows a stopping role from /api/status alone, with Start and Stop off", async () => {
    vi.useFakeTimers();
    scripted(STOPPING);
    await mount();
    expect(chips()).toBe("stopping/stopping");
    expect(document.getElementById("server-uptime")).toHaveTextContent("");
    expect(document.getElementById("server-status")?.textContent ?? "").toBe("");
    expect(btn()).toBeDisabled();
    vi.useRealTimers();
  });

  it("lets a running answer win once the hold is over, when the stop never took", async () => {
    vi.useFakeTimers();
    scripted(running("Logged in"));
    await mount();
    fireEvent.click(btn());
    await advance(0);
    expect(chips()).toBe("stopping/stopping");
    await advance(STOP_HOLD_MS + 4000);
    expect(chips()).toBe("running/running");
    expect(btn()).not.toBeDisabled();
    expect(btn()).toHaveTextContent("Stop");
    vi.useRealTimers();
  });
});

describe("BotsPanel uptime", () => {
  const uptime = () => document.getElementById("server-uptime")!.textContent;

  it("keeps counting when a status push or a poll answer leaves the uptime out", async () => {
    vi.useFakeTimers();
    let reply: StatusResponse = running("Starting");
    stubServer(() => reply);
    await mount();
    expect(uptime()).toBe("1m 5s");
    await advance(2000);
    expect(uptime()).toBe("1m 7s");

    // The bots' AdminBus.status() push: running and the note, no uptime_sec.
    act(() => FakeSocket.last!.emit({ kind: "status", role: "server", payload: { running: true, status: "Logged in as cc", discord: { ready: true } } }));
    expect(document.getElementById("server-status")).toHaveTextContent("Logged in as cc");
    expect(document.getElementById("server-state")).toHaveTextContent("running");
    expect(uptime()).toBe("1m 7s");
    await advance(1000);
    expect(uptime()).toBe("1m 8s");

    // The hub's {type: "status", source, data: {running}} frame changes nothing either.
    act(() => FakeSocket.last!.emit({ kind: "status", role: "unknown", payload: { type: "status", source: "server", data: { running: true } } }));
    expect(uptime()).toBe("1m 8s");
    expect(document.getElementById("server-status")).toHaveTextContent("Logged in as cc");

    // A poll answer without uptime_sec (the control socket timed out) keeps the last uptime and its stamp.
    reply = { server: { running: true, status: "Logged in as cc" }, client: { running: true, uptime_sec: 3700, status: "Syncing" } };
    await advance(1000);
    expect(uptime()).toBe("1m 9s");
    await advance(1000);
    expect(uptime()).toBe("1m 10s");

    // A real stop still clears it.
    act(() => FakeSocket.last!.emit({ kind: "status", role: "server", payload: { running: false, status: "Stopped" } }));
    expect(document.getElementById("server-state")).toHaveTextContent("stopped");
    expect(uptime()).toBe("");
    // The State chip already says stopped, so the Status cell stays empty.
    expect(document.getElementById("server-status")?.textContent ?? "").toBe("");
    vi.useRealTimers();
  });
});
