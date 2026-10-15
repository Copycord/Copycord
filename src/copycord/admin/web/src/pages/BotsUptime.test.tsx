// The Bots card's uptime (pages/dashboard/BotsPanel.tsx): one step a second between reports, and a
// report only moves the count when it is more than 2 s off.

import { act, render } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { StatusResponse } from "../types/dashboard";
import { BotsPanel, uptimeStart } from "./dashboard/BotsPanel";

class FakeSocket {
  static OPEN = 1;
  static last: FakeSocket | null = null;
  readyState = 0;
  onopen: (() => void) | null = null;
  onclose: (() => void) | null = null;
  onmessage: ((ev: MessageEvent) => void) | null = null;
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

const advance = (ms: number) =>
  act(async () => {
    await vi.advanceTimersByTimeAsync(ms);
  });

/** "1h 2m 3s" as seconds. */
function seconds(text: string | null): number {
  let total = 0;
  for (const [, n, unit] of (text ?? "").matchAll(/(\d+)([dhms])/g)) total += Number(n) * { d: 86400, h: 3600, m: 60, s: 1 }[unit as "d" | "h" | "m" | "s"];
  return total;
}
const shown = () => seconds(document.getElementById("server-uptime")!.textContent);

describe("Bots card uptime", () => {
  let started: number;
  // What each report adds to the true uptime: the moment it was read differs from when it lands.
  let skew: number[];
  let reports: number;

  beforeEach(() => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-10-15T12:00:00Z"));
    started = Date.now() - 65_400;
    skew = [0.3, -0.4, 0.8, -0.2, 0.6, -0.5];
    reports = 0;
    const uptime = () => (Date.now() - started) / 1000 + skew[reports++ % skew.length]!;
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL) => {
        if (String(input) !== "/api/status") return new Response("not found", { status: 404 });
        const body: StatusResponse = {
          server: { running: true, uptime_sec: uptime(), status: "Logged in" },
          client: { running: true, uptime_sec: uptime(), status: "Syncing" },
        };
        return new Response(JSON.stringify(body), { headers: { "content-type": "application/json" } });
      }),
    );
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.unstubAllGlobals();
  });

  it("steps one second a second while polls and pushes keep reporting", async () => {
    render(<BotsPanel onViewLogs={() => {}} WebSocketImpl={WsImpl} />);
    await advance(0);
    const t0 = Date.now();
    const changes: Array<{ at: number; value: number }> = [];
    let last = shown();
    expect(last).toBe(65);
    for (let at = 50; at <= 14_000; at += 50) {
      await advance(50);
      if (at % 1700 === 0) {
        const up = (Date.now() - started) / 1000 + 1.4;
        act(() => FakeSocket.last!.emit({ kind: "status", role: "server", payload: { running: true, uptime_sec: up, status: "Logged in" } }));
      }
      const value = shown();
      if (value !== last) {
        changes.push({ at: Date.now() - t0, value: value - last });
        last = value;
      }
    }
    // Three polls and eight pushes landed in between; none of them moved the count.
    expect(changes.map((c) => c.value)).toEqual(Array(14).fill(1));
    const gaps = changes.slice(1).map((c, i) => c.at - changes[i]!.at);
    expect(new Set(gaps)).toEqual(new Set([1000]));
  });

  it("takes a report more than 2 s off, then counts on from it", async () => {
    render(<BotsPanel onViewLogs={() => {}} WebSocketImpl={WsImpl} />);
    await advance(0);
    await advance(1000);
    expect(shown()).toBe(66);

    const push = (uptime: number) =>
      act(() => FakeSocket.last!.emit({ kind: "status", role: "server", payload: { running: true, uptime_sec: uptime, status: "Logged in" } }));
    push(66.4 + 1.9);
    expect(shown()).toBe(66);
    push(66.4 + 5);
    expect(shown()).toBe(71);
    await advance(1000);
    expect(shown()).toBe(72);

    // A restart: stopped, then running again with a small uptime.
    act(() => FakeSocket.last!.emit({ kind: "status", role: "server", payload: { running: false, status: "Stopped" } }));
    expect(document.getElementById("server-uptime")!.textContent).toBe("");
    push(2.2);
    expect(shown()).toBe(2);
    await advance(1000);
    expect(shown()).toBe(3);
  });

  it("keeps the held start within the drift window and takes the report outside it", () => {
    expect(uptimeStart(null, 65, 100_000)).toBe(35_000);
    expect(uptimeStart(35_000, 66.5, 101_000)).toBe(35_000);
    expect(uptimeStart(35_000, 63, 100_000)).toBe(37_000);
    expect(uptimeStart(35_000, 62.9, 100_000)).toBe(37_100);
  });
});
