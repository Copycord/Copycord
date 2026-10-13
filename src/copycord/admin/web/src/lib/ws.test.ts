import { describe, expect, it, vi } from "vitest";
import { connectWs, wsUrl } from "./ws";

class FakeSocket {
  static OPEN = 1;
  static instances: FakeSocket[] = [];
  readyState = 0;
  sent: string[] = [];
  onopen: (() => void) | null = null;
  onclose: (() => void) | null = null;
  onmessage: ((ev: MessageEvent) => void) | null = null;

  constructor(public url: string) {
    FakeSocket.instances.push(this);
  }
  send(data: string) {
    this.sent.push(data);
  }
  close() {
    this.readyState = 3;
    this.onclose?.();
  }
  open() {
    this.readyState = 1;
    this.onopen?.();
  }
  drop() {
    this.readyState = 3;
    this.onclose?.();
  }
  emit(data: string) {
    this.onmessage?.({ data } as MessageEvent);
  }
}

const Impl = FakeSocket as unknown as typeof WebSocket;

describe("ws", () => {
  it("builds ws and wss urls from the page location", () => {
    expect(wsUrl("/ws/ui", { protocol: "http:", host: "h:8080" } as Location)).toBe("ws://h:8080/ws/ui");
    expect(wsUrl("/ws/out", { protocol: "https:", host: "h" } as Location)).toBe("wss://h/ws/out");
  });

  it("parses JSON messages and skips others", () => {
    FakeSocket.instances = [];
    const got: unknown[] = [];
    connectWs("/ws/ui", { onMessage: (m) => got.push(m), WebSocketImpl: Impl });
    const s = FakeSocket.instances[0]!;
    s.open();
    s.emit('{"type":"status"}');
    s.emit("not json");
    expect(got).toEqual([{ type: "status" }]);
  });

  it("reconnects with backoff after a drop and stops after close", () => {
    vi.useFakeTimers();
    FakeSocket.instances = [];
    const client = connectWs("/ws/out", {
      onMessage: () => {},
      WebSocketImpl: Impl,
      baseDelay: 100,
      maxDelay: 300,
    });
    FakeSocket.instances[0]!.drop();
    vi.advanceTimersByTime(99);
    expect(FakeSocket.instances).toHaveLength(1);
    vi.advanceTimersByTime(1);
    expect(FakeSocket.instances).toHaveLength(2);
    FakeSocket.instances[1]!.drop();
    vi.advanceTimersByTime(200);
    expect(FakeSocket.instances).toHaveLength(3);
    FakeSocket.instances[2]!.open();
    expect(client.send({ a: 1 })).toBe(true);
    expect(FakeSocket.instances[2]!.sent).toEqual(['{"a":1}']);
    FakeSocket.instances[2]!.drop();
    vi.advanceTimersByTime(100);
    expect(FakeSocket.instances).toHaveLength(4);
    client.close();
    vi.advanceTimersByTime(10000);
    expect(FakeSocket.instances).toHaveLength(4);
  });
});
