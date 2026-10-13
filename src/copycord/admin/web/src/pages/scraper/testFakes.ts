// Stand ins for WebSocket and EventSource in the Scraper tests, so messages
// on /ws/out and /logs/stream/scraper can be injected by hand.

import { act } from "@testing-library/react";

export class FakeWs {
  static readonly CONNECTING = 0;
  static readonly OPEN = 1;
  static readonly CLOSING = 2;
  static readonly CLOSED = 3;
  static all: FakeWs[] = [];
  readyState = FakeWs.OPEN;
  onopen: (() => void) | null = null;
  onmessage: ((ev: MessageEvent) => void) | null = null;
  onclose: (() => void) | null = null;
  constructor(public url: string) {
    FakeWs.all.push(this);
    queueMicrotask(() => this.onopen?.());
  }
  send() {}
  close() {
    this.readyState = FakeWs.CLOSED;
  }
  /** Delivers one JSON message to every open socket on `path`. */
  static emit(path: string, msg: unknown) {
    act(() => {
      for (const ws of FakeWs.all) {
        if (ws.readyState === FakeWs.OPEN && ws.url.endsWith(path)) {
          ws.onmessage?.({ data: JSON.stringify(msg) } as MessageEvent);
        }
      }
    });
  }
}

export class FakeSource {
  static all: FakeSource[] = [];
  closed = false;
  onmessage: ((ev: MessageEvent) => void) | null = null;
  onerror: ((ev: Event) => void) | null = null;
  constructor(
    public url: string,
    public init?: EventSourceInit,
  ) {
    FakeSource.all.push(this);
  }
  close() {
    this.closed = true;
  }
  /** Delivers one raw data line to every open source on `url`. */
  static emit(url: string, data: string) {
    act(() => {
      for (const s of FakeSource.all) {
        if (!s.closed && s.url === url) s.onmessage?.({ data } as MessageEvent);
      }
    });
  }
}

/** A scraper event as /ws/out carries it. */
export function scraperMsg(payload: Record<string, unknown>) {
  return { kind: "scraper", payload };
}
