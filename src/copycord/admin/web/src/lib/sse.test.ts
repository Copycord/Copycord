import { describe, expect, it } from "vitest";
import { busStream, connectSse, logStream } from "./sse";

class FakeSource {
  static last: FakeSource | undefined;
  closed = false;
  onmessage: ((ev: MessageEvent) => void) | null = null;
  onerror: ((ev: Event) => void) | null = null;
  constructor(
    public url: string,
    public init?: EventSourceInit,
  ) {
    FakeSource.last = this;
  }
  close() {
    this.closed = true;
  }
  emit(data: string) {
    this.onmessage?.({ data } as MessageEvent);
  }
}

const Impl = FakeSource as unknown as typeof EventSource;

describe("sse", () => {
  it("parses JSON events, skips heartbeats and closes", () => {
    const got: unknown[] = [];
    const client = busStream({ onMessage: (m) => got.push(m), EventSourceImpl: Impl });
    const src = FakeSource.last!;
    expect(src.url).toBe("/bus/stream");
    expect(src.init?.withCredentials).toBe(true);
    src.emit('{"kind":"status"}');
    src.emit(": ping");
    expect(got).toEqual([{ kind: "status" }]);
    client.close();
    expect(src.closed).toBe(true);
  });

  it("passes raw lines for log streams", () => {
    const got: string[] = [];
    logStream("server", { onMessage: (m) => got.push(m), EventSourceImpl: Impl });
    expect(FakeSource.last!.url).toBe("/logs/stream/server");
    FakeSource.last!.emit("a log line");
    expect(got).toEqual(["a log line"]);
  });

  it("reports errors", () => {
    let errors = 0;
    connectSse("/x", { onMessage: () => {}, onError: () => (errors += 1), EventSourceImpl: Impl });
    FakeSource.last!.onerror?.(new Event("error"));
    expect(errors).toBe(1);
  });
});
