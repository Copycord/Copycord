import { act, fireEvent, render } from "@testing-library/react";
import { afterAll, beforeAll, beforeEach, describe, expect, it, vi } from "vitest";
import { LogModal } from "./LogModal";

class FakeSource {
  static instances: FakeSource[] = [];
  onmessage: ((ev: MessageEvent) => void) | null = null;
  onerror: ((ev: Event) => void) | null = null;
  constructor(public url: string) {
    FakeSource.instances.push(this);
  }
  close() {}
  emit(data: string) {
    this.onmessage?.({ data } as MessageEvent);
  }
}
const EsImpl = FakeSource as unknown as typeof EventSource;
const last = () => FakeSource.instances[FakeSource.instances.length - 1]!;

// jsdom has no layout: give elements a 100px viewport, 20px per text line and a real scrollTop.
const LINE_PX = 20;
const VIEW_PX = 100;
const tops = new WeakMap<Element, number>();
const saved = ["scrollTop", "scrollHeight", "clientHeight"].map(
  (k) => [k, Object.getOwnPropertyDescriptor(Element.prototype, k)] as const,
);

beforeAll(() => {
  Object.defineProperty(Element.prototype, "scrollHeight", {
    configurable: true,
    get(this: Element) {
      return Math.max(VIEW_PX, (this.textContent ?? "").split("\n").length * LINE_PX);
    },
  });
  Object.defineProperty(Element.prototype, "clientHeight", { configurable: true, get: () => VIEW_PX });
  Object.defineProperty(Element.prototype, "scrollTop", {
    configurable: true,
    get(this: Element) {
      return tops.get(this) ?? 0;
    },
    set(this: Element, v: number) {
      tops.set(this, Math.max(0, Math.min(v, this.scrollHeight - VIEW_PX)));
    },
  });
});

afterAll(() => {
  for (const [k, d] of saved) if (d) Object.defineProperty(Element.prototype, k, d);
});

beforeEach(() => {
  FakeSource.instances = [];
});

const many = (n: number, tag = "line") => JSON.stringify({ lines: Array.from({ length: n }, (_, i) => `${tag} ${i}`) });
const body = () => document.getElementById("log-body")!;
const atBottom = (el: HTMLElement) => el.scrollTop === el.scrollHeight - el.clientHeight;

describe("LogModal scroll position", () => {
  it("opens pinned to the bottom with the tail already in view", () => {
    const { rerender } = render(<LogModal which={null} onClose={() => {}} EventSourceImpl={EsImpl} />);
    rerender(<LogModal which="server" onClose={() => {}} EventSourceImpl={EsImpl} />);
    act(() => last().emit(many(40)));
    expect(body().scrollTop).toBeGreaterThan(0);
    expect(atBottom(body())).toBe(true);
  });

  it("reopens without painting the previous tail at the top", () => {
    const { rerender } = render(<LogModal which="client" onClose={() => {}} EventSourceImpl={EsImpl} />);
    act(() => last().emit(many(40)));
    rerender(<LogModal which={null} onClose={() => {}} EventSourceImpl={EsImpl} />);
    rerender(<LogModal which="client" onClose={() => {}} EventSourceImpl={EsImpl} />);
    // First render after reopen: no stale lines sitting at scrollTop 0.
    expect(body().textContent).toBe("");
    act(() => last().emit(many(40, "again")));
    expect(atBottom(body())).toBe(true);
  });

  it("keeps the body hidden while backlog batches arrive, then shows it once at the bottom", () => {
    vi.useFakeTimers();
    try {
      render(<LogModal which="server" onClose={() => {}} EventSourceImpl={EsImpl} />);
      act(() => last().emit(many(50)));
      act(() => vi.advanceTimersByTime(100));
      act(() => last().emit(many(50, "batch2")));
      expect(body().className).toContain("invisible");
      expect(document.querySelector("[data-testid=log-loading] [data-skeleton]")).not.toBeNull();
      act(() => vi.advanceTimersByTime(200));
      expect(body().className).not.toContain("invisible");
      expect(document.querySelector("[data-testid=log-loading]")).toBeNull();
      expect(atBottom(body())).toBe(true);
    } finally {
      vi.useRealTimers();
    }
  });

  it("follows new lines at the bottom and stays put once the reader scrolls up", () => {
    render(<LogModal which="server" onClose={() => {}} EventSourceImpl={EsImpl} />);
    act(() => last().emit(many(40)));
    act(() => last().emit(many(5, "more")));
    expect(atBottom(body())).toBe(true);

    body().scrollTop = 100;
    fireEvent.scroll(body());
    act(() => last().emit(many(5, "later")));
    expect(body().scrollTop).toBe(100);
  });
});
