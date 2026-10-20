import confetti from "canvas-confetti";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { celebrate } from "./celebrate";

// canvas-confetti is a no-op mock for every test (test/setup.ts).
const fire = vi.mocked(confetti);

describe("celebrate", () => {
  beforeEach(() => {
    fire.mockClear();
    vi.useFakeTimers();
  });
  afterEach(() => {
    delete (window as { matchMedia?: unknown }).matchMedia;
  });

  it("fires v3's confetti: a left and a right burst every 250 ms for three seconds, above every layer", async () => {
    await celebrate();
    expect(fire).not.toHaveBeenCalled();
    vi.advanceTimersByTime(250);
    expect(fire).toHaveBeenCalledTimes(2);
    const [left, right] = fire.mock.calls.map((c) => c[0]!);
    for (const burst of [left!, right!]) {
      expect(burst).toMatchObject({
        startVelocity: 30,
        spread: 360,
        ticks: 60,
        zIndex: 99999,
        colors: ["#8b5cf6", "#a78bfa", "#c4b5fd", "#ef4444", "#f87171", "#fca5a5"],
      });
      expect(burst.particleCount).toBeCloseTo(50 * (2750 / 3000));
      expect(burst.origin!.y).toBeGreaterThanOrEqual(-0.2);
      expect(burst.origin!.y).toBeLessThan(0.8);
    }
    expect(left!.origin!.x).toBeGreaterThanOrEqual(0.1);
    expect(left!.origin!.x).toBeLessThan(0.3);
    expect(right!.origin!.x).toBeGreaterThanOrEqual(0.7);
    expect(right!.origin!.x).toBeLessThan(0.9);

    // Ticks at 250 to 2750 ms fire; the tick at 3000 ms stops the interval.
    vi.advanceTimersByTime(3000);
    expect(fire).toHaveBeenCalledTimes(22);
    vi.advanceTimersByTime(1000);
    expect(fire).toHaveBeenCalledTimes(22);
  });

  it("still plays under prefers-reduced-motion", async () => {
    Object.defineProperty(window, "matchMedia", {
      configurable: true,
      value: (query: string) => ({ matches: query === "(prefers-reduced-motion: reduce)" }),
    });
    try {
      await celebrate();
      vi.advanceTimersByTime(4000);
      expect(fire).toHaveBeenCalledTimes(22);
    } finally {
      delete (window as { matchMedia?: unknown }).matchMedia;
    }
  });
});
