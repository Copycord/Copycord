import { describe, expect, it } from "vitest";
import { activeText, badgeText } from "./TasksSkeleton";
import { railTitle } from "./TypeRail";

describe("Rail counts", () => {
  it("never counts a queued run as running: the badge names running runs, else the queued ones", () => {
    expect(badgeText(1, 0)).toBe("1 running");
    expect(badgeText(3, 2)).toBe("1 running");
    expect(badgeText(2, 2)).toBe("2 queued");
    expect(activeText(3, 2)).toBe("1 running, 2 queued");
    expect(railTitle(4, 3, 2)).toBe("4 runs, 1 running, 2 queued");
    expect(railTitle(1, 1, 1)).toBe("1 run, 1 queued");
    expect(railTitle(2, 0)).toBe("2 runs");
  });
});
