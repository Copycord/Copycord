import { readdirSync, readFileSync } from "node:fs";
import { join, sep } from "node:path";
import { render, renderHook, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { REVEAL, Skeleton, SkeletonSection, useReveal } from "./Skeleton";

describe("Skeleton", () => {
  it("renders hidden shimmer blocks sized by props", () => {
    const { container } = render(
      <>
        <Skeleton.Line width="40%" />
        <Skeleton.Block width={96} height={96} />
        <Skeleton.Card height={118} testId="card" />
        <Skeleton.Row cols={4} />
      </>,
    );
    const blocks = container.querySelectorAll(".cc-skeleton");
    expect(blocks.length).toBe(1 + 1 + 3 + 4);
    for (const b of blocks) expect(b).toHaveAttribute("aria-hidden", "true");
    expect((blocks[1] as HTMLElement).style.width).toBe("96px");
    expect(screen.getByTestId("card").style.height).toBe("118px");
  });

  it("drops the sweep under reduced motion", () => {
    const { container } = render(<Skeleton.Line />);
    expect(container.querySelector(".cc-skeleton")).toHaveClass("motion-reduce:animate-none");
  });

  it("Text takes a sample of the text and a line of its own; Box takes the element's classes", () => {
    const { container } = render(
      <>
        <Skeleton.Text className="text-xs" block>
          12 runs
        </Skeleton.Text>
        <Skeleton.Box id="count" className="rounded-xl border px-2.5">
          0 tokens
        </Skeleton.Box>
      </>,
    );
    const [text, box] = container.querySelectorAll("[data-skeleton]");
    expect(text).toHaveClass("text-xs", "block", "cc-skeleton", "text-transparent");
    expect(text).toHaveTextContent("12 runs");
    expect(box).toHaveAttribute("id", "count");
    expect(box).toHaveClass("rounded-xl", "px-2.5", "cc-skeleton", "border-transparent", "text-transparent");
    for (const el of [text, box]) expect(el).toHaveAttribute("aria-hidden", "true");
  });

  it("no page styles a placeholder of its own: cc-skeleton and animate-pulse bars live here only", () => {
    // vitest runs from the web app folder.
    const root = join(process.cwd(), "src");
    const own = new Set(["components/ui/Skeleton.tsx"]);
    const offenders: string[] = [];
    for (const rel of readdirSync(root, { recursive: true }) as string[]) {
      const path = rel.split(sep).join("/");
      if (!/\.tsx?$/.test(path) || /\.test\.tsx?$/.test(path) || own.has(path)) continue;
      const src = readFileSync(join(root, rel), "utf8");
      // A pulsing status dot (Badge, the Scraper pill) is a live value, not a loading placeholder.
      if (src.includes("cc-skeleton") || /animate-pulse[^"]*bg-/.test(src)) offenders.push(path);
    }
    expect(offenders).toEqual([]);
  });

  it("announces a section once as busy", () => {
    const { container } = render(<SkeletonSection header rows={3} label="Loading guilds" />);
    const status = screen.getByRole("status", { name: "Loading guilds" });
    expect(status).toHaveAttribute("aria-busy", "true");
    expect(container.querySelectorAll(".cc-skeleton").length).toBe(1 + 3 * 3);
  });
});

describe("useReveal", () => {
  it("fades content in once it replaces a placeholder, and keeps the class through refetches", () => {
    const { result, rerender } = renderHook(({ ready }) => useReveal(ready), { initialProps: { ready: false } });
    expect(result.current).toBeUndefined();
    rerender({ ready: true });
    expect(result.current).toBe(REVEAL);
    rerender({ ready: true });
    expect(result.current).toBe(REVEAL);
  });

  it("does not fade content that was there from the first paint", () => {
    const { result, rerender } = renderHook(({ ready }) => useReveal(ready), { initialProps: { ready: true } });
    expect(result.current).toBeUndefined();
    rerender({ ready: true });
    expect(result.current).toBeUndefined();
  });
});
