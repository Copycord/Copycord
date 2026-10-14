import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { Skeleton, SkeletonSection } from "./Skeleton";

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

  it("announces a section once as busy", () => {
    const { container } = render(<SkeletonSection header rows={3} label="Loading guilds" />);
    const status = screen.getByRole("status", { name: "Loading guilds" });
    expect(status).toHaveAttribute("aria-busy", "true");
    expect(container.querySelectorAll(".cc-skeleton").length).toBe(1 + 3 * 3);
  });
});
