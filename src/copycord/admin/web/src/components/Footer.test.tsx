import { render } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { Footer } from "./Footer";

/** The version box: the element right inside the bdi, its classes and what it holds. */
function versionBox(version: string | null | undefined) {
  const { container, unmount } = render(<Footer version={version} />);
  const box = container.querySelector("bdi")!.firstElementChild!;
  const out = { className: box.className, text: box.textContent, skeletons: container.querySelectorAll("[data-skeleton], .cc-skeleton").length };
  unmount();
  return out;
}

describe("Footer version box", () => {
  it("has no shimmer: an empty box while /version loads, then the version or dev in that same box", () => {
    const loading = versionBox(undefined);
    const dev = versionBox(null);
    const tagged = versionBox("v3.28.0");
    expect(loading).toEqual({ className: dev.className, text: "", skeletons: 0 });
    expect(dev).toMatchObject({ text: "dev", skeletons: 0 });
    expect(tagged).toMatchObject({ className: dev.className, text: "v3.28.0", skeletons: 0 });
  });

  it("keeps the dev pill and the release link", () => {
    const { container } = render(<Footer version="v3.28.0" />);
    expect(container.querySelector("#footer-version-link")).toHaveAttribute("href", expect.stringContaining("/releases/tag/v3.28.0"));
  });
});
