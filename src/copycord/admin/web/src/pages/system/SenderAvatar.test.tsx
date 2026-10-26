import { fireEvent, render } from "@testing-library/react";
import { afterEach, describe, expect, it } from "vitest";
import { SenderAvatar, forgetAvatars } from "./SenderAvatar";

const REAL = "https://cdn.discordapp.com/avatars/175928847299117063/abc.png?size=64";
// (175928847299117063 >> 22) % 6 == 2
const DEFAULT = "https://cdn.discordapp.com/embed/avatars/2.png";
const BRAND = ["#5865F2", "#747F8D", "#3BA55D", "#FAA61A", "#ED4245", "#EB459E"].map((hex) => {
  const n = Number.parseInt(hex.slice(1), 16);
  return `rgb(${n >> 16}, ${(n >> 8) & 255}, ${n & 255})`;
});

const box = (c: HTMLElement) => c.querySelector<HTMLElement>("[data-avatar]")!;
const img = (c: HTMLElement) => c.querySelector("img");
const shimmer = (c: HTMLElement) => c.querySelector("[data-skeleton]");

/** No element of the avatar paints one of Discord's six brand colours, inline or by class. */
function noBrandColour(c: HTMLElement) {
  for (const el of [c, ...c.querySelectorAll<HTMLElement>("*")]) {
    expect(BRAND).not.toContain(el.style.backgroundColor);
    expect(el.className).not.toMatch(/#[0-9a-f]{6}/i);
  }
}

afterEach(() => {
  forgetAvatars();
});

describe("SenderAvatar", () => {
  it("shows a shimmer in the 24 px box while the image loads, the image hidden under it", () => {
    const { container } = render(<SenderAvatar url={REAL} id="t1" />);
    expect(box(container)).toHaveAttribute("data-avatar", "loading");
    expect(box(container).className).toContain("size-6");
    expect(shimmer(container)!.className).toContain("size-6");
    expect(img(container)).toHaveAttribute("src", REAL);
    expect(img(container)!.className).toContain("opacity-0");
    noBrandColour(container);
  });

  it("shows the real image once it loads, with nothing behind it", () => {
    const { container } = render(<SenderAvatar url={REAL} id="t1" />);
    fireEvent.load(img(container)!);
    expect(box(container)).toHaveAttribute("data-avatar", "img");
    expect(shimmer(container)).toBeNull();
    expect(img(container)!.className).not.toContain("opacity-0");
    expect(img(container)!.style.backgroundColor).toBe("");
    noBrandColour(container);
  });

  it("falls to the Discord default avatar image of the account id when the real one fails", () => {
    const { container } = render(<SenderAvatar url={REAL} id="t1" />);
    fireEvent.error(img(container)!);
    expect(img(container)).toHaveAttribute("src", DEFAULT);
    expect(shimmer(container)).not.toBeNull();
    fireEvent.load(img(container)!);
    expect(box(container)).toHaveAttribute("data-avatar", "img");
    noBrandColour(container);
  });

  it("falls to a muted circle when the default image fails too", () => {
    const { container } = render(<SenderAvatar url={REAL} id="t1" />);
    fireEvent.error(img(container)!);
    fireEvent.error(img(container)!);
    expect(img(container)).toBeNull();
    expect(box(container)).toHaveAttribute("data-avatar", "none");
    expect(box(container).className).toContain("size-6");
    expect(box(container).className).toContain("bg-line-2");
    noBrandColour(container);
  });

  it("an API default URL that fails goes straight to the muted circle", () => {
    const { container } = render(<SenderAvatar url="https://cdn.discordapp.com/embed/avatars/5.png" id="t1" />);
    fireEvent.error(img(container)!);
    expect(box(container)).toHaveAttribute("data-avatar", "none");
  });

  it("no URL loads the default image of a stable row id hash", () => {
    // "d".charCodeAt(0) = 100, 100 % 6 = 4
    const { container } = render(<SenderAvatar url={null} id="d" />);
    expect(img(container)).toHaveAttribute("src", "https://cdn.discordapp.com/embed/avatars/4.png");
  });

  it("a rerender with the same URL keeps the image and never returns to the shimmer", () => {
    const { container, rerender } = render(<SenderAvatar url={REAL} id="t1" />);
    const el = img(container)!;
    fireEvent.load(el);
    rerender(<SenderAvatar url={REAL} id="t1" />);
    expect(img(container)).toBe(el);
    expect(shimmer(container)).toBeNull();
  });

  it("a fresh mount of an already loaded URL shows the image at once", () => {
    const first = render(<SenderAvatar url={REAL} id="t1" />);
    fireEvent.load(img(first.container)!);
    first.unmount();
    const { container } = render(<SenderAvatar url={REAL} id="t1" />);
    expect(box(container)).toHaveAttribute("data-avatar", "img");
    expect(shimmer(container)).toBeNull();
  });

  it("a new URL shows the new image, the old one held on screen until it lands", () => {
    const { container, rerender } = render(<SenderAvatar url={DEFAULT} id="t1" />);
    const el = img(container)!;
    fireEvent.load(el);
    rerender(<SenderAvatar url={REAL} id="t1" />);
    expect(img(container)).toBe(el);
    expect(el).toHaveAttribute("src", REAL);
    expect(shimmer(container)).toBeNull();
    fireEvent.load(el);
    expect(box(container)).toHaveAttribute("data-avatar", "img");
    expect(el).toHaveAttribute("src", REAL);
  });

  it("is locked in place: not draggable, not selectable, a drag start cancelled", () => {
    const { container } = render(<SenderAvatar url={REAL} id="t1" />);
    const el = img(container)!;
    expect(el).toHaveAttribute("draggable", "false");
    expect(el).toHaveAttribute("decoding", "async");
    expect(el).not.toHaveAttribute("loading");
    expect(el.className).toContain("pointer-events-none");
    expect(box(container).className).toContain("select-none");
    const drag = new Event("dragstart", { bubbles: true, cancelable: true });
    el.dispatchEvent(drag);
    expect(drag.defaultPrevented).toBe(true);
  });
});
