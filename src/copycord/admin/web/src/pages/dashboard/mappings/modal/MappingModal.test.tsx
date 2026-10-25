import { render } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ToastProvider } from "../../../../components/Toast";
import { ConfirmProvider } from "../../../../components/ui/ConfirmDialog";
import { MappingModal } from "./MappingModal";

function renderModal() {
  vi.stubGlobal("fetch", vi.fn(async () => new Response("not found", { status: 404 })));
  render(
    <ToastProvider>
      <ConfirmProvider>
        <MappingModal mapping={null} mode="create" onClose={() => {}} onSaved={() => {}} />
      </ConfirmProvider>
    </ToastProvider>,
  );
  return {
    form: document.getElementById("mapping-form")!,
    toolbar: document.querySelector<HTMLElement>(".mapping-toolbar")!,
    footer: document.querySelector<HTMLElement>(".mapping-form-footer")!,
  };
}

const classes = (el: HTMLElement) => el.className.split(/\s+/);

afterEach(() => vi.unstubAllGlobals());

describe("MappingModal sticky search toolbar", () => {
  it("sticks flush at the top of the scroller with no top padding above it", () => {
    const { form } = renderModal();
    expect(classes(form)).toContain("px-6");
    expect(classes(form).filter((c) => /^(p|pt|py)-/.test(c))).toEqual([]);
    // The space above the first section lives inside it, so nothing scrolls into a band above the bar.
    expect(classes(form.querySelector<HTMLElement>(".mapping-section")!)).toContain("pt-[34px]");
  });

  it("is full bleed, opaque, edged and above the sections", () => {
    const { toolbar } = renderModal();
    const c = classes(toolbar);
    expect(c).toEqual(expect.arrayContaining(["sticky", "top-0", "-mx-6", "px-6", "bg-surface-deep", "border-b", "z-50"]));
    expect(c.some((x) => /^-mx-(?!6$)/.test(x) || /^px-(?!6$)/.test(x))).toBe(false);
    expect(c.some((x) => x.startsWith("bg-") && x.includes("/"))).toBe(false);
  });

  it("gives the footer the same full width", () => {
    const { footer } = renderModal();
    expect(classes(footer)).toEqual(expect.arrayContaining(["sticky", "bottom-0", "-mx-6", "px-6", "bg-surface-deep"]));
  });
});
