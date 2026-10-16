import { act, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ToastProvider, useToast } from "./Toast";

function Buttons() {
  const toast = useToast();
  return (
    <>
      <button onClick={() => toast.show("Saved")}>ok</button>
      <button onClick={() => toast.show("Done", { kind: "success" })}>yes</button>
      <button onClick={() => toast.show("Failed", { kind: "error", timeout: 0 })}>bad</button>
    </>
  );
}

function renderToasts() {
  render(
    <ToastProvider>
      <Buttons />
    </ToastProvider>,
  );
}

describe("Toast", () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  it("stacks toasts top right under the header like the old #toast-root", () => {
    renderToasts();
    act(() => screen.getByText("ok").click());
    const root = screen.getByText("Saved").closest("#toast-root");
    expect(root).not.toBeNull();
    expect(root).toHaveClass("fixed", "top-[calc(var(--spacing-header)+16px)]", "right-4", "items-end", "gap-2.5");
    expect(root).not.toHaveClass("bottom-4");
    expect(root).toHaveAttribute("aria-live", "polite");
  });

  it("gives each kind the old coloured bar", () => {
    renderToasts();
    act(() => screen.getByText("ok").click());
    act(() => screen.getByText("yes").click());
    act(() => screen.getByText("bad").click());
    const toastOf = (text: string) => screen.getByText(text).closest(".toast") as HTMLElement;
    const barOf = (text: string) => toastOf(text).querySelector(".toast-bar") as HTMLElement;
    expect(toastOf("Saved")).toHaveClass("toast-info", "rounded-[8px]", "border-l-transparent", "bg-control-top", "min-w-[280px]", "max-w-[440px]");
    expect(barOf("Saved")).toHaveClass("bg-[linear-gradient(180deg,var(--color-accent),var(--color-accent-deep)_80%)]");
    expect(toastOf("Done")).toHaveClass("toast-success");
    expect(barOf("Done")).toHaveClass("bg-[linear-gradient(180deg,var(--color-good),var(--color-good-deep)_80%)]");
    expect(toastOf("Failed")).toHaveClass("toast-error");
    expect(barOf("Failed")).toHaveClass("bg-[linear-gradient(180deg,var(--color-danger),var(--color-danger-deep)_80%)]");
    expect(screen.queryByLabelText("Dismiss")).toBeNull();
  });

  it("slides in on the next frame, starts leaving at 4000 ms and is gone 200 ms later", () => {
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "requestAnimationFrame", "cancelAnimationFrame"] });
    renderToasts();
    act(() => screen.getByText("ok").click());
    const toast = screen.getByText("Saved").closest(".toast") as HTMLElement;
    expect(toast).toHaveClass("opacity-0", "[transform:translateY(-10px)_scale(0.96)]");
    act(() => {
      vi.advanceTimersByTime(20);
    });
    expect(toast).toHaveClass("show", "opacity-100");
    act(() => {
      vi.advanceTimersByTime(3980);
    });
    expect(screen.getByText("Saved").closest(".toast")).not.toHaveClass("show");
    act(() => {
      vi.advanceTimersByTime(199);
    });
    expect(screen.getByText("Saved")).toBeInTheDocument();
    act(() => {
      vi.advanceTimersByTime(1);
    });
    expect(screen.queryByText("Saved")).toBeNull();
  });

  it("shows errors as alerts that stay until clicked", () => {
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "requestAnimationFrame", "cancelAnimationFrame"] });
    renderToasts();
    act(() => screen.getByText("bad").click());
    act(() => {
      vi.advanceTimersByTime(60000);
    });
    expect(screen.getByRole("alert")).toHaveTextContent("Failed");
    act(() => screen.getByRole("alert").click());
    act(() => {
      vi.advanceTimersByTime(200);
    });
    expect(screen.queryByRole("alert")).toBeNull();
  });

  it("throws outside a provider", () => {
    vi.spyOn(console, "error").mockImplementation(() => {});
    expect(() => render(<Buttons />)).toThrow(/ToastProvider/);
  });
});
