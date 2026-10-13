import { act, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { ToastProvider, useToast } from "./Toast";

function Buttons() {
  const toast = useToast();
  return (
    <>
      <button onClick={() => toast.show("Saved")}>ok</button>
      <button onClick={() => toast.show("Failed", { kind: "error", timeout: 0 })}>bad</button>
    </>
  );
}

describe("Toast", () => {
  it("shows a toast and hides it after the timeout", () => {
    vi.useFakeTimers();
    render(
      <ToastProvider>
        <Buttons />
      </ToastProvider>,
    );
    act(() => screen.getByText("ok").click());
    expect(screen.getByText("Saved")).toBeInTheDocument();
    act(() => {
      vi.advanceTimersByTime(4000);
    });
    expect(screen.queryByText("Saved")).toBeNull();
  });

  it("shows errors as alerts that stay until dismissed", () => {
    render(
      <ToastProvider>
        <Buttons />
      </ToastProvider>,
    );
    act(() => screen.getByText("bad").click());
    expect(screen.getByRole("alert")).toHaveTextContent("Failed");
    act(() => screen.getByLabelText("Dismiss").click());
    expect(screen.queryByRole("alert")).toBeNull();
  });

  it("throws outside a provider", () => {
    vi.spyOn(console, "error").mockImplementation(() => {});
    expect(() => render(<Buttons />)).toThrow(/ToastProvider/);
  });
});
