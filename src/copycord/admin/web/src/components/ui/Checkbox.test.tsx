// Checkbox and Radio: real inputs with the shared look, the checked state and the indeterminate flag.

import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { Checkbox, Radio } from "./Checkbox";

describe("Checkbox", () => {
  it("renders a styled native checkbox and reports changes", () => {
    const onChange = vi.fn();
    render(<Checkbox aria-label="Dry run" checked={false} onChange={onChange} />);
    const box = screen.getByRole("checkbox", { name: "Dry run" });
    expect(box).not.toBeChecked();
    expect(box.className).toContain("appearance-none");
    fireEvent.click(box);
    expect(onChange).toHaveBeenCalledOnce();
  });

  it("is checked when checked", () => {
    render(<Checkbox aria-label="Dry run" checked readOnly />);
    expect(screen.getByRole("checkbox", { name: "Dry run" })).toBeChecked();
  });

  it("sets the DOM indeterminate flag and clears it again", () => {
    const { rerender } = render(<Checkbox aria-label="Category" checked={false} indeterminate readOnly />);
    const box = screen.getByRole("checkbox", { name: "Category" }) as HTMLInputElement;
    expect(box.indeterminate).toBe(true);
    expect(box).toHaveAttribute("aria-checked", "mixed");
    rerender(<Checkbox aria-label="Category" checked readOnly />);
    expect(box.indeterminate).toBe(false);
    expect(box).not.toHaveAttribute("aria-checked");
    expect(box).toBeChecked();
  });

  it("is disabled when disabled", () => {
    render(<Checkbox aria-label="Busy" disabled />);
    expect(screen.getByRole("checkbox", { name: "Busy" })).toBeDisabled();
  });
});

describe("Radio", () => {
  it("renders a styled native radio and is checked when checked", () => {
    render(
      <>
        <Radio name="r" aria-label="One" checked readOnly />
        <Radio name="r" aria-label="Two" checked={false} readOnly />
      </>,
    );
    const one = screen.getByRole("radio", { name: "One" });
    expect(one).toBeChecked();
    expect(one.className).toContain("rounded-full");
    expect(screen.getByRole("radio", { name: "Two" })).not.toBeChecked();
  });
});
