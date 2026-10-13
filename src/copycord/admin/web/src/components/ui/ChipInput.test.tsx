import { fireEvent, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { useState } from "react";
import { describe, expect, it, vi } from "vitest";
import { ChipInput, mergeChips, splitChips } from "./ChipInput";
import type { ChipInputProps } from "./ChipInput";
import { Field } from "./Field";

function Harness(props: Partial<ChipInputProps> & { initial?: string[]; spy?: (v: string[]) => void }) {
  const { initial = [], spy, ...rest } = props;
  const [value, setValue] = useState<string[]>(initial);
  return (
    <ChipInput
      aria-label="Words"
      {...rest}
      value={value}
      onChange={(v) => {
        spy?.(v);
        setValue(v);
      }}
    />
  );
}

const chips = () => screen.queryAllByRole("button", { name: /^Remove / }).map((b) => b.textContent);

describe("splitChips", () => {
  it("splits words on comma, pipe and newline, trims and drops blanks", () => {
    expect(splitChips(" a , b|c\n\n d ,,")).toEqual(["a", "b", "c", "d"]);
  });

  it("caps words at 100 characters", () => {
    expect(splitChips("x".repeat(150))[0]).toHaveLength(100);
  });

  it("keeps only positive integer runs in ids mode", () => {
    expect(splitChips("123, 0 abc 456\n007 -9", "ids")).toEqual(["123", "456", "7", "9"]);
    expect(splitChips("no digits", "ids")).toEqual([]);
  });

  it("handles snowflakes beyond Number precision", () => {
    expect(splitChips("1234567890123456789012", "ids")).toEqual(["1234567890123456789012"]);
  });
});

describe("mergeChips", () => {
  it("dedupes against current values and inside the batch", () => {
    expect(mergeChips(["a"], ["a", "b", "b"]).values).toEqual(["a", "b"]);
  });

  it("reports values rejected by validate", () => {
    const r = mergeChips([], ["ok", "bad"], (v) => (v === "bad" ? "not allowed" : null));
    expect(r.values).toEqual(["ok"]);
    expect(r.rejected).toEqual([{ value: "bad", error: "not allowed" }]);
  });
});

describe("ChipInput", () => {
  it("commits on Enter and on comma, then clears the entry", async () => {
    const user = userEvent.setup();
    render(<Harness />);
    const entry = screen.getByLabelText("Words");
    await user.type(entry, "spam{Enter}eggs,");
    expect(chips()).toEqual(["spam", "eggs"]);
    expect(entry).toHaveValue("");
  });

  it("does not split on comma in ids mode but drops non digits", async () => {
    const user = userEvent.setup();
    render(<Harness mode="ids" />);
    await user.type(screen.getByLabelText("Words"), "12,34{Enter}");
    expect(chips()).toEqual(["12", "34"]);
  });

  it("splits pasted text", async () => {
    const user = userEvent.setup();
    render(<Harness />);
    await user.click(screen.getByLabelText("Words"));
    await user.paste("one, two\nthree|one");
    expect(chips()).toEqual(["one", "two", "three"]);
  });

  it("removes the last chip with Backspace or Delete on an empty entry", async () => {
    const user = userEvent.setup();
    render(<Harness initial={["a", "b", "c"]} />);
    await user.click(screen.getByLabelText("Words"));
    await user.keyboard("{Backspace}");
    expect(chips()).toEqual(["a", "b"]);
    await user.keyboard("{Delete}");
    expect(chips()).toEqual(["a"]);
  });

  it("Backspace with text in the entry edits text instead of removing chips", async () => {
    const user = userEvent.setup();
    render(<Harness initial={["a"]} />);
    await user.type(screen.getByLabelText("Words"), "xy{Backspace}");
    expect(chips()).toEqual(["a"]);
    expect(screen.getByLabelText("Words")).toHaveValue("x");
  });

  it("ignores duplicates without calling onChange", async () => {
    const spy = vi.fn();
    const user = userEvent.setup();
    render(<Harness initial={["a"]} spy={spy} />);
    await user.type(screen.getByLabelText("Words"), "a{Enter}");
    expect(spy).not.toHaveBeenCalled();
    expect(chips()).toEqual(["a"]);
  });

  it("removes a chip on click and on Delete when focused", async () => {
    const user = userEvent.setup();
    render(<Harness initial={["a", "b"]} />);
    await user.click(screen.getByRole("button", { name: "Remove a" }));
    expect(chips()).toEqual(["b"]);
    screen.getByRole("button", { name: "Remove b" }).focus();
    await user.keyboard("{Delete}");
    expect(chips()).toEqual([]);
  });

  it("shows validation errors and calls onReject", async () => {
    const onReject = vi.fn();
    const user = userEvent.setup();
    render(<Harness validate={(v) => (v.length < 3 ? "too short" : undefined)} onReject={onReject} />);
    await user.type(screen.getByLabelText("Words"), "ab|abcd{Enter}");
    expect(chips()).toEqual(["abcd"]);
    expect(screen.getByRole("alert")).toHaveTextContent("ab: too short");
    expect(onReject).toHaveBeenCalledWith([{ value: "ab", error: "too short" }]);
    expect(screen.getByLabelText("Words")).toHaveAttribute("aria-invalid", "true");
  });

  it("does nothing when locked", async () => {
    const spy = vi.fn();
    const user = userEvent.setup();
    render(<Harness initial={["a"]} spy={spy} locked />);
    const entry = screen.getByLabelText("Words");
    await user.type(entry, "b{Enter}");
    fireEvent.keyDown(entry, { key: "Backspace" });
    await user.click(screen.getByRole("button", { name: "Remove a" }));
    expect(spy).not.toHaveBeenCalled();
    expect(chips()).toEqual(["a"]);
  });

  it("takes its label and help from Field", () => {
    render(
      <Field label="Blocked words" help="Comma separated">
        <ChipInput value={[]} onChange={() => {}} />
      </Field>,
    );
    const entry = screen.getByLabelText("Blocked words");
    expect(entry).toHaveAccessibleDescription("Comma separated");
  });
});
