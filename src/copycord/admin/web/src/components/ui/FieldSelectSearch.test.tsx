import { render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { useState } from "react";
import { describe, expect, it } from "vitest";
import { Field, FieldSelect } from "./Field";
import type { SelectOption } from "./Field";

const OPTIONS: SelectOption[] = [
  { value: "", label: "Pick a server", disabled: true },
  { value: "111", label: "Alpha" },
  { value: "222", label: "Beta" },
  { value: "333", label: "Alphabet soup" },
];

function Picker({ initial = "", pending = false }: { initial?: string; pending?: boolean }) {
  const [value, setValue] = useState(initial);
  return (
    <>
      <Field label="Server">
        <FieldSelect
          searchable
          pending={pending}
          value={value}
          options={pending ? OPTIONS.slice(0, 1) : OPTIONS}
          onChange={setValue}
        />
      </Field>
      <output>{value || "none"}</output>
    </>
  );
}

const combo = () => screen.getByRole("combobox", { name: "Server" }) as HTMLInputElement;
const rows = () => within(screen.getByRole("listbox")).queryAllByRole("option").map((o) => o.textContent);

describe("FieldSelect searchable", () => {
  it("is a text box that shows the chosen label closed and filters by name or ID open", async () => {
    const user = userEvent.setup();
    render(<Picker initial="222" />);
    expect(combo().tagName).toBe("INPUT");
    expect(combo()).toHaveValue("Beta");
    expect(combo()).toHaveAttribute("aria-expanded", "false");
    await user.click(combo());
    expect(combo()).toHaveAttribute("aria-expanded", "true");
    expect(combo()).toHaveValue("");
    expect(rows()).toEqual(["Alpha", "Beta", "Alphabet soup"]);
    await user.type(combo(), "ALPHA");
    expect(rows()).toEqual(["Alpha", "Alphabet soup"]);
    await user.clear(combo());
    await user.type(combo(), "33");
    expect(rows()).toEqual(["Alphabet soup"]);
  });

  it("picks with the arrow keys and Enter", async () => {
    const user = userEvent.setup();
    render(<Picker />);
    await user.click(combo());
    await user.type(combo(), "alp");
    expect(combo()).toHaveAttribute("aria-activedescendant", `${combo().id}-listbox-1`);
    await user.keyboard("{ArrowDown}");
    expect(combo()).toHaveAttribute("aria-activedescendant", `${combo().id}-listbox-3`);
    await user.keyboard("{Enter}");
    expect(screen.queryByRole("listbox")).toBeNull();
    expect(screen.getByRole("status")).toHaveTextContent("333");
    expect(combo()).toHaveValue("Alphabet soup");
  });

  it("closes on Escape and keeps the old value, and closes on Tab", async () => {
    const user = userEvent.setup();
    render(<Picker initial="111" />);
    await user.click(combo());
    await user.type(combo(), "bet");
    await user.keyboard("{ArrowDown}{Escape}");
    expect(screen.queryByRole("listbox")).toBeNull();
    expect(combo()).toHaveValue("Alpha");
    expect(screen.getByRole("status")).toHaveTextContent("111");
    await user.click(combo());
    expect(screen.getByRole("listbox")).toBeInTheDocument();
    await user.tab();
    expect(screen.queryByRole("listbox")).toBeNull();
    expect(screen.getByRole("status")).toHaveTextContent("111");
  });

  it("shows a No matches row when nothing is left", async () => {
    const user = userEvent.setup();
    render(<Picker />);
    await user.click(combo());
    await user.type(combo(), "zzz");
    expect(rows()).toEqual([]);
    expect(screen.getByRole("listbox")).toHaveTextContent("No matches");
    expect(combo()).not.toHaveAttribute("aria-activedescendant");
    await user.keyboard("{Enter}");
    expect(screen.getByRole("status")).toHaveTextContent("none");
  });

  it("loads as the same read only box with its label shimmering, and a box opened while loading fills in", async () => {
    const user = userEvent.setup();
    const { rerender } = render(<Picker pending />);
    const box = combo();
    expect(box.tagName).toBe("INPUT");
    expect(box).toHaveAttribute("readonly");
    expect(box).not.toHaveAttribute("placeholder");
    expect(box.parentElement).toHaveAttribute("aria-busy", "true");
    expect(box.parentElement!.querySelector("[data-skeleton]")).not.toBeNull();
    const shape = box.className;
    await user.click(box);
    expect(rows()).toEqual([]);
    rerender(<Picker />);
    expect(combo()).toBe(box);
    expect(box.className).toBe(shape);
    expect(box).not.toHaveAttribute("readonly");
    expect(box.parentElement).not.toHaveAttribute("aria-busy");
    expect(box.parentElement!.querySelector("[data-skeleton]")).toBeNull();
    expect(rows()).toEqual(["Alpha", "Beta", "Alphabet soup"]);
    await user.type(box, "beta");
    expect(rows()).toEqual(["Beta"]);
  });
});
