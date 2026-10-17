import { fireEvent, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { useState } from "react";
import { describe, expect, it, vi } from "vitest";
import { Modal } from "../Modal";
import { Field, FieldSelect } from "./Field";
import type { SelectOption } from "./Field";

const OPTIONS: SelectOption[] = [
  { value: "", label: "Pick a server", disabled: true },
  { value: "1", label: "Alpha" },
  { value: "2", label: "Beta" },
  { value: "3", label: "Gamma" },
];

function Picker({ onChange = () => {}, onClose = () => {} }: { onChange?: (v: string) => void; onClose?: () => void }) {
  const [value, setValue] = useState("");
  return (
    <Modal open onClose={onClose} title="Run">
      <Field label="Server">
        <FieldSelect
          value={value}
          options={OPTIONS}
          onChange={(v) => {
            setValue(v);
            onChange(v);
          }}
        />
      </Field>
    </Modal>
  );
}

const combo = () => screen.getByRole("combobox", { name: "Server" });

describe("FieldSelect", () => {
  it("floats the open list outside the modal box and leaves the placeholder out of it", async () => {
    const user = userEvent.setup();
    render(<Picker />);
    expect(combo()).toHaveTextContent("Pick a server");
    expect(combo().querySelector(".text-muted")).not.toBeNull();

    await user.click(combo());
    const list = screen.getByRole("listbox");
    expect(screen.getByRole("dialog")).not.toContainElement(list);
    expect(list.parentElement).toBe(document.body);
    expect(list.style.position).toBe("fixed");
    expect(screen.getAllByRole("option").map((o) => o.textContent)).toEqual(["Alpha", "Beta", "Gamma"]);
    expect(screen.queryByRole("option", { name: "Pick a server" })).toBeNull();
    expect(screen.queryByRole("option", { selected: true })).toBeNull();
  });

  it("closes on Escape without closing the modal, on an outside click and on a page scroll", async () => {
    const user = userEvent.setup();
    const onClose = vi.fn();
    render(<Picker onClose={onClose} />);

    await user.click(combo());
    fireEvent.keyDown(combo(), { key: "Escape" });
    expect(screen.queryByRole("listbox")).toBeNull();
    expect(onClose).not.toHaveBeenCalled();

    await user.click(combo());
    fireEvent.mouseDown(screen.getByText("Run"));
    expect(screen.queryByRole("listbox")).toBeNull();

    await user.click(combo());
    fireEvent.scroll(screen.getByRole("listbox"));
    expect(screen.getByRole("listbox")).toBeInTheDocument();
    fireEvent.scroll(window);
    expect(screen.queryByRole("listbox")).toBeNull();
    expect(combo()).toHaveAttribute("aria-expanded", "false");
  });

  it("picks with the keyboard, stepping over the placeholder", async () => {
    const onChange = vi.fn();
    render(<Picker onChange={onChange} />);

    fireEvent.keyDown(combo(), { key: "ArrowDown" });
    expect(combo()).toHaveAttribute("aria-expanded", "true");
    expect(combo().getAttribute("aria-activedescendant")).toBe(screen.getByRole("option", { name: "Alpha" }).id);

    fireEvent.keyDown(combo(), { key: "ArrowUp" });
    expect(combo().getAttribute("aria-activedescendant")).toBe(screen.getByRole("option", { name: "Alpha" }).id);
    fireEvent.keyDown(combo(), { key: "End" });
    fireEvent.keyDown(combo(), { key: "ArrowUp" });
    expect(combo().getAttribute("aria-activedescendant")).toBe(screen.getByRole("option", { name: "Beta" }).id);
    fireEvent.keyDown(combo(), { key: "Enter" });

    expect(onChange).toHaveBeenCalledWith("2");
    expect(screen.queryByRole("listbox")).toBeNull();
    expect(combo()).toHaveTextContent("Beta");
    expect(combo().querySelector(".text-muted")).toBeNull();

    fireEvent.keyDown(combo(), { key: "ArrowDown" });
    expect(screen.getByRole("option", { name: "Beta" })).toHaveAttribute("aria-selected", "true");
    expect(combo().getAttribute("aria-activedescendant")).toBe(screen.getByRole("option", { name: "Beta" }).id);
  });

  it("opens upward when the room under the toggle is short", async () => {
    const user = userEvent.setup();
    render(<Picker />);
    vi.spyOn(combo(), "getBoundingClientRect").mockReturnValue({
      top: window.innerHeight - 60,
      bottom: window.innerHeight - 20,
      left: 10,
      right: 310,
      width: 300,
      height: 40,
      x: 10,
      y: window.innerHeight - 60,
      toJSON: () => ({}),
    });
    await user.click(combo());
    const list = screen.getByRole("listbox");
    expect(list.style.bottom).toBe("64px");
    expect(list.style.top).toBe("");
    expect(list.style.width).toBe("300px");
  });

  it("shows the placeholder as a muted note when nothing is left to pick", async () => {
    const user = userEvent.setup();
    render(
      <Field label="Server">
        <FieldSelect value="" options={[{ value: "", label: "No server matches", disabled: true }]} onChange={() => {}} />
      </Field>,
    );
    await user.click(combo());
    expect(screen.queryAllByRole("option")).toHaveLength(0);
    expect(screen.getByRole("listbox")).toHaveTextContent("No server matches");
  });
});
