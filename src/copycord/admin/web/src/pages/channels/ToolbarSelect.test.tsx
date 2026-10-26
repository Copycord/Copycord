import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { useState } from "react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ToolbarSelect } from "./ToolbarSelect";
import type { ToolbarOption } from "./ToolbarSelect";

const OPTIONS: ToolbarOption[] = [
  { value: "", label: "All mappings" },
  { value: "1", label: "wdr" },
  { value: "2", label: "osrs (paused)" },
  { value: "3", label: "A mapping with a really long name that runs past any sensible list width" },
];

function Picker({ variant, onChange = () => {} }: { variant?: "toolbar" | "field"; onChange?: (v: string) => void }) {
  const [value, setValue] = useState("1");
  return (
    <ToolbarSelect
      id="pick"
      label="Filter by mapping"
      value={value}
      options={OPTIONS}
      variant={variant}
      onChange={(v) => {
        setValue(v);
        onChange(v);
      }}
    />
  );
}

const width = window.innerWidth;

/** Puts every button (the toggle) at left..left+w in a viewport of vw. */
function placeToggle(left: number, w: number, vw: number) {
  window.innerWidth = vw;
  vi.spyOn(HTMLButtonElement.prototype, "getBoundingClientRect").mockReturnValue({
    left,
    right: left + w,
    width: w,
    top: 100,
    bottom: 134,
    height: 34,
    x: left,
    y: 100,
    toJSON: () => ({}),
  } as DOMRect);
}

afterEach(() => {
  vi.restoreAllMocks();
  window.innerWidth = width;
});

describe("ToolbarSelect toolbar list", () => {
  it("is at least as wide as the toggle, fits its longest label and never wraps one", async () => {
    placeToggle(20, 70, 1200);
    const user = userEvent.setup();
    render(<Picker />);
    await user.click(screen.getByRole("button"));
    const list = screen.getByRole("listbox", { name: "Filter by mapping" });
    expect(list.className).toContain("absolute");
    expect(list.className).toContain("min-w-full");
    expect(list.className).toContain("w-max");
    expect(list.className).not.toContain("inset-x-0");
    for (const o of screen.getAllByRole("option")) {
      expect(o.className).toContain("truncate");
      expect(o.className).not.toContain("break-words");
    }
    const long = screen.getByRole("option", { name: OPTIONS[3]!.label });
    expect(long).toHaveAttribute("title", OPTIONS[3]!.label);
    // Stays in the wrapper: no portal.
    expect(list.parentElement).toBe(screen.getByRole("button").parentElement);
  });

  it("grows rightwards from a toggle near the left edge", async () => {
    placeToggle(20, 70, 1200);
    const user = userEvent.setup();
    render(<Picker />);
    await user.click(screen.getByRole("button"));
    const list = screen.getByRole("listbox");
    expect(list.style.left).toBe("0px");
    expect(list.style.right).toBe("auto");
    expect(list.style.maxWidth).toBe("320px");
  });

  it("grows leftwards from a toggle at the right edge", async () => {
    placeToggle(1112, 70, 1200);
    const user = userEvent.setup();
    render(<Picker />);
    await user.click(screen.getByRole("button"));
    const list = screen.getByRole("listbox");
    expect(list.style.right).toBe("0px");
    expect(list.style.left).toBe("auto");
    expect(list.style.maxWidth).toBe("320px");
  });

  it("caps the list to the room on its side in a narrow window", async () => {
    // 390 wide: 374 px fit between the margins; the toggle at 150..250 leaves 232 right and 242 left.
    placeToggle(150, 100, 390);
    const user = userEvent.setup();
    render(<Picker />);
    await user.click(screen.getByRole("button"));
    const list = screen.getByRole("listbox");
    expect(list.style.right).toBe("0px");
    expect(list.style.maxWidth).toBe("242px");
  });

  it("opens and picks with the keyboard as before", async () => {
    placeToggle(20, 70, 1200);
    const onChange = vi.fn();
    const user = userEvent.setup();
    render(<Picker onChange={onChange} />);
    screen.getByRole("button").focus();
    await user.keyboard("{ArrowDown}");
    expect(screen.getByRole("listbox")).toBeInTheDocument();
    await user.keyboard("{ArrowDown}{Enter}");
    expect(onChange).toHaveBeenCalledWith("2");
    expect(screen.queryByRole("listbox")).not.toBeInTheDocument();
    expect(screen.getByRole("button")).toHaveTextContent("osrs (paused)");
    expect(document.getElementById("pick")).toHaveValue("2");
  });
});

describe("ToolbarSelect field variant", () => {
  it("keeps its portalled, fixed list under the toggle with wrapping rows", async () => {
    placeToggle(40, 300, 1200);
    const user = userEvent.setup();
    render(<Picker variant="field" />);
    await user.click(screen.getByRole("combobox"));
    const list = screen.getByRole("listbox");
    expect(list.parentElement).toBe(document.body);
    expect(list.style.position).toBe("fixed");
    expect(list.style.left).toBe("40px");
    expect(list.style.width).toBe("300px");
    expect(list.style.top).toBe("138px");
    expect(list.className).toContain("z-[70]");
    expect(list.className).not.toContain("w-max");
    for (const o of screen.getAllByRole("option")) {
      expect(o.className).toContain("break-words");
      expect(o.className).not.toContain("truncate");
      expect(o).not.toHaveAttribute("title");
    }
  });
});
