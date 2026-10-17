// Behaviour tests for the smaller components.

import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { useState } from "react";
import { describe, expect, it, vi } from "vitest";
import { Button } from "./Button";
import { Field, Input } from "./Field";
import { Tabs } from "./Tabs";
import { Toggle } from "./Toggle";
import { Tooltip } from "./Tooltip";

describe("Button", () => {
  it("defaults to type=button and fires onClick", async () => {
    const onClick = vi.fn();
    render(<Button onClick={onClick}>Save</Button>);
    const btn = screen.getByRole("button", { name: "Save" });
    expect(btn).toHaveAttribute("type", "button");
    await userEvent.setup().click(btn);
    expect(onClick).toHaveBeenCalledOnce();
  });

  it("is disabled and busy while loading", async () => {
    const onClick = vi.fn();
    render(
      <Button loading onClick={onClick}>
        Save
      </Button>,
    );
    const btn = screen.getByRole("button", { name: /Save/ });
    expect(btn).toBeDisabled();
    expect(btn).toHaveAttribute("aria-busy", "true");
    expect(screen.getByRole("status", { name: "Working" })).toBeInTheDocument();
    await userEvent.setup().click(btn);
    expect(onClick).not.toHaveBeenCalled();
  });
});

describe("Field", () => {
  it("labels the control and links help and error", () => {
    render(
      <Field label="Token" help="From the developer portal" error="Required">
        <Input />
      </Field>,
    );
    const input = screen.getByLabelText("Token");
    expect(input).toHaveAttribute("aria-invalid", "true");
    expect(input).toHaveAccessibleDescription("Required From the developer portal");
    expect(screen.getByRole("alert")).toHaveTextContent("Required");
  });
});

describe("Toggle", () => {
  function Harness({ disabled }: { disabled?: boolean }) {
    const [on, setOn] = useState(false);
    return <Toggle checked={on} onChange={setOn} label="Enable cloning" description="Copies new messages" disabled={disabled} />;
  }

  it("is a named switch toggled by click, Space and Enter", async () => {
    const user = userEvent.setup();
    render(<Harness />);
    const sw = screen.getByRole("switch", { name: "Enable cloning" });
    expect(sw).toHaveAccessibleDescription("Copies new messages");
    expect(sw).toHaveAttribute("aria-checked", "false");
    await user.click(sw);
    expect(sw).toHaveAttribute("aria-checked", "true");
    await user.keyboard(" ");
    expect(sw).toHaveAttribute("aria-checked", "false");
    await user.keyboard("{Enter}");
    expect(sw).toHaveAttribute("aria-checked", "true");
  });

  it("does not change when disabled", async () => {
    render(<Harness disabled />);
    const sw = screen.getByRole("switch");
    await userEvent.setup().click(sw);
    expect(sw).toHaveAttribute("aria-checked", "false");
  });
});

describe("Tabs", () => {
  const tabs = [
    { id: "a", label: "General", content: <p>General panel</p> },
    { id: "b", label: "Filters", content: <p>Filters panel</p>, disabled: true },
    { id: "c", label: "Advanced", content: <p>Advanced panel</p> },
  ];

  it("selects with click and arrow keys, skipping disabled tabs", async () => {
    const onChange = vi.fn();
    const user = userEvent.setup();
    render(<Tabs tabs={tabs} aria-label="Settings" onChange={onChange} />);
    expect(screen.getByRole("tablist", { name: "Settings" })).toBeInTheDocument();
    const general = screen.getByRole("tab", { name: "General" });
    expect(general).toHaveAttribute("aria-selected", "true");
    expect(screen.getByRole("tabpanel", { name: "General" })).toHaveTextContent("General panel");
    general.focus();
    await user.keyboard("{ArrowRight}");
    const advanced = screen.getByRole("tab", { name: "Advanced" });
    expect(advanced).toHaveAttribute("aria-selected", "true");
    expect(advanced).toHaveFocus();
    expect(onChange).toHaveBeenLastCalledWith("c");
    await user.keyboard("{ArrowRight}");
    expect(general).toHaveAttribute("aria-selected", "true");
    await user.keyboard("{End}");
    expect(advanced).toHaveAttribute("aria-selected", "true");
    await user.keyboard("{Home}");
    expect(general).toHaveAttribute("aria-selected", "true");
    await user.click(advanced);
    expect(screen.getByRole("tabpanel")).toHaveTextContent("Advanced panel");
    expect(general).toHaveAttribute("tabindex", "-1");
  });
});

describe("Tooltip", () => {
  it("shows on hover and focus, hides on Escape and leave", async () => {
    const user = userEvent.setup();
    render(
      <Tooltip content="Copies the server">
        <button>Clone</button>
      </Tooltip>,
    );
    const btn = screen.getByRole("button", { name: "Clone" });
    expect(screen.queryByRole("tooltip")).toBeNull();
    await user.hover(btn);
    expect(screen.getByRole("tooltip")).toHaveTextContent("Copies the server");
    expect(btn).toHaveAccessibleDescription("Copies the server");
    await user.unhover(btn);
    expect(screen.queryByRole("tooltip")).toBeNull();
    await user.tab();
    expect(screen.getByRole("tooltip")).toBeInTheDocument();
    await user.keyboard("{Escape}");
    expect(screen.queryByRole("tooltip")).toBeNull();
  });
});
