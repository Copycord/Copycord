// Behaviour tests for the smaller components.

import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { useState } from "react";
import { describe, expect, it, vi } from "vitest";
import { Badge, StatusPill } from "./Badge";
import { Button } from "./Button";
import { Card } from "./Card";
import { EmptyState } from "./EmptyState";
import { Field, Input, Select, Textarea } from "./Field";
import { Spinner } from "./Spinner";
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

  it("applies variant classes", () => {
    render(<Button variant="danger">Delete</Button>);
    expect(screen.getByRole("button").className).toContain("bg-[linear-gradient(180deg,#ef4444,#b91c1c)]");
  });

  it("holds an attached button still on hover and press, other buttons keep the lift", () => {
    render(
      <>
        <Button variant="ghost" attached>
          Search
        </Button>
        <Button variant="ghost">Other</Button>
      </>,
    );
    const still = screen.getByRole("button", { name: "Search" }).className;
    for (const c of ["enabled:hover:translate-y-0!", "enabled:hover:shadow-none!", "enabled:active:scale-100!"]) expect(still).toContain(c);
    expect(still).not.toContain("ring-0!");
    expect(screen.getByRole("button", { name: "Other" }).className).not.toContain("enabled:hover:translate-y-0!");
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

  it("works for Textarea and Select", async () => {
    const user = userEvent.setup();
    render(
      <>
        <Field label="Notes">
          <Textarea />
        </Field>
        <Field label="Mode">
          <Select
            defaultValue="a"
            options={[
              { value: "a", label: "Alpha" },
              { value: "b", label: "Beta" },
            ]}
          />
        </Field>
      </>,
    );
    await user.type(screen.getByLabelText("Notes"), "hi");
    expect(screen.getByLabelText("Notes")).toHaveValue("hi");
    await user.selectOptions(screen.getByLabelText("Mode"), "b");
    expect(screen.getByLabelText("Mode")).toHaveValue("b");
    expect(screen.getByLabelText("Mode")).not.toHaveAttribute("aria-invalid");
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

describe("Badge and StatusPill", () => {
  it("renders status text for each state", () => {
    render(
      <>
        <StatusPill status="online" />
        <StatusPill status="offline" />
        <StatusPill status="warn" label="Reconnecting" />
        <Badge tone="good">ok</Badge>
      </>,
    );
    const pills = screen.getAllByRole("status");
    expect(pills.map((p) => p.textContent)).toEqual(["Online", "Offline", "Reconnecting"]);
    expect(pills[1]).toHaveAttribute("data-status", "offline");
    expect(screen.getByText("ok").className).toContain("text-good");
  });
});

describe("Card, Spinner, EmptyState", () => {
  it("render their content", () => {
    render(
      <>
        <Card title="Server" actions={<button>Edit</button>}>
          body
        </Card>
        <Spinner label="Loading guilds" />
        <EmptyState title="No mappings" description="Add one to start" action={<button>Add</button>} />
      </>,
    );
    expect(screen.getByRole("heading", { name: "Server" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Edit" })).toBeInTheDocument();
    expect(screen.getByRole("status", { name: "Loading guilds" })).toBeInTheDocument();
    expect(screen.getByText("No mappings")).toBeInTheDocument();
    expect(screen.getByText("Add one to start")).toBeInTheDocument();
    expect(screen.getByText("No mappings").parentElement).toHaveClass("border-dashed");
  });

  it("EmptyState plain is centred text with no card border", () => {
    render(<EmptyState plain title="No runs" description="Run one above" />);
    const box = screen.getByText("No runs").parentElement!;
    expect(box.className).not.toMatch(/border/);
    expect(box).toHaveClass("text-center");
    expect(screen.getByText("Run one above")).toBeInTheDocument();
  });
});
