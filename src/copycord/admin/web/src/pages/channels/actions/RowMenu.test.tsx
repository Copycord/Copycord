import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it } from "vitest";
import { ActionMenu } from "../../guilds/ActionMenu";
import { RowMenu } from "./RowMenu";

// The menus sit on the #181822 control surface, so an item's hover and focus
// background has to be a wash over it, not the same surface colour.
describe("dropdown menu item hover", () => {
  it("washes row menu items on hover and focus, danger items in danger", async () => {
    render(
      <RowMenu
        kind="channel"
        items={[
          { act: "copy", label: "Copy", onSelect: () => {} },
          { act: "blacklist", label: "Blacklist", onSelect: () => {}, danger: true },
        ]}
      />,
    );
    await userEvent.click(screen.getByRole("button", { name: "Channel menu" }));
    const plain = screen.getByRole("menuitem", { name: "Copy" });
    expect(plain.className).toMatch(/\bhover:bg-menu-item-hover\b/);
    expect(plain.className).toMatch(/\bfocus:bg-menu-item-hover\b/);
    expect(plain.className).not.toMatch(/\bbg-card-solid\b/);
    const danger = screen.getByRole("menuitem", { name: "Blacklist" });
    expect(danger.className).toMatch(/\bhover:bg-danger-a10\b/);
    expect(danger.className).toMatch(/\bfocus:bg-danger-a10\b/);
    expect(danger.className).not.toMatch(/\bbg-menu-item-hover\b/);
  });

  it("washes guild card menu items on hover and keyboard focus", () => {
    render(
      <ActionMenu
        at={{ x: 10, y: 10 }}
        anchor={document.body}
        items={[{ key: "open", label: "Open", onSelect: () => {} }]}
        onClose={() => {}}
        label="Guild actions"
      />,
    );
    const item = screen.getByRole("menuitem", { name: "Open" });
    expect(item.className).toMatch(/\bhover:bg-menu-item-hover\b/);
    expect(item.className).toMatch(/\bfocus-visible:bg-menu-item-hover\b/);
    expect(item.className).not.toMatch(/\bbg-card-solid\b/);
  });
});
