import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { ConfirmProvider, useConfirm } from "./ConfirmDialog";

const results: boolean[] = [];

function Asker({ danger }: { danger?: boolean }) {
  const confirm = useConfirm();
  return (
    <button
      onClick={async () => {
        results.push(await confirm({ title: "Delete it?", body: "This cannot be undone.", confirmLabel: "Delete", danger }));
      }}
    >
      Ask
    </button>
  );
}

function setup(danger?: boolean) {
  results.length = 0;
  const user = userEvent.setup();
  render(
    <ConfirmProvider>
      <Asker danger={danger} />
    </ConfirmProvider>,
  );
  return user;
}

describe("useConfirm", () => {
  it("resolves true on confirm and closes", async () => {
    const user = setup(true);
    await user.click(screen.getByText("Ask"));
    const dialog = screen.getByRole("dialog", { name: "Delete it?" });
    expect(dialog).toHaveTextContent("This cannot be undone.");
    await user.click(screen.getByRole("button", { name: "Delete" }));
    expect(results).toEqual([true]);
    expect(screen.queryByRole("dialog")).toBeNull();
  });

  it("resolves false on cancel", async () => {
    const user = setup();
    await user.click(screen.getByText("Ask"));
    await user.click(screen.getByRole("button", { name: "Cancel" }));
    expect(results).toEqual([false]);
  });

  it("resolves false on Escape and returns focus to the opener", async () => {
    const user = setup();
    await user.click(screen.getByText("Ask"));
    await user.keyboard("{Escape}");
    expect(results).toEqual([false]);
    expect(screen.getByText("Ask")).toHaveFocus();
  });

  it("resolves false on overlay click", async () => {
    const user = setup();
    await user.click(screen.getByText("Ask"));
    await user.click(screen.getByTestId("modal-overlay"));
    expect(results).toEqual([false]);
  });

  it("renders Cancel then a ghost-red confirm button for danger, with no focus ring", async () => {
    const user = setup(true);
    await user.click(screen.getByText("Ask"));
    const cancel = screen.getByRole("button", { name: "Cancel" });
    const ok = screen.getByRole("button", { name: "Delete" });
    expect(cancel.compareDocumentPosition(ok) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
    expect(ok.className).toContain("enabled:hover:text-danger");
    expect(ok.className).not.toContain("var(--color-danger-deep)");
    expect(ok.className).toContain("ring-0!");
    expect(cancel.className).toContain("bg-primary-a10");
  });

  it("defaults to an OK ghost button and honours confirmVariant and cancelVariant", async () => {
    const user = userEvent.setup();
    function Variant() {
      const confirm = useConfirm();
      return (
        <>
          <button onClick={() => void confirm({ title: "Plain" })}>Plain</button>
          <button
            onClick={() => void confirm({ title: "Done?", confirmLabel: "Done", confirmVariant: "primary", cancelVariant: "ghost-red", danger: true })}
          >
            Primary
          </button>
        </>
      );
    }
    render(
      <ConfirmProvider>
        <Variant />
      </ConfirmProvider>,
    );
    await user.click(screen.getByText("Plain"));
    const ok = screen.getByRole("button", { name: "OK" });
    expect(ok.className).toContain("enabled:hover:border-primary-a50");
    expect(ok.className).not.toContain("hover:text-danger");
    await user.keyboard("{Escape}");
    await user.click(screen.getByText("Primary"));
    expect(screen.getByRole("button", { name: "Done" }).className).toContain("var(--color-primary)");
    expect(screen.getByRole("button", { name: "Cancel" }).className).toContain("enabled:hover:text-danger");
  });

  it("throws outside the provider", () => {
    vi.spyOn(console, "error").mockImplementation(() => {});
    expect(() => render(<Asker />)).toThrow(/ConfirmProvider/);
  });
});
