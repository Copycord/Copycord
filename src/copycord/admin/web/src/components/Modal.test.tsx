import { fireEvent, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { useState } from "react";
import { describe, expect, it, vi } from "vitest";
import { Modal } from "./Modal";

function Harness({ onClose }: { onClose?: () => void }) {
  const [open, setOpen] = useState(false);
  return (
    <>
      <button onClick={() => setOpen(true)}>Open</button>
      <Modal
        open={open}
        title="Edit"
        onClose={() => {
          onClose?.();
          setOpen(false);
        }}
        footer={<button>Save</button>}
      >
        <input aria-label="Name" />
      </Modal>
    </>
  );
}

describe("Modal", () => {
  it("is a labelled dialog that takes focus and returns it on close", async () => {
    const user = userEvent.setup();
    render(<Harness />);
    const opener = screen.getByText("Open");
    await user.click(opener);
    const dialog = screen.getByRole("dialog", { name: "Edit" });
    expect(dialog).toHaveAttribute("aria-modal", "true");
    expect(screen.getByLabelText("Close")).toHaveFocus();
    await user.keyboard("{Escape}");
    expect(screen.queryByRole("dialog")).toBeNull();
    expect(opener).toHaveFocus();
  });

  it("traps Tab and Shift+Tab inside the dialog", async () => {
    const user = userEvent.setup();
    render(<Harness />);
    await user.click(screen.getByText("Open"));
    expect(screen.getByLabelText("Close")).toHaveFocus();
    await user.tab();
    expect(screen.getByLabelText("Name")).toHaveFocus();
    await user.tab();
    expect(screen.getByText("Save")).toHaveFocus();
    await user.tab();
    expect(screen.getByLabelText("Close")).toHaveFocus();
    await user.tab({ shift: true });
    expect(screen.getByText("Save")).toHaveFocus();
  });

  it("closes on overlay click but not on a click inside", async () => {
    const onClose = vi.fn();
    const user = userEvent.setup();
    render(<Harness onClose={onClose} />);
    await user.click(screen.getByText("Open"));
    fireEvent.mouseDown(screen.getByLabelText("Name"));
    expect(onClose).not.toHaveBeenCalled();
    fireEvent.mouseDown(screen.getByTestId("modal-overlay"));
    expect(onClose).toHaveBeenCalledTimes(1);
    expect(screen.queryByRole("dialog")).toBeNull();
  });
});

describe("Modal scroll lock", () => {
  function Pair({ a, b }: { a: boolean; b: boolean }) {
    return (
      <>
        <Modal open={a} title="A" onClose={() => undefined}>
          a
        </Modal>
        <Modal open={b} title="B" onClose={() => undefined}>
          b
        </Modal>
      </>
    );
  }

  it("hides body overflow while open and restores the previous value on close", () => {
    document.body.style.overflow = "auto";
    const view = render(<Pair a={false} b={false} />);
    expect(document.body.style.overflow).toBe("auto");
    view.rerender(<Pair a b={false} />);
    expect(document.body.style.overflow).toBe("hidden");
    view.rerender(<Pair a={false} b={false} />);
    expect(document.body.style.overflow).toBe("auto");
    view.unmount();
    document.body.style.overflow = "";
  });

  it("stays locked while one of two open dialogs is still open", () => {
    const view = render(<Pair a b />);
    expect(document.body.style.overflow).toBe("hidden");
    view.rerender(<Pair a={false} b />);
    expect(document.body.style.overflow).toBe("hidden");
    view.rerender(<Pair a={false} b={false} />);
    expect(document.body.style.overflow).toBe("");
    view.unmount();
  });
});
