import { fireEvent, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { useState } from "react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { Modal, TipLabel } from "./Modal";

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

describe("TipLabel placement", () => {
  afterEach(() => vi.restoreAllMocks());

  // The scroller spans 0 to 400px; the label's own box sits at labelTop.
  function mockRects(labelTop: number) {
    vi.spyOn(HTMLElement.prototype, "getBoundingClientRect").mockImplementation(function (this: HTMLElement) {
      const rect = (top: number, height: number) =>
        ({ top, bottom: top + height, left: 0, right: 100, width: 100, height, x: 0, y: top, toJSON: () => ({}) }) as DOMRect;
      if (this.dataset.testid === "scroller") return rect(0, 400);
      if (this.classList.contains("has-tip")) return rect(labelTop, 20);
      return rect(0, 0);
    });
  }

  function renderTip() {
    render(
      <div data-testid="scroller" style={{ overflowY: "auto" }}>
        <TipLabel text="Ignore cloned" tip="Skips copies" tipLabel="What is it?" />
      </div>,
    );
    return screen.getByRole("tooltip", { hidden: true });
  }

  it("opens below the label when it sits in the upper half of its scroller", async () => {
    mockRects(40);
    const bubble = renderTip();
    await userEvent.setup().click(screen.getByRole("button", { name: "What is it?" }));
    expect(bubble).toHaveAttribute("data-open", "true");
    expect(bubble).toHaveAttribute("data-placement", "below");
    expect(bubble.className).toContain("top-[calc(100%+8px)]");
    expect(bubble.className).toContain("after:border-b-surface-deep");
  });

  it("flips above the label, arrow underneath, in the lower half", async () => {
    mockRects(320);
    const bubble = renderTip();
    await userEvent.setup().click(screen.getByRole("button", { name: "What is it?" }));
    expect(bubble).toHaveAttribute("data-open", "true");
    expect(bubble).toHaveAttribute("data-placement", "above");
    expect(bubble.className).toContain("bottom-[calc(100%+8px)]");
    expect(bubble.className).toContain("after:border-t-surface-deep");
    expect(bubble.className).not.toContain("after:border-b-surface-deep");
  });
});
