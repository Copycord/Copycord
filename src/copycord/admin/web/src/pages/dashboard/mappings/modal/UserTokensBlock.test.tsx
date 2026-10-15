import { fireEvent, render, screen, within } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { ToastProvider } from "../../../../components/Toast";
import { ConfirmProvider } from "../../../../components/ui/ConfirmDialog";
import { UserTokensBlock } from "./UserTokensBlock";

function renderBlock(strategy: string) {
  const setSetting = vi.fn();
  render(
    <ToastProvider>
      <ConfirmProvider>
        <UserTokensBlock mappingId={null} settings={{ USER_TOKEN_STRATEGY: strategy }} setSetting={setSetting} search="" />
      </ConfirmProvider>
    </ToastProvider>,
  );
  // The section starts collapsed, as in the mapping modal.
  document.getElementById("user-tokens-section")!.setAttribute("open", "");
  return setSetting;
}

describe("UserTokensBlock account selection", () => {
  it("is the app dropdown named by its label, keeps the id ut_strategy and saves the picked strategy", () => {
    const setSetting = renderBlock("round_robin");
    const select = screen.getByRole("combobox", { name: "Account selection" });
    expect(select).toHaveAttribute("id", "ut_strategy");
    expect(select).toHaveTextContent("Rotate evenly");
    expect(document.querySelector("select")).toBeNull();

    fireEvent.click(select);
    const list = screen.getByRole("listbox");
    expect(within(list).getAllByRole("option").map((o) => o.textContent)).toEqual(["Rotate evenly", "Sticky per author"]);
    fireEvent.click(within(list).getByRole("option", { name: "Sticky per author" }));
    expect(setSetting).toHaveBeenCalledWith("USER_TOKEN_STRATEGY", "sticky_author");
    expect(select).toHaveAttribute("aria-expanded", "false");
  });

  it("re-picking the current strategy changes nothing, like a native select", () => {
    const setSetting = renderBlock("sticky_author");
    const select = screen.getByRole("combobox", { name: "Account selection" });
    expect(select).toHaveTextContent("Sticky per author");
    fireEvent.click(select);
    fireEvent.click(within(screen.getByRole("listbox")).getByRole("option", { name: "Sticky per author" }));
    expect(setSetting).not.toHaveBeenCalled();
  });
});
