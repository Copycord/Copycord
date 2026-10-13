import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it } from "vitest";
import { ConfirmProvider } from "../components/ui/ConfirmDialog";
import { Kit } from "./Kit";

describe("Kit page", () => {
  it("renders the components and drives the confirm dialog", async () => {
    const user = userEvent.setup();
    render(
      <ConfirmProvider>
        <Kit />
      </ConfirmProvider>,
    );
    expect(screen.getByRole("heading", { name: "Component kit" })).toBeInTheDocument();
    expect(screen.getByRole("switch", { name: "Enable cloning" })).toBeInTheDocument();
    expect(screen.getByRole("tablist", { name: "Demo tabs" })).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Ask" }));
    await user.click(screen.getByRole("button", { name: "Cancel" }));
    expect(screen.getByText("Answer: cancelled")).toBeInTheDocument();
  });
});
