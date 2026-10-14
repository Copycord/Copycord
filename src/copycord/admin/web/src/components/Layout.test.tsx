import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter, Route, Routes } from "react-router";
import { describe, expect, it, vi } from "vitest";
import { Layout } from "./Layout";

function Boom(): never {
  throw new Error("boom");
}

function renderAt(path: string) {
  return render(
    <MemoryRouter initialEntries={[path]}>
      <Routes>
        <Route element={<Layout />}>
          <Route path="/" element={<h1>Fine page</h1>} />
          <Route path="/boom" element={<Boom />} />
        </Route>
      </Routes>
    </MemoryRouter>,
  );
}

describe("Layout error boundary", () => {
  it("shows the error card in place of a page that throws and keeps the sidebar", () => {
    vi.spyOn(console, "error").mockImplementation(() => undefined);
    renderAt("/boom");
    expect(screen.getByRole("alert")).toHaveTextContent("Something went wrong.");
    expect(screen.getByRole("button", { name: "Reload" })).toBeInTheDocument();
    expect(screen.getByRole("navigation", { name: "Site menu" })).toBeInTheDocument();
    expect(screen.getByRole("banner")).toBeInTheDocument();
  });

  it("renders a page that does not throw unchanged", () => {
    renderAt("/");
    expect(screen.getByRole("heading", { level: 1, name: "Fine page" })).toBeInTheDocument();
    expect(screen.queryByRole("alert")).not.toBeInTheDocument();
    expect(screen.queryByText("Something went wrong.")).not.toBeInTheDocument();
    expect(screen.getByRole("navigation", { name: "Site menu" })).toBeInTheDocument();
  });

  it("resets when navigating away from the broken page", async () => {
    vi.spyOn(console, "error").mockImplementation(() => undefined);
    renderAt("/boom");
    expect(screen.getByRole("alert")).toBeInTheDocument();
    await userEvent.click(screen.getByRole("link", { name: "Copycord" }));
    expect(screen.getByRole("heading", { level: 1, name: "Fine page" })).toBeInTheDocument();
    expect(screen.queryByRole("alert")).not.toBeInTheDocument();
  });
});
