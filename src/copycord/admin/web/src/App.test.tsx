import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter } from "react-router";
import { describe, expect, it, vi } from "vitest";
import { App, PAGES } from "./App";
import { CSRF_HEADER } from "./lib/api";
import { logout, Sidebar } from "./components/Sidebar";
import { ToastProvider } from "./components/Toast";
import { ConfirmProvider } from "./components/ui";

function renderAt(path: string) {
  return render(
    <MemoryRouter initialEntries={[path]}>
      <ToastProvider>
        <ConfirmProvider>
          <App />
        </ConfirmProvider>
      </ToastProvider>
    </MemoryRouter>,
  );
}

describe("App shell", () => {
  it.each(PAGES)("renders the $title page at $path", async ({ path, title }) => {
    renderAt(path);
    expect(await screen.findByRole("heading", { level: 1, name: title })).toBeInTheDocument();
  });

  it("lists the menu in the same order as the old sidebar and marks the active page", () => {
    renderAt("/guilds");
    const nav = screen.getByRole("navigation", { name: "Site menu" });
    const labels = within(nav)
      .getAllByRole("listitem")
      .map((li) => li.textContent);
    expect(labels).toEqual([
      "Configuration",
      "Channels",
      "Messages",
      "Guilds",
      "Tasks",
      "Forwarding",
      "System",
      "Logs",
      "Redeem code",
      "Log out",
    ]);
    expect(within(nav).getByText("Guilds").closest("a")).toHaveAttribute("aria-current", "page");
  });

  it("turns the Redeem code entry gold on hover, label, gift and wash, where the page links take the grey wash", () => {
    // The menu alone, so no page loads behind it.
    render(
      <MemoryRouter initialEntries={["/guilds"]}>
        <Sidebar />
      </MemoryRouter>,
    );
    const nav = screen.getByRole("navigation", { name: "Site menu" });
    const redeem = within(nav).getByRole("button", { name: "Redeem code" });
    expect(redeem).toHaveClass("hover:text-gold", "hover:bg-gold/10", "transition-colors");
    expect(redeem).not.toHaveClass("hover:bg-rail-hover");
    expect(redeem.querySelector("svg")).toHaveAttribute("stroke", "currentColor");
    expect(within(nav).getByText("Channels").closest("a")).toHaveClass("hover:bg-rail-hover");
    expect(within(nav).getByText("Channels").closest("a")).not.toHaveClass("hover:text-gold");
  });

  it("links the menu's version to its release, or to its commit on a dev build, like the page footer", () => {
    const { unmount } = render(
      <MemoryRouter initialEntries={["/guilds"]}>
        <Sidebar version="v4.0.0.3" />
      </MemoryRouter>,
    );
    expect(document.getElementById("sidebar-version-link")).toHaveAttribute(
      "href",
      expect.stringMatching(/\/releases\/tag\/v4\.0\.0\.3$/),
    );
    unmount();
    render(
      <MemoryRouter initialEntries={["/guilds"]}>
        <Sidebar version="dev (7f00ae9)" />
      </MemoryRouter>,
    );
    const dev = document.getElementById("sidebar-version-link");
    expect(dev).toHaveAttribute("href", expect.stringMatching(/\/commit\/7f00ae9$/));
    expect(dev).toHaveAccessibleName("Copycord dev (7f00ae9) commit");
  });

  it("renders the Tasks page at a run url, not NotFound, with Tasks lit in the menu", async () => {
    renderAt("/tasks/runs/4f2a9c1e7b3d4e5f8a6b0c2d1e3f5a7b");
    // The type rail is only in the page, not its skeleton, so the chunk has replaced the skeleton.
    await screen.findByRole("navigation", { name: "Task types" });
    expect(screen.getByRole("heading", { level: 1, name: "Tasks" })).toBeInTheDocument();
    expect(screen.queryByText(/Page not found/)).not.toBeInTheDocument();
    const nav = screen.getByRole("navigation", { name: "Site menu" });
    const item = within(nav).getByText("Tasks").closest("a");
    expect(item).toHaveAttribute("href", "/tasks");
    expect(item).toHaveAttribute("aria-current", "page");
    expect(item?.querySelector("svg")).not.toBeNull();
  });

  it("opens and closes the mobile drawer", async () => {
    const user = userEvent.setup();
    renderAt("/");
    const aside = document.getElementById("side-menu")!;
    expect(aside.dataset.open).toBe("false");
    await user.click(screen.getByLabelText("Open menu"));
    expect(aside.dataset.open).toBe("true");
    await user.click(screen.getByTestId("menu-backdrop"));
    expect(aside.dataset.open).toBe("false");
    await user.click(screen.getByLabelText("Open menu"));
    await user.keyboard("{Escape}");
    expect(aside.dataset.open).toBe("false");
  });

  it("logs out with a CSRF POST to /logout and goes to /login", async () => {
    const fetchMock = vi.fn(async () => new Response(null, { status: 204 }));
    vi.stubGlobal("fetch", fetchMock);
    const go = vi.fn();
    await logout(go);
    const [url, init] = fetchMock.mock.calls[0] as unknown as [string, RequestInit];
    expect(url).toBe("/logout");
    expect(init.method).toBe("POST");
    expect(new Headers(init.headers).get(CSRF_HEADER)).toBe("1");
    await waitFor(() => expect(go).toHaveBeenCalledWith("/login"));
  });

  it("still goes to /login when the logout request fails", async () => {
    vi.stubGlobal("fetch", vi.fn(async () => new Response("no", { status: 500 })));
    const go = vi.fn();
    await logout(go);
    expect(go).toHaveBeenCalledWith("/login");
  });
});
