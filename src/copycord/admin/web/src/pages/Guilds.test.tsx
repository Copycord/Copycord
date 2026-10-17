import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter, Route, Routes, useLocation } from "react-router";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { ToastProvider } from "../components/Toast";
import type { GuildDetails, GuildSummary } from "../types/guilds";
import { holdFetch } from "../test/holdFetch";
import { Guilds } from "./Guilds";

const GUILDS: GuildSummary[] = [
  { id: "200", name: "beta <b>bold</b>", icon_url: "https://cdn.discordapp.com/icons/200/a.png", member_count: 5 },
  { id: "100", name: "Alpha", icon_url: null, member_count: 1234 },
  { id: "300", name: "Gamma", icon_url: null, member_count: null },
];

const DETAILS: GuildDetails = {
  ...GUILDS[1]!,
  owner_id: "999",
  created_at: "2020-05-01T10:00:00+00:00",
  description: "<img src=x onerror=alert(1)> A test guild",
};

interface Server {
  up?: boolean;
  details?: { status: number; body: unknown };
}

const json = (body: unknown, status = 200) =>
  new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json" } });

function serve(s: Server = {}) {
  const fetchMock = vi.fn(async (input: RequestInfo | URL, _init?: RequestInit) => {
    const url = String(input);
    if (url === "/api/status") {
      const ready = s.up ?? true;
      return json({ server: { ready }, client: { ready } });
    }
    if (url === "/api/guilds") return json({ items: GUILDS });
    if (url.startsWith("/api/guilds/")) {
      const d = s.details ?? { status: 200, body: { ok: true, item: DETAILS } };
      return json(d.body, d.status);
    }
    return json({ error: "not-found" }, 404);
  });
  vi.stubGlobal("fetch", fetchMock);
  return fetchMock;
}

const calls = (m: ReturnType<typeof serve>, url: string) =>
  m.mock.calls.filter(([u]) => String(u) === url) as unknown as Array<[string, RequestInit]>;

function Where() {
  const loc = useLocation();
  return <p data-testid="where">{loc.pathname + loc.search}</p>;
}

function renderPage() {
  return render(
    <ToastProvider>
      <MemoryRouter initialEntries={["/guilds"]}>
        <Routes>
          <Route path="/guilds" element={<Guilds />} />
          <Route path="/tasks" element={<Where />} />
        </Routes>
      </MemoryRouter>
    </ToastProvider>,
  );
}

const cardNames = () =>
  Array.from(document.querySelectorAll("#guilds-root article")).map((a) => a.textContent);

async function openMenu(user: ReturnType<typeof userEvent.setup>, name: string) {
  await user.click(await screen.findByRole("button", { name }));
  return screen.getByRole("menu");
}

beforeEach(() => {
  sessionStorage.clear();
});

describe("Guilds grid", () => {
  it("loads once both sides are up and lists the guilds A to Z", async () => {
    const f = serve();
    renderPage();
    expect(screen.getByRole("heading", { level: 1, name: "Guilds" })).toBeInTheDocument();
    await screen.findByRole("button", { name: "Alpha" });
    expect(cardNames()).toEqual(["Alpha", "beta <b>bold</b>", "Gamma"]);
    expect(calls(f, "/api/guilds")).toHaveLength(1);
  });

  it("renders Discord text and icon URLs as text and attributes, never markup", async () => {
    serve();
    renderPage();
    const card = await screen.findByRole("button", { name: "beta <b>bold</b>" });
    expect(card.querySelector("b")).toBeNull();
    expect(card.querySelector("img")).toHaveAttribute("src", "https://cdn.discordapp.com/icons/200/a.png");
    const alpha = screen.getByRole("button", { name: "Alpha" });
    expect(alpha.querySelector("img")).toHaveAttribute("src", "/static/logo.png");
  });

  it("filters by search and shows the empty message", async () => {
    const user = userEvent.setup();
    serve();
    renderPage();
    await screen.findByRole("button", { name: "Alpha" });
    await user.type(screen.getByLabelText("Search guilds"), "GAM");
    expect(cardNames()).toEqual(["Gamma"]);
    await user.clear(screen.getByLabelText("Search guilds"));
    await user.type(screen.getByLabelText("Search guilds"), "nothing here");
    expect(cardNames()).toEqual([]);
    expect(screen.getByText("No guilds found.")).toBeInTheDocument();
  });

  it("stays gated and does not load guilds while the bot is down", async () => {
    const f = serve({ up: false });
    renderPage();
    expect(await screen.findByText("Waiting for Server and Client connection...")).toBeInTheDocument();
    expect(calls(f, "/api/guilds")).toHaveLength(0);
  });
});

describe("card menu", () => {
  it("opens on click with both actions and closes on Escape, outside click and a second click", async () => {
    const user = userEvent.setup();
    serve();
    renderPage();
    const menu = await openMenu(user, "Alpha");
    expect(within(menu).getAllByRole("menuitem").map((b) => b.textContent)).toEqual(["View details", "Export messages"]);
    expect(screen.getByRole("button", { name: "Alpha" })).toHaveAttribute("aria-expanded", "true");
    await user.keyboard("{Escape}");
    expect(screen.queryByRole("menu")).toBeNull();
    expect(screen.getByRole("button", { name: "Alpha" })).toHaveFocus();

    await openMenu(user, "Alpha");
    await user.click(screen.getByRole("heading", { name: "Guilds" }));
    expect(screen.queryByRole("menu")).toBeNull();

    await openMenu(user, "Alpha");
    await user.click(screen.getByRole("button", { name: "Alpha" }));
    expect(screen.queryByRole("menu")).toBeNull();
  });
});

describe("guild details dialog", () => {
  it("fetches the guild and shows its fields, then closes with X, Close and Escape", async () => {
    const user = userEvent.setup();
    const f = serve();
    renderPage();
    await openMenu(user, "Alpha");
    await user.click(screen.getByRole("menuitem", { name: "View details" }));
    const dialog = screen.getByRole("dialog", { name: "Guild details" });
    expect(await within(dialog).findByText("1,234 members")).toBeInTheDocument();
    expect(calls(f, "/api/guilds/100")).toHaveLength(1);
    expect(within(dialog).getByText("Owner ID").nextSibling).toHaveTextContent("999");
    expect(within(dialog).getByText("Created").nextSibling).toHaveTextContent("2020-05-01T10:00:00+00:00");
    expect(within(dialog).getByText("<img src=x onerror=alert(1)> A test guild")).toBeInTheDocument();
    expect(dialog.querySelectorAll("img")).toHaveLength(1);

    // The X in the header and the Close button in the footer.
    const [x] = within(dialog).getAllByRole("button", { name: "Close" });
    await user.click(x!);
    expect(screen.queryByRole("dialog")).toBeNull();
    expect(screen.getByRole("button", { name: "Alpha" })).toHaveFocus();

    await openMenu(user, "Alpha");
    await user.click(screen.getByRole("menuitem", { name: "View details" }));
    await user.click(within(screen.getByRole("dialog")).getAllByRole("button", { name: "Close" })[1]!);
    expect(screen.queryByRole("dialog")).toBeNull();

    await openMenu(user, "Alpha");
    await user.click(screen.getByRole("menuitem", { name: "View details" }));
    await user.keyboard("{Escape}");
    expect(screen.queryByRole("dialog")).toBeNull();
  });

  it("shows an error when the guild is gone", async () => {
    const user = userEvent.setup();
    serve({ details: { status: 404, body: { ok: false, error: "not-found" } } });
    renderPage();
    await openMenu(user, "Gamma");
    await user.click(screen.getByRole("menuitem", { name: "View details" }));
    const dialog = screen.getByRole("dialog", { name: "Guild details" });
    expect(await within(dialog).findByText("Error loading details")).toBeInTheDocument();
    expect(within(dialog).getByText("This guild is not in the database any more.")).toBeInTheDocument();
  });
});

describe("export messages", () => {
  it("opens the Export messages task with the guild picked", async () => {
    const user = userEvent.setup();
    serve();
    renderPage();
    await openMenu(user, "Alpha");
    await user.click(screen.getByRole("menuitem", { name: "Export messages" }));
    expect(await screen.findByTestId("where")).toHaveTextContent("/tasks?type=export_messages&guild=100");
    expect(screen.queryByRole("dialog")).toBeNull();
  });
});

describe("Guilds loading", () => {
  it("shows guild card skeletons until the list arrives", async () => {
    serve();
    const release = holdFetch("/api/guilds");
    renderPage();
    expect(await screen.findByRole("status", { name: "Loading guilds" })).toBeInTheDocument();
    release();
    await waitFor(() => expect(cardNames().length).toBeGreaterThan(0));
    expect(screen.queryByRole("status", { name: "Loading guilds" })).toBeNull();
  });
});
