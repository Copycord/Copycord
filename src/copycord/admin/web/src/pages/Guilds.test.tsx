import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { ToastProvider } from "../components/Toast";
import { CSRF_HEADER } from "../lib/api";
import type { ExportMessagesRequest, GuildDetails, GuildSummary } from "../types/guilds";
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
  exportReply?: { status: number; body: unknown };
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
    if (url === "/api/export/messages") {
      const r = s.exportReply ?? { status: 200, body: { ok: true, accepted: true } };
      return json(r.body, r.status);
    }
    return json({ error: "not-found" }, 404);
  });
  vi.stubGlobal("fetch", fetchMock);
  return fetchMock;
}

const calls = (m: ReturnType<typeof serve>, url: string) =>
  m.mock.calls.filter(([u]) => String(u) === url) as unknown as Array<[string, RequestInit]>;

function renderPage() {
  return render(
    <ToastProvider>
      <Guilds />
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

  it("toggles the sort direction", async () => {
    const user = userEvent.setup();
    serve();
    renderPage();
    await screen.findByRole("button", { name: "Alpha" });
    const sort = screen.getByRole("button", { name: "Toggle sort direction" });
    expect(sort).toHaveTextContent("A → Z");
    expect(sort).toHaveAttribute("aria-pressed", "false");
    await user.click(sort);
    expect(sort).toHaveTextContent("Z → A");
    expect(sort).toHaveAttribute("aria-pressed", "true");
    expect(cardNames()).toEqual(["Gamma", "beta <b>bold</b>", "Alpha"]);
  });

  it("stays gated and does not load guilds while the bot is down", async () => {
    const f = serve({ up: false });
    renderPage();
    expect(await screen.findByText("Waiting for Server and Client connection...")).toBeInTheDocument();
    expect(calls(f, "/api/guilds")).toHaveLength(0);
    // The old #status-gate: fixed over the whole page under the header, not just the card.
    const gate = screen.getByTestId("status-gate");
    expect(gate).toHaveClass("fixed", "inset-x-0", "top-header", "bottom-0");
    expect(gate).not.toHaveClass("absolute");
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

  it("moves between items with the arrow keys", async () => {
    const user = userEvent.setup();
    serve();
    renderPage();
    const menu = await openMenu(user, "Gamma");
    const [view, exp] = within(menu).getAllByRole("menuitem");
    await waitFor(() => expect(view).toHaveFocus());
    await user.keyboard("{ArrowDown}");
    expect(exp).toHaveFocus();
    await user.keyboard("{ArrowDown}");
    expect(view).toHaveFocus();
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

    // The X in the header and the Close button in the footer, as on the old page.
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

describe("export dialog", () => {
  async function openExport(user: ReturnType<typeof userEvent.setup>) {
    await openMenu(user, "Alpha");
    await user.click(screen.getByRole("menuitem", { name: "Export messages" }));
    return screen.getByRole("dialog", { name: "Export messages" });
  }

  const body = (f: ReturnType<typeof serve>) => {
    const [, init] = calls(f, "/api/export/messages")[0]!;
    return { payload: JSON.parse(String(init.body)) as ExportMessagesRequest, init };
  };

  it("posts the options with the CSRF header, toasts and closes", async () => {
    const user = userEvent.setup();
    const f = serve();
    renderPage();
    const dialog = await openExport(user);
    await user.type(within(dialog).getByLabelText("Channel ID (optional)"), " 555 ");
    await user.click(within(dialog).getByLabelText("Include system messages"));
    await user.click(within(dialog).getByLabelText("Include threads"));
    await user.click(within(dialog).getByLabelText("Images"));
    await user.click(within(dialog).getByLabelText("Other files"));
    await user.clear(within(dialog).getByLabelText("Min total reactions"));
    await user.type(within(dialog).getByLabelText("Min total reactions"), "3");
    await user.click(within(dialog).getByRole("button", { name: "Start export" }));

    expect(await screen.findByText("Export started. You’ll see progress in logs.")).toBeInTheDocument();
    expect(screen.queryByRole("dialog")).toBeNull();
    const { payload, init } = body(f);
    expect(init.method).toBe("POST");
    expect(new Headers(init.headers).get(CSRF_HEADER)).toBe("1");
    expect(payload.guild_id).toBe("100");
    expect(payload.channel_id).toBe("555");
    expect(payload.user_id).toBeNull();
    expect(payload.filters.system).toBe(true);
    expect(payload.filters.threads).toBe(false);
    expect(payload.filters.private_threads).toBe(false);
    expect(payload.filters.min_reactions).toBe(3);
    expect(payload.filters.download_media).toEqual({ images: true, videos: false, audio: false, other: true });
  });

  it("enables the word box only with Includes word", async () => {
    const user = userEvent.setup();
    serve();
    renderPage();
    const dialog = await openExport(user);
    const word = within(dialog).getByLabelText("Word to include");
    expect(word).toBeDisabled();
    await user.click(within(dialog).getByLabelText("Includes word"));
    expect(word).toBeEnabled();
  });

  it("picks attachment types in the sub dialog, Cancel and Escape keep the export dialog open", async () => {
    const user = userEvent.setup();
    const f = serve();
    renderPage();
    const dialog = await openExport(user);
    const tip = within(dialog).getByRole("button", { name: "Attachment types" });

    await user.click(tip);
    let sub = screen.getByRole("dialog", { name: "Attachment types" });
    expect(within(sub).getByLabelText("Images")).toHaveFocus();
    await user.click(within(sub).getByLabelText("Videos"));
    await user.click(within(sub).getByRole("button", { name: "Cancel" }));
    expect(screen.queryByRole("dialog", { name: "Attachment types" })).toBeNull();
    expect(screen.getByRole("dialog", { name: "Export messages" })).toBeInTheDocument();

    await user.click(tip);
    sub = screen.getByRole("dialog", { name: "Attachment types" });
    expect(within(sub).getByLabelText("Videos")).toBeChecked();
    await user.keyboard("{Escape}");
    expect(screen.queryByRole("dialog", { name: "Attachment types" })).toBeNull();
    expect(screen.getByRole("dialog", { name: "Export messages" })).toBeInTheDocument();

    await user.click(tip);
    sub = screen.getByRole("dialog", { name: "Attachment types" });
    await user.click(within(sub).getByLabelText("Videos"));
    await user.click(within(sub).getByRole("button", { name: "Apply" }));
    expect(within(dialog).getByText("Images, Audio, Other files")).toBeInTheDocument();

    await user.click(within(dialog).getByRole("button", { name: "Start export" }));
    await screen.findByText("Export started. You’ll see progress in logs.");
    expect(body(f).payload.filters.att_types).toEqual({ images: true, videos: false, audio: true, other: true });
  });

  it("disables the attachment types button when attachments are off", async () => {
    const user = userEvent.setup();
    serve();
    renderPage();
    const dialog = await openExport(user);
    await user.click(within(dialog).getByLabelText("Include attachments"));
    expect(within(dialog).getByRole("button", { name: "Attachment types" })).toBeDisabled();
  });

  it("keeps the dialog open and toasts a plain error when the client is down", async () => {
    const user = userEvent.setup();
    serve({ exportReply: { status: 502, body: { ok: false, error: "client-agent-unreachable" } } });
    renderPage();
    const dialog = await openExport(user);
    await user.click(within(dialog).getByRole("button", { name: "Start export" }));
    expect(await screen.findByText("The client is not running, so the export could not start.")).toBeInTheDocument();
    expect(screen.getByRole("dialog", { name: "Export messages" })).toBeInTheDocument();
    expect(within(dialog).getByRole("button", { name: "Start export" })).toBeEnabled();
  });

  it("closes with Cancel without posting", async () => {
    const user = userEvent.setup();
    const f = serve();
    renderPage();
    const dialog = await openExport(user);
    await user.click(within(dialog).getByRole("button", { name: "Cancel" }));
    expect(screen.queryByRole("dialog")).toBeNull();
    expect(calls(f, "/api/export/messages")).toHaveLength(0);
  });
});
