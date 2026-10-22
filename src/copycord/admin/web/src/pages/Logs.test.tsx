import { act, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { ToastProvider } from "../components/Toast";
import { ConfirmProvider } from "../components/ui/ConfirmDialog";
import { CSRF_HEADER } from "../lib/api";
import type { EventLog } from "../types/logs";
import { holdFetch } from "../test/holdFetch";
import { Logs } from "./Logs";

function entry(id: string, over: Partial<EventLog> = {}): EventLog {
  return {
    log_id: id,
    event_type: "channel_created",
    details: `Event ${id}`,
    guild_id: null,
    guild_name: "Test Clone",
    channel_id: null,
    channel_name: null,
    category_id: null,
    category_name: null,
    extra_json: null,
    created_at: 1_700_000_000,
    ...over,
  };
}

const SEED: EventLog[] = [
  entry("a", { details: "Created channel #general" }),
  entry("b", { event_type: "channel_deleted", details: "Deleted channel #old-news" }),
  entry("c", { event_type: "role_created", details: "Created role Moderator", guild_name: null }),
];

interface Call {
  method: string;
  url: URL;
  body: unknown;
  csrf: string | null;
}

function json(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json" } });
}

/** A tiny in-memory /api/event-logs that records every request. */
function server(initial: EventLog[]) {
  let logs = [...initial];
  const calls: Call[] = [];
  const types = () => [...new Set(logs.map((l) => l.event_type))].sort();
  const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
    const url = new URL(String(input), "http://cc.test");
    const method = init?.method ?? "GET";
    const body = typeof init?.body === "string" ? (JSON.parse(init.body) as unknown) : undefined;
    calls.push({ method, url, body, csrf: new Headers(init?.headers).get(CSRF_HEADER) });
    const path = url.pathname;
    if (method === "GET" && path === "/api/event-log-types") return json({ ok: true, types: types() });
    if (method === "GET" && path === "/api/event-logs") {
      const type = url.searchParams.get("event_type");
      const search = url.searchParams.get("search")?.toLowerCase();
      const rows = logs.filter(
        (l) => (!type || l.event_type === type) && (!search || (l.details ?? "").toLowerCase().includes(search)),
      );
      const offset = Number(url.searchParams.get("offset") ?? 0);
      const limit = Number(url.searchParams.get("limit") ?? 200);
      return json({ ok: true, logs: rows.slice(offset, offset + limit), total: rows.length, types: types() });
    }
    if (method === "DELETE" && path === "/api/event-logs") {
      const deleted = logs.length;
      logs = [];
      return json({ ok: true, deleted });
    }
    if (method === "DELETE" && path.startsWith("/api/event-logs/")) {
      const id = decodeURIComponent(path.slice("/api/event-logs/".length));
      logs = logs.filter((l) => l.log_id !== id);
      return json({ ok: true });
    }
    return json({ detail: "not found" }, 404);
  });
  vi.stubGlobal("fetch", fetchMock);
  const lists = () => calls.filter((c) => c.method === "GET" && c.url.pathname === "/api/event-logs");
  const mutations = () => calls.filter((c) => c.method !== "GET");
  return { calls, lists, mutations };
}

function setup(initial: EventLog[] = SEED) {
  const srv = server(initial);
  const user = userEvent.setup();
  render(
    <ToastProvider>
      <ConfirmProvider>
        <Logs />
      </ConfirmProvider>
    </ToastProvider>,
  );
  return { ...srv, user };
}

const tbody = () => document.getElementById("logs-tbody")!;

/** Observers the LoadMore sentinel made; scrollToEnd tells the live ones their target came into view. */
let observers: Array<{ cb: IntersectionObserverCallback; el: Element | null; live: boolean }> = [];

class FakeObserver {
  entry: { cb: IntersectionObserverCallback; el: Element | null; live: boolean };
  constructor(cb: IntersectionObserverCallback) {
    this.entry = { cb, el: null, live: true };
    observers.push(this.entry);
  }
  observe(el: Element) {
    this.entry.el = el;
  }
  unobserve() {}
  disconnect() {
    this.entry.live = false;
  }
  takeRecords() {
    return [];
  }
}

function scrollToEnd() {
  for (const o of observers.filter((x) => x.live && x.el))
    o.cb([{ isIntersecting: true, target: o.el } as unknown as IntersectionObserverEntry], {} as IntersectionObserver);
}

beforeEach(() => {
  observers = [];
  vi.stubGlobal("IntersectionObserver", FakeObserver);
});

async function loaded(text = "Created channel #general") {
  await waitFor(() => expect(tbody()).toHaveTextContent(text));
}

describe("Logs page", () => {
  it("lists the first page with labels, guild fallback and the count", async () => {
    const { lists, calls } = setup();
    await loaded();
    expect(screen.getByRole("heading", { level: 1, name: "Logs" })).toBeInTheDocument();
    expect(screen.getByRole("heading", { level: 2, name: "Event Logs" })).toBeInTheDocument();
    expect(lists()[0]!.url.search).toBe("?limit=50&offset=0");
    expect(calls.some((c) => c.url.pathname === "/api/event-log-types")).toBe(true);
    expect(within(tbody()).getByText("Channel Deleted")).toBeInTheDocument();
    expect(within(tbody()).getByText("SYSTEM")).toBeInTheDocument();
    expect(screen.getByText("Showing 1-3 of 3")).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Clear Filters" })).not.toBeInTheDocument();
  });

  it("fills the type filter from the server and sends event_type, clear filters resets", async () => {
    const { user, lists } = setup();
    await loaded();
    const select = screen.getByLabelText("Filter by type");
    await waitFor(() =>
      expect(within(select).getAllByRole("option", { hidden: true }).map((o) => o.textContent)).toEqual([
        "All Events",
        "Channel Created",
        "Channel Deleted",
        "Role Created",
      ]),
    );
    await user.selectOptions(select, "role_created");
    await waitFor(() => expect(tbody()).not.toHaveTextContent("Created channel #general"));
    expect(lists().at(-1)!.url.searchParams.get("event_type")).toBe("role_created");
    expect(lists().at(-1)!.url.searchParams.get("offset")).toBe("0");

    await user.click(screen.getByRole("button", { name: "Clear Filters" }));
    await loaded();
    expect(lists().at(-1)!.url.searchParams.has("event_type")).toBe(false);
    expect(select).toHaveValue("");
    expect(screen.queryByRole("button", { name: "Clear Filters" })).not.toBeInTheDocument();
  });

  it("debounces search into a single request and trims it", async () => {
    const { user, lists } = setup();
    await loaded();
    const before = lists().length;
    await user.type(screen.getByLabelText("Search logs"), "  Moder");
    await waitFor(() => expect(tbody()).not.toHaveTextContent("Created channel #general"));
    expect(tbody()).toHaveTextContent("Created role Moderator");
    const searches = lists()
      .slice(before)
      .map((c) => c.url.searchParams.get("search"));
    expect(searches).toEqual(["Moder"]);
    await user.click(screen.getByRole("button", { name: "Clear Filters" }));
    await loaded();
    expect(screen.getByLabelText("Search logs")).toHaveValue("");
  });

  it("loads the next 50 by offset when the end of the table scrolls into view, then stops at the end", async () => {
    const many = Array.from({ length: 120 }, (_, i) => entry(`id${i}`, { details: `Row ${i}`, created_at: 2_000 - i }));
    const { user, lists } = setup(many);
    await loaded("Row 0");
    expect(screen.getByText("Showing 1-50 of 120")).toBeInTheDocument();
    expect(screen.queryByRole("navigation", { name: "Pagination" })).toBeNull();
    await act(async () => scrollToEnd());
    await loaded("Row 99");
    expect(lists().at(-1)!.url.searchParams.get("offset")).toBe("50");
    // Appended: the first page is still there.
    expect(tbody()).toHaveTextContent("Row 0");
    expect(screen.getByText("Showing 1-100 of 120")).toBeInTheDocument();
    await act(async () => scrollToEnd());
    await loaded("Row 119");
    expect(lists().at(-1)!.url.searchParams.get("offset")).toBe("100");
    expect(screen.getByText("Showing 1-120 of 120")).toBeInTheDocument();
    // Everything is in: no sentinel, no Loading more, no further fetch.
    expect(document.querySelector("[data-load-more]")).toBeNull();
    const n = lists().length;
    await act(async () => scrollToEnd());
    expect(lists()).toHaveLength(n);
    // A new filter starts again from the first page.
    await user.type(screen.getByLabelText("Search logs"), "Row 1");
    await waitFor(() => expect(lists().at(-1)!.url.searchParams.get("search")).toBe("Row 1"));
    expect(lists().at(-1)!.url.searchParams.get("offset")).toBe("0");
    await waitFor(() => expect(tbody()).not.toHaveTextContent("Row 99"));
  });

  it("fetches the next page once while it is out, showing Loading more", async () => {
    const many = Array.from({ length: 120 }, (_, i) => entry(`id${i}`, { details: `Row ${i}`, created_at: 2_000 - i }));
    const { lists } = setup(many);
    await loaded("Row 0");
    await act(async () => {
      scrollToEnd();
      scrollToEnd();
    });
    expect(lists().filter((c) => c.url.searchParams.get("offset") === "50")).toHaveLength(1);
    await loaded("Row 99");
    expect(screen.queryByText("Loading more")).toBeNull();
  });

  it("clear all asks first: cancel sends nothing, confirm sends DELETE and shows the empty state", async () => {
    const { user, mutations } = setup();
    await loaded();
    await user.click(screen.getByRole("button", { name: "Clear all logs" }));
    const dialog = await screen.findByRole("dialog", { name: "Delete All Logs" });
    expect(dialog).toHaveTextContent("Are you sure you want to delete all event logs? This cannot be undone.");
    await user.click(within(dialog).getByRole("button", { name: "Cancel" }));
    await waitFor(() => expect(screen.queryByRole("dialog")).not.toBeInTheDocument());
    expect(mutations()).toEqual([]);

    await user.click(screen.getByRole("button", { name: "Clear all logs" }));
    await user.click(within(await screen.findByRole("dialog")).getByRole("button", { name: "Delete All" }));
    await waitFor(() => expect(screen.getByText("No event logs yet")).toBeInTheDocument());
    expect(mutations().map((c) => `${c.method} ${c.url.pathname}`)).toEqual(["DELETE /api/event-logs"]);
    expect(mutations()[0]!.csrf).toBe("1");
    expect(screen.getByText("All logs cleared")).toBeInTheDocument();
  });

  it("deletes one row only after confirming", async () => {
    const { user, mutations } = setup();
    await loaded();
    const row = tbody().querySelector('tr[data-log-id="b"]') as HTMLElement;
    await user.click(within(row).getByRole("button", { name: "Delete log" }));
    await user.click(within(await screen.findByRole("dialog", { name: "Delete Log" })).getByRole("button", { name: "Cancel" }));
    expect(mutations()).toEqual([]);

    await user.click(within(row).getByRole("button", { name: "Delete log" }));
    await user.click(within(await screen.findByRole("dialog")).getByRole("button", { name: "Delete" }));
    await waitFor(() => expect(tbody()).not.toHaveTextContent("Deleted channel #old-news"));
    expect(mutations().map((c) => `${c.method} ${c.url.pathname}`)).toEqual(["DELETE /api/event-logs/b"]);
    expect(screen.getByText("Showing 1-2 of 2")).toBeInTheDocument();
  });

  it("renders Discord text as text, never as markup", async () => {
    const evil = '<img src=x onerror="alert(1)"><b>bold</b>';
    setup([entry("x", { details: evil, guild_name: "<i>guild</i>", channel_name: "<script>" })]);
    await loaded(evil);
    expect(tbody().querySelector("img, b, i, script")).toBeNull();
    expect(within(tbody()).getByText("<i>guild</i>")).toBeInTheDocument();
  });

  it("expands a row with metadata and sorts the page from the headers", async () => {
    const { user } = setup([
      entry("m", {
        details: "beta",
        guild_id: "222",
        extra_json: JSON.stringify({ original_channel_id: "4", clone_channel_id: "5" }),
        created_at: 10,
      }),
      entry("n", { details: "alpha", guild_name: null, created_at: 20 }),
    ]);
    await loaded("beta");
    const row = tbody().querySelector('tr[data-log-id="m"]') as HTMLElement;
    expect(row).toHaveAttribute("aria-expanded", "false");
    expect(tbody().querySelector('tr[data-log-id="n"]')).not.toHaveAttribute("aria-expanded");
    await user.click(within(row).getByText("beta"));
    expect(row).toHaveAttribute("aria-expanded", "true");
    const detail = tbody().querySelector('tr[data-detail-for="m"]') as HTMLElement;
    expect(detail).toHaveTextContent("Guild ID222");
    expect(detail).toHaveTextContent("Source Channel ID4");
    expect(detail).toHaveTextContent("Clone Channel ID5");
    await user.click(within(row).getByText("beta"));
    expect(tbody().querySelector('tr[data-detail-for="m"]')).toBeNull();

    const order = () => Array.from(tbody().querySelectorAll("tr[data-log-id]")).map((r) => r.getAttribute("data-log-id"));
    expect(order()).toEqual(["n", "m"]);
    await user.click(screen.getByRole("button", { name: /Details/ }));
    expect(order()).toEqual(["n", "m"]);
    await user.click(screen.getByRole("button", { name: /Details/ }));
    expect(order()).toEqual(["m", "n"]);
    expect(screen.getByRole("columnheader", { name: /Details/ })).toHaveAttribute("aria-sort", "descending");
  });

  it("shows an error when the list cannot load", async () => {
    vi.stubGlobal("fetch", vi.fn(async () => json({ detail: "boom" }, 500)));
    // No providers here, like the App shell test: the page must still render.
    render(<Logs />);
    expect(await screen.findByText("Could not load event logs: boom")).toBeInTheDocument();
  });
});

describe("Logs loading", () => {
  it("shows skeleton rows until the first page arrives", async () => {
    server(SEED);
    const release = holdFetch("/api/event-logs");
    render(
      <ToastProvider>
        <ConfirmProvider>
          <Logs />
        </ConfirmProvider>
      </ToastProvider>,
    );
    expect(await screen.findByRole("status", { name: "Loading event logs" })).toBeInTheDocument();
    release();
    await loaded();
    expect(screen.queryByRole("status", { name: "Loading event logs" })).toBeNull();
  });
});
