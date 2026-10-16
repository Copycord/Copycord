import { act, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { MessageRow } from "../types/messages";
import { Messages } from "./Messages";
import { COLUMNS_KEY, SEARCH_DEBOUNCE_MS } from "./messages/model";

function row(n: number, extra: Partial<MessageRow> = {}): MessageRow {
  return {
    original_message_id: `13000000000000${String(n).padStart(5, "0")}`,
    original_guild_id: "111",
    original_guild_name: "Source Server",
    original_channel_id: "10",
    original_channel_name: "general",
    cloned_message_id: `14000000000000${String(n).padStart(5, "0")}`,
    cloned_guild_id: "222",
    cloned_guild_name: "Clone Server",
    cloned_channel_id: "20",
    cloned_channel_name: "general",
    mapping_id: "m1",
    mapping_name: "Alpha",
    author_id: "4242",
    author_name: `Ann ${n}`,
    author_username: "ann",
    attachment_count: n % 2,
    embed_count: 0,
    created_at: 1760630000 - n,
    cloned_at: 1760630002 - n,
    original_url: "https://discord.com/channels/111/10/1",
    cloned_url: "https://discord.com/channels/222/20/2",
    ...extra,
  };
}

const json = (body: unknown, status = 200, headers: Record<string, string> = {}) =>
  new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json", ...headers } });

interface Call {
  url: string;
  signal: AbortSignal | undefined;
  resolve: (r: Response) => void;
}

/** A fetch that parks every request until the test answers it; facets answer at once. */
function holdingFetch() {
  const calls: Call[] = [];
  const fn = vi.fn((input: RequestInfo | URL, init?: RequestInit) => {
    const url = String(input);
    if (url.startsWith("/api/messages/facets")) {
      return Promise.resolve(
        json({
          ok: true,
          mappings: [{ mapping_id: "m1", mapping_name: "Alpha", original_guild_id: "111", cloned_guild_id: "222" }],
          channels: [{ original_channel_id: "10", original_guild_id: "111", cloned_guild_id: "222", name: "general", count: 3 }],
        }),
      );
    }
    return new Promise<Response>((resolve, reject) => {
      const signal = init?.signal ?? undefined;
      signal?.addEventListener("abort", () => reject(new DOMException("aborted", "AbortError")));
      calls.push({ url, signal, resolve });
    });
  });
  vi.stubGlobal("fetch", fn);
  const lists = () => calls.filter((c) => c.url.startsWith("/api/messages?"));
  return { calls, lists, last: () => lists()[lists().length - 1]! };
}

let observers: Array<{ cb: IntersectionObserverCallback; el: Element | null }> = [];
beforeEach(() => {
  observers = [];
  vi.stubGlobal(
    "IntersectionObserver",
    class {
      entry: { cb: IntersectionObserverCallback; el: Element | null };
      constructor(cb: IntersectionObserverCallback) {
        this.entry = { cb, el: null };
        observers.push(this.entry);
      }
      observe(el: Element) {
        this.entry.el = el;
      }
      disconnect() {}
      unobserve() {}
      takeRecords() {
        return [];
      }
    },
  );
});

function intersect() {
  for (const o of observers) {
    if (!o.el) continue;
    o.cb([{ isIntersecting: true, target: o.el } as unknown as IntersectionObserverEntry], {} as IntersectionObserver);
  }
}

async function answer(call: Call, items: MessageRow[], next: string | null) {
  await act(async () => call.resolve(json({ ok: true, items, next_cursor: next })));
}

describe("Messages page", () => {
  it("shows skeleton rows only before the first page, then the rows", async () => {
    const f = holdingFetch();
    render(<Messages />);
    expect(screen.getByRole("heading", { level: 1, name: "Messages" })).toBeInTheDocument();
    expect(document.querySelectorAll("[data-skeleton-row]").length).toBeGreaterThan(0);
    await waitFor(() => expect(f.lists()).toHaveLength(1));
    await answer(f.last(), [row(1), row(2)], null);
    expect(document.querySelectorAll("[data-skeleton-row]")).toHaveLength(0);
    expect(screen.getAllByText("Ann 1").length).toBeGreaterThan(0);
    expect(screen.getByText(/End of list, 2 messages/)).toBeInTheDocument();
  });

  it("debounces the search and aborts a superseded request, keeping old rows meanwhile", async () => {
    const f = holdingFetch();
    render(<Messages />);
    await waitFor(() => expect(f.lists()).toHaveLength(1));
    await answer(f.last(), [row(1)], null);

    fireEvent.change(screen.getByRole("searchbox", { name: "Search messages" }), { target: { value: "an" } });
    fireEvent.change(screen.getByRole("searchbox", { name: "Search messages" }), { target: { value: "ann" } });
    await new Promise((r) => setTimeout(r, SEARCH_DEBOUNCE_MS / 2));
    expect(f.lists()).toHaveLength(1);
    await waitFor(() => expect(f.lists()).toHaveLength(2));
    const searched = f.last();
    expect(new URL(searched.url, "http://x").searchParams.get("q")).toBe("ann");
    // Old rows stay up, the table is busy.
    expect(screen.getByText("Ann 1")).toBeInTheDocument();
    expect(document.querySelector("[aria-busy='true']")).not.toBeNull();

    // A new filter supersedes the search request.
    fireEvent.change(document.getElementById("messages-filter-attachments")!, { target: { value: "yes" } });
    await waitFor(() => expect(f.lists()).toHaveLength(3));
    expect(searched.signal?.aborted).toBe(true);
    await answer(f.last(), [row(7)], null);
    expect(screen.queryByText("Ann 1")).not.toBeInTheDocument();
    expect(screen.getByText("Ann 7")).toBeInTheDocument();
  });

  it("loads the next page with the cursor when the sentinel shows", async () => {
    const f = holdingFetch();
    render(<Messages />);
    await waitFor(() => expect(f.lists()).toHaveLength(1));
    await answer(f.last(), [row(1), row(2)], "CUR1");
    act(() => intersect());
    await waitFor(() => expect(f.lists()).toHaveLength(2));
    expect(new URL(f.last().url, "http://x").searchParams.get("cursor")).toBe("CUR1");
    expect(screen.getByText("Loading more messages")).toBeInTheDocument();
    await answer(f.last(), [row(2), row(3)], null);
    expect(screen.getAllByText(/^Ann \d$/).map((e) => e.textContent)).toEqual(["Ann 1", "Ann 2", "Ann 3"]);
    expect(screen.getByText(/End of list, 3 messages/)).toBeInTheDocument();
    act(() => intersect());
    expect(f.lists()).toHaveLength(2);
  });

  it("shows the empty state after the fetch settles and resets the filters from it", async () => {
    const f = holdingFetch();
    render(<Messages />);
    await waitFor(() => expect(f.lists()).toHaveLength(1));
    await answer(f.last(), [row(1)], null);
    fireEvent.change(screen.getByRole("searchbox", { name: "Search messages" }), { target: { value: "zzz" } });
    await waitFor(() => expect(f.lists()).toHaveLength(2));
    expect(screen.queryByText("No messages match")).not.toBeInTheDocument();
    await answer(f.last(), [], null);
    const empty = document.getElementById("messages-empty")!;
    expect(within(empty).getByText("No messages match")).toBeInTheDocument();
    fireEvent.click(within(empty).getByRole("button", { name: "Reset filters" }));
    await waitFor(() => expect(f.lists()).toHaveLength(3));
    expect(new URL(f.last().url, "http://x").searchParams.get("q")).toBeNull();
    expect(screen.getByRole("searchbox", { name: "Search messages" })).toHaveValue("");
    expect(document.getElementById("messages-reset")).toBeNull();
  });

  it("shows an inline error with Retry", async () => {
    const f = holdingFetch();
    render(<Messages />);
    await waitFor(() => expect(f.lists()).toHaveLength(1));
    await act(async () => f.last().resolve(json({ detail: "boom" }, 500)));
    expect(screen.getByRole("alert")).toHaveTextContent("boom");
    fireEvent.click(screen.getByRole("button", { name: "Retry" }));
    await waitFor(() => expect(f.lists()).toHaveLength(2));
  });

  it("toggles columns from the Columns menu and remembers them", async () => {
    const f = holdingFetch();
    const { unmount } = render(<Messages />);
    await waitFor(() => expect(f.lists()).toHaveLength(1));
    await answer(f.last(), [row(1)], null);
    fireEvent.click(screen.getByRole("button", { name: "Columns" }));
    const menu = screen.getByRole("group", { name: "Visible columns" });
    fireEvent.click(within(menu).getByLabelText("Author ID"));
    fireEvent.click(within(menu).getByLabelText("Mapping"));
    expect(screen.getByRole("columnheader", { name: "Author ID" })).toBeInTheDocument();
    expect(screen.queryByRole("columnheader", { name: "Mapping" })).toBeNull();
    const saved = JSON.parse(localStorage.getItem(COLUMNS_KEY)!) as string[];
    expect(saved).toContain("author_id");
    expect(saved).not.toContain("mapping");
    unmount();

    holdingFetch();
    render(<Messages />);
    expect(screen.getByRole("columnheader", { name: "Author ID" })).toBeInTheDocument();
    expect(screen.queryByRole("columnheader", { name: "Mapping" })).toBeNull();
  });

  it("shows the default columns, groups the menu and restores the defaults", async () => {
    const f = holdingFetch();
    render(<Messages />);
    await waitFor(() => expect(f.lists()).toHaveLength(1));
    await answer(f.last(), [row(1)], null);
    const heads = screen.getAllByRole("columnheader").map((h) => h.textContent);
    expect(heads).toEqual(["Time", "Author name", "Username", "Original channel", "Mapping", "Attachments", "Links", "Message ID", "Preview"]);
    fireEvent.click(screen.getByRole("button", { name: "Columns" }));
    const menu = screen.getByRole("group", { name: "Visible columns" });
    for (const g of ["Message", "Author", "Channel", "Guild", "Other"]) {
      expect(within(menu).getByRole("group", { name: `${g} columns` })).toBeInTheDocument();
    }
    const guild = within(menu).getByRole("group", { name: "Guild columns" });
    expect(within(guild).getAllByRole("checkbox")).toHaveLength(4);
    fireEvent.click(within(menu).getByLabelText("Original guild ID"));
    expect(screen.getByRole("columnheader", { name: "Original guild ID" })).toBeInTheDocument();
    fireEvent.click(within(menu).getByRole("button", { name: "Restore defaults" }));
    expect(screen.queryByRole("columnheader", { name: "Original guild ID" })).toBeNull();
  });

  it("puts one value in each cell: display name, username, names and ids", async () => {
    localStorage.setItem(
      COLUMNS_KEY,
      JSON.stringify([
        "author_name", "username", "author_id", "original_channel", "original_channel_id", "cloned_channel",
        "cloned_channel_id", "original_guild", "original_guild_id", "cloned_guild", "cloned_guild_id", "embeds",
      ]),
    );
    const f = holdingFetch();
    render(<Messages />);
    await waitFor(() => expect(f.lists()).toHaveLength(1));
    await answer(
      f.last(),
      [
        row(1, { author_name: "Mac", author_username: "xmacj", cloned_channel_name: "general-clone", embed_count: 2 }),
        row(2, { original_guild_name: null, cloned_channel_id: null }),
      ],
      null,
    );
    const [first, second] = screen.getAllByRole("row").filter((r) => r.hasAttribute("data-message-row"));
    const cells = within(first!).getAllByRole("cell").map((c) => c.textContent);
    expect(cells).toEqual(["Mac", "xmacj", "4242", "#general", "10", "#general-clone", "20", "Source Server", "111", "Clone Server", "222", "2", "Preview"]);
    expect(within(first!).getByTitle("Mac")).toBeInTheDocument();
    expect(within(first!).getByRole("button", { name: "Copy original guild ID" })).toBeInTheDocument();
    const cells2 = within(second!).getAllByRole("cell").map((c) => c.textContent);
    expect(cells2[6]).toBe("-");
    expect(cells2[7]).toBe("-");
  });
});

describe("Preview modal", () => {
  async function openPreview(content: (url: string) => Promise<Response>) {
    const f = holdingFetch();
    const base = f.calls;
    const inner = globalThis.fetch as unknown as ReturnType<typeof vi.fn>;
    const holding = inner.getMockImplementation()!;
    inner.mockImplementation((input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input);
      return url.includes("/content") ? content(url) : holding(input, init);
    });
    render(<Messages />);
    await waitFor(() => expect(base.filter((c) => c.url.startsWith("/api/messages?"))).toHaveLength(1));
    await answer(f.last(), [row(1)], null);
    fireEvent.click(screen.getByRole("button", { name: /Preview message/ }));
    return screen.getByRole("dialog");
  }

  it("shows a shimmer, then the live text with line breaks, embeds and attachments", async () => {
    let done!: (r: Response) => void;
    const dialog = await openPreview(() => new Promise((r) => (done = r)));
    expect(within(dialog).getByRole("status", { name: "Loading message" })).toBeInTheDocument();
    expect(within(dialog).getByText("Ann 1 (ann)")).toBeInTheDocument();
    await act(async () =>
      done(
        json({
          ok: true,
          cached: false,
          source: "clone",
          message: {
            id: "9",
            channel_id: "20",
            content: "line one\nline two",
            author: { id: "1", name: "Hook", username: "Copycord" },
            created_at: null,
            edited_at: null,
            attachments: [{ filename: "a.png", url: "https://cdn.discordapp.com/a.png", size: 2048 }],
            embeds: [{ title: "Embed title", description: "D", fields: [{ name: "k", value: "v" }] }],
          },
        }),
      ),
    );
    const text = dialog.querySelector("[data-message-content]")!;
    expect(text.textContent).toBe("line one\nline two");
    expect(text.className).toContain("whitespace-pre-wrap");
    expect(within(dialog).getByText("Embed title")).toBeInTheDocument();
    expect(within(dialog).getByRole("link", { name: "a.png" })).toHaveAttribute("href", "https://cdn.discordapp.com/a.png");
    fireEvent.keyDown(document, { key: "Escape" });
    expect(screen.queryByRole("dialog")).toBeNull();
  });

  it.each([
    [503, { detail: "bot-not-running" }, /server bot is not running/],
    [404, { detail: "message-deleted" }, /deleted on Discord/],
    [403, { detail: "no-access" }, /cannot read this channel/],
  ])("explains a %s", async (status, body, text) => {
    const dialog = await openPreview(() => Promise.resolve(json(body, status)));
    expect(await within(dialog).findByText(text)).toBeInTheDocument();
    expect(within(dialog).getByRole("button", { name: "Retry" })).toBeEnabled();
  });

  it("holds Retry for the rate limit wait", async () => {
    const dialog = await openPreview(() =>
      Promise.resolve(json({ ok: false, error: "rate-limited", retry_after: 2.5 }, 429, { "Retry-After": "3" })),
    );
    expect(await within(dialog).findByText(/rate limiting/)).toBeInTheDocument();
    expect(within(dialog).getByText("Try again in 3 s.")).toBeInTheDocument();
    expect(within(dialog).getByRole("button", { name: "Retry" })).toBeDisabled();
  });
});
