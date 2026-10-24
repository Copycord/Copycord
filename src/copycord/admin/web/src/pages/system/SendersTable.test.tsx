import { act, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { Sender } from "../../types/system";
import { SendersTableSkeleton } from "./SendersSkeleton";
import { SendersTable } from "./SendersTable";
import { SystemSkeleton } from "./SystemSkeleton";
import { SENDERS_POLL_MS, SENDERS_URL } from "./useSenders";

const NOW = 1_800_000_000;

function sender(id: string, over: Partial<Sender> = {}): Sender {
  return {
    id,
    username: `relay.${id}`,
    display_name: `Name ${id}`,
    avatar_url: `https://cdn.discordapp.com/embed/avatars/0.png`,
    state: "offline",
    presence: null,
    since: null,
    last_used: null,
    use_count: 0,
    ...over,
  };
}

const ITEMS: Sender[] = [
  sender("a", { state: "online", presence: "online", since: NOW - 125, last_used: NOW - 300, use_count: 1284 }),
  sender("b", { state: "online", presence: "dnd", since: NOW - 60, use_count: 977 }),
  sender("c", { state: "online", presence: "invisible", since: NOW - 3600, use_count: 612 }),
  sender("d", { state: "connecting", avatar_url: null }),
  sender("e", { state: "offline", last_used: NOW - 720, use_count: 305 }),
  sender("f", { state: "revoked", display_name: null, last_used: NOW - 7200, use_count: 88 }),
  sender("g", { state: "disabled", username: null, display_name: null, use_count: 3 }),
];

const json = (body: unknown, status = 200) =>
  new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json" } });

/** Stubs fetch: each senders call takes the next reply, the last one repeats. */
function serve(...replies: Array<() => Response>) {
  let n = 0;
  const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
    if (String(input) !== SENDERS_URL) return new Response("not found", { status: 404 });
    const reply = replies[Math.min(n, replies.length - 1)]!;
    n += 1;
    return reply();
  });
  vi.stubGlobal("fetch", fetchMock);
  return fetchMock;
}

const ok = (items: Sender[]) => () => json({ ok: true, items });

const COLORS = ["#5865F2", "#747F8D", "#3BA55D", "#FAA61A", "#ED4245", "#EB459E"];
/** A hex colour as jsdom reports an inline background. */
function rgb(hex: string) {
  const n = Number.parseInt(hex.slice(1), 16);
  return `rgb(${n >> 16}, ${(n >> 8) & 255}, ${n & 255})`;
}

const order = () => [...document.querySelectorAll("tr[data-sender]")].map((r) => r.getAttribute("data-sender"));

const rowOf = (id: string) => document.querySelector(`tr[data-sender="${id}"]`) as HTMLTableRowElement;
const cellText = (id: string, col: number) => rowOf(id).cells[col]!.textContent;

afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllGlobals();
  localStorage.clear();
});

describe("SendersTable", () => {
  it("renders every column and every state", async () => {
    vi.useFakeTimers({ toFake: ["Date"] });
    vi.setSystemTime(NOW * 1000);
    serve(ok(ITEMS));
    render(<SendersTable />);
    const table = screen.getByRole("table", { name: "Sending accounts" });
    expect(within(table).getAllByRole("columnheader").map((h) => h.textContent)).toEqual([
      "Account",
      "Username",
      "Status",
      "Online for",
      "Last send",
      "Messages sent",
    ]);
    await waitFor(() => expect(rowOf("a")).toBeTruthy());

    const status = (id: string) => rowOf(id).getAttribute("data-status");
    expect(ITEMS.map((s) => status(s.id))).toEqual(["online", "busy", "invisible", "connecting", "offline", "revoked", "disabled"]);
    expect(ITEMS.map((s) => cellText(s.id, 2))).toEqual(["Online", "Busy", "Invisible", "Connecting", "Offline", "Revoked", "Disabled"]);

    // Dots: filled for live presence, a ring while connecting or offline, pulsing only while connecting, none otherwise.
    const dot = (id: string) => rowOf(id).querySelector("[data-dot]");
    expect(dot("a")!.className).toContain("bg-good");
    expect(dot("b")!.className).toContain("bg-danger");
    expect(dot("c")!.className).toContain("bg-muted");
    expect(dot("d")!.className).toContain("motion-safe:animate-pulse");
    expect(dot("e")!.className).toContain("border-muted");
    expect(dot("e")!.className).not.toContain("animate");
    expect(dot("a")!.className).not.toContain("animate");
    expect(dot("f")).toBeNull();
    expect(dot("g")).toBeNull();
    expect(within(rowOf("f")).getByText("Revoked").className).toContain("text-warn");

    // Account: display name, then username, then id; username muted with a dash when missing.
    expect(cellText("a", 0)).toBe("Name a");
    expect(cellText("f", 0)).toBe("relay.f");
    expect(cellText("g", 0)).toBe("g");
    expect(within(rowOf("a")).getByText("relay.a").className).toContain("text-muted");
    expect(cellText("g", 1)).toBe("-");

    // Online for only while online; last send relative, a dash when never used.
    expect(cellText("a", 3)).toBe("2m 5s");
    expect(cellText("c", 3)).toBe("1h 0m 0s");
    expect(cellText("d", 3)).toBe("-");
    expect(cellText("f", 3)).toBe("-");
    expect(cellText("a", 4)).toBe("5m ago");
    expect(cellText("f", 4)).toBe("2h ago");
    expect(cellText("b", 4)).toBe("-");

    // Messages sent: right aligned, tabular, grouped.
    const sent = rowOf("a").cells[5]!;
    expect(sent.textContent).toBe((1284).toLocaleString());
    expect(sent.className).toContain("text-right");
    expect(sent.querySelector("span")!.className).toContain("tabular-nums");

    // Every cell is one truncating line with its full value in title.
    const name = within(rowOf("a")).getByText("Name a");
    expect(name.className).toContain("truncate");
    expect(name).toHaveAttribute("title", "Name a");
  });

  it("dims revoked and disabled rows only", async () => {
    serve(ok(ITEMS));
    render(<SendersTable />);
    await waitFor(() => expect(rowOf("a")).toBeTruthy());
    const dimmed = ITEMS.filter((s) => rowOf(s.id).className.includes("opacity-60")).map((s) => s.id);
    expect(dimmed).toEqual(["f", "g"]);
  });

  it("shows the avatar lazily over its default colour and the colour circle when there is none or it fails", async () => {
    serve(ok(ITEMS));
    render(<SendersTable />);
    await waitFor(() => expect(rowOf("a")).toBeTruthy());
    const img = rowOf("a").querySelector("img")!;
    expect(img).toHaveAttribute("loading", "lazy");
    expect(img).toHaveAttribute("src", "https://cdn.discordapp.com/embed/avatars/0.png");
    expect(img).toHaveAttribute("alt", "");
    expect(img.className).toContain("size-6");
    expect(img.style.backgroundColor).toBe(rgb("#5865F2"));
    expect(rowOf("d").querySelector('[data-avatar="default"]')).not.toBeNull();
    fireEvent.error(img);
    expect(rowOf("a").querySelector("img")).toBeNull();
    const circle = rowOf("a").querySelector<HTMLElement>('[data-avatar="default"]')!;
    expect(circle.className).toContain("size-6");
    expect(circle.style.backgroundColor).toBe(rgb("#5865F2"));
    expect(circle.textContent).toBe("");
  });

  it("ticks Online for every second", async () => {
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "Date"] });
    vi.setSystemTime(NOW * 1000);
    serve(ok(ITEMS));
    render(<SendersTable />);
    await vi.waitFor(() => expect(rowOf("a")).toBeTruthy());
    const seconds = () => Number(/(\d+)s$/.exec(cellText("a", 3) ?? "")![1]);
    const before = seconds();
    await act(async () => {
      vi.advanceTimersByTime(3000);
    });
    expect(seconds()).toBe(before + 3);
  });

  it("keeps the same bordered wrap with one muted line when there are no accounts", async () => {
    serve(ok([]));
    render(<SendersTable />);
    const line = await screen.findByText("No sending accounts yet");
    expect(line.className).toContain("text-center");
    expect(line.className).toContain("text-muted");
    expect(line.className).toContain("absolute inset-0 flex items-center justify-center");
    const wrap = document.getElementById("system-senders")!;
    expect(line.parentElement).toBe(wrap);
    expect(wrap.className).toContain("rounded-xl border border-border");
    expect(wrap.className).toContain("h-[520px]");
    expect(wrap.className).not.toContain("dashed");
    expect(document.getElementById("system-senders-count")).toHaveTextContent("0 accounts");
    expect(screen.getAllByRole("columnheader")).toHaveLength(6);
    expect(document.querySelectorAll("tr[data-sender]")).toHaveLength(0);
  });

  it("polls every 5 s and keeps the previous rows when a refetch fails", async () => {
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
    const fetchMock = serve(ok(ITEMS), () => json({ detail: "boom" }, 500));
    render(<SendersTable />);
    await vi.waitFor(() => expect(rowOf("a")).toBeTruthy());
    expect(fetchMock).toHaveBeenCalledTimes(1);
    await act(async () => {
      vi.advanceTimersByTime(SENDERS_POLL_MS);
    });
    await vi.waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(2));
    // The failed poll has landed once the next one is scheduled.
    await act(async () => {
      vi.advanceTimersByTime(SENDERS_POLL_MS);
    });
    await vi.waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(3));
    expect(document.querySelectorAll("tr[data-sender]")).toHaveLength(ITEMS.length);
    expect(screen.queryByText("Could not load sending accounts")).toBeNull();
    expect(screen.queryByText("No sending accounts yet")).toBeNull();
  });

  it("aborts a poll still running when a newer one starts and ignores its answer", async () => {
    const signals: AbortSignal[] = [];
    let first!: (r: Response) => void;
    vi.stubGlobal(
      "fetch",
      vi.fn((_: RequestInfo | URL, init: RequestInit = {}) => {
        signals.push(init.signal!);
        if (signals.length === 1) return new Promise<Response>((r) => (first = r));
        return Promise.resolve(json({ ok: true, items: [sender("new", { state: "online", presence: "online", since: NOW })] }));
      }),
    );
    render(<SendersTable />);
    await waitFor(() => expect(signals).toHaveLength(1));
    act(() => {
      document.dispatchEvent(new Event("visibilitychange"));
    });
    await waitFor(() => expect(rowOf("new")).toBeTruthy());
    expect(signals[0]!.aborted).toBe(true);
    first(json({ ok: true, items: ITEMS }));
    await act(async () => {
      await Promise.resolve();
    });
    expect(rowOf("a")).toBeNull();
    expect(document.querySelectorAll("tr[data-sender]")).toHaveLength(1);
  });
});

describe("SendersTable loading", () => {
  it("draws loading rows that share the head, columns and cell classes of loaded rows", async () => {
    let release!: () => void;
    const gate = new Promise<void>((r) => (release = r));
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => {
        await gate;
        return json({ ok: true, items: ITEMS.slice(0, 3) });
      }),
    );
    const { container } = render(<SendersTable />);
    const wrap = document.getElementById("system-senders")!;
    const skelRows = [...container.querySelectorAll("tr[data-skeleton-row]")] as HTMLTableRowElement[];
    expect(skelRows).toHaveLength(3);
    expect(screen.getByRole("status", { name: "Loading sending accounts" })).toBeInTheDocument();
    // The head is real text, the bars sit only in the data cells.
    expect(container.querySelectorAll("thead [data-skeleton]")).toHaveLength(0);
    const skelCells = skelRows[0]!.cells;
    const skelClasses = [...skelCells].map((c) => c.className);
    for (const cell of skelCells) expect(cell.querySelector("[data-skeleton]")).not.toBeNull();
    const wrapClass = wrap.className;

    release();
    await waitFor(() => expect(rowOf("a")).toBeTruthy());
    const loaded = rowOf("a");
    expect(loaded.cells).toHaveLength(skelCells.length);
    expect([...loaded.cells].map((c) => c.className)).toEqual(skelClasses);
    expect(loaded.className).toBe(skelRows[0]!.className);
    expect(document.getElementById("system-senders")!.className).toBe(wrapClass);
    expect(container.querySelectorAll("tr[data-skeleton-row]")).toHaveLength(0);
    // The next visit draws as many loading rows as this one loaded.
    expect(localStorage.getItem("skel:system.senders")).toBe("3");
  });

  it("the chunk skeleton has the same header, toolbar and table under the Database card", async () => {
    let release!: () => void;
    const gate = new Promise<void>((r) => (release = r));
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => {
        await gate;
        return json({ ok: true, items: ITEMS });
      }),
    );
    const skel = render(<SendersTableSkeleton />);
    const page = render(<SendersTable />);
    const heads = (root: HTMLElement) =>
      within(root)
        .getAllByRole("columnheader", { hidden: true })
        .map((h) => [h.textContent, h.className, h.style.width, h.querySelector("button")?.className]);
    const firstCells = (root: HTMLElement) => [...root.querySelector("tr[data-skeleton-row]")!.querySelectorAll("td")].map((c) => c.className);
    // The section's tree of tags and classes, down to the table and the count box: header, toolbar card, search, wrap.
    const leaf = (el: Element) => el.tagName === "TABLE" || el.id === "system-senders-count";
    const shape = (el: Element): unknown => [el.tagName, el.getAttribute("class"), leaf(el) ? [] : [...el.children].map(shape)];
    const section = (root: HTMLElement) => root.querySelector("section")!;
    expect(heads(skel.container)).toEqual(heads(page.container));
    expect(firstCells(skel.container)).toEqual(firstCells(page.container));
    expect(shape(section(skel.container))).toEqual(shape(section(page.container)));
    expect(within(section(skel.container)).getByRole("heading", { level: 2, hidden: true })).toHaveTextContent("Token Message Cloning");
    expect(skel.container.querySelector("#system-senders-count [data-skeleton]")).not.toBeNull();
    expect(within(skel.container).getByRole("searchbox", { hidden: true })).toHaveAttribute("placeholder", "Search accounts");
    const skelWrap = skel.container.querySelector("table")!.parentElement!;
    expect(skelWrap.className).toBe(document.getElementById("system-senders")!.className);

    // Loaded: the header, toolbar and wrap keep their classes, only the count and rows change.
    const loadingShape = shape(section(page.container));
    release();
    await waitFor(() => expect(rowOf("a")).toBeTruthy());
    expect(shape(section(page.container))).toEqual(loadingShape);
    expect(heads(page.container)).toEqual(heads(skel.container));

    const sys = render(<SystemSkeleton />);
    const card = within(sys.container).getByTestId("card-skeleton");
    const table = within(sys.container).getByRole("table", { name: "Sending accounts", hidden: true });
    expect(card.compareDocumentPosition(table) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
    expect(within(table).getAllByRole("columnheader", { hidden: true })).toHaveLength(6);
  });
});

const many = (n: number) => Array.from({ length: n }, (_, i) => sender(`s${i}`, { state: "online", presence: "online", since: NOW - i }));

describe("SendersTable size", () => {
  it.each([3, 100])("keeps the same fixed height wrap and sticky head with %i accounts", async (n) => {
    serve(ok(many(n)));
    render(<SendersTable />);
    await waitFor(() => expect(document.querySelectorAll("tr[data-sender]")).toHaveLength(n));
    const wrap = document.getElementById("system-senders")!;
    for (const c of ["h-[520px]", "max-h-[calc(100vh-420px)]", "min-h-[360px]", "overflow-auto", "cc-hscroll", "relative"]) {
      expect(wrap.className.split(" ")).toContain(c);
    }
    expect(wrap.className).not.toMatch(/(^| )h-auto|max-h-\[min/);
    expect(wrap.getAttribute("style")).toBeNull();
    expect(wrap.querySelector("thead")!.className).toContain("sticky top-0");
    expect(screen.queryByText(/end of list/i)).toBeNull();
  });
});

describe("SendersTable avatars", () => {
  it("falls back to the Discord default colour of the URL index, or of a stable id hash", async () => {
    const items = [
      ...COLORS.map((_, n) => sender(`u${n}`, { avatar_url: `https://cdn.discordapp.com/embed/avatars/${n}.png` })),
      sender("a", { avatar_url: null }),
      sender("b", { avatar_url: "https://cdn.discordapp.com/avatars/2/abc.png" }),
    ];
    serve(ok(items));
    render(<SendersTable />);
    await waitFor(() => expect(rowOf("u0")).toBeTruthy());
    for (const img of document.querySelectorAll("tr[data-sender] img")) fireEvent.error(img);
    const circle = (id: string) => rowOf(id).querySelector<HTMLElement>('[data-avatar="default"]')!;
    COLORS.forEach((hex, n) => {
      expect(circle(`u${n}`).style.backgroundColor).toBe(rgb(hex));
      expect(circle(`u${n}`)).toHaveAttribute("data-avatar-index", String(n));
    });
    // No default URL: ("a".charCodeAt(0) = 97) % 6 = 1 grey; "b" = 98 % 6 = 2 green, the same on every render.
    expect(circle("a").style.backgroundColor).toBe(rgb(COLORS[1]!));
    expect(circle("b").style.backgroundColor).toBe(rgb(COLORS[2]!));
  });

  it("swaps the default for the real avatar on a later poll in the same box", async () => {
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
    const real = "https://cdn.discordapp.com/avatars/1/abc.png?size=64";
    serve(ok([sender("a", { avatar_url: "https://cdn.discordapp.com/embed/avatars/3.png" })]), ok([sender("a", { avatar_url: real })]));
    render(<SendersTable />);
    await vi.waitFor(() => expect(rowOf("a")).toBeTruthy());
    const img = rowOf("a").querySelector("img")!;
    expect(img.style.backgroundColor).toBe(rgb(COLORS[3]!));
    const cls = img.className;
    await act(async () => {
      vi.advanceTimersByTime(SENDERS_POLL_MS);
    });
    await vi.waitFor(() => expect(rowOf("a").querySelector("img")).toHaveAttribute("src", real));
    expect(rowOf("a").querySelector("img")).toBe(img);
    expect(img.className).toBe(cls);
    expect(img).toHaveAttribute("loading", "lazy");
  });
});

describe("SendersTable header and search", () => {
  it("titles the section and counts the accounts", async () => {
    serve(ok(ITEMS));
    render(<SendersTable />);
    expect(screen.getByRole("heading", { level: 2, name: "Token Message Cloning" })).toBeInTheDocument();
    expect(document.querySelector("#system-senders-count [data-skeleton]")).not.toBeNull();
    await waitFor(() => expect(document.getElementById("system-senders-count")).toHaveTextContent(/^7 accounts$/));
    expect(screen.getByRole("searchbox", { name: "Search accounts" })).toHaveAttribute("placeholder", "Search accounts");
  });

  it("says 1 account for one", async () => {
    serve(ok(ITEMS.slice(0, 1)));
    render(<SendersTable />);
    await waitFor(() => expect(document.getElementById("system-senders-count")).toHaveTextContent(/^1 account$/));
  });

  it("filters by name, username and status label, case-insensitive, with an M of N count; Escape clears", async () => {
    serve(ok(ITEMS));
    render(<SendersTable />);
    await waitFor(() => expect(rowOf("a")).toBeTruthy());
    const box = screen.getByRole("searchbox", { name: "Search accounts" });
    const count = document.getElementById("system-senders-count")!;

    fireEvent.change(box, { target: { value: "NAME B" } });
    expect(order()).toEqual(["b"]);
    expect(count).toHaveTextContent(/^1 of 7 accounts$/);
    fireEvent.change(box, { target: { value: "relay.f" } });
    expect(order()).toEqual(["f"]);
    fireEvent.change(box, { target: { value: "busy" } });
    expect(order()).toEqual(["b"]);
    fireEvent.change(box, { target: { value: "online" } });
    expect(order()).toEqual(["a"]);
    fireEvent.change(box, { target: { value: "relay" } });
    expect(order()).toEqual(["a", "b", "c", "d", "e", "f"]);
    expect(count).toHaveTextContent(/^6 of 7 accounts$/);

    fireEvent.keyDown(box, { key: "Escape" });
    expect(box).toHaveValue("");
    expect(order()).toEqual(ITEMS.map((s) => s.id));
    expect(count).toHaveTextContent(/^7 accounts$/);
  });

  it("keeps the wrap its size with one muted centred line when nothing matches", async () => {
    serve(ok(ITEMS));
    render(<SendersTable />);
    await waitFor(() => expect(rowOf("a")).toBeTruthy());
    const wrapClass = document.getElementById("system-senders")!.className;
    fireEvent.change(screen.getByRole("searchbox"), { target: { value: "zzz" } });
    const line = screen.getByText("No matching accounts");
    expect(line.className).toContain("absolute inset-0 flex items-center justify-center");
    expect(line.className).toContain("text-muted");
    expect(document.getElementById("system-senders")!.className).toBe(wrapClass);
    expect(order()).toEqual([]);
    expect(screen.getAllByRole("columnheader")).toHaveLength(6);
    expect(document.getElementById("system-senders-count")).toHaveTextContent(/^0 of 7 accounts$/);
    expect(screen.queryByText("No sending accounts yet")).toBeNull();
  });
});

describe("SendersTable sort", () => {
  const head = (name: string) => screen.getByRole("button", { name });
  const th = (name: string) => head(name).closest("th")!;

  it.each([
    ["Account", ["g", "a", "b", "c", "d", "e", "f"], ["f", "e", "d", "c", "b", "a", "g"]],
    ["Username", ["a", "b", "c", "d", "e", "f", "g"], ["f", "e", "d", "c", "b", "a", "g"]],
    ["Status", ["a", "b", "c", "d", "e", "g", "f"], ["f", "g", "e", "d", "c", "b", "a"]],
    ["Online for", ["b", "a", "c", "d", "e", "f", "g"], ["c", "a", "b", "d", "e", "f", "g"]],
    ["Last send", ["f", "e", "a", "b", "c", "d", "g"], ["a", "e", "f", "b", "c", "d", "g"]],
    ["Messages sent", ["d", "g", "f", "e", "c", "b", "a"], ["a", "b", "c", "e", "f", "g", "d"]],
  ])("%s sorts ascending, descending, then back to the API order, unknown values last", async (name, asc, desc) => {
    serve(ok(ITEMS));
    render(<SendersTable />);
    await waitFor(() => expect(rowOf("a")).toBeTruthy());
    expect(screen.getAllByRole("columnheader").filter((h) => h.hasAttribute("aria-sort"))).toHaveLength(0);

    fireEvent.click(head(name));
    expect(order()).toEqual(asc);
    expect(th(name)).toHaveAttribute("aria-sort", "ascending");
    expect(head(name).textContent).toBe(`${name}▲`);
    expect(head(name).className).toContain("text-text");
    expect(screen.getAllByRole("columnheader").filter((h) => h.hasAttribute("aria-sort"))).toHaveLength(1);

    fireEvent.click(head(name));
    expect(order()).toEqual(desc);
    expect(th(name)).toHaveAttribute("aria-sort", "descending");
    expect(head(name).textContent).toBe(`${name}▼`);

    fireEvent.click(head(name));
    expect(order()).toEqual(ITEMS.map((s) => s.id));
    expect(th(name)).not.toHaveAttribute("aria-sort");
    expect(head(name).textContent).toBe(name);
  });

  it("another column starts ascending", async () => {
    serve(ok(ITEMS));
    render(<SendersTable />);
    await waitFor(() => expect(rowOf("a")).toBeTruthy());
    fireEvent.click(head("Messages sent"));
    fireEvent.click(head("Messages sent"));
    fireEvent.click(head("Username"));
    expect(th("Username")).toHaveAttribute("aria-sort", "ascending");
    expect(th("Messages sent")).not.toHaveAttribute("aria-sort");
  });

  it("keeps the sort and the search across a poll, with the new rows swapped in and no skeleton", async () => {
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
    const next = ITEMS.map((s) => (s.id === "d" ? { ...s, use_count: 5000 } : s));
    serve(ok(ITEMS), ok(next));
    render(<SendersTable />);
    await vi.waitFor(() => expect(rowOf("a")).toBeTruthy());
    fireEvent.click(head("Messages sent"));
    fireEvent.click(head("Messages sent"));
    expect(order()).toEqual(["a", "b", "c", "e", "f", "g", "d"]);
    fireEvent.change(screen.getByRole("searchbox"), { target: { value: "name" } });
    expect(order()).toEqual(["a", "b", "c", "e", "d"]);

    await act(async () => {
      vi.advanceTimersByTime(SENDERS_POLL_MS);
    });
    await vi.waitFor(() => expect(order()[0]).toBe("d"));
    expect(order()).toEqual(["d", "a", "b", "c", "e"]);
    expect(th("Messages sent")).toHaveAttribute("aria-sort", "descending");
    expect(screen.getByRole("searchbox")).toHaveValue("name");
    expect(document.getElementById("system-senders-count")).toHaveTextContent(/^5 of 7 accounts$/);
    expect(document.querySelectorAll("tr[data-skeleton-row]")).toHaveLength(0);
  });
});
