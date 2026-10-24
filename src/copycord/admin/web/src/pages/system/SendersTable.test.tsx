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

  it("shows the avatar lazily and a neutral circle when there is none or it fails", async () => {
    serve(ok(ITEMS));
    render(<SendersTable />);
    await waitFor(() => expect(rowOf("a")).toBeTruthy());
    const img = rowOf("a").querySelector("img")!;
    expect(img).toHaveAttribute("loading", "lazy");
    expect(img).toHaveAttribute("src", "https://cdn.discordapp.com/embed/avatars/0.png");
    expect(img).toHaveAttribute("alt", "");
    expect(rowOf("d").querySelector('[data-avatar="none"]')).not.toBeNull();
    fireEvent.error(img);
    expect(rowOf("a").querySelector("img")).toBeNull();
    expect(rowOf("a").querySelector('[data-avatar="none"]')).not.toBeNull();
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
    expect(line.tagName).toBe("TD");
    expect(line.className).toContain("text-center");
    expect(line.className).toContain("text-muted");
    expect(line).toHaveAttribute("colspan", "6");
    const wrap = document.getElementById("system-senders")!;
    expect(wrap.className).toContain("rounded-xl border border-border");
    expect(wrap.className).not.toContain("dashed");
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

  it("the chunk skeleton has the same table under the Database card", () => {
    vi.stubGlobal("fetch", vi.fn(() => new Promise<Response>(() => {})));
    const skel = render(<SendersTableSkeleton />);
    const page = render(<SendersTable />);
    const heads = (root: HTMLElement) => within(root).getAllByRole("columnheader", { hidden: true }).map((h) => [h.textContent, h.className, h.style.width]);
    expect(heads(skel.container)).toEqual(heads(page.container));
    const firstCells = (root: HTMLElement) => [...root.querySelector("tr[data-skeleton-row]")!.querySelectorAll("td")].map((c) => c.className);
    expect(firstCells(skel.container)).toEqual(firstCells(page.container));
    expect((skel.container.firstElementChild as HTMLElement).className).toBe(document.getElementById("system-senders")!.className);

    const sys = render(<SystemSkeleton />);
    const card = within(sys.container).getByTestId("card-skeleton");
    const table = within(sys.container).getByRole("table", { name: "Sending accounts", hidden: true });
    expect(card.compareDocumentPosition(table) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
    expect(within(table).getAllByRole("columnheader", { hidden: true })).toHaveLength(6);
  });
});
