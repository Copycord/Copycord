import { act, render, screen, waitFor, within } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { SenderMapping } from "../../types/system";
import { SystemSenders } from "./SendersTable";
import { SystemSkeleton } from "./SystemSkeleton";
import { SENDERS_POLL_MS, SENDERS_URL } from "./useSenders";

function mapping(id: string, over: Partial<SenderMapping> = {}): SenderMapping {
  return {
    id,
    name: `Mapping ${id}`,
    status: "active",
    tokens_enabled: true,
    total: 0,
    online: 0,
    connecting: 0,
    offline: 0,
    disabled: 0,
    revoked: 0,
    ...over,
  };
}

const MAPPINGS: SenderMapping[] = [
  mapping("a", { name: "Mapping A", total: 73, online: 53, connecting: 3, offline: 17 }),
  mapping("p", { name: "Paused one", status: "paused", total: 1016, offline: 1010, disabled: 4, revoked: 2 }),
  mapping("o", { name: "Cloning off", tokens_enabled: false, total: 5, offline: 5 }),
  mapping("n", { name: "Empty" }),
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

const ok = (mappings: SenderMapping[]) => () => json({ ok: true, enabled: true, items: [], mappings });
const fail = () => json({ ok: false }, 500);

const card = (id: string) => document.querySelector(`li[data-mapping="${id}"]`) as HTMLLIElement;
const counts = (id: string) =>
  Object.fromEntries([...card(id).querySelectorAll("[data-count]")].map((e) => [e.getAttribute("data-count"), e.textContent]));

afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllGlobals();
  localStorage.clear();
});

describe("MappingsSection", () => {
  it("renders one card per mapping with its live counts", async () => {
    serve(ok(MAPPINGS));
    render(<SystemSenders />);
    await waitFor(() => expect(card("a")).not.toBeNull());
    const list = screen.getByRole("list", { name: "Mappings" });
    expect(within(list).getAllByRole("listitem").map((li) => li.getAttribute("data-mapping"))).toEqual(["a", "p", "o", "n"]);
    expect(document.getElementById("system-mappings-count")!.textContent).toBe("4 mappings");

    expect(within(card("a")).getByText("Mapping A")).toBeTruthy();
    expect(counts("a")).toEqual({ online: "53 online", connecting: "3 connecting", offline: "17 offline" });
    expect(within(card("a")).getByText("Active")).toBeTruthy();

    // Zero counts are hidden, paused has a text chip.
    expect(counts("p")).toEqual({ online: "0 online", offline: "1,010 offline", disabled: "4 disabled", revoked: "2 revoked" });
    expect(card("p").querySelector("[data-mapping-chip]")!.getAttribute("data-mapping-chip")).toBe("paused");
    expect(card("p").querySelector("[data-chip-label]")!.textContent).toBe("Paused");

    expect(within(card("o")).getByText("Token cloning off")).toBeTruthy();
    expect(counts("o")).toEqual({});
    expect(within(card("n")).getByText("No tokens")).toBeTruthy();
  });

  it("labels the bar with the counts and sizes its segments by count", async () => {
    serve(ok(MAPPINGS));
    render(<SystemSenders />);
    await waitFor(() => expect(card("a")).not.toBeNull());
    expect(within(card("a")).getByRole("img").getAttribute("aria-label")).toBe(
      "53 online, 3 connecting, 17 offline of 73 tokens",
    );
    expect(within(card("p")).getByRole("img").getAttribute("aria-label")).toBe(
      "0 online, 1,010 offline, 4 disabled, 2 revoked of 1,016 tokens",
    );
    const seg = (id: string, p: string) => (card(id).querySelector(`[data-segment="${p}"]`) as HTMLElement | null)?.style.flexGrow;
    expect(seg("a", "online")).toBe("53");
    expect(seg("a", "connecting")).toBe("3");
    expect(seg("a", "revoked")).toBeUndefined();
    // No bar label for a mapping with cloning off or no tokens.
    expect(within(card("o")).queryByRole("img")).toBeNull();
    expect(within(card("n")).queryByRole("img")).toBeNull();
  });

  it("updates in place on the next poll and keeps the last values when a poll fails", async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true });
    const next = MAPPINGS.map((m) => (m.id === "a" ? { ...m, online: 60, connecting: 0, offline: 13 } : m));
    const fetchMock = serve(ok(MAPPINGS), ok(next), fail);
    render(<SystemSenders />);
    await waitFor(() => expect(card("a")).not.toBeNull());
    const before = card("a");

    await act(async () => {
      await vi.advanceTimersByTimeAsync(SENDERS_POLL_MS);
    });
    await waitFor(() => expect(counts("a")).toEqual({ online: "60 online", offline: "13 offline" }));
    expect(card("a")).toBe(before);

    await act(async () => {
      await vi.advanceTimersByTimeAsync(SENDERS_POLL_MS);
    });
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(3));
    expect(counts("a")).toEqual({ online: "60 online", offline: "13 offline" });
    expect(card("a")).toBe(before);
  });

  it("polls once for both sections", async () => {
    const fetchMock = serve(ok(MAPPINGS));
    render(<SystemSenders />);
    await waitFor(() => expect(card("a")).not.toBeNull());
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });

  it("shows loading cards, then a note when the first poll fails", async () => {
    let release: (r: Response) => void = () => {};
    vi.stubGlobal("fetch", vi.fn(() => new Promise<Response>((r) => (release = r))));
    render(<SystemSenders />);
    expect(screen.getByRole("status", { name: "Loading mappings" })).toBeTruthy();
    expect(document.querySelectorAll("li[data-skeleton-card]").length).toBe(2);
    await act(async () => release(fail()));
    await waitFor(() => expect(screen.getByText("Could not load mappings")).toBeTruthy());
    expect(document.querySelector("li[data-skeleton-card]")).toBeNull();
  });

  it("the page skeleton has the same loading cards as the section", () => {
    localStorage.setItem("skel:system.mappings", "3");
    render(<SystemSkeleton />);
    expect(document.getElementById("system-mappings-title")!.textContent).toBe("Mappings");
    expect(document.querySelectorAll("li[data-skeleton-card]").length).toBe(3);
  });
});
