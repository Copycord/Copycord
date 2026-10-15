import { render, screen, waitFor } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { ToastProvider } from "../../components/Toast";
import { ConfirmProvider } from "../../components/ui/ConfirmDialog";
import { holdFetch } from "../../test/holdFetch";
import { cells, shared, structure } from "../../test/structure";
import type { DashboardMapping } from "../../types/dashboard-mappings";
import { Dashboard } from "../Dashboard";
import { DashboardSkeleton, MAPPINGS_COUNT_KEY } from "./DashboardSkeleton";

class FakeSocket {
  static OPEN = 1;
  readyState = 0;
  onopen: (() => void) | null = null;
  onclose: (() => void) | null = null;
  onmessage: ((ev: MessageEvent) => void) | null = null;
  send() {}
  close() {}
}

const MAPPING = {
  mapping_id: "m1",
  mapping_name: "E2E Mapping",
  original_guild_id: "1",
  original_guild_name: "Source",
  original_guild_icon_url: null,
  cloned_guild_id: "2",
  cloned_guild_name: "Clone",
  status: "active",
} as unknown as DashboardMapping;

const json = (body: unknown) => new Response(JSON.stringify(body), { headers: { "content-type": "application/json" } });

function serve() {
  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: RequestInfo | URL) =>
      String(input) === "/api/guild-mappings" ? json({ mappings: [MAPPING] }) : new Response("not found", { status: 404 }),
    ),
  );
}

const page = () =>
  render(
    <ToastProvider>
      <ConfirmProvider>
        <Dashboard WebSocketImpl={FakeSocket as unknown as typeof WebSocket} />
      </ConfirmProvider>
    </ToastProvider>,
  );

const ROWS = "[data-skeleton-row], .guild-card:not(#new-mapping-card)";

describe("Dashboard skeleton", () => {
  it("is the page's own loading state: the same cards, titles and controls", async () => {
    const skel = render(<DashboardSkeleton />);
    const skeleton = structure(skel.container, ROWS);
    skel.unmount();

    serve();
    holdFetch("/");
    const view = page();
    await screen.findByRole("heading", { name: "Bots" });
    expect(structure(view.container, ROWS)).toEqual(skeleton);
  });

  it("has the loaded page's card titles in order and a mapping tile per mapping", async () => {
    const skel = render(<DashboardSkeleton />);
    const skeleton = structure(skel.container, ROWS);
    skel.unmount();

    serve();
    const view = page();
    await waitFor(() => expect(view.container.querySelectorAll(".guild-card[data-id]")).toHaveLength(1));
    expect(structure(view.container, ROWS).headings).toEqual(skeleton.headings);
    expect(localStorage.getItem(`skel:${MAPPINGS_COUNT_KEY}`)).toBe("1");
  });

  it("draws a loading mapping tile with the boxes and buttons of a mapping card", async () => {
    const skel = render(<DashboardSkeleton />);
    const tile = skel.container.querySelector("[data-testid=mapping-skeleton]")!;
    const inner = tile.firstElementChild!;
    const skelShape = { inner: cells(tile), parts: cells(inner), head: cells(inner.children[0]!), buttons: inner.children[1]!.children.length };
    const skelBox = shared(tile);
    skel.unmount();

    serve();
    const view = page();
    await waitFor(() => expect(view.container.querySelectorAll(".guild-card[data-id]")).toHaveLength(1));
    const card = view.container.querySelector(".guild-card[data-id]")!;
    const realInner = card.firstElementChild!;
    expect(skelBox.startsWith("relative flex min-w-0 flex-col items-stretch overflow-hidden rounded-xl border p-3")).toBe(true);
    expect(shared(card, ["guild-card"]).startsWith(skelBox.split(" border-border")[0]!)).toBe(true);
    expect(cells(card, ["guild-card-inner"])).toEqual(skelShape.inner);
    expect(cells(realInner, ["guild-card-header", "guild-card-actions"])).toEqual(skelShape.parts);
    expect(cells(realInner.children[0]!, ["guild-card-avatar", "guild-card-info"]).map((c) => c.split(" ")[0])).toEqual(
      skelShape.head.map((c) => (c.startsWith("span") ? "img" : c.split(" ")[0])),
    );
    expect(realInner.children[1]!.children).toHaveLength(skelShape.buttons);
  });
});
