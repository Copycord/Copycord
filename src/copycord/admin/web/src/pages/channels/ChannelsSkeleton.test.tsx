import { render, screen, waitFor } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { ToastProvider } from "../../components/Toast";
import { CARD_HOVER, ConfirmProvider } from "../../components/ui";
import { holdFetch } from "../../test/holdFetch";
import { cells, shared, structure } from "../../test/structure";
import type { ChannelRow } from "../../types/channels";
import { Channels } from "../Channels";
import { ChannelsSkeleton } from "./ChannelsSkeleton";

const row = (id: string, name: string, cat: string, cloned = ""): ChannelRow => ({
  original_channel_id: id,
  original_channel_name: name,
  cloned_channel_id: cloned,
  clone_channel_name: "",
  channel_type: 0,
  category_name: cat,
});

const MAPPINGS = [
  { mapping_id: "m1", mapping_name: "Main", original_guild_id: "1", original_guild_name: "Src", cloned_guild_id: "2", cloned_guild_name: "Dst" },
];
const ROWS = [row("10", "general", "Lobby", "110"), row("11", "rules", "Lobby"), row("12", "news", "Info", "112")];

const json = (body: unknown) => new Response(JSON.stringify(body), { headers: { "content-type": "application/json" } });

function serve() {
  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input);
      if (url === "/api/status") return json({ server: { ready: true }, client: { ready: true } });
      if (url === "/api/guild-mappings") return json({ ok: true, mappings: MAPPINGS });
      if (url.startsWith("/api/channels")) return json({ items: ROWS });
      return new Response("{}", { status: 404 });
    }),
  );
}

const page = () =>
  render(
    <ToastProvider>
      <ConfirmProvider>
        <Channels />
      </ConfirmProvider>
    </ToastProvider>,
  );

const gateOpen = () => waitFor(() => expect(screen.queryByTestId("status-gate")).toBeNull());
const searchBox = (root: Element) => root.querySelector<HTMLInputElement>('input[aria-label="Search channels"]');
// The tree of either state: the loaded tree and the loading tree (the Card is a section too).
const ROWS_SEL = "#channels-root, [aria-label='Loading channels']";

beforeEach(() => {
  sessionStorage.clear();
  serve();
});

describe("Channels skeleton", () => {
  it("has the loaded page's title and control row, the search box included, at every stage", async () => {
    const skel = render(<ChannelsSkeleton />);
    await gateOpen();
    const skeleton = structure(skel.container, ROWS_SEL);
    expect(searchBox(skel.container)!.hidden).toBe(false);
    skel.unmount();

    const release = holdFetch("/api/channels");
    const loading = page();
    await screen.findByRole("status", { name: "Loading channels" });
    expect(searchBox(loading.container)!.hidden).toBe(false);
    await waitFor(() => expect(structure(loading.container, ROWS_SEL, ["Main"])).toEqual(skeleton));
    release();
    await waitFor(() => expect(loading.container.querySelectorAll("#channels-root article")).toHaveLength(3));
    const loaded = structure(loading.container, ROWS_SEL, ["Main"]);

    expect(loaded).toEqual(skeleton);
    expect(loaded.headings).toEqual(["h2 Channels"]);
    expect(loaded.controls).toEqual([
      "button Guild mapping: <data>",
      "input:text Search channels",
      "button Filter: All channels",
      "button Sort: Name",
      "button Delete 0 orphans",
    ]);
  });

  it("draws loading sections and cards with the boxes of the real ones", async () => {
    const skel = render(<ChannelsSkeleton />);
    await gateOpen();
    const skelSection = skel.container.querySelector("[aria-label='Loading channels'] section")!;
    const skelCard = skelSection.querySelector("article")!;
    const skelShape = { section: cells(skelSection), card: shared(skelCard), head: cells(skelCard), parts: Array.from(skelCard.children).map((c) => cells(c)) };
    skel.unmount();

    const loaded = page();
    await waitFor(() => expect(loaded.container.querySelectorAll("#channels-root article")).toHaveLength(3));
    const section = loaded.container.querySelector("#channels-root section")!;
    // A cloned channel has no badge: its card is the name row and the id row, as the loading card.
    const card = loaded.container.querySelector('article[data-cid="10"]')!;
    expect(cells(section)).toEqual(skelShape.section);
    // The hover lift is interaction only, the loading card has no hover.
    expect(shared(card, ["ch-card", ...CARD_HOVER.split(" ")])).toBe(skelShape.card);
    expect(cells(card)).toEqual(skelShape.head);
    const parts = Array.from(card.children).map((c) => cells(c));
    // The name cell (type icon and name) and the menu box, then the id row (one id while loading, the clone id after).
    expect(parts).toHaveLength(2);
    expect(skelShape.parts).toHaveLength(2);
    expect(parts[0]).toEqual(skelShape.parts[0]);
    expect(skelShape.parts[1]!.length).toBeGreaterThanOrEqual(1);
  });
});
