import { render, screen, waitFor } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { ToastProvider } from "../../components/Toast";
import { holdFetch } from "../../test/holdFetch";
import { cells, shared, structure } from "../../test/structure";
import { Guilds } from "../Guilds";
import { GuildsSkeleton } from "./GuildsSkeleton";

const GUILDS = [
  { id: "100", name: "Alpha", icon_url: null, member_count: 3 },
  { id: "200", name: "Beta", icon_url: null, member_count: 5 },
];

const json = (body: unknown) => new Response(JSON.stringify(body), { headers: { "content-type": "application/json" } });

beforeEach(() => {
  sessionStorage.clear();
  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input);
      if (url === "/api/status") return json({ server: { ready: true }, client: { ready: true } });
      if (url === "/api/guilds") return json({ items: GUILDS });
      return new Response("{}", { status: 404 });
    }),
  );
});

const page = () =>
  render(
    <ToastProvider>
      <Guilds />
    </ToastProvider>,
  );

const gateOpen = () => waitFor(() => expect(screen.queryByTestId("status-gate")).toBeNull());

describe("Guilds skeleton", () => {
  it("has the loaded page's title, search box and sort button, while loading and once loaded", async () => {
    const skel = render(<GuildsSkeleton />);
    await gateOpen();
    const skeleton = structure(skel.container, "article");
    skel.unmount();

    const release = holdFetch("/api/guilds");
    const view = page();
    await screen.findByRole("status", { name: "Loading guilds" });
    expect(structure(view.container, "article")).toEqual(skeleton);
    release();
    await waitFor(() => expect(view.container.querySelectorAll("#guilds-root article")).toHaveLength(2));
    const loaded = structure(view.container, "article");

    expect(loaded).toEqual(skeleton);
    expect(loaded).toMatchObject({
      headings: ["h2 Guilds"],
      controls: ["input:text Search guilds", "button Toggle sort direction"],
    });
  });

  it("draws loading tiles with the boxes of a guild tile", async () => {
    const skel = render(<GuildsSkeleton />);
    await gateOpen();
    const tile = skel.container.querySelector("[aria-label='Loading guilds'] article")!;
    const skelShape = { tile: shared(tile), body: cells(tile), parts: cells(tile.firstElementChild!) };
    skel.unmount();

    const view = page();
    await waitFor(() => expect(view.container.querySelectorAll("#guilds-root article")).toHaveLength(2));
    const real = view.container.querySelector("#guilds-root article")!;
    expect(shared(real).startsWith(skelShape.tile)).toBe(true);
    expect(cells(real).map((c) => c.split(" ")[0])).toEqual(["button"]);
    expect(skelShape.body.map((c) => c.split(" ")[0])).toEqual(["span"]);
    // The icon frame and the name line, in both.
    const realParts = cells(real.firstElementChild!);
    expect(realParts).toHaveLength(skelShape.parts.length);
    expect(realParts[1]).toBe(skelShape.parts[1]);
    expect(realParts[0]!.startsWith("span mx-auto mt-1.5 mb-2.5 grid size-24")).toBe(true);
    expect(skelShape.parts[0]!.startsWith("span mx-auto mt-1.5 mb-2.5 grid size-24")).toBe(true);
  });
});
