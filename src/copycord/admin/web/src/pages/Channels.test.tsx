import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { ChannelRow, GuildMapping } from "../types/channels";
import { ConfirmProvider } from "../components/ui";
import { ToastProvider } from "../components/Toast";
import { Channels } from "./Channels";

const renderPage = () =>
  render(
    <ToastProvider>
      <ConfirmProvider>
        <Channels />
      </ConfirmProvider>
    </ToastProvider>,
  );

const MAPPINGS: GuildMapping[] = [
  { mapping_id: "m1", mapping_name: "Main", original_guild_id: "1", original_guild_name: "Src", cloned_guild_id: "2", cloned_guild_name: "Dst" },
  { mapping_id: "m2", mapping_name: null, original_guild_id: "3", original_guild_name: "Other", cloned_guild_id: "4", cloned_guild_name: "Copy" },
];

function row(id: string, name: string, cat: string, type = 0, cloned = ""): ChannelRow {
  return {
    original_channel_id: id,
    original_channel_name: name,
    cloned_channel_id: cloned,
    clone_channel_name: "",
    channel_type: type,
    category_name: cat,
  };
}

const CHANNELS: Record<string, ChannelRow[]> = {
  m1: [
    row("10", "zeta", "Lobby", 0, "110"),
    row("11", "alpha <b>x</b>", "Lobby", 2),
    row("12", "rules", "Info", 5),
  ],
  m2: [row("20", "other-chat", "Elsewhere")],
};

const json = (body: unknown, status = 200) =>
  new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json" } });

function serve(mappings: GuildMapping[] = MAPPINGS, up = true) {
  const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
    const url = String(input);
    if (url === "/api/status") return json({ server: { ready: up }, client: { ready: up } });
    if (url === "/api/guild-mappings") return json({ ok: true, mappings });
    if (url.startsWith("/api/channels")) {
      const mid = new URL(url, "http://x").searchParams.get("mapping_id") ?? "";
      return json({ items: mid ? (CHANNELS[mid] ?? []) : [...CHANNELS.m1!, ...CHANNELS.m2!] });
    }
    return json({ error: "not-found" }, 404);
  });
  vi.stubGlobal("fetch", fetchMock);
  return fetchMock;
}

const channelUrls = (m: ReturnType<typeof serve>) =>
  m.mock.calls.map(([u]) => String(u)).filter((u) => u.startsWith("/api/channels"));

const sections = () =>
  Array.from(document.querySelectorAll("#channels-root section")).map((s) => [
    s.querySelector("h3 [data-cat-name]")?.firstChild?.textContent?.trim(),
    Array.from(s.querySelectorAll("article")).map((a) => a.getAttribute("data-cid")),
  ]);

const el = <T extends HTMLElement>(id: string) => document.getElementById(id) as T;

beforeEach(() => {
  sessionStorage.clear();
});

describe("Channels page", () => {
  it("preselects the first mapping and loads its tree", async () => {
    const f = serve();
    renderPage();
    expect(screen.getByRole("heading", { level: 2, name: "Channels" })).toBeInTheDocument();
    await waitFor(() => expect(document.querySelectorAll("#channels-root article")).toHaveLength(3));

    const sel = el<HTMLSelectElement>("mapping-select");
    expect(sel.value).toBe("m1");
    expect(Array.from(sel.options).map((o) => o.textContent)).toEqual(["Main", "Other → Copy"]);
    expect(channelUrls(f)).toEqual(["/api/channels?mapping_id=m1"]);
    expect(sections()).toEqual([
      ["Lobby", ["11", "10"]],
      ["Info", ["12"]],
    ]);
  });

  it("renders names as text, with type, clone chip and ids", async () => {
    serve();
    renderPage();
    const card = await waitFor(() => {
      const a = document.querySelector('article[data-cid="11"]');
      expect(a).not.toBeNull();
      return a!;
    });
    expect(card.querySelector("b")).toBeNull();
    expect(card).toHaveTextContent("# alpha <b>x</b>");
    expect(card).toHaveTextContent("Voice");
    const cloned = document.querySelector('article[data-cid="10"]')!;
    expect(cloned).toHaveTextContent("Clone");
    expect(cloned).toHaveTextContent("→ 110");
  });

  it("refetches the tree when the mapping changes", async () => {
    const f = serve();
    const user = userEvent.setup();
    renderPage();
    await waitFor(() => expect(sections()).toHaveLength(2));
    await user.selectOptions(el("mapping-select"), "m2");
    await waitFor(() => expect(sections()).toEqual([["Elsewhere", ["20"]]]));
    expect(channelUrls(f)).toEqual(["/api/channels?mapping_id=m1", "/api/channels?mapping_id=m2"]);
  });

  it("searches, filters and sorts", async () => {
    serve();
    const user = userEvent.setup();
    renderPage();
    await waitFor(() => expect(sections()).toHaveLength(2));

    await user.type(el("ch-search"), "#ZET");
    await waitFor(() => expect(sections()).toEqual([["Lobby", ["10"]]]));
    await user.clear(el("ch-search"));
    await user.type(el("ch-search"), "info");
    await waitFor(() => expect(sections()).toEqual([["Info", ["12"]]]));
    await user.type(el("ch-search"), "zzz");
    await waitFor(() => expect(screen.getByText("No channels found.")).toBeInTheDocument());
    expect(el("channels-empty")).toBeInTheDocument();
    await user.clear(el("ch-search"));

    await user.selectOptions(el("ch-filter"), "orphans");
    await waitFor(() => expect(sections()).toEqual([]));
    await user.selectOptions(el("ch-filter"), "nonorphans");
    await waitFor(() => expect(sections()).toHaveLength(2));

    await user.selectOptions(el("ch-sort"), "category");
    await waitFor(() =>
      expect(sections()).toEqual([
        ["Info", ["12"]],
        ["Lobby", ["11", "10"]],
      ]),
    );
    await user.selectOptions(el("ch-sort"), "type");
    await waitFor(() =>
      expect(sections()).toEqual([
        ["Lobby", ["10", "11"]],
        ["Info", ["12"]],
      ]),
    );
    expect(Array.from(el<HTMLSelectElement>("ch-sort").options).map((o) => o.textContent)).toEqual([
      "Name",
      "Category",
      "Type",
    ]);
  });

  it("waits for Server and Client before loading the tree", async () => {
    const f = serve(MAPPINGS, false);
    renderPage();
    await waitFor(() => expect(screen.getByTestId("status-gate")).toBeInTheDocument());
    await waitFor(() => expect(f.mock.calls.some(([u]) => String(u) === "/api/guild-mappings")).toBe(true));
    expect(channelUrls(f)).toEqual([]);
    expect(el("channels-empty")).not.toBeVisible();
  });

  it("lists every channel when no mapping is configured", async () => {
    const f = serve([]);
    renderPage();
    await waitFor(() => expect(document.querySelectorAll("#channels-root article")).toHaveLength(4));
    const sel = el<HTMLSelectElement>("mapping-select");
    expect(sel).toBeDisabled();
    expect(sel.options[0]).toHaveTextContent("(no mappings configured)");
    expect(channelUrls(f)).toEqual(["/api/channels"]);
  });
});
