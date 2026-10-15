import { render, screen, waitFor } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { ToastProvider } from "../../components/Toast";
import { ConfirmProvider } from "../../components/ui/ConfirmDialog";
import { holdFetch } from "../../test/holdFetch";
import { cells, shared, structure } from "../../test/structure";
import type { ScraperScrape, ScraperToken } from "../../types/scraper";
import { Scraper } from "../Scraper";
import { ScraperSkeleton, ScrapesPlaceholder, SCRAPES_COUNT_KEY, TokenListPlaceholder, TOKENS_COUNT_KEY } from "./ScraperSkeleton";
import { FakeSource, FakeWs } from "./testFakes";

const TOKENS: ScraperToken[] = [
  { token_id: "t1", label: "Main", masked: "MTAxMj…ab12", is_valid: true, username: "scrapey" },
  { token_id: "t2", label: "Spare", masked: "NzQ5ND…cd34", is_valid: false, username: null },
];
const SCRAPE = {
  filename: "g1.json",
  path: "g1.json",
  guild_id: "1",
  guild_name: "E2E Guild",
  total_count: 12,
  file_size: 2048,
  scraped_at: "2026-10-14T18:00:00Z",
  metadata: { method: "gateway" },
} as ScraperScrape;

const json = (body: unknown) => new Response(JSON.stringify(body), { headers: { "content-type": "application/json" } });

function serve(tokens: ScraperToken[], scrapes: ScraperScrape[]) {
  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input);
      if (url === "/api/scraper/tokens") return json({ ok: true, tokens });
      if (url === "/api/scraper/status") return json({ running: false, queue: [] });
      if (url === "/api/scraper/proxies") return json({ ok: true, proxies: [] });
      if (url === "/api/scraper/scrapes") return json({ ok: true, scrapes });
      return new Response("{}", { status: 404 });
    }),
  );
}

const page = () =>
  render(
    <ToastProvider>
      <ConfirmProvider>
        <Scraper />
      </ConfirmProvider>
    </ToastProvider>,
  );

const ROWS = "[data-skeleton-row], .sc-token-item, .sc-scrape-item";

beforeEach(() => {
  vi.stubGlobal("WebSocket", FakeWs);
  vi.stubGlobal("EventSource", FakeSource);
  serve(TOKENS, [SCRAPE]);
});

describe("Scraper skeleton", () => {
  it("has the loaded page's titles, cards and controls, while loading and once loaded", async () => {
    const skel = render(<ScraperSkeleton />);
    const skeleton = structure(skel.container, ROWS);
    skel.unmount();

    const release = holdFetch("/api/scraper/");
    const view = page();
    expect(structure(view.container, ROWS)).toEqual(skeleton);
    release();
    await waitFor(() => expect(view.container.querySelectorAll(".sc-token-item")).toHaveLength(2));
    await waitFor(() => expect(view.container.querySelectorAll(".sc-scrape-item")).toHaveLength(1));
    const loaded = structure(view.container, ROWS);

    expect(loaded).toEqual(skeleton);
    expect(loaded.headings).toEqual([
      "h1 Scraper",
      "h2 Member Scraper",
      "h3 Tokens",
      "h3 Configuration",
      "h3 Proxies",
      "h3 Scraper Logs",
      "h3 Past Scrapes",
    ]);
  });

  it("draws token and scrape loading rows with the cells of the real rows, as many as last time", async () => {
    const view = page();
    await waitFor(() => expect(view.container.querySelectorAll(".sc-scrape-item")).toHaveLength(1));
    await waitFor(() => expect(view.container.querySelectorAll(".sc-token-item")).toHaveLength(2));
    const token = view.container.querySelector(".sc-token-item")!;
    const scrape = view.container.querySelector(".sc-scrape-item")!;
    const real = {
      tokenBox: shared(token, ["sc-token-item"]),
      tokenParts: cells(token),
      tokenMain: cells(token.children[0]!),
      tokenText: cells(token.children[0]!.children[1]!, ["sc-token-name", "sc-token-masked"]),
      scrapeBox: shared(scrape, ["sc-scrape-item"]),
      scrapeParts: cells(scrape),
      scrapeText: cells(scrape.children[0]!, ["sc-scrape-guild", "sc-scrape-meta", "sc-scrape-date"]),
    };
    expect(localStorage.getItem(`skel:${TOKENS_COUNT_KEY}`)).toBe("2");
    expect(localStorage.getItem(`skel:${SCRAPES_COUNT_KEY}`)).toBe("1");
    view.unmount();

    const rows = render(
      <>
        <TokenListPlaceholder />
        <ScrapesPlaceholder />
      </>,
    );
    const [t1, t2, s1] = Array.from(rows.container.querySelectorAll("[data-skeleton-row]"));
    expect(rows.container.querySelectorAll("[data-skeleton-row]")).toHaveLength(3);
    expect(t2).toBeDefined();
    expect(shared(t1!)).toBe(real.tokenBox);
    expect(cells(t1!)).toEqual(real.tokenParts);
    // The validity badge box (its colour is data) and the text column.
    const main = cells(t1!.children[0]!);
    expect(main[1]).toBe(real.tokenMain[1]);
    expect(main[0]!.startsWith("span inline-flex size-7 shrink-0")).toBe(true);
    expect(real.tokenMain[0]!.startsWith("span inline-flex size-7 shrink-0")).toBe(true);
    // The name and masked value lines, a bar in each.
    const tag = (c: string) => c.split(" ")[0];
    expect(cells(t1!.children[0]!.children[1]!).map(tag)).toEqual(real.tokenText.map(tag));
    expect(shared(s1!)).toBe(real.scrapeBox);
    expect(cells(s1!)).toEqual(real.scrapeParts);
    expect(cells(s1!.children[0]!).map(tag)).toEqual(real.scrapeText.map(tag));
  });

  it("shows the empty-state bars on a first visit, before any count is known", () => {
    render(<ScraperSkeleton />);
    expect(screen.getByRole("status", { name: "Loading tokens", hidden: true })).toBeInTheDocument();
    expect(document.querySelectorAll("[data-skeleton-row]")).toHaveLength(0);
  });
});
