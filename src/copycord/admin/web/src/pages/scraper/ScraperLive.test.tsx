import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { ToastProvider } from "../../components/Toast";
import { ConfirmProvider } from "../../components/ui/ConfirmDialog";
import { CSRF_HEADER } from "../../lib/api";
import type { ScraperQueueItem, ScraperScrape, ScraperToken } from "../../types/scraper";
import { Scraper } from "../Scraper";
import { AUTOCLEAR_KEY, filterLines, parseLogEvent } from "./ScraperLogsCard";
import { fmtSec, scrapeUrl } from "./scraperApi";
import { FakeSource, FakeWs, scraperMsg } from "./testFakes";

const LOGS = "/logs/stream/scraper";

const TOKENS: ScraperToken[] = [{ token_id: "t1", label: "Main", masked: "MTAxMj…ab12", is_valid: true, username: "scrapey" }];

const SCRAPE: ScraperScrape = {
  filename: "scrape_20261013.json",
  path: "123456/scrape_20261013.json",
  guild_id: "123456",
  guild_name: "<b>Test & Guild</b>",
  total_count: 42,
  scraped_at: "2026-10-13T09:41:00Z",
  metadata: { method: "gateway" },
  file_size: 2048,
};

function jsonResponse(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json" } });
}

type Handler = (init: RequestInit) => Response;

function server(routes: Record<string, Handler>) {
  const fetchMock = vi.fn(async (input: RequestInfo | URL, init: RequestInit = {}) => {
    const method = (init.method ?? "GET").toUpperCase();
    const handler = routes[`${method} ${String(input)}`];
    return handler ? handler(init) : new Response("not found", { status: 404 });
  });
  vi.stubGlobal("fetch", fetchMock);
  return fetchMock;
}

function callsTo(fetchMock: ReturnType<typeof server>, method: string, url: string): RequestInit[] {
  return fetchMock.mock.calls
    .filter(([u, init]) => String(u) === url && (init?.method ?? "GET").toUpperCase() === method)
    .map(([, init]) => init ?? {});
}

function baseRoutes(opts: { queue?: ScraperQueueItem[]; running?: boolean; scrapes?: ScraperScrape[] } = {}): Record<string, Handler> {
  return {
    "GET /api/scraper/tokens": () => jsonResponse({ ok: true, tokens: TOKENS }),
    "GET /api/scraper/status": () => jsonResponse({ running: opts.running ?? false, queue: opts.queue ?? [] }),
    "GET /api/scraper/proxies": () => jsonResponse({ ok: true, proxies: [] }),
    "GET /api/scraper/scrapes": () => jsonResponse({ ok: true, scrapes: opts.scrapes ?? [] }),
    "POST /api/scraper/logs/clear": () => jsonResponse({ ok: true }),
  };
}

function renderPage() {
  return render(
    <ToastProvider>
      <ConfirmProvider>
        <Scraper />
      </ConfirmProvider>
    </ToastProvider>,
  );
}

const byId = (id: string) => document.getElementById(id)!;

/** Waits for the page's /ws/out socket, then delivers a scraper event on it. */
async function ws(payload: Record<string, unknown>) {
  await waitFor(() => expect(FakeWs.all.some((s) => s.url.endsWith("/ws/out"))).toBe(true));
  FakeWs.emit("/ws/out", scraperMsg(payload));
}

async function sse(data: string) {
  await waitFor(() => expect(FakeSource.all.some((s) => s.url === LOGS && !s.closed)).toBe(true));
  FakeSource.emit(LOGS, data);
}

beforeEach(() => {
  FakeWs.all = [];
  FakeSource.all = [];
  localStorage.clear();
  vi.stubGlobal("WebSocket", FakeWs);
  vi.stubGlobal("EventSource", FakeSource);
});

describe("helpers", () => {
  it("parses tail batches, single lines and raw text", () => {
    expect(parseLogEvent('{"lines":["a","b"]}')).toEqual(["a", "b"]);
    expect(parseLogEvent('{"line":"c"}')).toEqual(["c"]);
    expect(parseLogEvent("not json")).toEqual(["not json"]);
    expect(parseLogEvent("{}")).toEqual([]);
    expect(filterLines(["Alpha", "beta"], " ALP ")).toEqual(["Alpha"]);
  });

  it("encodes the whole scrape path as one segment and formats seconds", () => {
    expect(scrapeUrl("123/scrape a#1.json")).toBe("/api/scraper/scrapes/123%2Fscrape%20a%231.json");
    expect(fmtSec(42)).toBe("42s");
    expect(fmtSec(187)).toBe("3m 7s");
    expect(fmtSec(null)).toBe("—");
  });
});

describe("Scraper queue card", () => {
  const QUEUE: ScraperQueueItem[] = [
    { id: "7", guild_id: "111", status: "running" },
    { id: "8", guild_id: "222", status: "pending" },
  ];

  it("is hidden while the queue is empty", async () => {
    server(baseRoutes());
    renderPage();
    await screen.findByText("scrapey");
    expect(byId("sc-queue-card")).not.toBeVisible();
  });

  it("lists the status queue, removes a pending item, clears and cancels all after the confirm", async () => {
    const user = userEvent.setup();
    const fetchMock = server({
      ...baseRoutes({ queue: QUEUE, running: true }),
      "DELETE /api/scraper/queue/8": () => jsonResponse({ ok: true }),
      "POST /api/scraper/queue/clear": () => jsonResponse({ ok: true }),
      "POST /api/scraper/cancel-all": () => jsonResponse({ ok: true }),
    });
    renderPage();
    await waitFor(() => expect(byId("sc-queue-card")).toBeVisible());
    expect(byId("sc-queue-count")).toHaveTextContent("2 items");
    const items = document.querySelectorAll(".sc-queue-item");
    expect(items).toHaveLength(2);
    expect(within(items[0] as HTMLElement).getByText("Scraping…")).toBeInTheDocument();
    expect(items[0]!.querySelector(".sc-queue-remove")).toBeNull();
    expect(within(items[1] as HTMLElement).getByText("Pending")).toBeInTheDocument();

    await user.click(items[1]!.querySelector<HTMLElement>(".sc-queue-remove")!);
    await waitFor(() => expect(callsTo(fetchMock, "DELETE", "/api/scraper/queue/8")).toHaveLength(1));
    expect(new Headers(callsTo(fetchMock, "DELETE", "/api/scraper/queue/8")[0]!.headers).get(CSRF_HEADER)).toBe("1");

    await user.click(byId("sc-queue-clear"));
    await waitFor(() => expect(callsTo(fetchMock, "POST", "/api/scraper/queue/clear")).toHaveLength(1));

    await user.click(byId("sc-cancel-all-btn"));
    let dialog = await screen.findByRole("dialog", { name: "Cancel all?" });
    await user.click(within(dialog).getByRole("button", { name: "Keep" }));
    await waitFor(() => expect(screen.queryByRole("dialog")).not.toBeInTheDocument());
    expect(callsTo(fetchMock, "POST", "/api/scraper/cancel-all")).toHaveLength(0);

    await user.click(byId("sc-cancel-all-btn"));
    dialog = await screen.findByRole("dialog", { name: "Cancel all?" });
    await user.click(within(dialog).getByRole("button", { name: "Cancel All" }));
    await waitFor(() => expect(callsTo(fetchMock, "POST", "/api/scraper/cancel-all")).toHaveLength(1));
  });

  it("follows queue_update messages and starts the queue from Start with no guild id", async () => {
    const user = userEvent.setup();
    const fetchMock = server({
      ...baseRoutes(),
      "POST /api/scraper/queue/start": () => jsonResponse({ ok: true }),
    });
    renderPage();
    await screen.findByText("scrapey");
    await ws({ type: "queue_update", running: false, queue: [{ id: "9", guild_id: "333", status: "pending" }] });
    expect(byId("sc-queue-card")).toBeVisible();
    expect(byId("sc-queue-count")).toHaveTextContent("1 item");
    expect(byId("sc-queue-list")).toHaveTextContent("333");

    await user.click(screen.getByRole("button", { name: "Start Scrape" }));
    await waitFor(() => expect(callsTo(fetchMock, "POST", "/api/scraper/queue/start")).toHaveLength(1));
    expect(callsTo(fetchMock, "POST", "/api/scraper/start")).toHaveLength(0);
    await waitFor(() => expect(byId("sc-status-label")).toHaveTextContent("Running"));

    await ws({ type: "queue_update", running: false, queue: [] });
    expect(byId("sc-queue-card")).not.toBeVisible();
    expect(byId("sc-status-label")).toHaveTextContent("Idle");
  });
});

describe("Scraper progress card", () => {
  it("shows progress, ETA and the member target from ws messages, then the result", async () => {
    const fetchMock = server(baseRoutes());
    renderPage();
    await screen.findByText("scrapey");
    expect(byId("sc-progress-card")).not.toBeVisible();

    await ws({ type: "queue_update", running: true, queue: [{ id: "1", guild_id: "42", status: "running" }] });
    expect(byId("sc-progress-card")).toBeVisible();
    expect(byId("sc-status-pill")).toHaveAttribute("data-status", "running");
    expect(byId("sc-progress-text")).toHaveTextContent("Initializing…");

    await ws({ type: "progress", current: 50, total: 2000, message: "Fetching members" });
    expect(byId("sc-progress-pct")).toHaveTextContent("3%");
    expect(byId("sc-progress-fill").style.width).toBe("3%");
    expect(byId("sc-progress-text")).toHaveTextContent("Fetching members");
    expect(byId("sc-member-count")).toHaveTextContent(`${(2000).toLocaleString()} members`);
    expect(byId("sc-eta").textContent).toMatch(/^~.+ left$/);

    await ws({ type: "progress", current: 80, total: 0 });
    expect(byId("sc-progress-text")).toHaveTextContent("80 found");

    const before = callsTo(fetchMock, "GET", "/api/scraper/scrapes").length;
    await ws({ type: "complete", success: true, guild_id: "42", total_count: 2000, elapsed_seconds: 65 });
    expect(byId("sc-progress-text")).toHaveTextContent("Done: 2000 members");
    expect(byId("sc-progress-pct")).toHaveTextContent("100%");
    expect(byId("sc-progress-fill").style.width).toBe("100%");
    expect(byId("sc-eta")).toHaveTextContent("");
    expect(byId("sc-status-label")).toHaveTextContent("Complete");
    expect(await screen.findByText("Scrape complete (42): 2000 members in 1m 5s")).toBeInTheDocument();
    await waitFor(() => expect(callsTo(fetchMock, "GET", "/api/scraper/scrapes").length).toBe(before + 1));
  });

  it("shows a failure with a dash and keeps going while the queue has pending items", async () => {
    server(baseRoutes());
    renderPage();
    await screen.findByText("scrapey");
    await ws({
      type: "queue_update",
      running: true,
      queue: [
        { id: "1", guild_id: "42", status: "running" },
        { id: "2", guild_id: "43", status: "pending" },
      ],
    });
    await ws({ type: "complete", success: false, error: "boom" });
    expect(byId("sc-progress-text")).toHaveTextContent("Starting next in queue…");
    expect(byId("sc-status-label")).toHaveTextContent("Running Queue");

    await ws({ type: "queue_update", running: true, queue: [{ id: "2", guild_id: "43", status: "running" }] });
    await ws({ type: "complete", success: false, error: "boom" });
    expect(byId("sc-progress-text")).toHaveTextContent("Failed: boom");
    expect(byId("sc-progress-pct")).toHaveTextContent("—");
    expect(byId("sc-status-pill")).toHaveAttribute("data-status", "error");
  });

  it("ignores complete when nothing is running and messages of other kinds", async () => {
    server(baseRoutes());
    renderPage();
    await screen.findByText("scrapey");
    await ws({ type: "complete", success: true, total_count: 5 });
    FakeWs.emit("/ws/out", { kind: "other", payload: { type: "queue_update", running: true } });
    expect(byId("sc-progress-card")).not.toBeVisible();
    expect(byId("sc-status-label")).toHaveTextContent("Idle");
  });
});

describe("Scraper logs card", () => {
  it("appends stream lines as text, filters by search and clears with the file", async () => {
    const user = userEvent.setup();
    const fetchMock = server(baseRoutes());
    renderPage();
    await screen.findByText("scrapey");
    await sse(JSON.stringify({ lines: ["alpha one", "<img src=x onerror=alert(1)>"] }));
    await sse(JSON.stringify({ line: "beta two" }));
    await sse("alpha raw");
    const body = byId("sc-log-body");
    expect(body.textContent).toBe("alpha one\n<img src=x onerror=alert(1)>\nbeta two\nalpha raw\n");
    expect(body.querySelector("img")).toBeNull();

    await user.type(byId("sc-log-search"), "ALPHA");
    await waitFor(() => expect(body.textContent).toBe("alpha one\nalpha raw\n"));
    await user.keyboard("{Escape}");
    expect(byId("sc-log-search")).toHaveValue("");
    await waitFor(() => expect(body.textContent).toContain("beta two"));

    await user.click(byId("sc-log-clear"));
    expect(body.textContent).toBe("");
    await waitFor(() => expect(callsTo(fetchMock, "POST", "/api/scraper/logs/clear")).toHaveLength(1));
  });

  it("clears on start and after a finished run when Auto-clear is on", async () => {
    const fetchMock = server(baseRoutes());
    renderPage();
    await screen.findByText("scrapey");
    expect(byId("sc-log-autoclear")).toBeChecked();
    await sse(JSON.stringify({ line: "old run" }));
    await ws({ type: "queue_update", running: true, queue: [{ id: "1", guild_id: "42", status: "running" }] });
    expect(byId("sc-log-body").textContent).toBe("");

    await sse(JSON.stringify({ line: "this run" }));
    await ws({ type: "complete", success: true, total_count: 0, elapsed_seconds: 1 });
    expect(byId("sc-log-body").textContent).toBe("this run\n");
    await waitFor(() => expect(byId("sc-log-body").textContent).toBe(""), { timeout: 5000 });
    expect(byId("sc-progress-card")).not.toBeVisible();
    expect(callsTo(fetchMock, "POST", "/api/scraper/logs/clear")).toHaveLength(1);
  });

  it("keeps the lines when Auto-clear is off and remembers the choice", async () => {
    const user = userEvent.setup();
    const fetchMock = server(baseRoutes());
    renderPage();
    await screen.findByText("scrapey");
    await user.click(byId("sc-log-autoclear"));
    expect(byId("sc-log-autoclear")).not.toBeChecked();
    expect(localStorage.getItem(AUTOCLEAR_KEY)).toBe("false");

    await sse(JSON.stringify({ line: "kept" }));
    await ws({ type: "queue_update", running: true, queue: [{ id: "1", guild_id: "42", status: "running" }] });
    await ws({ type: "complete", success: true, total_count: 0, elapsed_seconds: 1 });
    await waitFor(() => expect(byId("sc-progress-card")).not.toBeVisible(), { timeout: 5000 });
    expect(byId("sc-log-body").textContent).toBe("kept\n");
    expect(callsTo(fetchMock, "POST", "/api/scraper/logs/clear")).toHaveLength(0);
  });

  it("starts unchecked when the saved choice is off", async () => {
    localStorage.setItem(AUTOCLEAR_KEY, "false");
    server(baseRoutes());
    renderPage();
    await screen.findByText("scrapey");
    expect(byId("sc-log-autoclear")).not.toBeChecked();
  });
});

describe("Scraper past scrapes card", () => {
  it("shows the empty state with no saved scrapes", async () => {
    server(baseRoutes());
    renderPage();
    await screen.findByText("scrapey");
    await waitFor(() => expect(byId("sc-scrapes-empty")).toBeVisible());
    expect(byId("sc-scrapes-empty")).toHaveTextContent("No saved scrapes yet");
  });

  it("lists scrapes as text with an encoded download link and refreshes", async () => {
    const user = userEvent.setup();
    const fetchMock = server(baseRoutes({ scrapes: [SCRAPE, { filename: "bad.json", path: "bad.json", error: "unreadable" }] }));
    renderPage();
    const item = await waitFor(() => {
      const el = document.querySelector<HTMLElement>(".sc-scrape-item");
      if (!el) throw new Error("no scrape row");
      return el;
    });
    expect(document.querySelectorAll(".sc-scrape-item")).toHaveLength(1);
    expect(byId("sc-scrapes-empty")).not.toBeVisible();
    expect(within(item).getByText("<b>Test & Guild</b>")).toBeInTheDocument();
    expect(item.querySelector("b")).toBeNull();
    expect(item.querySelector(".sc-scrape-guild")).toHaveAttribute("title", "Guild ID: 123456");
    expect(item.querySelector(".sc-scrape-meta")).toHaveTextContent("42 members · gateway · 2.0 KB");
    const link = within(item).getByRole("link", { name: /Download/ });
    expect(link).toHaveAttribute("href", "/api/scraper/scrapes/123456%2Fscrape_20261013.json");
    expect(link).toHaveAttribute("download");

    await user.click(byId("sc-scrapes-refresh"));
    await waitFor(() => expect(callsTo(fetchMock, "GET", "/api/scraper/scrapes")).toHaveLength(2));
  });

  it("deletes only after the confirm, with the path encoded, then reloads", async () => {
    const user = userEvent.setup();
    let scrapes: ScraperScrape[] = [SCRAPE];
    const url = "/api/scraper/scrapes/123456%2Fscrape_20261013.json";
    const fetchMock = server({
      ...baseRoutes(),
      "GET /api/scraper/scrapes": () => jsonResponse({ ok: true, scrapes }),
      [`DELETE ${url}`]: () => {
        scrapes = [];
        return jsonResponse({ ok: true });
      },
    });
    renderPage();
    const del = await screen.findByRole("button", { name: "Delete <b>Test & Guild</b>" });

    await user.click(del);
    let dialog = await screen.findByRole("dialog", { name: "Delete scrape?" });
    expect(dialog).toHaveTextContent('Delete scrape file "123456/scrape_20261013.json"?');
    await user.click(within(dialog).getByRole("button", { name: "Cancel" }));
    await waitFor(() => expect(screen.queryByRole("dialog")).not.toBeInTheDocument());
    expect(fetchMock.mock.calls.filter(([, init]) => init?.method === "DELETE")).toHaveLength(0);

    await user.click(del);
    dialog = await screen.findByRole("dialog", { name: "Delete scrape?" });
    await user.click(within(dialog).getByRole("button", { name: "Delete" }));
    await waitFor(() => expect(callsTo(fetchMock, "DELETE", url)).toHaveLength(1));
    expect(new Headers(callsTo(fetchMock, "DELETE", url)[0]!.headers).get(CSRF_HEADER)).toBe("1");
    await waitFor(() => expect(byId("sc-scrapes-empty")).toBeVisible());
    expect(document.querySelector(".sc-scrape-item")).toBeNull();
  });
});
