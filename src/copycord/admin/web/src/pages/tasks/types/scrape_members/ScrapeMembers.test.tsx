import { act, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { useState } from "react";
import { MemoryRouter, Route, Routes, useParams } from "react-router";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { ToastProvider } from "../../../../components/Toast";
import type { TaskRun, TaskType } from "../../../../types/tasks";
import { typeLabel } from "../../typeLabels";
import { describeRunInputs, entryFor, heroStat } from "..";
import { GONE, ScrapeDownloadCard, debugReportOf, includeWords, scrapeDebugUrl, scrapeFileUrl, scrapePathOf } from "./entry";
import { DEBUG_REPORT_HELP, SCRAPE_EXPLAINER, ScrapeMembersForm, guildIdError, guildOptionLabel, tokenTotal } from "./ScrapeMembersForm";
import { AUTOSAVE_MS, ScraperSettingsCard, TOKENS_URL, openScraperSettings, textLines } from "./ScraperSettingsCard";

const TYPE: TaskType = { key: "scrape_members", title: "Scrape members", description: "", inputs_schema: {}, active: 0, total: 0 };
const GUILDS = [
  { id: "111", name: "Alpha", tokens: 2 },
  { id: "222", name: "Beta", tokens: 1 },
];

type Call = { method: string; url: string; body: unknown };

function serve(routes: Record<string, (body: unknown) => { status?: number; body: unknown }>) {
  const calls: Call[] = [];
  vi.stubGlobal(
    "fetch",
    vi.fn(async (url: string, init?: RequestInit) => {
      const method = init?.method ?? "GET";
      const raw = init?.body;
      const body = typeof raw === "string" ? (JSON.parse(raw) as unknown) : raw instanceof FormData ? Object.fromEntries(raw.entries()) : undefined;
      calls.push({ method, url, body });
      const route = routes[`${method} ${url}`];
      if (!route) return Response.json({ detail: "not found" }, { status: 404 });
      const reply = route(body);
      return Response.json(reply.body, { status: reply.status ?? 200 });
    }),
  );
  return calls;
}

function serveForm(options: unknown = { guilds: GUILDS, tokens: { valid: 3, total: 3 }, proxies: 4 }) {
  return serve({
    "GET /api/tasks/types/scrape_members/options": () => ({ body: options }),
    "POST /api/tasks/runs": () => ({ status: 201, body: { run: { id: "r9", number: 9 } } }),
  });
}

function RunPage() {
  const { id } = useParams();
  return <p>Run page {id}</p>;
}

function renderForm(onClose = () => {}, type: TaskType = TYPE) {
  render(
    <ToastProvider>
      <MemoryRouter initialEntries={["/tasks"]}>
        <Routes>
          <Route path="/tasks" element={<ScrapeMembersForm type={type} open onClose={onClose} />} />
          <Route path="/tasks/runs/:id" element={<RunPage />} />
        </Routes>
      </MemoryRouter>
    </ToastProvider>,
  );
}

const submit = () => fireEvent.click(screen.getByRole("button", { name: "Scrape members" }));
const posts = (calls: Call[]) => calls.filter((c) => c.method === "POST" && c.url === "/api/tasks/runs").map((c) => c.body);

beforeEach(() => localStorage.clear());
afterEach(() => vi.unstubAllGlobals());

describe("ScrapeMembersForm", () => {
  it("asks for a server before any start", async () => {
    const calls = serveForm();
    renderForm();
    await screen.findByText("4 saved proxies.");
    submit();
    expect(await screen.findByText("Pick a server or enter a server ID.")).toBeInTheDocument();
    expect(posts(calls)).toEqual([]);
  });

  it("lists the servers by name and token count and starts with the chosen one and the toggles", async () => {
    const calls = serveForm();
    renderForm();
    fireEvent.click(await screen.findByRole("combobox", { name: "Server" }));
    fireEvent.click(await screen.findByRole("option", { name: "Beta (1 token)" }));
    fireEvent.click(screen.getByRole("switch", { name: "Bio" }));
    fireEvent.click(screen.getByRole("switch", { name: "Use proxies" }));
    submit();
    expect(await screen.findByText("Run page r9")).toBeInTheDocument();
    expect(posts(calls)).toEqual([
      {
        type: "scrape_members",
        inputs: {
          guild_id: "222",
          include_username: true,
          include_avatar_url: true,
          include_bio: true,
          include_roles: false,
          use_proxies: false,
          sessions_per_token: 1,
        },
      },
    ]);
    expect(calls.some((c) => c.url.includes("guild_id=") || c.url.includes("validate"))).toBe(false);
  });

  it("offers no sessions per token choice and always posts 1 session per token", async () => {
    const calls = serveForm();
    renderForm();
    await screen.findByText("4 saved proxies.");
    expect(screen.queryByRole("combobox", { name: "Sessions per token" })).not.toBeInTheDocument();
    expect(screen.queryByText("More is faster but riskier.")).not.toBeInTheDocument();
    fireEvent.change(screen.getByRole("textbox", { name: "Or a server ID" }), { target: { value: "999" } });
    submit();
    expect(await screen.findByText("Run page r9")).toBeInTheDocument();
    expect((posts(calls)[0] as { inputs: { sessions_per_token: number } }).inputs.sessions_per_token).toBe(1);
  });

  it("starts with a typed server ID for a server not listed, and refuses one that is not a number", async () => {
    const calls = serveForm();
    renderForm();
    await screen.findByText("4 saved proxies.");
    const box = screen.getByRole("textbox", { name: "Or a server ID" });
    fireEvent.change(box, { target: { value: "12ab" } });
    submit();
    expect(await screen.findByText("Enter a numeric server ID.")).toBeInTheDocument();
    expect(posts(calls)).toEqual([]);
    fireEvent.change(box, { target: { value: " 999888777 " } });
    submit();
    expect(await screen.findByText("Run page r9")).toBeInTheDocument();
    expect((posts(calls)[0] as { inputs: { guild_id: string } }).inputs.guild_id).toBe("999888777");
  });

  it("shows the server's field message on a refused start", async () => {
    serve({
      "GET /api/tasks/types/scrape_members/options": () => ({ body: { guilds: GUILDS, tokens: { total: 1 }, proxies: 0 } }),
      "POST /api/tasks/runs": () => ({ status: 422, body: { errors: { guild_id: "Value error, Pick a server or enter a numeric server ID" } } }),
    });
    renderForm();
    expect(await screen.findByText("No proxies saved, so none are used.")).toBeInTheDocument();
    fireEvent.change(screen.getByRole("textbox", { name: "Or a server ID" }), { target: { value: "5" } });
    submit();
    expect(await screen.findByText("Pick a server or enter a numeric server ID")).toBeInTheDocument();
  });

  it("points at the settings card when no tokens are saved", async () => {
    const calls = serveForm({ guilds: [], tokens: { valid: 0, total: 0 }, proxies: 0, error: "No valid scraper tokens. Add one first." });
    const onClose = vi.fn();
    renderForm(onClose);
    expect(await screen.findByText("No scraper tokens saved")).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Scrape members" })).not.toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "Go to scraper tokens" }));
    expect(onClose).toHaveBeenCalled();
    expect(posts(calls)).toEqual([]);
  });

  it("has no Debug report toggle outside dev mode and posts no debug_report", async () => {
    const calls = serveForm();
    renderForm(() => {}, { ...TYPE, dev: false });
    await screen.findByText("4 saved proxies.");
    expect(screen.queryByRole("switch", { name: "Debug report" })).not.toBeInTheDocument();
    expect(screen.queryByText(DEBUG_REPORT_HELP)).not.toBeInTheDocument();
    fireEvent.change(screen.getByRole("textbox", { name: "Or a server ID" }), { target: { value: "999" } });
    submit();
    expect(await screen.findByText("Run page r9")).toBeInTheDocument();
    expect(posts(calls)[0] as { inputs: Record<string, unknown> }).toHaveProperty("inputs");
    expect("debug_report" in (posts(calls)[0] as { inputs: Record<string, unknown> }).inputs).toBe(false);
  });

  it("shows the Debug report toggle in dev mode from first paint, off, and posts it", async () => {
    const calls = serveForm();
    renderForm(() => {}, { ...TYPE, dev: true });
    // Known from the type listing, before the options answer.
    const toggle = screen.getByRole("switch", { name: "Debug report" });
    expect(toggle).toHaveAttribute("aria-checked", "false");
    expect(screen.getByText(DEBUG_REPORT_HELP)).toBeInTheDocument();
    await screen.findByText("4 saved proxies.");
    fireEvent.click(toggle);
    expect(toggle).toHaveAttribute("aria-checked", "true");
    fireEvent.change(screen.getByRole("textbox", { name: "Or a server ID" }), { target: { value: "999" } });
    submit();
    expect(await screen.findByText("Run page r9")).toBeInTheDocument();
    expect((posts(calls)[0] as { inputs: { debug_report: boolean } }).inputs.debug_report).toBe(true);
  });

  it("posts debug_report false in dev mode when the toggle is left off", async () => {
    const calls = serveForm();
    renderForm(() => {}, { ...TYPE, dev: true });
    await screen.findByText("4 saved proxies.");
    fireEvent.change(screen.getByRole("textbox", { name: "Or a server ID" }), { target: { value: "999" } });
    submit();
    expect(await screen.findByText("Run page r9")).toBeInTheDocument();
    expect((posts(calls)[0] as { inputs: { debug_report: boolean } }).inputs.debug_report).toBe(false);
  });

  it("shows the server's refusal under the Debug report toggle", async () => {
    serve({
      "GET /api/tasks/types/scrape_members/options": () => ({ body: { guilds: GUILDS, tokens: { total: 1 }, proxies: 0 } }),
      "POST /api/tasks/runs": () => ({ status: 422, body: { errors: { debug_report: "Value error, The debug report is only available when COPYCORD_ENV is dev" } } }),
    });
    renderForm(() => {}, { ...TYPE, dev: true });
    await screen.findByText("No proxies saved, so none are used.");
    fireEvent.click(screen.getByRole("switch", { name: "Debug report" }));
    fireEvent.change(screen.getByRole("textbox", { name: "Or a server ID" }), { target: { value: "5" } });
    submit();
    expect(await screen.findByText("The debug report is only available when COPYCORD_ENV is dev")).toBeInTheDocument();
  });

  it("helpers", () => {
    expect(guildIdError("")).toBeUndefined();
    expect(guildIdError("123")).toBeUndefined();
    expect(guildIdError("12 3")).toBe("Enter a numeric server ID.");
    expect(guildOptionLabel({ id: "1", name: "A", tokens: 3 })).toBe("A (3 tokens)");
    expect(guildOptionLabel({ id: "1", name: "A" })).toBe("A");
    expect(tokenTotal({ tokens: { valid: 2, total: 5 } })).toBe(5);
    expect(tokenTotal({ tokens: 4 })).toBe(4);
    expect(tokenTotal(undefined)).toBe(0);
  });
});

function runWith(path: string | null): TaskRun {
  return {
    id: "r9",
    number: 9,
    type: "scrape_members",
    status: "success",
    inputs: { guild_id: "111", include_username: true, include_avatar_url: false, include_bio: false, include_roles: true, use_proxies: true },
    context: { label: "Alpha", guild_id: "111" },
    summary: { members: 1204, tokens: 2, tokens_skipped: 1 },
    report: { output: { path, size: 4096 } },
  } as unknown as TaskRun;
}

describe("Scrape members run page", () => {
  it("shows the file name, member count and size with a Download JSON button", async () => {
    const calls = serve({
      "GET /api/tasks/scrape-members/r9": () => ({ body: { file: { filename: "scrape_111_20261016_101500.json", size: 2048, members: 1204 } } }),
    });
    render(<ScrapeDownloadCard run={runWith("111/scrape_111_20261016_101500.json")} />);
    const button = await screen.findByRole("button", { name: "Download JSON" });
    expect(screen.getByText("scrape_111_20261016_101500.json")).toBeInTheDocument();
    expect(screen.getByText("1,204 members, 2.0 KB")).toBeInTheDocument();
    expect(calls.map((c) => c.url)).toEqual(["/api/tasks/scrape-members/r9"]);
    const clicked: string[] = [];
    const click = vi.spyOn(HTMLAnchorElement.prototype, "click").mockImplementation(function (this: HTMLAnchorElement) {
      clicked.push(`${this.getAttribute("href")} ${this.download}`);
    });
    fireEvent.click(button);
    expect(clicked).toEqual([`${scrapeFileUrl("r9")} scrape_111_20261016_101500.json`]);
    click.mockRestore();
  });

  it("shows nothing when the run saved no file or the reply is another 404", async () => {
    const calls = serve({ "GET /api/tasks/scrape-members/r9": () => ({ status: 404, body: { detail: "No file for this run" } }) });
    const { container, unmount } = render(<ScrapeDownloadCard run={runWith(null)} />);
    expect(container).toBeEmptyDOMElement();
    expect(calls).toEqual([]);
    unmount();
    const second = render(<ScrapeDownloadCard run={runWith("111/x.json")} />);
    await waitFor(() => expect(calls).toHaveLength(1));
    expect(second.container).toBeEmptyDOMElement();
  });

  it("adds a Download debug report link when the run wrote one, the same way as the JSON file", async () => {
    serve({
      "GET /api/tasks/scrape-members/r9": () => ({ body: { file: { filename: "scrape_111_20261016_101500.json", size: 2048, members: 1204 } } }),
    });
    const run = runWith("111/scrape_111_20261016_101500.json");
    (run.report as Record<string, unknown>).debug_report = { path: "111/debug_111_20261016_101500.json", size: 3072 };
    expect(debugReportOf(run)).toEqual({ filename: "debug_111_20261016_101500.json", size: 3072 });
    render(<ScrapeDownloadCard run={run} />);
    await screen.findByRole("button", { name: "Download JSON" });
    expect(screen.getByText("debug_111_20261016_101500.json")).toBeInTheDocument();
    expect(screen.getByText("Debug report, 3.0 KB")).toBeInTheDocument();
    const clicked: string[] = [];
    const click = vi.spyOn(HTMLAnchorElement.prototype, "click").mockImplementation(function (this: HTMLAnchorElement) {
      clicked.push(`${this.getAttribute("href")} ${this.download}`);
    });
    fireEvent.click(screen.getByRole("button", { name: "Download debug report" }));
    expect(clicked).toEqual([`${scrapeDebugUrl("r9")} debug_111_20261016_101500.json`]);
    expect(scrapeDebugUrl("r9")).toBe("/api/tasks/scrape-members/r9/debug-report");
    click.mockRestore();
  });

  it("shows the debug report alone for a run that saved no members file", () => {
    const calls = serve({});
    const run = runWith(null);
    (run.report as Record<string, unknown>).debug_report = { path: "111/debug_111_20261016_101500.json", size: null };
    render(<ScrapeDownloadCard run={run} />);
    expect(screen.getByRole("button", { name: "Download debug report" })).toBeInTheDocument();
    expect(screen.getByText("Debug report")).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Download JSON" })).not.toBeInTheDocument();
    expect(calls).toEqual([]);
    expect(debugReportOf(runWith(null))).toBeNull();
  });
});

function renderSettings() {
  return render(
    <ToastProvider>
      <ScraperSettingsCard />
    </ToastProvider>,
  );
}

const cardToggle = () => screen.getByRole("button", { name: /Scraper tokens and proxies/ });
const expand = () => fireEvent.click(cardToggle());
const puts = (calls: Call[]) => calls.filter((c) => c.method === "PUT");

/** Edits a list on fake timers and runs `step` (advance the debounce, blur), back on real timers after. */
function editWith(input: HTMLElement, value: string, step: () => void) {
  vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
  try {
    fireEvent.change(input, { target: { value } });
    step();
  } finally {
    vi.useRealTimers();
  }
}

describe("ScraperSettingsCard", () => {
  function serveSettings(tokens: unknown = ["MTAxMjM0.tok.one", "OTg3NjU0.tok.two"], proxies = ["1.2.3.4:80"], status = 200) {
    return serve({
      "GET /api/scraper/tokens": () => ({ status, body: status === 200 ? { ok: true, tokens } : { ok: false, error: "Could not read the token file" } }),
      "PUT /api/scraper/tokens": () => ({ body: { ok: true, count: 3 } }),
      "GET /api/scraper/proxies": () => ({ body: { ok: true, proxies } }),
      "PUT /api/scraper/proxies": () => ({ body: { ok: true, count: 2 } }),
    });
  }

  afterEach(() => vi.useRealTimers());

  it("is collapsed by default with the saved counts in its head, and expands and collapses on click", async () => {
    serveSettings();
    renderSettings();
    const toggle = cardToggle();
    expect(toggle).toHaveAttribute("aria-expanded", "false");
    const panel = document.getElementById(toggle.getAttribute("aria-controls") ?? "");
    expect(panel).not.toBeNull();
    expect(panel).not.toBeVisible();
    expect(await screen.findByText("2 tokens, 1 proxy")).toBeInTheDocument();
    expect(screen.queryByRole("textbox", { name: "Tokens" })).not.toBeInTheDocument();
    expand();
    expect(toggle).toHaveAttribute("aria-expanded", "true");
    expect(panel).toBeVisible();
    expect(screen.getByRole("textbox", { name: "Tokens" })).toBeVisible();
    expect(screen.getByRole("textbox", { name: "Proxies" })).toBeVisible();
    expect(screen.queryByText("2 tokens, 1 proxy")).not.toBeInTheDocument();
    expand();
    expect(toggle).toHaveAttribute("aria-expanded", "false");
    expect(screen.queryByRole("textbox", { name: "Tokens" })).not.toBeInTheDocument();
  });

  it("opens when the start form's link asks for it", async () => {
    serveSettings();
    const before = Element.prototype.scrollIntoView;
    const scroll = vi.fn();
    Element.prototype.scrollIntoView = scroll;
    renderSettings();
    await screen.findByText("2 tokens, 1 proxy");
    act(() => openScraperSettings());
    expect(cardToggle()).toHaveAttribute("aria-expanded", "true");
    expect(screen.getByRole("textbox", { name: "Tokens" })).toBeVisible();
    expect(scroll).toHaveBeenCalled();
    Element.prototype.scrollIntoView = before;
  });

  it("shows the saved tokens one per line in a plain textbox, with no Save buttons, list, add, rename or delete", async () => {
    const calls = serveSettings();
    renderSettings();
    expand();
    expect(await screen.findByText("2 tokens saved")).toBeInTheDocument();
    const input = screen.getByRole("textbox", { name: "Tokens" });
    expect(input.tagName).toBe("TEXTAREA");
    expect(input).toHaveAttribute("rows", "6");
    expect(input).toHaveAttribute("spellcheck", "false");
    expect(input).toHaveValue("MTAxMjM0.tok.one\nOTg3NjU0.tok.two");
    expect(screen.queryByText(/not validated/)).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /save/i })).not.toBeInTheDocument();
    expect(screen.queryByRole("list")).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /add token|edit|delete|validate/i })).not.toBeInTheDocument();
    expect(calls.filter((c) => c.method !== "GET")).toEqual([]);
  });

  it("saves the tokens by itself after the last edit, once, as the deduped list, and keeps the text as typed", async () => {
    const calls = serveSettings();
    renderSettings();
    expand();
    await screen.findByText("2 tokens saved");
    const input = screen.getByRole("textbox", { name: "Tokens" });
    const typed = "MTAxMjM0.tok.one\n\n  OTg3NjU0.tok.two  \nMTAxMjM0.tok.one\nnew.tok.three\n";
    editWith(input, typed, () => {
      act(() => vi.advanceTimersByTime(AUTOSAVE_MS - 1));
      expect(puts(calls)).toEqual([]);
      act(() => vi.advanceTimersByTime(1));
    });
    expect(puts(calls)).toEqual([
      { method: "PUT", url: "/api/scraper/tokens", body: { tokens: ["MTAxMjM0.tok.one", "OTg3NjU0.tok.two", "new.tok.three"] } },
    ]);
    expect(await screen.findByText("3 tokens saved")).toBeInTheDocument();
    expect(input).toHaveValue(typed);
    expect(screen.queryByText("Tokens saved")).not.toBeInTheDocument();
    fireEvent.blur(input);
    expect(puts(calls)).toHaveLength(1);
    expect(textLines(" a \r\nb\n\na\n")).toEqual(["a", "b"]);
  });

  it("saves at once on blur and on unmount", async () => {
    const calls = serveSettings();
    const { unmount } = renderSettings();
    expand();
    expect(await screen.findByText("1 proxy saved")).toBeInTheDocument();
    const input = screen.getByRole("textbox", { name: "Proxies" });
    expect(input).toHaveValue("1.2.3.4:80");
    editWith(input, "1.2.3.4:80\n\n  5.6.7.8:3128  \n", () => fireEvent.blur(input));
    expect(puts(calls).map((c) => c.body)).toEqual([{ proxies: ["1.2.3.4:80", "5.6.7.8:3128"] }]);
    expect(await screen.findByText("2 proxies saved")).toBeInTheDocument();
    fireEvent.change(input, { target: { value: "9.9.9.9:1" } });
    unmount();
    expect(puts(calls).map((c) => c.body)).toEqual([{ proxies: ["1.2.3.4:80", "5.6.7.8:3128"] }, { proxies: ["9.9.9.9:1"] }]);
  });

  it("does not save when the lines did not change", async () => {
    const calls = serveSettings();
    renderSettings();
    expand();
    await screen.findByText("2 tokens saved");
    const input = screen.getByRole("textbox", { name: "Tokens" });
    editWith(input, "MTAxMjM0.tok.one  \n\nOTg3NjU0.tok.two\nMTAxMjM0.tok.one\n", () => {
      act(() => vi.advanceTimersByTime(AUTOSAVE_MS * 2));
      fireEvent.blur(input);
    });
    expect(puts(calls)).toEqual([]);
  });

  it("says so when the tokens cannot be loaded and never saves over the file", async () => {
    const calls = serveSettings(undefined, ["1.2.3.4:80"], 500);
    renderSettings();
    expand();
    expect(await screen.findByText("Could not read the token file")).toBeInTheDocument();
    const input = screen.getByRole("textbox", { name: "Tokens" });
    expect(input).toHaveAttribute("readonly");
    editWith(input, "typed.anyway", () => {
      act(() => vi.advanceTimersByTime(AUTOSAVE_MS * 2));
      fireEvent.blur(input);
    });
    expect(await screen.findByText("1 proxy saved")).toBeInTheDocument();
    expand();
    expect(screen.queryByText(/tokens?, /)).not.toBeInTheDocument();
    expect(puts(calls)).toEqual([]);
  });

  it("keeps the text and the saved count on a failed save, with one error, and tries again on the next edit", async () => {
    const calls = serve({
      "GET /api/scraper/tokens": () => ({ body: { ok: true, tokens: ["a.b.c"] } }),
      "PUT /api/scraper/tokens": () => ({ status: 500, body: { ok: false, error: "Could not write the token file" } }),
      "GET /api/scraper/proxies": () => ({ body: { ok: true, proxies: [] } }),
    });
    renderSettings();
    expand();
    await screen.findByText("1 token saved");
    const input = screen.getByRole("textbox", { name: "Tokens" });
    editWith(input, "a.b.c\nd.e.f", () => fireEvent.blur(input));
    expect(await screen.findByText("Could not write the token file")).toBeInTheDocument();
    expect(screen.getAllByText("Could not write the token file")).toHaveLength(1);
    expect(input).toHaveValue("a.b.c\nd.e.f");
    expect(screen.getByText("1 token saved")).toBeInTheDocument();
    editWith(input, "a.b.c\nd.e.f\n", () => act(() => vi.advanceTimersByTime(AUTOSAVE_MS)));
    expect(puts(calls)).toHaveLength(2);
  });

  it("ignores an older save's late reply", async () => {
    const replies: ((r: Response) => void)[] = [];
    const calls: Call[] = [];
    vi.stubGlobal(
      "fetch",
      vi.fn((url: string, init?: RequestInit) => {
        const method = init?.method ?? "GET";
        calls.push({ method, url, body: typeof init?.body === "string" ? JSON.parse(init.body) : undefined });
        if (method === "PUT") return new Promise<Response>((r) => replies.push(r));
        return Promise.resolve(Response.json(url === TOKENS_URL ? { ok: true, tokens: ["a"] } : { ok: true, proxies: [] }));
      }),
    );
    renderSettings();
    expand();
    await screen.findByText("1 token saved");
    const input = screen.getByRole("textbox", { name: "Tokens" });
    editWith(input, "a\nb", () => fireEvent.blur(input));
    editWith(input, "a\nb\nc", () => fireEvent.blur(input));
    expect(puts(calls).map((c) => c.body)).toEqual([{ tokens: ["a", "b"] }, { tokens: ["a", "b", "c"] }]);
    // "Saving" takes the count's place in the same cell while a save is out.
    const cell = within(screen.getByText("1 token saved").parentElement as HTMLElement);
    expect(cell.getByText("Saving")).not.toHaveClass("invisible");
    expect(cell.getByText("1 token saved")).toHaveClass("invisible");
    await act(async () => replies[1]?.(Response.json({ ok: true, tokens: ["a", "b", "c"] })));
    expect(await cell.findByText("3 tokens saved")).not.toHaveClass("invisible");
    expect(cell.getByText("Saving")).toHaveClass("invisible");
    await act(async () => replies[0]?.(Response.json({ ok: true, tokens: ["a", "b"] })));
    expect(screen.getByText("3 tokens saved")).toBeInTheDocument();
    expect(screen.queryByText("2 tokens saved")).not.toBeInTheDocument();
    expect(input).toHaveValue("a\nb\nc");
  });

});

const OPTIONS_URL = "/api/tasks/types/scrape_members/options";

/** The type page's two parts: the settings card, and Run task mounting a fresh form as Tasks.tsx does. */
function TypePage() {
  const [open, setOpen] = useState(false);
  return (
    <ToastProvider>
      <MemoryRouter initialEntries={["/tasks"]}>
        <ScraperSettingsCard />
        <button type="button" onClick={() => setOpen(true)}>
          Run task
        </button>
        {open ? <ScrapeMembersForm type={TYPE} open onClose={() => setOpen(false)} /> : null}
      </MemoryRouter>
    </ToastProvider>
  );
}

/** The card's lists (tokens as given, no proxies, a save echoes its list); the form's options wait for `answer`. */
function servePage(tokens: string[]) {
  let reply: (r: Response) => void = () => {};
  vi.stubGlobal(
    "fetch",
    vi.fn((url: string, init?: RequestInit) => {
      const method = init?.method ?? "GET";
      if (url === OPTIONS_URL) return new Promise<Response>((r) => (reply = r));
      if (method === "PUT") return Promise.resolve(Response.json({ ok: true, ...(JSON.parse(String(init?.body)) as object) }));
      return Promise.resolve(Response.json(url === TOKENS_URL ? { ok: true, tokens } : { ok: true, proxies: [] }));
    }),
  );
  return { answer: (body: unknown) => act(async () => reply(Response.json(body))) };
}

/** Whether the start form (its explainer) is ever added to the page from now on, however briefly. */
function watchForm() {
  let shown = false;
  const scan = (records: MutationRecord[]) => {
    for (const r of records) if (Array.from(r.addedNodes).some((n) => n.textContent?.includes(SCRAPE_EXPLAINER))) shown = true;
  };
  const obs = new MutationObserver(scan);
  obs.observe(document.body, { childList: true, subtree: true });
  return () => {
    scan(obs.takeRecords());
    obs.disconnect();
    return shown;
  };
}

const runTask = () => fireEvent.click(screen.getByRole("button", { name: "Run task" }));
const NO_TOKENS = { guilds: [], tokens: { total: 0 }, proxies: 0, error: "No valid scraper tokens. Add one first." };

describe("Run task on the Scrape members page", () => {
  afterEach(() => vi.useRealTimers());

  it("opens straight to the empty state when the card loaded no tokens, before the options answer", async () => {
    const page = servePage([]);
    render(<TypePage />);
    await screen.findByText("0 tokens, 0 proxies");
    const formShown = watchForm();
    runTask();
    expect(screen.getByText("No scraper tokens saved")).toBeInTheDocument();
    expect(screen.queryByText(SCRAPE_EXPLAINER)).not.toBeInTheDocument();
    await page.answer(NO_TOKENS);
    expect(screen.getByText("No scraper tokens saved")).toBeInTheDocument();
    expect(formShown()).toBe(false);
  });

  it("opens straight to the form when the card loaded saved tokens", async () => {
    const page = servePage(["a.b.c", "d.e.f"]);
    render(<TypePage />);
    await screen.findByText("2 tokens, 0 proxies");
    runTask();
    expect(screen.getByText(SCRAPE_EXPLAINER)).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Scrape members" })).toBeInTheDocument();
    expect(screen.queryByText("No scraper tokens saved")).not.toBeInTheDocument();
    await page.answer({ guilds: GUILDS, tokens: { total: 2 }, proxies: 0 });
    expect(await screen.findByText("No proxies saved, so none are used.")).toBeInTheDocument();
    expect(screen.queryByText("No scraper tokens saved")).not.toBeInTheDocument();
  });

  it("opens to the form after a token was autosaved", async () => {
    const page = servePage([]);
    render(<TypePage />);
    await screen.findByText("0 tokens, 0 proxies");
    expand();
    editWith(screen.getByRole("textbox", { name: "Tokens" }), "a.b.c\n", () => act(() => vi.advanceTimersByTime(AUTOSAVE_MS)));
    expect(await screen.findByText("1 token saved")).toBeInTheDocument();
    runTask();
    expect(screen.getByText(SCRAPE_EXPLAINER)).toBeInTheDocument();
    expect(screen.queryByText("No scraper tokens saved")).not.toBeInTheDocument();
    await page.answer({ guilds: GUILDS, tokens: { total: 1 }, proxies: 0 });
    expect(await screen.findByText("No proxies saved, so none are used.")).toBeInTheDocument();
  });

  it("without the card's count, shows the form while the options load and the empty state once they say none", async () => {
    const page = servePage([]);
    renderForm();
    expect(screen.getByText(SCRAPE_EXPLAINER)).toBeInTheDocument();
    await page.answer(NO_TOKENS);
    expect(await screen.findByText("No scraper tokens saved")).toBeInTheDocument();
    expect(screen.queryByText(SCRAPE_EXPLAINER)).not.toBeInTheDocument();
  });

  it("trusts the options reply when it disagrees with the card's count", async () => {
    const page = servePage([]);
    render(<TypePage />);
    await screen.findByText("0 tokens, 0 proxies");
    runTask();
    await page.answer({ guilds: GUILDS, tokens: { total: 2 }, proxies: 0 });
    expect(await screen.findByText(SCRAPE_EXPLAINER)).toBeInTheDocument();
    expect(screen.queryByText("No scraper tokens saved")).not.toBeInTheDocument();
  });
});
