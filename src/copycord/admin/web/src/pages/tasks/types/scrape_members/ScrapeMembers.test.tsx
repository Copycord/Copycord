import { fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { MemoryRouter, Route, Routes, useParams } from "react-router";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { ToastProvider } from "../../../../components/Toast";
import { ConfirmProvider } from "../../../../components/ui/ConfirmDialog";
import type { TaskRun, TaskType } from "../../../../types/tasks";
import { typeLabel } from "../../typeLabels";
import { describeRunInputs, entryFor, heroStat } from "..";
import { GONE, ScrapeDownloadCard, includeWords, scrapeFileUrl, scrapePathOf } from "./entry";
import { SCRAPE_EXPLAINER, ScrapeMembersForm, guildIdError, guildOptionLabel, tokenTotal } from "./ScrapeMembersForm";
import { ScraperSettingsCard, displayMask } from "./ScraperSettingsCard";

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

function renderForm(onClose = () => {}) {
  render(
    <ToastProvider>
      <MemoryRouter initialEntries={["/tasks"]}>
        <Routes>
          <Route path="/tasks" element={<ScrapeMembersForm type={TYPE} open onClose={onClose} />} />
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
  it("explains the task, starts on today's include defaults and shows the saved proxy count", async () => {
    serveForm();
    renderForm();
    expect(screen.getByText(SCRAPE_EXPLAINER)).toBeInTheDocument();
    expect(screen.getByRole("switch", { name: "Username" })).toHaveAttribute("aria-checked", "true");
    expect(screen.getByRole("switch", { name: "Avatar URL" })).toHaveAttribute("aria-checked", "true");
    expect(screen.getByRole("switch", { name: "Bio" })).toHaveAttribute("aria-checked", "false");
    expect(screen.getByRole("switch", { name: "Roles" })).toHaveAttribute("aria-checked", "false");
    expect(screen.getByRole("switch", { name: "Use proxies" })).toHaveAttribute("aria-checked", "true");
    expect(await screen.findByText("4 saved proxies.")).toBeInTheDocument();
    expect(screen.queryByText(/valid/i)).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /validate/i })).not.toBeInTheDocument();
  });

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
        inputs: { guild_id: "222", include_username: true, include_avatar_url: true, include_bio: true, include_roles: false, use_proxies: false },
      },
    ]);
    expect(calls.some((c) => c.url.includes("guild_id=") || c.url.includes("validate"))).toBe(false);
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
  it("is registered with its words, hero count and download card", () => {
    const entry = entryFor("scrape_members");
    expect(entry.RunCard).toBe(ScrapeDownloadCard);
    expect(heroStat(runWith("111/x.json"))).toEqual({ key: "members", label: "members found", value: 1204 });
    expect(describeRunInputs(runWith("111/x.json"))).toEqual([
      { label: "Server", value: "Alpha (111)" },
      { label: "Include", value: "Usernames, Roles" },
      { label: "Proxies", value: "Yes" },
    ]);
    expect(includeWords({})).toBe("IDs only");
    expect(typeLabel("scrape_members").title).toBe("Scrape members");
    expect(scrapePathOf(runWith(null))).toBeNull();
  });

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

  it("says File no longer on disk when the file was deleted", async () => {
    serve({ "GET /api/tasks/scrape-members/r9": () => ({ status: 404, body: { detail: GONE } }) });
    render(<ScrapeDownloadCard run={runWith("111/x.json")} />);
    expect(await screen.findByText(GONE)).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Download JSON" })).not.toBeInTheDocument();
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
});

const TOKENS = [
  { token_id: "t1", label: "Main", masked: "MTAxMj…ab12", username: "scrapey", is_valid: false },
  { token_id: "t2", label: "Spare", masked: "plainlongtokenvalue1234", username: null, is_valid: true },
];

function renderSettings() {
  render(
    <ToastProvider>
      <ConfirmProvider>
        <ScraperSettingsCard />
      </ConfirmProvider>
    </ToastProvider>,
  );
}

describe("ScraperSettingsCard", () => {
  function serveSettings(tokens = TOKENS, proxies = ["1.2.3.4:80"]) {
    let held = [...tokens];
    return serve({
      "GET /api/scraper/tokens": () => ({ body: { ok: true, tokens: held } }),
      "POST /api/scraper/tokens/add": (b) => {
        held = [...held, { token_id: "t3", label: String((b as { label: string }).label), masked: "abcdef…wxyz", username: null, is_valid: false }];
        return { body: { ok: true, is_valid: false } };
      },
      "POST /api/scraper/tokens/t1/update": () => ({ body: { ok: true } }),
      "DELETE /api/scraper/tokens/t2": () => {
        held = held.filter((t) => t.token_id !== "t2");
        return { body: { ok: true } };
      },
      "GET /api/scraper/proxies": () => ({ body: { ok: true, proxies } }),
      "PUT /api/scraper/proxies": () => ({ body: { ok: true, count: 2 } }),
    });
  }

  it("lists the tokens masked, with no validity marks or validate buttons", async () => {
    serveSettings();
    renderSettings();
    const list = await screen.findByRole("list", { name: "Scraper tokens" });
    expect(within(list).getByText("scrapey")).toBeInTheDocument();
    expect(within(list).getByText("MTAxMj…ab12")).toBeInTheDocument();
    expect(within(list).getByText("plainl…1234")).toBeInTheDocument();
    expect(screen.queryByText("plainlongtokenvalue1234")).not.toBeInTheDocument();
    expect(screen.getByText("2 tokens")).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /validate/i })).not.toBeInTheDocument();
    expect(screen.queryByTitle(/valid/i)).not.toBeInTheDocument();
    expect(displayMask("short")).toBe("sh…");
  });

  it("adds a token as a form post and reloads the list", async () => {
    const calls = serveSettings();
    renderSettings();
    await screen.findByRole("list", { name: "Scraper tokens" });
    fireEvent.change(screen.getByRole("textbox", { name: "Discord token" }), { target: { value: " secret.token.value " } });
    fireEvent.change(screen.getByRole("textbox", { name: "Token label" }), { target: { value: "Third" } });
    fireEvent.click(screen.getByRole("button", { name: "Add token" }));
    expect(await screen.findByText("Third")).toBeInTheDocument();
    expect(calls.find((c) => c.url === "/api/scraper/tokens/add")?.body).toEqual({ token_value: "secret.token.value", label: "Third" });
    expect(screen.getByRole("textbox", { name: "Discord token" })).toHaveValue("");
    expect(calls.some((c) => c.url.includes("validate"))).toBe(false);
  });

  it("renames a token", async () => {
    const calls = serveSettings();
    renderSettings();
    fireEvent.click(await screen.findByRole("button", { name: "Edit scrapey" }));
    const box = screen.getByRole("textbox", { name: "Label of scrapey" });
    expect(box).toHaveValue("Main");
    fireEvent.change(box, { target: { value: "Primary" } });
    fireEvent.click(screen.getByRole("button", { name: "Save" }));
    await waitFor(() => expect(calls.find((c) => c.url === "/api/scraper/tokens/t1/update")?.body).toEqual({ label: "Primary" }));
    expect(await screen.findByRole("button", { name: "Edit scrapey" })).toBeInTheDocument();
  });

  it("deletes a token after the confirm", async () => {
    const calls = serveSettings();
    renderSettings();
    fireEvent.click(await screen.findByRole("button", { name: "Delete Spare" }));
    fireEvent.click(await screen.findByRole("button", { name: "Delete" }));
    await waitFor(() => expect(calls.some((c) => c.method === "DELETE" && c.url === "/api/scraper/tokens/t2")).toBe(true));
    await waitFor(() => expect(screen.queryByText("Spare")).not.toBeInTheDocument());
  });

  it("says so when no tokens are saved", async () => {
    serveSettings([]);
    renderSettings();
    expect(await screen.findByText("No tokens saved. Add one to start scraping members.")).toBeInTheDocument();
  });

  it("saves the proxy list", async () => {
    const calls = serveSettings();
    renderSettings();
    expect(await screen.findByText("1 proxy saved")).toBeInTheDocument();
    const save = screen.getByRole("button", { name: "Save proxies" });
    expect(save).toBeDisabled();
    const input = screen.getByRole("textbox", { name: "Proxies" });
    fireEvent.change(input, { target: { value: "5.6.7.8:3128" } });
    fireEvent.keyDown(input, { key: "Enter" });
    await waitFor(() => expect(save).toBeEnabled());
    fireEvent.click(save);
    await waitFor(() => expect(calls.find((c) => c.method === "PUT")?.body).toEqual({ proxies: ["1.2.3.4:80", "5.6.7.8:3128"] }));
    expect(await screen.findByText("2 proxies saved")).toBeInTheDocument();
  });
});
