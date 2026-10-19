import { act, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { MemoryRouter, Route, Routes, useParams } from "react-router";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { ToastProvider } from "../../../../components/Toast";
import type { TaskRun, TaskType } from "../../../../types/tasks";
import { typeLabel } from "../../typeLabels";
import { TASK_TYPES, describeRunInputs, entryFor, heroStat, summaryStats } from "..";
import { GONE, fileUrl } from "../export_dms/entry";
import { EXPORT_ASSETS_BASE, ExportAssetsDownloadCard, archiveFacts, archivePathOf } from "./entry";
import { EXPORT_ASSETS_EXPLAINER, ExportAssetsForm } from "./ExportAssetsForm";

const TYPE: TaskType = { key: "export_assets", title: "Export assets", description: "", inputs_schema: {}, active: 0, total: 0 };
const GUILDS = [
  { id: "111", name: "Alpha", icon: "a1" },
  { id: "222", name: "Beta" },
];

function serve(opts: { guilds?: unknown; error?: string; post?: { status: number; body: unknown }; hold?: Promise<void> } = {}) {
  const posts: Record<string, unknown>[] = [];
  vi.stubGlobal(
    "fetch",
    vi.fn(async (url: string, init?: RequestInit) => {
      if (url === "/api/tasks/types/export_assets/options") {
        if (opts.hold) await opts.hold;
        return Response.json({ guilds: opts.guilds ?? GUILDS, ...(opts.error ? { error: opts.error } : {}) });
      }
      if (url === "/api/tasks/runs" && init?.method === "POST") {
        posts.push(JSON.parse(String(init.body)) as Record<string, unknown>);
        if (opts.post) return Response.json(opts.post.body, { status: opts.post.status });
        return Response.json({ run: { id: "r9", number: 9 } }, { status: 201 });
      }
      return Response.json({ detail: "not found" }, { status: 404 });
    }),
  );
  return posts;
}

function RunPage() {
  const { id } = useParams();
  return <p>Run page {id}</p>;
}

function renderForm() {
  return render(
    <ToastProvider>
      <MemoryRouter initialEntries={["/tasks"]}>
        <Routes>
          <Route path="/tasks" element={<ExportAssetsForm type={TYPE} open onClose={() => {}} />} />
          <Route path="/tasks/runs/:id" element={<RunPage />} />
        </Routes>
      </MemoryRouter>
    </ToastProvider>,
  );
}

const server = () => screen.getByRole("combobox", { name: "Server" });
const assets = () => screen.getByRole("combobox", { name: "Assets" });
const submit = () => fireEvent.click(screen.getByRole("button", { name: "Export assets" }));

async function pickServer(name: string) {
  await waitFor(() => expect(server()).not.toHaveAttribute("readonly"));
  fireEvent.click(server());
  fireEvent.click(await screen.findByRole("option", { name }));
}

beforeEach(() => localStorage.clear());
afterEach(() => vi.unstubAllGlobals());

describe("ExportAssetsForm", () => {
  it("explains the export and starts on emojis and stickers", async () => {
    serve();
    renderForm();
    expect(screen.getByText(EXPORT_ASSETS_EXPLAINER)).toBeInTheDocument();
    expect(assets()).toHaveTextContent("Emojis and stickers");
    expect(screen.getByRole("button", { name: "Export assets" })).toBeInTheDocument();
    await waitFor(() => expect(server()).not.toHaveAttribute("readonly"));
  });

  it("lists the client account's servers from the options", async () => {
    serve();
    renderForm();
    await waitFor(() => expect(server()).not.toHaveAttribute("readonly"));
    fireEvent.click(server());
    const names = within(await screen.findByRole("listbox")).getAllByRole("option").map((o) => o.textContent);
    expect(names).toEqual(["Pick a server", "Alpha", "Beta"]);
  });

  it("keeps the same controls while the servers load and after", async () => {
    let release!: () => void;
    serve({ hold: new Promise<void>((r) => (release = r)) });
    renderForm();
    const shape = () =>
      [...document.querySelectorAll("[role=dialog] form > *, [role=dialog] [role=combobox], [role=dialog] button")].map(
        (el) => `${el.tagName}:${el.getAttribute("role") ?? ""}`,
      );
    expect(server()).toHaveAttribute("readonly");
    const before = shape();
    await act(async () => release());
    await waitFor(() => expect(server()).not.toHaveAttribute("readonly"));
    expect(shape()).toEqual(before);
  });

  it("asks for a server before any start", async () => {
    const posts = serve();
    renderForm();
    await waitFor(() => expect(server()).not.toHaveAttribute("readonly"));
    submit();
    expect(await screen.findByText("Pick a server.")).toBeInTheDocument();
    expect(posts).toEqual([]);
  });

  it("posts the server and both kinds by default, then opens the run", async () => {
    const posts = serve();
    renderForm();
    await pickServer("Beta");
    submit();
    expect(await screen.findByText("Run page r9")).toBeInTheDocument();
    expect(posts).toEqual([{ type: "export_assets", inputs: { guild_id: "222", asset: "both" } }]);
  });

  it("posts the chosen kind", async () => {
    const posts = serve();
    renderForm();
    await pickServer("Alpha");
    fireEvent.click(assets());
    fireEvent.click(await screen.findByRole("option", { name: "Stickers" }));
    expect(assets()).toHaveTextContent("Stickers");
    submit();
    expect(await screen.findByText("Run page r9")).toBeInTheDocument();
    expect(posts).toEqual([{ type: "export_assets", inputs: { guild_id: "111", asset: "stickers" } }]);
  });

  it("shows a 422 from the server on the server field", async () => {
    serve({ post: { status: 422, body: { errors: { guild_id: "Value error, The client account is not in this server" } } } });
    renderForm();
    await pickServer("Alpha");
    submit();
    expect(await screen.findByText("The client account is not in this server")).toBeInTheDocument();
    expect(server()).toHaveAttribute("aria-invalid", "true");
  });

  it("says when the servers could not be listed", async () => {
    serve({ guilds: [], error: "Start the client to list its servers" });
    renderForm();
    expect(await screen.findByText("Start the client to list its servers")).toBeInTheDocument();
  });
});

function runWith(archivePath: string | null, summary: Record<string, number> = { emojis_saved: 12, emojis_total: 13, stickers_saved: 1, stickers_total: 1 }): TaskRun {
  return {
    id: "r9",
    type: "export_assets",
    status: "success",
    inputs: { guild_id: "222", asset: "both" },
    context: { label: "Beta" },
    summary,
    report: { output: { archive_path: archivePath } },
  } as unknown as TaskRun;
}

const PATH = "/data/assets/Beta_222/Beta_222__20261019-150000.tar.gz";
const FILE = { filename: "Beta_222__20261019-150000.tar.gz", size: 2 * 1024 * 1024, emojis: 12, stickers: 1 };

function serveFile(reply: { status: number; body: unknown }) {
  const urls: string[] = [];
  vi.stubGlobal(
    "fetch",
    vi.fn(async (url: string) => {
      urls.push(url);
      return Response.json(reply.body, { status: reply.status });
    }),
  );
  return urls;
}

describe("Export assets run page download card", () => {
  it("shows the archive with a Download archive button and never the path", async () => {
    const urls = serveFile({ status: 200, body: { file: FILE } });
    const { container } = render(<ExportAssetsDownloadCard run={runWith(PATH)} />);
    const button = await screen.findByRole("button", { name: "Download archive" });
    expect(urls).toEqual(["/api/tasks/export-assets/r9"]);
    expect(screen.getByText(FILE.filename)).toBeInTheDocument();
    expect(screen.getByText("12 emojis, 1 sticker, 2.0 MB")).toBeInTheDocument();
    expect(container.textContent).not.toContain("/data/assets");
    const clicked: string[] = [];
    const click = vi.spyOn(HTMLAnchorElement.prototype, "click").mockImplementation(function (this: HTMLAnchorElement) {
      clicked.push(`${this.getAttribute("href")} ${this.download}`);
    });
    fireEvent.click(button);
    expect(clicked).toEqual([`${fileUrl("r9", EXPORT_ASSETS_BASE)} ${FILE.filename}`]);
    click.mockRestore();
  });

  it("leaves out the kind that was not exported", async () => {
    serveFile({ status: 200, body: { file: { ...FILE, stickers: null, size: null } } });
    render(<ExportAssetsDownloadCard run={runWith(PATH)} />);
    expect(await screen.findByText("12 emojis")).toBeInTheDocument();
    expect(archiveFacts({ filename: "x", size: 512, emojis: null, stickers: 3 })).toEqual(["3 stickers", "512 bytes"]);
  });

  it("says File no longer on disk when the archive was deleted", async () => {
    serveFile({ status: 404, body: { detail: GONE } });
    render(<ExportAssetsDownloadCard run={runWith(PATH)} />);
    expect(await screen.findByText(GONE)).toBeInTheDocument();
    expect(screen.getByText("Archive")).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Download archive" })).toBeNull();
  });

  it("shows no card without an archive", async () => {
    const urls = serveFile({ status: 404, body: { detail: "No export file" } });
    const off = render(<ExportAssetsDownloadCard run={runWith(null)} />);
    expect(off.container).toBeEmptyDOMElement();
    expect(urls).toEqual([]);
    off.unmount();
    const none = render(<ExportAssetsDownloadCard run={runWith(PATH)} />);
    await waitFor(() => expect(urls).toEqual(["/api/tasks/export-assets/r9"]));
    await act(async () => {});
    expect(none.container).toBeEmptyDOMElement();
    expect(archivePathOf(runWith(PATH))).toBe(PATH);
    expect(archivePathOf({ report: null })).toBeNull();
  });
});

describe("export_assets registration", () => {
  it("sits next to Export DMs with its form, card, inputs rows, summary words and hero", () => {
    const keys = Object.keys(TASK_TYPES);
    expect(keys.indexOf("export_assets")).toBe(keys.indexOf("export_dms") + 1);
    expect(entryFor("export_assets").Form).toBe(ExportAssetsForm);
    expect(entryFor("export_assets").RunCard).toBe(ExportAssetsDownloadCard);
    const run = runWith(PATH);
    expect(describeRunInputs(run)).toEqual([
      { label: "Server", value: "Beta" },
      { label: "Assets", value: "Emojis and stickers" },
    ]);
    expect(describeRunInputs({ ...run, context: null, inputs: { guild_id: "222", asset: "emojis" } } as unknown as TaskRun)).toEqual([
      { label: "Server", value: "222" },
      { label: "Assets", value: "Emojis" },
    ]);
    expect(summaryStats(run).map((s) => `${s.label} ${s.value}`)).toEqual([
      "Emojis saved 12",
      "Emojis found 13",
      "Stickers saved 1",
      "Stickers found 1",
    ]);
    expect(heroStat(run)).toEqual({ key: "emojis_saved", label: "emojis saved", value: 12 });
    expect(heroStat(runWith(PATH, { stickers_saved: 2, stickers_total: 2 }))).toBeNull();
    expect(typeLabel("export_assets").title).toBe("Export assets");
  });
});
