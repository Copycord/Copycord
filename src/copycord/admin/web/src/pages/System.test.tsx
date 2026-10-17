import { fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { ToastProvider } from "../components/Toast";
import { ConfirmProvider } from "../components/ui/ConfirmDialog";
import { CSRF_HEADER } from "../lib/api";
import type { BackupArchive, BackupInfo } from "../types/system";
import { holdFetch } from "../test/holdFetch";
import { System } from "./System";
import { SHRUG } from "./system/format";
import { SystemSkeleton } from "./system/SystemSkeleton";

// Noon UTC keeps the calendar day the same in every test runner time zone.
const OCT_9 = Date.UTC(2026, 9, 9, 12) / 1000;
const OCT_8 = Date.UTC(2026, 9, 8, 12) / 1000;

const ARCHIVES: BackupArchive[] = [
  { name: "copycord-2026-10-09.tar.gz", size: 2_500_000, mtime: OCT_9 },
  { name: "copycord 2026-10-08.tar.gz", size: 512, mtime: OCT_8 },
];

function info(archives: BackupArchive[] = ARCHIVES): BackupInfo {
  return {
    ok: true,
    last_backup_at: "",
    last_backup_file: "",
    last_backup_size: 0,
    dir: "/data/backups",
    archives,
  };
}

function jsonResponse(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json" } });
}

type Handler = (init: RequestInit) => Response;

/** Stubs fetch with one handler per URL and returns the mock for call checks. */
function server(routes: Record<string, Handler>) {
  const fetchMock = vi.fn(async (input: RequestInfo | URL, init: RequestInit = {}) => {
    const handler = routes[String(input)];
    return handler ? handler(init) : new Response("not found", { status: 404 });
  });
  vi.stubGlobal("fetch", fetchMock);
  return fetchMock;
}

function callsTo(fetchMock: ReturnType<typeof server>, url: string): RequestInit[] {
  return fetchMock.mock.calls.filter(([u]) => String(u) === url).map(([, init]) => init ?? {});
}

function renderPage() {
  return render(
    <ToastProvider>
      <ConfirmProvider>
        <System />
      </ConfirmProvider>
    </ToastProvider>,
  );
}

async function openPanel(user: ReturnType<typeof userEvent.setup>) {
  await user.click(screen.getByRole("button", { name: "Expand panel" }));
}

describe("System page", () => {
  it("renders the backup info: last backup, archive rows, download links", async () => {
    server({ "/api/backup/info": () => jsonResponse(info()) });
    const user = userEvent.setup();
    renderPage();

    expect(screen.getByRole("heading", { level: 1, name: "System" })).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: "Database" })).toBeInTheDocument();
    await waitFor(() => expect(document.getElementById("adv-last-backup")).toHaveTextContent("Oct 9 2026"));

    // Collapsed: the actions and the table are in the DOM but hidden.
    expect(document.getElementById("adv-backup-now")).not.toBeVisible();
    await openPanel(user);
    expect(document.getElementById("adv-backup-now")).toBeVisible();

    const rows = within(document.getElementById("adv-archives")!).getAllByRole("row");
    expect(rows).toHaveLength(3);
    const first = within(rows[1]!);
    expect(first.getByText("copycord-2026-10-09")).toHaveAttribute("title", "copycord-2026-10-09.tar.gz");
    expect(first.getByText("2.4 MB")).toBeInTheDocument();
    expect(first.getByText("Oct 9 2026")).toBeInTheDocument();
    expect(first.getByRole("link", { name: "Download" })).toHaveAttribute(
      "href",
      "/api/backup/download/copycord-2026-10-09.tar.gz",
    );
    const second = within(rows[2]!);
    expect(second.getByText("512 B")).toBeInTheDocument();
    expect(second.getByRole("link", { name: "Download" })).toHaveAttribute(
      "href",
      "/api/backup/download/copycord%202026-10-08.tar.gz",
    );

    const latest = document.getElementById("adv-download-latest")!;
    expect(latest).toHaveAttribute("href", "/api/backup/download/copycord-2026-10-09.tar.gz");
    expect(latest).not.toHaveAttribute("aria-disabled");
  });

  it("shows the shrug and a disabled download latest when there are no backups", async () => {
    server({ "/api/backup/info": () => jsonResponse(info([])) });
    const user = userEvent.setup();
    renderPage();
    await waitFor(() => expect(document.getElementById("adv-last-backup")).toHaveTextContent(SHRUG));
    await openPanel(user);
    const latest = document.getElementById("adv-download-latest")!;
    expect(latest).not.toHaveAttribute("href");
    expect(latest).toHaveAttribute("aria-disabled", "true");
    expect(screen.getByText("No backups yet.")).toBeVisible();
  });

  it("says so when the info call fails", async () => {
    server({ "/api/backup/info": () => jsonResponse({ detail: "boom" }, 500) });
    const user = userEvent.setup();
    renderPage();
    await openPanel(user);
    expect(await screen.findByRole("alert")).toHaveTextContent("Could not load backups.");
  });

  it("backup now posts to /admin/backup-now, reloads the info and toasts", async () => {
    let backups = [ARCHIVES[1]!];
    const fetchMock = server({
      "/api/backup/info": () => jsonResponse(info(backups)),
      "/admin/backup-now": () => {
        backups = ARCHIVES;
        return jsonResponse({ ok: true, file: ARCHIVES[0]!.name });
      },
    });
    const user = userEvent.setup();
    renderPage();
    await waitFor(() => expect(document.getElementById("adv-last-backup")).toHaveTextContent("Oct 8 2026"));
    await openPanel(user);
    await user.click(screen.getByRole("button", { name: "Backup now" }));

    expect(await screen.findByText("Backup created")).toBeInTheDocument();
    const [post] = callsTo(fetchMock, "/admin/backup-now");
    expect(post?.method).toBe("POST");
    expect(new Headers(post?.headers).get(CSRF_HEADER)).toBe("1");
    await waitFor(() => expect(document.getElementById("adv-last-backup")).toHaveTextContent("Oct 9 2026"));
  });

  it("restore by upload asks first, does nothing on cancel, posts the file on confirm", async () => {
    const fetchMock = server({
      "/api/backup/info": () => jsonResponse(info()),
      "/api/backup/restore": () => jsonResponse({ ok: true, restored_from: "restore-1.tar.gz" }),
    });
    const user = userEvent.setup();
    renderPage();
    await openPanel(user);
    const input = document.getElementById("adv-restore-file") as HTMLInputElement;
    expect(input).toHaveAttribute("accept", ".gz");
    const file = new File(["tar bytes"], "mine.tar.gz", { type: "application/gzip" });

    fireEvent.change(input, { target: { files: [file] } });
    let dialog = await screen.findByRole("dialog", { name: "Restore database?" });
    expect(dialog).toHaveTextContent(
      "Restore database from uploaded file “mine.tar.gz”? This will stop bots and replace the database.",
    );
    await user.click(within(dialog).getByRole("button", { name: "Cancel" }));
    await waitFor(() => expect(screen.queryByRole("dialog")).not.toBeInTheDocument());
    expect(callsTo(fetchMock, "/api/backup/restore")).toHaveLength(0);

    fireEvent.change(input, { target: { files: [file] } });
    dialog = await screen.findByRole("dialog", { name: "Restore database?" });
    await user.click(within(dialog).getByRole("button", { name: "Restore" }));

    expect(await screen.findByText("Database restored from upload.")).toBeInTheDocument();
    const [post] = callsTo(fetchMock, "/api/backup/restore");
    expect(post?.method).toBe("POST");
    expect(new Headers(post?.headers).get(CSRF_HEADER)).toBe("1");
    const body = post?.body as FormData;
    expect(body).toBeInstanceOf(FormData);
    expect(body.get("source")).toBe("upload");
    expect((body.get("file") as File).name).toBe("mine.tar.gz");
    expect(input.value).toBe("");
  });

  it("restore from an archive row sends source=existing and the name", async () => {
    const fetchMock = server({
      "/api/backup/info": () => jsonResponse(info()),
      "/api/backup/restore": () => jsonResponse({ ok: true, restored_from: ARCHIVES[1]!.name }),
    });
    const user = userEvent.setup();
    renderPage();
    await openPanel(user);
    const row = await screen.findByText("copycord 2026-10-08");
    await user.click(within(row.closest("tr")!).getByRole("button", { name: "Restore" }));
    const dialog = await screen.findByRole("dialog", { name: "Restore database?" });
    expect(dialog).toHaveTextContent("Restore database from “copycord 2026-10-08.tar.gz”?");
    await user.click(within(dialog).getByRole("button", { name: "Restore" }));

    expect(await screen.findByText("Database restored.")).toBeInTheDocument();
    const body = callsTo(fetchMock, "/api/backup/restore")[0]?.body as FormData;
    expect(Object.fromEntries(body.entries())).toEqual({ source: "existing", name: "copycord 2026-10-08.tar.gz" });
  });

  it("toasts a failed restore", async () => {
    server({
      "/api/backup/info": () => jsonResponse(info()),
      "/api/backup/restore": () => new Response("archive missing data.db", { status: 400 }),
    });
    const user = userEvent.setup();
    renderPage();
    await openPanel(user);
    fireEvent.change(document.getElementById("adv-restore-file")!, {
      target: { files: [new File(["x"], "bad.tar.gz")] },
    });
    const dialog = await screen.findByRole("dialog");
    await user.click(within(dialog).getByRole("button", { name: "Restore" }));
    expect(await screen.findByText("archive missing data.db")).toBeInTheDocument();
  });

  it("delete asks first and posts the archive name as a form field", async () => {
    let backups = ARCHIVES;
    const fetchMock = server({
      "/api/backup/info": () => jsonResponse(info(backups)),
      "/api/backup/delete": (init) => {
        const name = (init.body as FormData).get("name");
        backups = backups.filter((a) => a.name !== name);
        return jsonResponse({ ok: true, deleted: name });
      },
    });
    const user = userEvent.setup();
    renderPage();
    await openPanel(user);
    const row = (await screen.findByText("copycord-2026-10-09")).closest("tr")!;

    await user.click(within(row).getByRole("button", { name: "Delete" }));
    let dialog = await screen.findByRole("dialog", { name: "Delete backup?" });
    expect(dialog).toHaveTextContent("Permanently delete “copycord-2026-10-09.tar.gz”? This cannot be undone.");
    await user.click(within(dialog).getByRole("button", { name: "Cancel" }));
    expect(callsTo(fetchMock, "/api/backup/delete")).toHaveLength(0);

    await user.click(within(row).getByRole("button", { name: "Delete" }));
    dialog = await screen.findByRole("dialog", { name: "Delete backup?" });
    await user.click(within(dialog).getByRole("button", { name: "Delete" }));

    expect(await screen.findByText("Backup deleted")).toBeInTheDocument();
    const [post] = callsTo(fetchMock, "/api/backup/delete");
    expect(post?.method).toBe("POST");
    expect(new Headers(post?.headers).get(CSRF_HEADER)).toBe("1");
    expect(Object.fromEntries((post?.body as FormData).entries())).toEqual({ name: "copycord-2026-10-09.tar.gz" });
    await waitFor(() => expect(screen.queryByText("copycord-2026-10-09")).not.toBeInTheDocument());
  });
});

describe("System loading", () => {
  it("keeps the real title and the one Database card while loading, as the chunk skeleton does", async () => {
    server({ "/api/backup/info": () => jsonResponse(info()) });
    const release = holdFetch("/api/backup/info");
    const { container } = renderPage();
    expect(screen.getByRole("heading", { level: 2, name: "Database" })).toBeInTheDocument();
    expect(container.querySelectorAll("#adv-data-card")).toHaveLength(1);
    release();
    await waitFor(() => expect(document.getElementById("adv-last-backup")).toHaveTextContent("Oct 9 2026"));
    expect(container.querySelectorAll("#adv-data-card")).toHaveLength(1);

    const skel = render(<SystemSkeleton />);
    expect(within(skel.container).getByRole("heading", { level: 2, name: "Database" })).toBeInTheDocument();
    expect(within(skel.container).getAllByTestId("card-skeleton")).toHaveLength(1);
  });
});
