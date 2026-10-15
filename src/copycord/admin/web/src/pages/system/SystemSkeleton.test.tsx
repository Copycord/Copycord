import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { ToastProvider } from "../../components/Toast";
import { ConfirmProvider } from "../../components/ui/ConfirmDialog";
import { holdFetch } from "../../test/holdFetch";
import { cells, structure } from "../../test/structure";
import type { BackupInfo } from "../../types/system";
import { System } from "../System";
import { ArchiveRowsLoading, ArchivesHead, SystemSkeleton } from "./SystemSkeleton";

const INFO: BackupInfo = {
  ok: true,
  last_backup_at: "",
  last_backup_file: "",
  last_backup_size: 0,
  dir: "/data/backups",
  archives: [{ name: "copycord-2026-10-09.tar.gz", size: 2_500_000, mtime: Date.UTC(2026, 9, 9, 12) / 1000 }],
};

const json = (body: unknown) => new Response(JSON.stringify(body), { headers: { "content-type": "application/json" } });

beforeEach(() => {
  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: RequestInfo | URL) => (String(input) === "/api/backup/info" ? json(INFO) : new Response("{}", { status: 404 }))),
  );
});

const page = () =>
  render(
    <ToastProvider>
      <ConfirmProvider>
        <System />
      </ConfirmProvider>
    </ToastProvider>,
  );

describe("System skeleton", () => {
  it("has the loaded page's collapsed Database card: title, last backup line, expand button", async () => {
    const skel = render(<SystemSkeleton />);
    const skeleton = structure(skel.container, "tbody tr");
    skel.unmount();

    const release = holdFetch("/api/backup/info");
    const view = page();
    expect(structure(view.container, "tbody tr")).toEqual(skeleton);
    release();
    await waitFor(() => expect(document.getElementById("adv-last-backup")).toHaveTextContent("Oct 9 2026"));
    const loaded = structure(view.container, "tbody tr");

    expect(loaded).toEqual(skeleton);
    expect(loaded).toMatchObject({ headings: ["h1 System", "h2 Database"], controls: ["button Expand panel"] });
  });

  it("a loading archive row has the four cells of an archive row and the same row buttons", async () => {
    const view = page();
    await waitFor(() => expect(document.getElementById("adv-last-backup")).toHaveTextContent("Oct 9 2026"));
    await userEvent.setup().click(screen.getByRole("button", { name: "Expand panel" }));
    const real = view.container.querySelector("tr[data-archive]")!;
    const realCells = cells(real);
    const realButtons = Array.from(real.querySelectorAll("a, button")).map((b) => b.textContent);
    expect(view.container.querySelectorAll("#adv-archives thead th")).toHaveLength(4);
    view.unmount();

    const { container } = render(
      <table>
        <ArchivesHead hidden={false} />
        <tbody>
          <ArchiveRowsLoading rows={1} />
        </tbody>
      </table>,
    );
    const skeleton = container.querySelector("tbody tr")!;
    expect(cells(skeleton)).toEqual(realCells);
    expect(Array.from(skeleton.querySelectorAll("a, button")).map((b) => b.textContent)).toEqual(realButtons);
    expect(container.querySelectorAll("thead th")).toHaveLength(4);
  });
});
