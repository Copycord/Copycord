import { act, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { MemoryRouter, Route, Routes, useParams } from "react-router";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { ToastProvider } from "../../../../components/Toast";
import type { TaskRun, TaskType } from "../../../../types/tasks";
import { typeLabel } from "../../typeLabels";
import { describeRunInputs, entryFor, heroStat } from "..";
import { AVATARS_EXPLAINER, AvatarsForm, hoursError } from "./AvatarsForm";
import { AvatarsDownloadCard, downloadOf, sizeWords } from "./entry";

const TYPE: TaskType = { key: "download_avatars", title: "Download avatars", description: "", inputs_schema: {}, active: 0, total: 0 };
const GUILDS = [
  { id: "111", name: "Alpha" },
  { id: "222", name: "Beta" },
];

function serve(guilds: { id: string; name: string }[] = GUILDS) {
  const posts: Record<string, unknown>[] = [];
  vi.stubGlobal(
    "fetch",
    vi.fn(async (url: string, init?: RequestInit) => {
      if (url === "/api/tasks/types/download_avatars/options") return Response.json({ guilds });
      if (url === "/api/tasks/runs" && init?.method === "POST") {
        posts.push(JSON.parse(String(init.body)) as Record<string, unknown>);
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
  render(
    <ToastProvider>
      <MemoryRouter initialEntries={["/tasks"]}>
        <Routes>
          <Route path="/tasks" element={<AvatarsForm type={TYPE} open onClose={() => {}} />} />
          <Route path="/tasks/runs/:id" element={<RunPage />} />
        </Routes>
      </MemoryRouter>
    </ToastProvider>,
  );
}

const submit = () => fireEvent.click(screen.getByRole("button", { name: "Download avatars" }));

beforeEach(() => localStorage.clear());
afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllGlobals();
});

describe("AvatarsForm", () => {
  it("explains the task and asks for a server before any start", async () => {
    const posts = serve();
    renderForm();
    expect(screen.getByText(AVATARS_EXPLAINER)).toBeInTheDocument();
    expect(screen.getByRole("spinbutton", { name: "Link expires after (hours)" })).toHaveValue(24);
    submit();
    expect(await screen.findByText("Pick a server.")).toBeInTheDocument();
    expect(posts).toEqual([]);
  });

  it("starts with the chosen server, its name and the hours, then opens the run", async () => {
    const posts = serve();
    renderForm();
    fireEvent.click(await screen.findByRole("combobox", { name: "Server" }));
    fireEvent.click(await screen.findByRole("option", { name: "Beta" }));
    submit();
    expect(await screen.findByText("Run page r9")).toBeInTheDocument();
    expect(posts).toEqual([{ type: "download_avatars", inputs: { guild_id: "222", guild_name: "Beta", expires_hours: 24 } }]);
  });

  it("checks the hours", async () => {
    const posts = serve();
    renderForm();
    fireEvent.change(screen.getByRole("spinbutton", { name: "Link expires after (hours)" }), { target: { value: "200" } });
    submit();
    expect(await screen.findByText("Enter a whole number of hours from 1 to 168.")).toBeInTheDocument();
    expect(posts).toEqual([]);
  });

  it("filters the servers in the server box itself", async () => {
    serve(Array.from({ length: 12 }, (_, i) => ({ id: String(100 + i), name: `Server ${i}` })));
    renderForm();
    expect(screen.queryByRole("searchbox", { name: "Find a server" })).toBeNull();
    const box = await screen.findByRole("combobox", { name: "Server" });
    expect(box.tagName).toBe("INPUT");
    await waitFor(() => expect(box).not.toHaveAttribute("readonly"));
    fireEvent.focus(box);
    fireEvent.change(box, { target: { value: "111" } });
    expect(within(screen.getByRole("listbox")).getAllByRole("option").map((o) => o.textContent)).toEqual(["Server 11"]);
  });

  it("validates in words", () => {
    expect(hoursError("1")).toBeUndefined();
    expect(hoursError("168")).toBeUndefined();
    for (const bad of ["0", "169", "1.5", ""]) expect(hoursError(bad)).toBeDefined();
  });
});

function runWith(download: unknown): TaskRun {
  return {
    id: "r9",
    type: "download_avatars",
    inputs: { guild_id: "222", guild_name: "Beta", expires_hours: 24 },
    context: { label: "Beta" },
    summary: { members: 3, downloaded: 3, failed: 0, skipped: 0 },
    report: { download },
  } as unknown as TaskRun;
}

const DL = { url: "/api/tasks/avatars/tok", filename: "avatars-Beta-9.zip", files: 3, size: 2048, expires_at: "2026-10-17T12:00:00Z" };

describe("Download avatars run page", () => {
  it("shows the link as a button with its expiry", () => {
    vi.useFakeTimers({ toFake: ["Date", "setTimeout", "clearTimeout"] });
    vi.setSystemTime(new Date("2026-10-16T12:00:00Z"));
    render(<AvatarsDownloadCard run={runWith(DL)} />);
    expect(screen.getByRole("button", { name: "Download zip" })).toBeInTheDocument();
    expect(screen.getByText(/^Expires /)).toBeInTheDocument();
    expect(screen.getByText("3 avatars, 2.0 KB")).toBeInTheDocument();
    expect(screen.queryByText(/Link expired/)).toBeNull();
  });

  it("says Link expired once past the expiry, also while open", () => {
    vi.useFakeTimers({ toFake: ["Date", "setTimeout", "clearTimeout"] });
    vi.setSystemTime(new Date("2026-10-17T11:59:59Z"));
    render(<AvatarsDownloadCard run={runWith(DL)} />);
    expect(screen.getByRole("button", { name: "Download zip" })).toBeInTheDocument();
    act(() => {
      vi.advanceTimersByTime(2000);
    });
    expect(screen.queryByRole("button", { name: "Download zip" })).toBeNull();
    expect(screen.getByText(/^Link expired /)).toBeInTheDocument();
  });

  it("shows nothing without a download", () => {
    const { container } = render(<AvatarsDownloadCard run={runWith(null)} />);
    expect(container).toBeEmptyDOMElement();
    expect(downloadOf(runWith(null))).toBeNull();
  });

  it("registers its form, words and hero", () => {
    const run = runWith(DL);
    expect(entryFor("download_avatars").Form).toBe(AvatarsForm);
    expect(entryFor("download_avatars").RunCard).toBe(AvatarsDownloadCard);
    expect(describeRunInputs(run)).toEqual([
      { label: "Server", value: "Beta" },
      { label: "Link expires after", value: "24 hours" },
    ]);
    expect(heroStat(run)).toEqual({ key: "downloaded", label: "avatars downloaded", value: 3 });
    expect(typeLabel("download_avatars").title).toBe("Download avatars");
    expect(sizeWords(512)).toBe("512 bytes");
    expect(sizeWords(5 * 1024 * 1024)).toBe("5.0 MB");
  });
});
