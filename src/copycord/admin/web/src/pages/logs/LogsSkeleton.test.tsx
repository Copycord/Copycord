import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { ToastProvider } from "../../components/Toast";
import { ConfirmProvider } from "../../components/ui/ConfirmDialog";
import { holdFetch } from "../../test/holdFetch";
import { cells, structure } from "../../test/structure";
import type { EventLog } from "../../types/logs";
import { Logs } from "../Logs";
import { LogRow } from "./LogRow";
import {
  BADGE,
  DELETE_BTN,
  DETAILS_TEXT,
  GUILD_TEXT,
  LOADING_ROWS,
  LogsLoadingRows,
  LogsSkeleton,
  TIMESTAMP_TEXT,
} from "./LogsSkeleton";

const LOG: EventLog = {
  log_id: "a",
  event_type: "channel_created",
  details: "Created channel #general",
  guild_id: "2",
  guild_name: "E2E Clone",
  channel_id: null,
  channel_name: null,
  category_id: null,
  category_name: null,
  extra_json: null,
  created_at: 1_700_000_000,
};

const json = (body: unknown) => new Response(JSON.stringify(body), { headers: { "content-type": "application/json" } });

function serve(logs: EventLog[]) {
  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: RequestInfo | URL) => {
      const path = new URL(String(input), "http://x").pathname;
      if (path === "/api/event-log-types") return json({ ok: true, types: ["channel_created"] });
      return json({ ok: true, logs, total: logs.length, types: ["channel_created"] });
    }),
  );
}

const page = () =>
  render(
    <ToastProvider>
      <ConfirmProvider>
        <Logs />
      </ConfirmProvider>
    </ToastProvider>,
  );

describe("Logs skeleton", () => {
  it("has the loaded page's header, toolbar controls, five column heads and the pager", async () => {
    const skel = render(<LogsSkeleton />);
    const skeleton = structure(skel.container, "tbody tr");
    skel.unmount();

    serve([LOG]);
    const loaded = page();
    await screen.findByText("Created channel #general");
    const real = structure(loaded.container, "tbody tr");

    expect(skeleton).toEqual(real);
    expect(real).toMatchObject({ headings: ["h1 Logs", "h2 Event Logs"], columns: [5], pagers: 1 });
    expect(real.controls).toEqual(
      expect.arrayContaining(["input:text Search logs", "button Refresh logs", "button Clear all logs", "button Previous page", "button Page 1", "button Next page"]),
    );
  });

  it("the page's own loading state is the skeleton: same chrome, the pager in place, loading rows", async () => {
    const skel = render(<LogsSkeleton />);
    const skeleton = structure(skel.container, "tbody tr");
    skel.unmount();

    serve([LOG]);
    const release = holdFetch("/api/event-logs");
    const loading = page();
    await screen.findByRole("status", { name: "Loading event logs" });
    expect(structure(loading.container, "tbody tr")).toEqual(skeleton);
    expect(loading.container.querySelectorAll("tbody tr[data-skeleton-row]")).toHaveLength(LOADING_ROWS);
    release();
    await screen.findByText("Created channel #general");
    expect(document.getElementById("logs-pagination")).toBeInTheDocument();
  });

  it("a loading row has the five cells of a log row, each holding the same text box", () => {
    const { container } = render(
      <table>
        <tbody>
          <LogRow log={LOG} expanded={false} deleting={false} onToggle={() => {}} onDelete={() => {}} />
          <LogsLoadingRows rows={2} />
        </tbody>
      </table>,
    );
    const [real, , skeleton] = Array.from(container.querySelectorAll("tr"));
    expect(cells(skeleton!)).toEqual(cells(real!));
    expect(real!.children).toHaveLength(5);
    const inner = (tr: Element) => Array.from(tr.children).map((td) => td.lastElementChild!.className);
    const texts = [TIMESTAMP_TEXT, BADGE, GUILD_TEXT, DETAILS_TEXT, DELETE_BTN];
    inner(real!).forEach((cls, i) => expect(cls.startsWith(texts[i]!)).toBe(true));
    inner(skeleton!).forEach((cls, i) => expect(cls.startsWith(texts[i]!)).toBe(true));
  });

  it("keeps the rows and the pager while the next page loads", async () => {
    const many = Array.from({ length: 60 }, (_, i) => ({ ...LOG, log_id: `l${i}`, details: `Row ${i}` }));
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL) => {
        const url = new URL(String(input), "http://x");
        if (url.pathname === "/api/event-log-types") return json({ ok: true, types: ["channel_created"] });
        const offset = Number(url.searchParams.get("offset") ?? 0);
        return json({ ok: true, logs: many.slice(offset, offset + 50), total: many.length, types: ["channel_created"] });
      }),
    );
    page();
    await screen.findByText("Row 0");
    const release = holdFetch("/api/event-logs");
    await userEvent.setup().click(screen.getByRole("button", { name: "Page 2" }));
    await waitFor(() => expect(document.getElementById("logs-table")).toHaveAttribute("aria-busy", "true"));
    expect(screen.getByText("Row 0")).toBeInTheDocument();
    expect(screen.queryByRole("status", { name: "Loading event logs" })).toBeNull();
    expect(document.getElementById("logs-pagination")).toBeInTheDocument();
    release();
    await screen.findByText("Row 50");
  });
});
