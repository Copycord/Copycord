import { describe, expect, it } from "vitest";
import { ApiError } from "../../lib/api";
import {
  COLUMNS_KEY,
  DEFAULT_COLUMNS,
  NO_FILTERS,
  dayBound,
  filtersActive,
  loadColumns,
  messagesUrl,
  previewError,
  relativeTime,
  saveColumns,
  toggleColumn,
} from "./model";

describe("columns persistence", () => {
  it("starts from the defaults and keeps a saved choice in table order", () => {
    expect(loadColumns()).toEqual([...DEFAULT_COLUMNS]);
    saveColumns(["message_id", "author", "author_id"]);
    expect(JSON.parse(localStorage.getItem(COLUMNS_KEY)!)).toEqual(["message_id", "author", "author_id"]);
    expect(loadColumns()).toEqual(["author", "message_id", "author_id"]);
  });

  it("falls back to the defaults for junk, unknown keys or an empty list", () => {
    localStorage.setItem(COLUMNS_KEY, "{not json");
    expect(loadColumns()).toEqual([...DEFAULT_COLUMNS]);
    localStorage.setItem(COLUMNS_KEY, JSON.stringify(["nope"]));
    expect(loadColumns()).toEqual([...DEFAULT_COLUMNS]);
    localStorage.setItem(COLUMNS_KEY, JSON.stringify([]));
    expect(loadColumns()).toEqual([...DEFAULT_COLUMNS]);
  });

  it("toggles a column and never hides the last one", () => {
    expect(toggleColumn(["time", "author"], "author")).toEqual(["time"]);
    expect(toggleColumn(["time"], "time")).toEqual(["time"]);
    expect(toggleColumn(["author"], "time")).toEqual(["time", "author"]);
  });
});

describe("messagesUrl", () => {
  it("maps sort, filters, dates and the cursor to query params", () => {
    const url = new URL(
      messagesUrl({ ...NO_FILTERS, q: " ann ", mapping: "m1", channel: "10", attachments: "yes", from: "2026-10-01", to: "2026-10-02" }, "oldest", "CUR"),
      "http://x",
    );
    const p = url.searchParams;
    expect(url.pathname).toBe("/api/messages");
    expect(p.get("q")).toBe("ann");
    expect(p.get("mapping_id")).toBe("m1");
    expect(p.get("channel_id")).toBe("10");
    expect(p.get("has_attachments")).toBe("true");
    expect(p.get("created_from")).toBe(String(dayBound("2026-10-01", false)));
    expect(p.get("created_to")).toBe(String(dayBound("2026-10-02", true)));
    expect(Number(p.get("created_to")) - Number(p.get("created_from"))).toBe(2 * 86400 - 1);
    expect(p.get("sort")).toBe("created_at");
    expect(p.get("order")).toBe("asc");
    expect(p.get("cursor")).toBe("CUR");
  });

  it("sends only sort, order and limit without filters", () => {
    const p = new URL(messagesUrl(NO_FILTERS, "author"), "http://x").searchParams;
    expect([...p.keys()].sort()).toEqual(["limit", "order", "sort"]);
    expect(p.get("sort")).toBe("author");
    expect(filtersActive(NO_FILTERS)).toBe(false);
    expect(filtersActive({ ...NO_FILTERS, attachments: "no" })).toBe(true);
  });
});

describe("relativeTime", () => {
  const now = 1_760_000_000_000;
  it("reads like a feed", () => {
    expect(relativeTime(now / 1000 - 10, now)).toBe("just now");
    expect(relativeTime(now / 1000 - 300, now)).toBe("5m ago");
    expect(relativeTime(now / 1000 - 3 * 3600, now)).toBe("3h ago");
    expect(relativeTime(now / 1000 - 4 * 86400, now)).toBe("4d ago");
    expect(relativeTime(null, now)).toBe("-");
  });
});

describe("previewError", () => {
  const err = (status: number, body: unknown) => new ApiError(status, "/api/messages/1/content", body);
  it("words each live fetch failure", () => {
    expect(previewError(err(503, { detail: "bot-not-running" })).title).toMatch(/not running/);
    expect(previewError(err(404, { detail: "message-deleted" })).title).toMatch(/deleted/);
    expect(previewError(err(404, { detail: "message-not-recorded" })).title).toMatch(/no longer recorded/);
    expect(previewError(err(403, { detail: "no-access" })).title).toMatch(/cannot read/);
    const rl = previewError(err(429, { ok: false, error: "rate-limited", retry_after: 2.5 }));
    expect(rl.retryAfter).toBe(2.5);
    expect(rl.hint).toBe("Try again in 3 s.");
    expect(previewError(new TypeError("x")).hint).toMatch(/Network/);
  });
});
