import { describe, expect, it } from "vitest";
import { ApiError } from "../../lib/api";
import {
  COLUMN_GROUPS,
  COLUMN_SORT,
  COLUMNS,
  COLUMNS_KEY,
  COLUMNS_KEY_V1,
  DEFAULT_COLUMNS,
  DEFAULT_SORT,
  SORT_OPTIONS,
  migrateV1,
  nextSort,
  sortBy,
  tableMinWidth,
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
    saveColumns(["message_id", "username", "author_id"]);
    expect(JSON.parse(localStorage.getItem(COLUMNS_KEY)!)).toEqual(["message_id", "username", "author_id"]);
    expect(loadColumns()).toEqual(["username", "author_id", "message_id"]);
  });

  it("has the v2 defaults, every column in a group, Preview not toggleable", () => {
    expect(COLUMNS_KEY).toBe("cc.messages.columns.v2");
    expect([...DEFAULT_COLUMNS]).toEqual(["time", "author_name", "username", "original_channel", "mapping", "attachments", "links", "message_id"]);
    expect(COLUMNS).toHaveLength(19);
    for (const c of COLUMNS) expect(COLUMN_GROUPS).toContain(c.group);
    expect(COLUMNS.map((c) => c.label)).not.toContain("Preview");
    expect(tableMinWidth(["time"])).toBe(96 + 96);
  });

  it("migrates a v1 choice: author and guild ids split, new columns off, v1 removed", () => {
    localStorage.setItem(COLUMNS_KEY_V1, JSON.stringify(["time", "author", "guild_ids", "counts", "cloned_channel_id", "gone"]));
    const cols = ["time", "author_name", "username", "cloned_channel_id", "original_guild_id", "cloned_guild_id", "attachments", "embeds"];
    expect(loadColumns()).toEqual(cols);
    expect(JSON.parse(localStorage.getItem(COLUMNS_KEY)!)).toEqual(cols);
    expect(localStorage.getItem(COLUMNS_KEY_V1)).toBeNull();
    expect(migrateV1(["channel", "message_id"])).toEqual(["original_channel", "message_id"]);
  });

  it("prefers a v2 choice over a v1 one and ignores an unusable v1", () => {
    localStorage.setItem(COLUMNS_KEY, JSON.stringify(["links"]));
    localStorage.setItem(COLUMNS_KEY_V1, JSON.stringify(["author"]));
    expect(loadColumns()).toEqual(["links"]);
    localStorage.clear();
    localStorage.setItem(COLUMNS_KEY_V1, JSON.stringify(["nope"]));
    expect(loadColumns()).toEqual([...DEFAULT_COLUMNS]);
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
    expect(toggleColumn(["time", "username"], "username")).toEqual(["time"]);
    expect(toggleColumn(["time"], "time")).toEqual(["time"]);
    expect(toggleColumn(["username"], "time")).toEqual(["time", "username"]);
  });

  it("sorts by username and original guild through the API", () => {
    expect(new URL(messagesUrl(NO_FILTERS, sortBy("username")), "http://x").searchParams.get("sort")).toBe("username");
    expect(new URL(messagesUrl(NO_FILTERS, sortBy("guild")), "http://x").searchParams.get("sort")).toBe("guild");
  });

  it("gives every column but Links a server sort, one dropdown entry per sort", () => {
    expect(COLUMNS.filter((c) => !COLUMN_SORT[c.key]).map((c) => c.key)).toEqual(["links"]);
    const values = SORT_OPTIONS.map((o) => o.value);
    expect(new Set(values).size).toBe(values.length);
    expect(SORT_OPTIONS[0]).toEqual({ value: "created_at", label: "Time" });
    expect(SORT_OPTIONS.find((o) => o.value === "cloned_guild")?.label).toBe("Cloned guild");
  });

  it("flips the active sort and starts another one at its default direction", () => {
    expect(DEFAULT_SORT).toEqual({ by: "created_at", dir: "desc" });
    expect(nextSort(DEFAULT_SORT, "created_at")).toEqual({ by: "created_at", dir: "asc" });
    expect(nextSort({ by: "created_at", dir: "asc" }, "created_at")).toEqual({ by: "created_at", dir: "desc" });
    expect(nextSort(DEFAULT_SORT, "attachments")).toEqual({ by: "attachments", dir: "asc" });
    expect(nextSort({ by: "attachments", dir: "asc" }, "attachments")).toEqual({ by: "attachments", dir: "desc" });
  });
});

describe("messagesUrl", () => {
  it("maps sort, filters, dates and the cursor to query params", () => {
    const url = new URL(
      messagesUrl({ ...NO_FILTERS, q: " ann ", mapping: "m1", channel: "10", attachments: "yes", from: "2026-10-01", to: "2026-10-02" }, { by: "created_at", dir: "asc" }, "CUR"),
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
    const p = new URL(messagesUrl(NO_FILTERS, sortBy("author")), "http://x").searchParams;
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
