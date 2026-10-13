import { describe, expect, it } from "vitest";
import type { EventLog } from "../../types/logs";
import {
  DEFAULT_SORT,
  PAGE_SIZE,
  buildLogsQuery,
  bulkDeletePayload,
  countText,
  fmtTimestamp,
  guildText,
  hasMetadata,
  metadataRows,
  nextSort,
  pageRange,
  sortLogs,
  totalPages,
  typeMeta,
  typeOptions,
} from "./model";

function log(over: Partial<EventLog> = {}): EventLog {
  return {
    log_id: "a",
    event_type: "channel_created",
    details: "Created channel #general",
    guild_id: null,
    guild_name: null,
    channel_id: null,
    channel_name: null,
    category_id: null,
    category_name: null,
    extra_json: null,
    created_at: 1_700_000_000,
    ...over,
  };
}

describe("buildLogsQuery", () => {
  it("always sends limit and offset, and only the filters that are set", () => {
    expect(buildLogsQuery({ page: 1, type: "", search: "" })).toBe(`limit=${PAGE_SIZE}&offset=0`);
    expect(buildLogsQuery({ page: 3, type: "role_created", search: "  Mod  " })).toBe(
      "limit=50&offset=100&event_type=role_created&search=Mod",
    );
  });

  it("encodes search text and never asks for a negative offset", () => {
    const q = new URLSearchParams(buildLogsQuery({ page: 0, type: "", search: "#general & co" }));
    expect(q.get("offset")).toBe("0");
    expect(q.get("search")).toBe("#general & co");
  });
});

describe("pagination", () => {
  it("counts pages with a floor of one", () => {
    expect(totalPages(0)).toBe(1);
    expect(totalPages(50)).toBe(1);
    expect(totalPages(51)).toBe(2);
  });

  it("shows first, last and two either side, with gaps", () => {
    expect(pageRange(1, 1)).toEqual([]);
    expect(pageRange(1, 3)).toEqual([1, 2, 3]);
    expect(pageRange(1, 10)).toEqual([1, 2, 3, null, 10]);
    expect(pageRange(6, 10)).toEqual([1, null, 4, 5, 6, 7, 8, null, 10]);
    expect(pageRange(10, 10)).toEqual([1, null, 8, 9, 10]);
  });

  it("words the count like the old page", () => {
    expect(countText(1, 4, 4)).toBe("Showing 1-4 of 4");
    expect(countText(3, 20, 120)).toBe("Showing 101-120 of 120");
    expect(countText(2, 0, 7)).toBe("No results for current filters (7 total)");
    expect(countText(1, 0, 0)).toBe("");
  });
});

describe("types", () => {
  it("labels known and unknown types", () => {
    expect(typeMeta("role_created")).toEqual({ label: "Role Created", tone: "role" });
    expect(typeMeta("something_new")).toEqual({ label: "Something New", tone: "default" });
  });

  it("builds dropdown options sorted by label without duplicates", () => {
    expect(typeOptions(["role_created", "channel_deleted", "role_created", "category_created"])).toEqual([
      { value: "category_created", label: "Category Created" },
      { value: "channel_deleted", label: "Channel Deleted" },
      { value: "role_created", label: "Role Created" },
    ]);
  });
});

describe("rows", () => {
  it("formats timestamps in local time and dashes missing ones", () => {
    expect(fmtTimestamp(null)).toBe("—");
    const d = new Date(2026, 9, 13, 8, 5, 9);
    expect(fmtTimestamp(d.getTime() / 1000)).toBe("2026-10-13 08:05:09");
  });

  it("falls back from guild name to guild id to SYSTEM", () => {
    expect(guildText(log({ guild_name: "Clone" }))).toBe("Clone");
    expect(guildText(log({ guild_id: 42 }))).toBe("42");
    expect(guildText(log())).toBe("SYSTEM");
  });

  it("lists metadata in the old order and parses extra_json", () => {
    const l = log({
      guild_id: "222",
      channel_name: "general",
      category_id: "9",
      category_name: "Text",
      extra_json: JSON.stringify({
        some_flag: true,
        changed_fields: ["name", "topic"],
        clone_channel_id: "5",
        original_channel_id: "4",
        sync_task_id: "t1",
      }),
    });
    expect(hasMetadata(l)).toBe(true);
    expect(metadataRows(l)).toEqual([
      ["Sync Task", "t1"],
      ["Guild ID", "222"],
      ["Channel", "general"],
      ["Category", "Text"],
      ["Category ID", "9"],
      ["Source Channel ID", "4"],
      ["Clone Channel ID", "5"],
      ["Changed Fields", "name, topic"],
      ["Some Flag", "true"],
    ]);
  });

  it("treats a row without ids, names or extra as not expandable and ignores bad JSON", () => {
    expect(hasMetadata(log())).toBe(false);
    expect(hasMetadata(log({ extra_json: "{not json" }))).toBe(false);
  });
});

describe("sorting", () => {
  it("flips the same column and starts a new one asc, time desc", () => {
    expect(nextSort(DEFAULT_SORT, "created_at")).toEqual({ column: "created_at", dir: "asc" });
    expect(nextSort(DEFAULT_SORT, "details")).toEqual({ column: "details", dir: "asc" });
    expect(nextSort({ column: "details", dir: "asc" }, "created_at")).toEqual({ column: "created_at", dir: "desc" });
  });

  it("sorts the page rows by the chosen column", () => {
    const rows = [
      log({ log_id: "1", details: "b", created_at: 2 }),
      log({ log_id: "2", details: "a", created_at: 3 }),
      log({ log_id: "3", details: "c", created_at: 1 }),
    ];
    expect(sortLogs(rows, DEFAULT_SORT).map((r) => r.log_id)).toEqual(["2", "1", "3"]);
    expect(sortLogs(rows, { column: "details", dir: "asc" }).map((r) => r.log_id)).toEqual(["2", "1", "3"]);
    expect(sortLogs(rows, { column: "details", dir: "desc" }).map((r) => r.log_id)).toEqual(["3", "1", "2"]);
  });
});

describe("bulkDeletePayload", () => {
  it("sends only selected ids that are still on the page, in page order", () => {
    const rows = [log({ log_id: "x" }), log({ log_id: "y" }), log({ log_id: "z" })];
    expect(bulkDeletePayload(new Set(["z", "gone", "x"]), rows)).toEqual({ ids: ["x", "z"] });
    expect(bulkDeletePayload(new Set(), rows)).toEqual({ ids: [] });
  });
});
