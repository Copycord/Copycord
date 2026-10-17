import { describe, expect, it } from "vitest";
import type { ChannelRow, Orphans } from "../../types/channels";
import {
  buildCategoryPins,
  buildTree,
  categoryBadge,
  channelBadges,
  channelsUrl,
  chTypeIcon,
  chTypeLabel,
  deleteOrphansLabel,
  idSides,
  mappingLabel,
  NO_ORPHANS,
  nextSort,
  normalize,
  searchRows,
} from "./model";
import type { TreeOptions } from "./model";

function row(p: Partial<ChannelRow> & { original_channel_id: string; original_channel_name: string }): ChannelRow {
  return { cloned_channel_id: "", clone_channel_name: "", channel_type: 0, category_name: "Uncategorized", ...p };
}

const ROWS: ChannelRow[] = [
  row({ original_channel_id: "3", original_channel_name: "zeta", category_name: "Lobby", channel_type: 0 }),
  row({ original_channel_id: "1", original_channel_name: "alpha", category_name: "Lobby", channel_type: 2, cloned_channel_id: "11" }),
  row({
    original_channel_id: "2",
    original_channel_name: "café",
    clone_channel_name: "coffee",
    cloned_channel_id: "22",
    category_name: "Hangout",
    original_category_name: "Social",
    cloned_category_name: "Hangout",
    channel_type: 5,
  }),
  row({ original_channel_id: "4", original_channel_name: "loose", category_name: "" }),
];

const OPTS: TreeOptions = { query: "", filter: "all", sort: "name", dir: "asc" };
const pins = buildCategoryPins(ROWS);
const tree = (o: Partial<TreeOptions> = {}, orphans: Orphans = NO_ORPHANS) =>
  buildTree(ROWS, orphans, pins, { ...OPTS, ...o });
const shape = (t: ReturnType<typeof tree>) => t.sections.map((s) => [s.displayName, s.channels.map((c) => c.originalName)]);

describe("helpers", () => {
  it("normalizes like channels.js: lowercase, no leading #, no diacritics", () => {
    expect(normalize("# Café")).toBe("cafe");
    expect(normalize(null)).toBe("");
  });

  it("labels channel types as chTypeLabel did", () => {
    expect(chTypeLabel(0)).toBe("Text");
    expect(chTypeLabel(15)).toBe("Forum");
    expect(chTypeLabel(4)).toBe("Type 4");
    expect(chTypeLabel(null)).toBe("Type -");
  });

  it("labels mappings by name, else original to clone", () => {
    const base = { mapping_id: "m", original_guild_id: 1, cloned_guild_id: 2 };
    expect(mappingLabel({ ...base, mapping_name: " Main ", original_guild_name: "A", cloned_guild_name: "B" })).toBe("Main");
    expect(mappingLabel({ ...base, mapping_name: "", original_guild_name: "A", cloned_guild_name: "B" })).toBe("A → B");
  });

  it("builds the channels url with or without a mapping", () => {
    expect(channelsUrl("")).toBe("/api/channels");
    expect(channelsUrl("a b")).toBe("/api/channels?mapping_id=a%20b");
  });

  it("starts a new sort ascending and flips the same one", () => {
    expect(nextSort({ sort: "name", dir: "desc" }, "category")).toEqual({ sort: "category", dir: "asc" });
    expect(nextSort({ sort: "type", dir: "asc" }, "type")).toEqual({ sort: "type", dir: "desc" });
  });
});

describe("search", () => {
  it("matches names, customized names, both category names and ids", () => {
    const ids = (q: string) => searchRows(ROWS, q, pins).map((r) => r.original_channel_id);
    expect(ids("CAFE")).toEqual(["2"]);
    expect(ids("coffee")).toEqual(["2"]);
    expect(ids("social")).toEqual(["2"]);
    expect(ids("hangout")).toEqual(["2"]);
    expect(ids("22")).toEqual(["2"]);
    expect(ids("lobby")).toEqual(["3", "1"]);
    expect(ids("uncategorized")).toEqual(["4"]);
    expect(ids("#  ")).toHaveLength(4);
  });
});

describe("tree", () => {
  it("groups by category in API order and sorts channels by name", () => {
    expect(shape(tree())).toEqual([
      ["Lobby", ["alpha", "zeta"]],
      ["Hangout", ["café"]],
      ["Uncategorized", ["loose"]],
    ]);
  });

  it("shows a customized category under its pin with the original as data and tooltip", () => {
    const s = tree().sections[1]!;
    expect(s.originalName).toBe("Social");
    expect(s.customized).toBe(true);
    expect(s.tooltip).toBe("Cloned category = Social\nCustomized category = Hangout");
    const c = s.channels[0]!;
    expect(c.displayName).toBe("coffee");
    expect(c.title).toBe("Cloned channel = #café\nCustomized channel = #coffee");
  });

  it("sorts categories A to Z, then Z to A, when sorting by category", () => {
    expect(tree({ sort: "category" }).sections.map((s) => s.key)).toEqual(["Hangout", "Lobby", "Uncategorized"]);
    expect(tree({ sort: "category", dir: "desc" }).sections.map((s) => s.key)).toEqual(["Uncategorized", "Lobby", "Hangout"]);
  });

  it("sorts by type then name, and reverses inside sections when descending", () => {
    expect(tree({ sort: "type" }).sections[0]!.channels.map((c) => c.originalName)).toEqual(["zeta", "alpha"]);
    expect(tree({ dir: "desc" }).sections[0]!.channels.map((c) => c.originalName)).toEqual(["zeta", "alpha"]);
  });

  it("drops sections the search empties and reports empty when nothing matches", () => {
    expect(shape(tree({ query: "zet" }))).toEqual([["Lobby", ["zeta"]]]);
    expect(tree({ query: "nothing" })).toEqual({ sections: [], empty: true });
  });

  it("merges orphans in and filters orphans or non-orphans", () => {
    const orphans: Orphans = {
      categories: [{ id: "90", name: "Old stuff" }],
      channels: [
        { id: "91", name: "ghost", type: 0, parent_id: "90" },
        { id: "92", name: "stray", type: 2 },
      ],
    };
    expect(shape(tree({}, orphans))).toEqual([
      ["Lobby", ["alpha", "zeta"]],
      ["Hangout", ["café"]],
      ["Uncategorized", ["loose", "stray"]],
      ["Old stuff", ["ghost"]],
    ]);
    const only = tree({ filter: "orphans" }, orphans);
    expect(shape(only)).toEqual([
      ["Uncategorized", ["stray"]],
      ["Old stuff", ["ghost"]],
    ]);
    expect(only.sections[1]!.orphanCategoryId).toBe("90");
    expect(shape(tree({ filter: "nonorphans" }, orphans))).toEqual([
      ["Lobby", ["alpha", "zeta"]],
      ["Hangout", ["café"]],
      ["Uncategorized", ["loose"]],
      ["Old stuff", []],
    ]);
    // With orphans present the tree is never "empty".
    expect(tree({ query: "nothing" }, orphans).empty).toBe(false);
  });
});

describe("channel type icons and badges", () => {
  it("maps each channel type to its Discord style icon, any other type to the #", () => {
    expect([0, 2, 5, 13, 15, 4, 99].map(chTypeIcon)).toEqual(["text", "voice", "announcement", "stage", "forum", "text", "text"]);
    expect(chTypeLabel(13)).toBe("Stage");
    expect(chTypeLabel(99)).toBe("Type 99");
  });

  it("badges only the exceptions: cloned is the normal state", () => {
    const c = (orphan: boolean, clonedId: string, customized: boolean) => channelBadges({ orphan, clonedId, customized });
    expect(c(false, "110", false)).toEqual([]);
    expect(c(false, "", false)).toEqual(["notcloned"]);
    expect(c(false, "110", true)).toEqual(["custom"]);
    expect(c(false, "", true)).toEqual(["notcloned", "custom"]);
    expect(c(true, "", false)).toEqual(["orphan"]);
  });

  it("badges an orphan category before a customized one, and a plain one not at all", () => {
    expect(categoryBadge({ orphanCategory: false, customized: false })).toBeNull();
    expect(categoryBadge({ orphanCategory: false, customized: true })).toBe("custom");
    expect(categoryBadge({ orphanCategory: true, customized: false })).toBe("orphan");
    expect(categoryBadge({ orphanCategory: true, customized: true })).toBe("orphan");
  });

  it("dashes the missing side of the id row", () => {
    expect(idSides({ orphan: false, id: "444", clonedId: "555" })).toEqual({ original: "444", cloned: "555" });
    expect(idSides({ orphan: false, id: "444", clonedId: "" })).toEqual({ original: "444", cloned: null });
    expect(idSides({ orphan: true, id: "912", clonedId: "" })).toEqual({ original: null, cloned: "912" });
  });

  it("labels Delete orphans with the live count", () => {
    expect(deleteOrphansLabel(0)).toBe("Delete 0 orphans");
    expect(deleteOrphansLabel(1)).toBe("Delete 1 orphan");
    expect(deleteOrphansLabel(3)).toBe("Delete 3 orphans");
  });
});
