import { describe, expect, it } from "vitest";
import {
  buildSaveForm,
  categoryNames,
  EMPTY_FILTERS,
  filterObjects,
  filterRoles,
  isDirty,
  parentLabel,
  parseFilters,
  pickerHelp,
  pickerTitle,
  selectState,
  setMany,
  toggleOne,
} from "./model";

const CATS = categoryNames([
  { id: "1", name: "Text Zone" },
  { id: "2", name: "Voice Zone" },
]);
const CHANNELS = [
  { id: "10", name: "general", parent_id: "1" },
  { id: "11", name: "lounge", parent_id: "2" },
  { id: "12", name: "side", parent_id: "10", parent_name: "general", is_thread: true },
];

describe("filters model", () => {
  it("parses a response, stringifying ids and defaulting missing lists", () => {
    const s = parseFilters({ wl_channels: [5, "6"], blocked_words: ["a"], blocked_role_ids: [7] });
    expect(s.wl_channels).toEqual(["5", "6"]);
    expect(s.blocked_role_ids).toEqual(["7"]);
    expect(s.bl_users).toEqual([]);
    expect(parseFilters(null)).toEqual(EMPTY_FILTERS);
  });

  it("builds the save form with one comma joined field per list", () => {
    const fd = buildSaveForm({ ...EMPTY_FILTERS, wl_categories: ["1", "2"], blocked_words: ["a b", "c"] });
    expect(fd.get("wl_categories")).toBe("1,2");
    expect(fd.get("blocked_words")).toBe("a b,c");
    expect(fd.get("bl_users")).toBe("");
    expect(Array.from(fd.keys())).toEqual(Object.keys(EMPTY_FILTERS));
  });

  it("detects changes against the baseline", () => {
    const base = { ...EMPTY_FILTERS, wl_users: ["1"] };
    expect(isDirty(base, { ...base })).toBe(false);
    expect(isDirty({ ...base, wl_users: ["1", "2"] }, base)).toBe(true);
  });

  it("titles and helps the four pickers", () => {
    expect(pickerTitle("wl_categories")).toBe("Allow Categories");
    expect(pickerTitle("ex_channels")).toBe("Block Channels & Threads");
    expect(pickerHelp("ex_categories")).toContain("will not be cloned");
    expect(pickerHelp("wl_channels")).toContain("Only the selected channels");
  });

  it("labels a channel by category and a thread by its parent channel", () => {
    expect(parentLabel(CHANNELS[0]!, CATS)).toBe("Text Zone");
    expect(parentLabel(CHANNELS[2]!, CATS)).toBe("general");
  });

  it("searches objects by name, id and category name", () => {
    expect(filterObjects(CHANNELS, "", "channel", CATS)).toHaveLength(3);
    expect(filterObjects(CHANNELS, "LOUN", "channel", CATS).map((o) => o.id)).toEqual(["11"]);
    expect(filterObjects(CHANNELS, "12", "channel", CATS).map((o) => o.id)).toEqual(["12"]);
    expect(filterObjects(CHANNELS, "voice", "channel", CATS).map((o) => o.id)).toEqual(["11"]);
    expect(filterObjects(CHANNELS, "voice", "category", CATS)).toEqual([]);
  });

  it("searches roles by name or id", () => {
    const roles = [{ id: "1", name: "Admin" }, { id: "22", name: "Mod" }];
    expect(filterRoles(roles, "adm").map((r) => r.id)).toEqual(["1"]);
    expect(filterRoles(roles, "22").map((r) => r.id)).toEqual(["22"]);
  });

  it("selects, toggles and reports the select-all state over visible rows", () => {
    const sel = new Set(["a"]);
    expect(selectState(sel, ["a", "b"])).toBe("some");
    expect(selectState(sel, ["a"])).toBe("all");
    expect(selectState(sel, ["b"])).toBe("none");
    expect(selectState(sel, [])).toBe("none");
    expect([...setMany(sel, ["b", "c"], true)]).toEqual(["a", "b", "c"]);
    expect([...setMany(sel, ["a"], false)]).toEqual([]);
    expect([...toggleOne(sel, "a")]).toEqual([]);
    expect([...toggleOne(sel, "z")]).toEqual(["a", "z"]);
    expect([...sel]).toEqual(["a"]);
  });
});
