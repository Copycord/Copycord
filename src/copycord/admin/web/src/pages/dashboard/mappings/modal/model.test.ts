import { describe, expect, it } from "vitest";
import type { DashboardMapping } from "../../../../types/dashboard-mappings";
import {
  DEFAULT_MAPPING_SETTINGS,
  buildPayload,
  initialForm,
  invalidFields,
  modeOf,
  normalizeStrategy,
  settingMatches,
  snapshot,
} from "./model";

const M: DashboardMapping = {
  mapping_id: "m1",
  mapping_name: "Alpha",
  original_guild_id: 111,
  cloned_guild_id: "222",
  settings: { CLONE_MESSAGES: "false", ANONYMIZE_USERS: true, USER_TOKEN_STRATEGY: "random", USER_TOKEN_MIN_DELAY: 2 },
};

describe("mapping modal model", () => {
  it("picks the mode from the mapping and the clone option", () => {
    expect(modeOf(null)).toBe("create");
    expect(modeOf(M)).toBe("edit");
    expect(modeOf(M, "clone")).toBe("clone");
    expect(modeOf(null, "clone")).toBe("create");
  });

  it("create uses the defaults", () => {
    const f = initialForm(null, "create");
    expect(f.mapping_id).toBe("");
    expect(f.settings).toEqual(DEFAULT_MAPPING_SETTINGS);
  });

  it("edit loads the row, normalizing strings, booleans and the legacy strategy", () => {
    const f = initialForm(M, "edit");
    expect(f).toMatchObject({ mapping_id: "m1", mapping_name: "Alpha", original_guild_id: "111", cloned_guild_id: "222" });
    expect(f.settings.CLONE_MESSAGES).toBe(false);
    expect(f.settings.ANONYMIZE_USERS).toBe(true);
    expect(f.settings.USER_TOKEN_STRATEGY).toBe("round_robin");
    expect(f.settings.USER_TOKEN_MIN_DELAY).toBe(2);
    expect(f.settings.DELETE_ROLES).toBe(true);
  });

  it("clone copies the settings but leaves id, name and guilds empty", () => {
    const f = initialForm(M, "clone");
    expect(f).toMatchObject({ mapping_id: "", mapping_name: "", original_guild_id: "", cloned_guild_id: "" });
    expect(f.settings.ANONYMIZE_USERS).toBe(true);
  });

  it("flags empty fields in form order", () => {
    const f = initialForm(null, "create");
    expect(invalidFields(f)).toEqual(["mapping_name", "original_guild_id", "cloned_guild_id"]);
    expect(invalidFields({ ...f, mapping_name: " x ", original_guild_id: "1", cloned_guild_id: "2" })).toEqual([]);
  });

  it("builds the save payload with trimmed fields and numeric delays", () => {
    const f = initialForm(M, "edit");
    f.mapping_name = "  Beta ";
    f.settings.USER_TOKEN_MAX_DELAY = "3.5";
    f.settings.USER_TOKEN_MIN_DELAY = "abc";
    const p = buildPayload(f);
    expect(p).toMatchObject({ mapping_name: "Beta", original_guild_id: "111", cloned_guild_id: "222", original_guild_name: "", cloned_guild_name: "" });
    expect(p.settings.USER_TOKEN_MAX_DELAY).toBe(3.5);
    expect(p.settings.USER_TOKEN_MIN_DELAY).toBe(0);
    expect(Object.keys(p.settings).length).toBe(Object.keys(DEFAULT_MAPPING_SETTINGS).length);
  });

  it("snapshot changes with any edit", () => {
    const a = initialForm(M, "edit");
    const b = initialForm(M, "edit");
    expect(snapshot(a)).toBe(snapshot(b));
    b.settings.APPEND_AUTHOR = true;
    expect(snapshot(a)).not.toBe(snapshot(b));
  });

  it("settings search matches key, name and description", () => {
    expect(settingMatches("CLONE_EMOJI", "")).toBe(true);
    expect(settingMatches("CLONE_EMOJI", "emoji")).toBe(true);
    expect(settingMatches("CLONE_EMOJI", "stickers")).toBe(false);
    expect(settingMatches("RENAME_CHANNELS", "categories")).toBe(true);
  });

  it("normalizes the strategy", () => {
    expect(normalizeStrategy("sticky_author")).toBe("sticky_author");
    expect(normalizeStrategy("random")).toBe("round_robin");
  });
});
