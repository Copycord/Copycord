import { describe, expect, it } from "vitest";
import type { DashboardMapping } from "../../../../types/dashboard-mappings";
import {
  DEFAULT_MAPPING_SETTINGS,
  PRESENCE_VALUES,
  buildPayload,
  initialForm,
  invalidFields,
  modeOf,
  normalizePresence,
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

  it("edit loads the row, normalizing strings and booleans and dropping the retired strategy", () => {
    const f = initialForm(M, "edit");
    expect(f).toMatchObject({ mapping_id: "m1", mapping_name: "Alpha", original_guild_id: "111", cloned_guild_id: "222" });
    expect(f.settings.CLONE_MESSAGES).toBe(false);
    expect(f.settings.ANONYMIZE_USERS).toBe(true);
    expect("USER_TOKEN_STRATEGY" in f.settings).toBe(false);
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
    expect(Object.keys(p.settings).length).toBe(52);
    expect(p.settings.USER_TOKEN_PRESENCE_ROTATE).toBe(false);
  });

  it("rotate status defaults off and loads and saves as a boolean", () => {
    expect(DEFAULT_MAPPING_SETTINGS.USER_TOKEN_PRESENCE_ROTATE).toBe(false);
    expect(initialForm(M, "edit").settings.USER_TOKEN_PRESENCE_ROTATE).toBe(false);
    const f = initialForm({ ...M, settings: { USER_TOKEN_PRESENCE_ROTATE: "true" } }, "edit");
    expect(f.settings.USER_TOKEN_PRESENCE_ROTATE).toBe(true);
    expect(initialForm({ ...M, settings: { USER_TOKEN_PRESENCE_ROTATE: "false" } }, "edit").settings.USER_TOKEN_PRESENCE_ROTATE).toBe(false);
    // A fixed status keeps the saved value; it is ignored, not cleared.
    f.settings.USER_TOKEN_PRESENCE = "busy";
    expect(buildPayload(f).settings.USER_TOKEN_PRESENCE_ROTATE).toBe(true);
    f.settings.USER_TOKEN_PRESENCE_ROTATE = "false";
    expect(buildPayload(f).settings.USER_TOKEN_PRESENCE_ROTATE).toBe(false);
  });

  it("defaults presence to random and has no keep online setting", () => {
    const f = initialForm(null, "create");
    expect(f.settings.USER_TOKEN_PRESENCE).toBe("random");
    expect("USER_TOKEN_KEEP_ONLINE" in DEFAULT_MAPPING_SETTINGS).toBe(false);
    expect("USER_TOKEN_KEEP_ONLINE" in f.settings).toBe(false);
  });

  it("a keep online value stored by an older version is neither loaded nor sent", () => {
    const f = initialForm({ ...M, settings: { USER_TOKEN_KEEP_ONLINE: 2 } }, "edit");
    expect("USER_TOKEN_KEEP_ONLINE" in f.settings).toBe(false);
    expect("USER_TOKEN_KEEP_ONLINE" in buildPayload(f).settings).toBe(false);
    expect("USER_TOKEN_KEEP_ONLINE" in initialForm({ ...M, settings: { USER_TOKEN_KEEP_ONLINE: 2 } }, "clone").settings).toBe(false);
  });

  it("normalizes presence to the four values", () => {
    expect(PRESENCE_VALUES).toEqual(["random", "online", "idle", "busy"]);
    expect(normalizePresence("random")).toBe("random");
    expect(normalizePresence("busy")).toBe("busy");
    expect(normalizePresence("ONLINE")).toBe("online");
    expect(normalizePresence("idle")).toBe("idle");
    expect(normalizePresence("Idle")).toBe("idle");
    expect(normalizePresence("invisible")).toBe("online");
    expect(normalizePresence("Invisible")).toBe("online");
    expect(normalizePresence("dnd")).toBe("random");
    expect(normalizePresence("offline")).toBe("random");
    expect(normalizePresence(undefined)).toBe("random");
  });

  it("loads and saves the token pool settings normalized", () => {
    const f = initialForm({ ...M, settings: { USER_TOKEN_PRESENCE: "nope" } }, "edit");
    expect(f.settings.USER_TOKEN_PRESENCE).toBe("random");
    f.settings.USER_TOKEN_PRESENCE = "busy";
    const p = buildPayload(f);
    expect(p.settings.USER_TOKEN_PRESENCE).toBe("busy");
  });

  it("a saved invisible status loads and saves as online", () => {
    const f = initialForm({ ...M, settings: { USER_TOKEN_PRESENCE: "invisible" } }, "edit");
    expect(f.settings.USER_TOKEN_PRESENCE).toBe("online");
    f.settings.USER_TOKEN_PRESENCE = "invisible";
    expect(buildPayload(f).settings.USER_TOKEN_PRESENCE).toBe("online");
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

  it("has no strategy setting; a stored round_robin or sticky_author is neither loaded nor sent", () => {
    expect("USER_TOKEN_STRATEGY" in DEFAULT_MAPPING_SETTINGS).toBe(false);
    for (const stored of ["round_robin", "sticky_author"]) {
      const f = initialForm({ ...M, settings: { USER_TOKEN_STRATEGY: stored } }, "edit");
      expect("USER_TOKEN_STRATEGY" in f.settings).toBe(false);
      expect("USER_TOKEN_STRATEGY" in buildPayload(f).settings).toBe(false);
      expect("USER_TOKEN_STRATEGY" in initialForm({ ...M, settings: { USER_TOKEN_STRATEGY: stored } }, "clone").settings).toBe(false);
    }
  });
});
