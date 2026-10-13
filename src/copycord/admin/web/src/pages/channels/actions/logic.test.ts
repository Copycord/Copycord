import { describe, expect, it } from "vitest";
import type { ChannelRow } from "../../../types/channels";
import { buildCategoryPins } from "../model";
import {
  blacklistBody,
  customizeCategoryBody,
  customizeChannelBody,
  deleteAllPayload,
  deleteOnePayload,
  failureText,
  orphanIds,
  resolveCategoryIds,
  resultId,
  verifyEnvelope,
  wasDeleted,
  withoutDeleted,
} from "./logic";

const row = (over: Partial<ChannelRow> = {}): ChannelRow => ({
  original_channel_id: "10",
  original_channel_name: "general",
  cloned_channel_id: "110",
  clone_channel_name: "",
  channel_type: 0,
  category_name: "Chat Zone",
  original_category_name: "Community",
  cloned_category_name: "Chat Zone",
  original_category_id: "900",
  cloned_category_id: "901",
  original_guild_id: "1",
  cloned_guild_id: "2",
  ...over,
});

describe("category ids", () => {
  const rows = [row({ original_channel_id: "11", original_category_id: undefined }), row()];
  const pins = buildCategoryPins(rows);

  it("resolves by original or customized name, skipping rows without ids", () => {
    const want = { originalCatId: "900", clonedCatId: "901", originalGuildId: "1", clonedGuildId: "2" };
    expect(resolveCategoryIds(rows, "Community", pins)).toEqual(want);
    expect(resolveCategoryIds(rows, "chat zone", pins)).toEqual(want);
  });

  it("returns nulls for blank or unknown names", () => {
    expect(resolveCategoryIds(rows, " ", pins).originalCatId).toBeNull();
    expect(resolveCategoryIds(rows, "Nope", pins).clonedGuildId).toBeNull();
  });
});

describe("customize bodies", () => {
  it("sends the trimmed name, or null to clear it", () => {
    expect(customizeChannelBody(row(), "  funny  ")).toEqual({
      original_channel_id: "10",
      cloned_guild_id: "2",
      clone_channel_name: "funny",
    });
    expect(customizeChannelBody(row(), "   ")?.clone_channel_name).toBeNull();
  });

  it("needs the cloned guild id", () => {
    expect(customizeChannelBody(row({ cloned_guild_id: "" }), "x")).toBeNull();
    const ids = { originalCatId: "900", clonedCatId: null, originalGuildId: "1", clonedGuildId: "2" };
    expect(customizeCategoryBody(ids, " Lobby ")).toEqual({
      original_category_id: "900",
      cloned_guild_id: "2",
      custom_category_name: "Lobby",
    });
    expect(customizeCategoryBody(ids, "")?.custom_category_name).toBeNull();
    expect(customizeCategoryBody({ ...ids, clonedGuildId: null }, "x")).toBeNull();
  });
});

describe("blacklist and orphan payloads", () => {
  it("stringifies every id", () => {
    expect(blacklistBody("channel", "10", "m1", "1", undefined)).toEqual({
      scope: "channel",
      obj_id: "10",
      mapping_id: "m1",
      original_guild_id: "1",
      cloned_guild_id: "",
    });
  });

  it("builds the /ws/in frames", () => {
    const orphans = { categories: [{ id: "5", name: "c" }], channels: [{ id: "6", name: "a" }, { id: "7", name: "b" }] };
    expect(orphanIds(orphans)).toEqual(["5", "6", "7"]);
    expect(verifyEnvelope("m1", deleteOnePayload("category", "5"))).toEqual({
      kind: "verify",
      role: "ui",
      payload: { mapping_id: "m1", action: "delete_one", kind: "category", id: "5" },
    });
    expect(deleteAllPayload(orphans)).toEqual({ action: "delete_all", ids: ["5", "6", "7"] });
  });

  it("reads delete results", () => {
    expect(resultId({ channel_id: 6 })).toBe("6");
    expect(wasDeleted({ status: "Deleted" })).toBe(true);
    expect(wasDeleted({ reason: "protected" })).toBe(false);
    expect(failureText({ name: "x", reason: "protected" })).toBe(`"x" can't be deleted. Manual action required.`);
    expect(failureText({ reason: "boom" })).toBe(`Failed to delete "Item".`);
  });

  it("drops deleted ids from the orphans", () => {
    const o = { categories: [{ id: "5", name: "c" }], channels: [{ id: "6", name: "a" }] };
    expect(withoutDeleted(o, new Set(["6"]))).toEqual({ categories: o.categories, channels: [] });
  });
});
