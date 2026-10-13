import { describe, expect, it } from "vitest";
import type { UserToken } from "../../../../types/user-tokens";
import {
  brokenIds,
  bulkSummary,
  countLabel,
  delayText,
  filterTokens,
  orderVerify,
  tokenName,
  tokenSub,
  tokensPath,
} from "./userTokens";

const tok = (o: Partial<UserToken>): UserToken => ({
  id: "t1",
  label: "",
  username: "",
  user_id: "",
  enabled: true,
  token_masked: "fake12…abcd",
  use_count: 0,
  last_used: null,
  ...o,
});

describe("user token helpers", () => {
  it("builds encoded paths", () => {
    expect(tokensPath("a b")).toBe("/api/guild-mappings/a%20b/user-tokens");
    expect(tokensPath("m1", "/bulk")).toBe("/api/guild-mappings/m1/user-tokens/bulk");
  });
  it("names and describes a row", () => {
    expect(tokenName(tok({ username: "alice" }))).toBe("alice");
    expect(tokenName(tok({ user_id: "42" }))).toBe("42");
    expect(tokenName(tok({}))).toBe("User token");
    expect(tokenSub(tok({ username: "alice" }))).toBe("fake12…abcd · alice");
    expect(tokenSub(tok({}))).toBe("fake12…abcd");
  });
  it("filters by name or masked value, case blind", () => {
    const rows = [tok({ id: "a", username: "Alice" }), tok({ id: "b", username: "bob", token_masked: "zzz999…1111" })];
    expect(filterTokens(rows, "").length).toBe(2);
    expect(filterTokens(rows, " ALI ").map((r) => r.id)).toEqual(["a"]);
    expect(filterTokens(rows, "zzz9").map((r) => r.id)).toEqual(["b"]);
    expect(filterTokens(rows, "nope")).toEqual([]);
  });
  it("summaries", () => {
    expect(countLabel(1)).toBe("1 token");
    expect(countLabel(3)).toBe("3 tokens");
    expect(bulkSummary(2, 2)).toBe("Added 2 of 2.");
    expect(bulkSummary(1, 3)).toBe("Added 1 of 3, 2 skipped.");
  });
  it("orders verify results with failures first and lists broken ids", () => {
    const r = [
      { id: "1", masked: "a", username: "", ok: true },
      { id: "2", masked: "b", username: "", ok: false },
    ];
    expect(orderVerify(r).map((x) => x.id)).toEqual(["2", "1"]);
    expect(brokenIds(r)).toEqual(["2"]);
  });
  it("delay text", () => {
    expect(delayText(0)).toBe("0");
    expect(delayText(2.5)).toBe("2.5");
    expect(delayText(undefined)).toBe("");
  });
});
