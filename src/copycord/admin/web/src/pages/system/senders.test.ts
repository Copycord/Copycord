import { describe, expect, it } from "vitest";
import type { Sender } from "../../types/system";
import {
  STATUS_LABEL,
  STATUS_ORDER,
  avatarSources,
  defaultAvatarIndex,
  filterSenders,
  senderStatus,
  sortSenders,
} from "./senders";

function sender(id: string, state: Sender["state"], presence: Sender["presence"] = null): Sender {
  return { id, username: `relay.${id}`, display_name: null, avatar_url: null, state, presence, since: null, last_used: null, use_count: 0, mapping_ids: [] };
}

describe("senderStatus", () => {
  it("maps an online session by its presence and anything else by its state", () => {
    expect(senderStatus({ state: "online", presence: "online" })).toBe("online");
    expect(senderStatus({ state: "online", presence: "idle" })).toBe("idle");
    expect(senderStatus({ state: "online", presence: "dnd" })).toBe("busy");
    expect(senderStatus({ state: "online", presence: null })).toBe("online");
    expect(senderStatus({ state: "connecting", presence: "idle" })).toBe("connecting");
    expect(senderStatus({ state: "offline", presence: null })).toBe("offline");
    expect(senderStatus({ state: "disabled", presence: null })).toBe("disabled");
    expect(senderStatus({ state: "revoked", presence: null })).toBe("revoked");
  });

  it("labels Idle and orders it between Online and Busy", () => {
    expect(STATUS_LABEL.idle).toBe("Idle");
    expect(STATUS_ORDER).toEqual(["online", "idle", "busy", "connecting", "offline", "disabled", "revoked"]);
  });

  it("has no Invisible status; an unknown presence from an older server shows as Online", () => {
    expect(Object.values(STATUS_LABEL)).toEqual(["Online", "Idle", "Busy", "Connecting", "Offline", "Revoked", "Disabled"]);
    expect(senderStatus({ state: "online", presence: "invisible" as unknown as Sender["presence"] })).toBe("online");
  });

  it("sorts every status in order and searches by each label", () => {
    const items = [
      sender("r", "revoked"),
      sender("x", "disabled"),
      sender("f", "offline"),
      sender("c", "connecting"),
      sender("b", "online", "dnd"),
      sender("i", "online", "idle"),
      sender("o", "online", "online"),
    ];
    expect(sortSenders(items, { by: "status", dir: "asc" }).map((s) => s.id)).toEqual(["o", "i", "b", "c", "f", "x", "r"]);
    expect(sortSenders(items, { by: "status", dir: "desc" }).map((s) => s.id)).toEqual(["r", "x", "f", "c", "b", "i", "o"]);
    expect(filterSenders(items, "online").map((s) => s.id)).toEqual(["o"]);
    expect(filterSenders(items, "busy").map((s) => s.id)).toEqual(["b"]);
    expect(filterSenders(items, "invisible")).toEqual([]);
  });

  it("sorts and searches an idle account by its status", () => {
    const items = [
      sender("r", "revoked"),
      sender("b", "online", "dnd"),
      sender("i", "online", "idle"),
      sender("o", "online", "online"),
    ];
    expect(sortSenders(items, { by: "status", dir: "asc" }).map((s) => s.id)).toEqual(["o", "i", "b", "r"]);
    expect(filterSenders(items, "IDLE").map((s) => s.id)).toEqual(["i"]);
  });
});

describe("avatarSources", () => {
  const CDN = "https://cdn.discordapp.com";

  it("tries a real avatar, then the default image of its Discord user id", () => {
    // (175928847299117063 >> 22) % 6 == 2
    const real = `${CDN}/avatars/175928847299117063/a_abc.gif?size=64`;
    expect(avatarSources(real, "t1")).toEqual([real, `${CDN}/embed/avatars/2.png`]);
    expect(defaultAvatarIndex(real, "t1")).toBe(2);
  });

  it("an API default is its own only source", () => {
    expect(avatarSources(`${CDN}/embed/avatars/5.png`, "t1")).toEqual([`${CDN}/embed/avatars/5.png`]);
  });

  it("no URL, or one without a user id, uses a stable hash of the row id", () => {
    // "d".charCodeAt(0) = 100, 100 % 6 = 4
    expect(avatarSources(null, "d")).toEqual([`${CDN}/embed/avatars/4.png`]);
    expect(avatarSources("https://example.com/me.png", "d")).toEqual(["https://example.com/me.png", `${CDN}/embed/avatars/4.png`]);
  });
});
