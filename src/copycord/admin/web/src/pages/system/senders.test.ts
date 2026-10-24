import { describe, expect, it } from "vitest";
import type { Sender } from "../../types/system";
import { STATUS_LABEL, STATUS_ORDER, filterSenders, senderStatus, sortSenders } from "./senders";

function sender(id: string, state: Sender["state"], presence: Sender["presence"] = null): Sender {
  return { id, username: `relay.${id}`, display_name: null, avatar_url: null, state, presence, since: null, last_used: null, use_count: 0 };
}

describe("senderStatus", () => {
  it("maps an online session by its presence and anything else by its state", () => {
    expect(senderStatus({ state: "online", presence: "online" })).toBe("online");
    expect(senderStatus({ state: "online", presence: "idle" })).toBe("idle");
    expect(senderStatus({ state: "online", presence: "dnd" })).toBe("busy");
    expect(senderStatus({ state: "online", presence: "invisible" })).toBe("invisible");
    expect(senderStatus({ state: "online", presence: null })).toBe("online");
    expect(senderStatus({ state: "connecting", presence: "idle" })).toBe("connecting");
    expect(senderStatus({ state: "offline", presence: null })).toBe("offline");
    expect(senderStatus({ state: "disabled", presence: null })).toBe("disabled");
    expect(senderStatus({ state: "revoked", presence: null })).toBe("revoked");
  });

  it("labels Idle and orders it between Online and Busy", () => {
    expect(STATUS_LABEL.idle).toBe("Idle");
    expect(STATUS_ORDER).toEqual(["online", "idle", "busy", "invisible", "connecting", "offline", "disabled", "revoked"]);
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
