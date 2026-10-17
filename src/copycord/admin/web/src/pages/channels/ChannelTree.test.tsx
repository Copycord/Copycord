import { render } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { ChannelTree } from "./ChannelTree";
import type { TreeChannel, TreeSection } from "./model";

const chan = (p: Partial<TreeChannel> & { id: string }): TreeChannel => ({
  row: null,
  orphan: false,
  originalName: p.id,
  displayName: p.id,
  customized: false,
  title: p.id,
  type: 0,
  typeLabel: "Text",
  clonedId: "",
  ...p,
});

const section = (p: Partial<TreeSection> & { key: string; channels: TreeChannel[] }): TreeSection => ({
  originalName: p.key,
  displayName: p.key,
  customized: false,
  tooltip: "",
  orphanCategory: false,
  orphanCategoryId: null,
  uncategorized: false,
  ...p,
});

const LONG = "release-announcements-and-community-updates-archive";

function tree() {
  return render(
    <ChannelTree
      tree={{
        empty: false,
        sections: [
          section({ key: "Info", channels: [chan({ id: "401", clonedId: "501" })] }),
          section({ key: "Community", displayName: "Chat Zone", customized: true, channels: [chan({ id: "402", clonedId: "502" })] }),
          section({
            key: "Old Archive",
            orphanCategory: true,
            orphanCategoryId: "910",
            channels: [chan({ id: "911", orphan: true })],
          }),
          section({ key: "Support", channels: [chan({ id: "403", displayName: LONG, title: LONG })] }),
        ],
      }}
      channelActions={() => <button type="button">⋯</button>}
    />,
  );
}

const head = (key: string) => document.querySelector(`[data-cat-name="${key}"]`)!;
const ids = (cid: string) => document.querySelector(`article[data-cid="${cid}"]`)!.lastElementChild!;

describe("ChannelTree", () => {
  it("names a customized or orphan category with the card badge and a tooltip, never a bare dot", () => {
    tree();
    expect(head("Info").querySelector("[data-cat-badge]")).toBeNull();
    const custom = head("Community").querySelector("[data-cat-badge]")!;
    expect(custom).toHaveTextContent("Custom");
    expect(custom).toHaveAttribute("title", "Category renamed in the clone");
    const orphan = head("Old Archive").querySelector("[data-cat-badge]")!;
    expect(orphan).toHaveTextContent("Orphan");
    expect(orphan.getAttribute("title")).toMatch(/no longer in the host server/);
    expect(document.querySelector('[role="img"][aria-label$="category"]')).toBeNull();
  });

  it("keeps the arrow and dashes the side a channel does not have", () => {
    tree();
    expect(ids("401")).toHaveTextContent(/^401\s*→ 501$/);
    expect(ids("401").querySelector("[data-id-dash]")).toBeNull();
    // Not cloned: host id, arrow, dash.
    expect(ids("403")).toHaveTextContent(/^403\s*→ —$/);
    expect(ids("403").lastElementChild!.querySelector("[data-id-dash]")).toHaveAttribute("title", "Not cloned yet");
    // Orphan: dash, arrow, its clone id.
    expect(ids("911")).toHaveTextContent(/^—\s*→ 911$/);
    expect(ids("911").firstElementChild!.querySelector("[data-id-dash]")).not.toBeNull();
  });

  it("truncates a long name with its full name as title, and keeps the badge and menu from shrinking", () => {
    tree();
    const card = document.querySelector('article[data-cid="403"]')!;
    const name = card.querySelector("bdi")!;
    expect(name).toHaveAttribute("title", LONG);
    expect(name.className).toMatch(/\bmin-w-0\b/);
    expect(name.className).toMatch(/\btruncate\b/);
    expect(card.querySelector("[data-badge]")!.parentElement!.className).toMatch(/\bshrink-0\b/);
    expect(card.querySelector("button")!.parentElement!.className).toMatch(/\bshrink-0\b/);
  });
});
