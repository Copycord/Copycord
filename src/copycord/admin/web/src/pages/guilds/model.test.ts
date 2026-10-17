import { describe, expect, it } from "vitest";
import type { GuildSummary } from "../../types/guilds";
import {
  DEFAULT_EXPORT_FORM,
  attachmentSummary,
  buildExportPayload,
  formatMembers,
  localToIso,
  parseCount,
  placeMenu,
  sortLabel,
  visibleGuilds,
} from "./model";

const g = (id: string, name: string): GuildSummary => ({ id, name, icon_url: null, member_count: null });
const GUILDS = [g("1", "beta"), g("2", "Alpha"), g("3", "gamma ray"), g("4", "Delta")];
const names = (list: GuildSummary[]) => list.map((x) => x.name);

describe("visibleGuilds", () => {
  it("sorts by name ignoring case, A to Z and Z to A", () => {
    expect(names(visibleGuilds(GUILDS, "", "asc"))).toEqual(["Alpha", "beta", "Delta", "gamma ray"]);
    expect(names(visibleGuilds(GUILDS, "", "desc"))).toEqual(["gamma ray", "Delta", "beta", "Alpha"]);
  });

  it("filters by a case-insensitive substring of the name and ignores outer spaces", () => {
    expect(names(visibleGuilds(GUILDS, "ALP", "asc"))).toEqual(["Alpha"]);
    expect(names(visibleGuilds(GUILDS, "  ta ", "asc"))).toEqual(["beta", "Delta"]);
    expect(names(visibleGuilds(GUILDS, "a r", "asc"))).toEqual(["gamma ray"]);
    expect(visibleGuilds(GUILDS, "zzz", "asc")).toEqual([]);
  });

  it("does not reorder the input list", () => {
    const input = [...GUILDS];
    visibleGuilds(input, "", "desc");
    expect(input).toEqual(GUILDS);
  });

  it("labels the sort button like the old page", () => {
    expect(sortLabel("asc")).toBe("A → Z");
    expect(sortLabel("desc")).toBe("Z → A");
  });
});

describe("formatMembers", () => {
  it("groups thousands and picks singular or plural", () => {
    expect(formatMembers(1)).toBe("1 member");
    expect(formatMembers(12345)).toBe("12,345 members");
    expect(formatMembers(0)).toBe("0 members");
    expect(formatMembers(null)).toBe("Member count unknown");
  });
});

describe("buildExportPayload", () => {
  it("sends the old dialog defaults", () => {
    expect(buildExportPayload("42", DEFAULT_EXPORT_FORM)).toEqual({
      guild_id: "42",
      channel_id: null,
      user_id: null,
      webhook_url: null,
      has_attachments: false,
      after_iso: null,
      before_iso: null,
      filters: {
        has_content: true,
        embeds: true,
        attachments: true,
        att_types: { images: true, videos: true, audio: true, other: true },
        links: true,
        emojis: true,
        word_on: false,
        word: "",
        replies: true,
        bots: true,
        system: false,
        min_length: 0,
        min_reactions: 0,
        pinned: true,
        stickers: true,
        mentions: true,
        threads: true,
        forum_threads: true,
        private_threads: true,
        download_media: { images: false, videos: false, audio: false, other: false },
      },
    });
  });

  it("trims ids, webhook and word, and turns blanks into null", () => {
    const p = buildExportPayload("1", {
      ...DEFAULT_EXPORT_FORM,
      channelId: " 123 ",
      userId: "   ",
      webhookUrl: " https://discord.com/api/webhooks/1/abc ",
      wordOn: true,
      word: "  hello ",
    });
    expect(p.channel_id).toBe("123");
    expect(p.user_id).toBeNull();
    expect(p.webhook_url).toBe("https://discord.com/api/webhooks/1/abc");
    expect(p.filters.word_on).toBe(true);
    expect(p.filters.word).toBe("hello");
  });

  it("sends no attachment types when attachments are off", () => {
    const p = buildExportPayload("1", {
      ...DEFAULT_EXPORT_FORM,
      attachments: false,
      attTypes: { images: true, videos: false, audio: true, other: true },
    });
    expect(p.filters.attachments).toBe(false);
    expect(p.filters.att_types).toEqual({ images: false, videos: false, audio: false, other: false });
  });

  it("keeps the picked attachment types and media downloads", () => {
    const p = buildExportPayload("1", {
      ...DEFAULT_EXPORT_FORM,
      attTypes: { images: true, videos: false, audio: true, other: false },
      download: { images: true, videos: true, audio: false, other: false },
    });
    expect(p.filters.att_types).toEqual({ images: true, videos: false, audio: true, other: false });
    expect(p.filters.download_media).toEqual({ images: true, videos: true, audio: false, other: false });
  });

  it("applies the threads switch to forum and private threads too", () => {
    const f = buildExportPayload("1", { ...DEFAULT_EXPORT_FORM, threads: false }).filters;
    expect([f.threads, f.forum_threads, f.private_threads]).toEqual([false, false, false]);
  });

  it("clamps the minimum counts and converts the date range to UTC", () => {
    const p = buildExportPayload("1", {
      ...DEFAULT_EXPORT_FORM,
      minLength: "-5",
      minReactions: "7",
      after: "2026-01-02T03:04:05",
      before: "nonsense",
    });
    expect(p.filters.min_length).toBe(0);
    expect(p.filters.min_reactions).toBe(7);
    expect(p.after_iso).toBe(new Date("2026-01-02T03:04:05").toISOString());
    expect(p.before_iso).toBeNull();
  });
});

describe("helpers", () => {
  it("parseCount reads whole numbers and falls back to 0", () => {
    expect(parseCount("")).toBe(0);
    expect(parseCount("12.9")).toBe(12);
    expect(parseCount("abc")).toBe(0);
    expect(parseCount("-1")).toBe(0);
  });

  it("localToIso ignores blanks", () => {
    expect(localToIso("  ")).toBeNull();
  });

  it("attachmentSummary names the picked types only when some are off", () => {
    expect(attachmentSummary(DEFAULT_EXPORT_FORM)).toBe("");
    const some = { ...DEFAULT_EXPORT_FORM, attTypes: { images: true, videos: false, audio: true, other: false } };
    expect(attachmentSummary(some)).toBe("Images, Audio");
    const none = { ...DEFAULT_EXPORT_FORM, attTypes: { images: false, videos: false, audio: false, other: false } };
    expect(attachmentSummary(none)).toBe("No types selected");
    expect(attachmentSummary({ ...some, attachments: false })).toBe("");
  });
});

describe("placeMenu", () => {
  const vp = { width: 1000, height: 800 };
  const size = { width: 200, height: 100 };

  it("opens 8px below and right of the click", () => {
    expect(placeMenu({ x: 300, y: 300 }, size, vp)).toEqual({ x: 308, y: 308 });
  });

  it("stays inside the right and bottom edges", () => {
    expect(placeMenu({ x: 990, y: 790 }, size, vp)).toEqual({ x: 792, y: 692 });
  });

  it("never goes left of the edge or above the header", () => {
    expect(placeMenu({ x: -50, y: 0 }, size, vp)).toEqual({ x: 8, y: 68 });
    expect(placeMenu({ x: 0, y: 0 }, size, vp, 0)).toEqual({ x: 8, y: 8 });
  });
});
