// Channels page stubs shared by parity.spec.ts and baseline.spec.ts. The page waits for both
// Server and Client to be on Discord, so /api/status is stubbed, and the tree comes from a
// stubbed GET /api/channels. The mapping select uses the seeded mapping from the real DB.
// The same selectors work on the old page and on the SPA.
import type { Locator, Page } from "@playwright/test";

function row(id: string, name: string, cat: string, type: number, cloned = "", custom = "", origCat = cat) {
  return {
    original_channel_id: id,
    original_channel_name: name,
    cloned_channel_id: cloned,
    clone_channel_name: custom,
    channel_type: type,
    category_name: cat,
    original_category_name: origCat,
    cloned_category_name: cat,
  };
}

export const CHANNELS = [
  row("444444444444444401", "welcome", "Information", 0, "555555555555555501"),
  row("444444444444444402", "announcements", "Information", 5, "555555555555555502"),
  row("444444444444444403", "rules", "Information", 0),
  row("444444444444444404", "general", "Chat Zone", 0, "555555555555555504", "", "Community"),
  row("444444444444444405", "memes", "Chat Zone", 0, "555555555555555505", "funny-stuff", "Community"),
  row("444444444444444406", "Lounge", "Chat Zone", 2, "555555555555555506", "", "Community"),
  row("444444444444444407", "forum-help", "Support", 15),
  row("444444444444444408", "stage-talks", "Support", 13, "555555555555555508"),
  row("444444444444444409", "café", "Uncategorized", 0, "555555555555555509", "", ""),
];

/** A not cloned channel whose name is wider than a card: the name ellipsizes, the badge and the menu stay put.
 *  Kept out of CHANNELS so the specs' card counts hold; review shots add it. */
export const LONG_CHANNEL = row("444444444444444410", "release-announcements-and-community-updates-archive", "Support", 0);

/** Category chips in API order, as both pages show them before any category sort. */
export const CATEGORIES = ["Information", "Chat Zone", "Support", "Uncategorized"];

export async function stubChannels(page: Page, seen: string[] = []): Promise<void> {
  const side = { ready: true, status: "ready" };
  await page.route("**/api/status", (r) => r.fulfill({ json: { server: side, client: side } }));
  await page.route(
    (url) => url.pathname === "/api/channels",
    (r) => {
      seen.push(r.request().url());
      return r.fulfill({ json: { items: CHANNELS } });
    },
  );
}

export const channelCards = (page: Page): Locator => page.locator("#channels-root .ch-card[data-cid]");
export const categoryChips = (page: Page): Locator => page.locator("#channels-root .ch-section h3");
