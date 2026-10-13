// Guilds page stubs shared by parity.spec.ts and baseline.spec.ts. The page waits for both
// Server and Client to be on Discord and lists guilds from the DB, so /api/status and the guild
// routes are stubbed. The same selectors work on the old page and on the SPA.
import { expect, type Locator, type Page } from "@playwright/test";

export const GUILDS = [
  { id: "333333333333333301", name: "Copycord Lounge", icon_url: null, member_count: 1234 },
  { id: "333333333333333302", name: "Art & Design", icon_url: null, member_count: 87 },
  { id: "333333333333333303", name: "beta testers", icon_url: null, member_count: 1 },
  { id: "333333333333333304", name: "Gaming Hub", icon_url: null, member_count: 5021 },
  { id: "333333333333333305", name: "Zeta Testing Grounds With A Rather Long Name", icon_url: null, member_count: 12 },
];

export const FIRST = GUILDS[1]!; // "Art & Design" sorts first A to Z
export const LAST = GUILDS[4]!; // "Zeta ..." sorts first Z to A

export const DETAILS = {
  ...FIRST,
  owner_id: "123456789012345678",
  created_at: "2021-03-04T05:06:07+00:00",
  description: "Seeded for the e2e suite.",
};

export async function stubGuilds(page: Page): Promise<void> {
  const side = { ready: true, status: "ready" };
  await page.route("**/api/status", (r) => r.fulfill({ json: { server: side, client: side } }));
  await page.route("**/api/guilds", (r) => r.fulfill({ json: { items: GUILDS } }));
  await page.route("**/api/guilds/*", (r) => {
    const id = decodeURIComponent(new URL(r.request().url()).pathname.split("/").pop() ?? "");
    const g = GUILDS.find((x) => x.id === id);
    return g
      ? r.fulfill({ json: { ok: true, item: id === DETAILS.id ? DETAILS : { ...g, owner_id: null, created_at: null, description: null } } })
      : r.fulfill({ status: 404, json: { ok: false, error: "not-found" } });
  });
}

export function guildCard(page: Page, id: string): Locator {
  return page.locator(`#guilds-root [data-gid="${id}"]`);
}

/** Click a card at a fixed spot (the menu opens where the click was) and pick a menu item. */
export async function guildAction(page: Page, id: string, item?: "View details" | "Export messages"): Promise<void> {
  await guildCard(page, id).click({ position: { x: 40, y: 40 } });
  const menuItem = page.getByRole("menuitem", { name: "View details" });
  await expect(menuItem).toBeVisible();
  if (item) await page.getByRole("menuitem", { name: item }).click();
}

export function dialog(page: Page, name: "Guild details" | "Export messages"): Locator {
  return page.getByRole("dialog", { name });
}
