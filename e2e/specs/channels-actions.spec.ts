// Inventory items 31 and 32: channel and category row actions and Delete all orphans.
// The save routes are stubbed so the seeded database stays untouched.
import { expect, test, type Page } from "@playwright/test";
import { pageUrl } from "../lib/env.ts";
import { settle } from "../lib/ui.ts";
import { CHANNELS, channelCards, stubChannels } from "./channels-fixtures.ts";

const OG = "900000000000000001";
const CG = "900000000000000002";
const ORIG_CAT: Record<string, string> = { Information: "700000000000000001", Community: "700000000000000002", Support: "700000000000000003" };
const CLONE_CAT: Record<string, string> = { Information: "800000000000000001", Community: "800000000000000002" };

const WELCOME = "444444444444444401";
const WELCOME_CLONE = "555555555555555501";
const RULES = "444444444444444403";

type Sent = { url: string; body: Record<string, unknown> };

/** Replace the stubbed list with rows that carry guild and category ids, and stub the action routes. */
async function stubActions(page: Page): Promise<Sent[]> {
  await stubChannels(page);
  await page.route(
    (url) => url.pathname === "/api/channels",
    (r) =>
      r.fulfill({
        json: {
          items: CHANNELS.map((c) => ({
            ...c,
            original_guild_id: OG,
            cloned_guild_id: CG,
            original_category_id: ORIG_CAT[c.original_category_name] ?? "",
            cloned_category_id: CLONE_CAT[c.original_category_name] ?? "",
          })),
        },
      }),
  );
  const sent: Sent[] = [];
  for (const path of ["/api/channels/customize", "/api/categories/customize", "/api/filters/blacklist"]) {
    await page.route(
      (url) => url.pathname === path,
      (r) => {
        sent.push({ url: new URL(r.request().url()).pathname, body: r.request().postDataJSON() as Record<string, unknown> });
        return r.fulfill({ json: { ok: true } });
      },
    );
  }
  return sent;
}

async function openChannels(page: Page): Promise<void> {
  const errors: string[] = [];
  page.on("pageerror", (e) => errors.push(e.message));
  await page.goto(pageUrl("channels"));
  await settle(page);
  await expect(channelCards(page)).toHaveCount(CHANNELS.length);
  expect(errors, "channels page errors").toEqual([]);
}

const card = (page: Page, cid: string) => page.locator(`#channels-root .ch-card[data-cid="${cid}"]`);
const menu = (page: Page) => page.locator("#ch-menu");
const item = (page: Page, name: string) => menu(page).getByRole("menuitem", { name, exact: true });

async function channelMenu(page: Page, cid: string): Promise<void> {
  await card(page, cid).getByRole("button", { name: "Channel menu" }).click();
  await expect(menu(page)).toBeVisible();
}

async function categoryMenu(page: Page, name: string): Promise<void> {
  await page.locator(`#channels-root [data-cat-name="${name}"]`).getByRole("button", { name: "Category menu" }).click();
  await expect(menu(page)).toBeVisible();
}

const confirm = (page: Page, name: string) => page.locator("[role=dialog], #confirm-modal").getByRole("button", { name, exact: true });

test.describe("channels actions", () => {
  test.beforeEach(async ({ context }) => {
    await context.grantPermissions(["clipboard-read", "clipboard-write"]);
  });

  test("31 customize channel is offered for cloned channels and posts the trimmed name", async ({ page }) => {
    const sent = await stubActions(page);
    await openChannels(page);

    await channelMenu(page, RULES);
    await expect(item(page, "Customize")).toBeHidden();
    await page.keyboard.press("Escape");

    await channelMenu(page, WELCOME);
    await item(page, "Customize").click();
    await page.locator("#customize-name").fill("  front door ");
    await page.locator("#customize-save").click();
    await expect.poll(() => sent.length).toBe(1);
    expect(sent[0]).toEqual({
      url: "/api/channels/customize",
      body: { original_channel_id: WELCOME, cloned_guild_id: CG, clone_channel_name: "front door" },
    });
  });

  test("31 customize channel with an empty name clears the customization", async ({ page }) => {
    const sent = await stubActions(page);
    await openChannels(page);
    await channelMenu(page, WELCOME);
    await item(page, "Customize").click();
    await page.locator("#customize-name").fill("");
    await page.locator("#customize-save").click();
    await expect.poll(() => sent.length).toBe(1);
    expect(sent[0]!.body.clone_channel_name).toBeNull();
  });

  test("31 customize category posts the original category id", async ({ page }) => {
    const sent = await stubActions(page);
    await openChannels(page);
    await categoryMenu(page, "Information");
    await item(page, "Customize category").click();
    await page.locator("#customize-cat-name").fill("Start here");
    await page.locator("#customize-cat-save").click();
    await expect.poll(() => sent.length).toBe(1);
    expect(sent[0]).toEqual({
      url: "/api/categories/customize",
      body: { original_category_id: ORIG_CAT.Information, cloned_guild_id: CG, custom_category_name: "Start here" },
    });
  });

  test("31 blacklist a channel asks first, then posts scope channel", async ({ page }) => {
    const sent = await stubActions(page);
    await openChannels(page);
    await channelMenu(page, WELCOME);
    await item(page, "Add channel to blacklist").click();
    await confirm(page, "Cancel").click();
    expect(sent).toHaveLength(0);

    await channelMenu(page, WELCOME);
    await item(page, "Add channel to blacklist").click();
    await confirm(page, "Add to blacklist").click();
    await expect.poll(() => sent.length).toBe(1);
    const mid = await page.locator("#mapping-select").inputValue();
    expect(sent[0]).toEqual({
      url: "/api/filters/blacklist",
      body: { scope: "channel", obj_id: WELCOME, mapping_id: mid, original_guild_id: OG, cloned_guild_id: CG },
    });
  });

  test("31 blacklist a category posts scope category with its original id", async ({ page }) => {
    const sent = await stubActions(page);
    await openChannels(page);
    await categoryMenu(page, "Information");
    await item(page, "Add category to blacklist").click();
    await confirm(page, "Add to blacklist").click();
    await expect.poll(() => sent.length).toBe(1);
    expect(sent[0]!.body).toMatchObject({ scope: "category", obj_id: ORIG_CAT.Information, original_guild_id: OG, cloned_guild_id: CG });
  });

  test("31 copy original and clone ids for channels and categories", async ({ page }) => {
    await stubActions(page);
    await openChannels(page);
    const clip = () => page.evaluate(() => navigator.clipboard.readText());

    await channelMenu(page, WELCOME);
    await item(page, "Copy original channel ID").click();
    await expect.poll(clip).toBe(WELCOME);

    await channelMenu(page, WELCOME);
    await item(page, "Copy clone channel ID").click();
    await expect.poll(clip).toBe(WELCOME_CLONE);

    await categoryMenu(page, "Information");
    await item(page, "Copy original category ID").click();
    await expect.poll(clip).toBe(ORIG_CAT.Information);

    await categoryMenu(page, "Information");
    await item(page, "Copy clone category ID").click();
    await expect.poll(clip).toBe(CLONE_CAT.Information);
  });

  test("32 Delete all orphans stays disabled until a scan finds orphans", async ({ page }) => {
    await stubActions(page);
    await openChannels(page);
    await expect(page.locator("#orph-delall")).toBeDisabled();
    await expect(page.locator("#orph-delall")).toHaveText("Delete 0 orphans");
  });
});
