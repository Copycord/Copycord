// Inventory items 19 and 20: mapping modal #mapping-modal / #mapping-form and the message features
// sub-modal #msg-features-modal.
// The mapping list and every write are stubbed so the seeded mapping stays
// intact and no Discord check runs.
import { expect, test, type Locator, type Page, type Request } from "@playwright/test";
import { pageUrl } from "../lib/env.ts";
import { settle } from "../lib/ui.ts";

const MODAL = "#mapping-modal.show";
const FEATURES = "#msg-features-modal.show";
const CONFIRM = "[role=dialog], #confirm-modal";

const ROWS = [
  {
    mapping_id: "e2e-a",
    mapping_name: "Stub Alpha",
    original_guild_id: "111",
    original_guild_name: "Host A",
    cloned_guild_id: "222",
    cloned_guild_name: "Clone A",
    status: "active",
    settings: { CLONE_EMOJI: false, ANONYMIZE_USERS: true, APPEND_AUTHOR: true },
  },
];

interface Sent {
  req: Request;
  body: { mapping_name: string; original_guild_id: string; cloned_guild_id: string; settings: Record<string, unknown> };
}

/** Serve ROWS on GET and record POST/PATCH bodies; `fail` makes writes answer 400 with that error. */
async function stubWrites(page: Page, fail?: string): Promise<Sent[]> {
  const sent: Sent[] = [];
  await page.route("**/api/guild-mappings**", async (route) => {
    const req = route.request();
    if (req.method() === "GET") return route.fulfill({ json: { ok: true, mappings: ROWS } });
    if (req.method() !== "POST" && req.method() !== "PATCH") return route.continue();
    sent.push({ req, body: req.postDataJSON() });
    if (fail) return route.fulfill({ status: 400, json: { ok: false, error: fail } });
    return route.fulfill({ json: { ok: true, mapping_id: "e2e-new" } });
  });
  return sent;
}

async function openDashboard(page: Page): Promise<void> {
  const errors: string[] = [];
  page.on("pageerror", (e) => errors.push(e.message));
  await page.goto(pageUrl("dashboard"));
  await settle(page);
  await expect(page.locator("#guild-mapping-list .guild-card[data-id]").first()).toBeVisible();
  expect(errors, "dashboard page errors").toEqual([]);
}

const card = (page: Page) => page.locator('#guild-mapping-list .guild-card[data-id="e2e-a"]');

/** Old UI: a hidden checkbox in a label.toggle; SPA: a role=switch button. */
async function isOn(row: Locator): Promise<boolean> {
  return row.locator("input[type=checkbox], [role=switch]").first().evaluate((el) => {
    const input = el as HTMLInputElement;
    return input.type === "checkbox" ? input.checked : input.getAttribute("aria-checked") === "true";
  });
}
const flip = (row: Locator) => row.locator("[role=switch], .toggle-track").first().click();

/** Setting rows sit in collapsed sections; searching for the key opens the one holding it. */
async function setting(page: Page, key: string): Promise<Locator> {
  await page.locator("#mappingSettingsSearch").fill(key.toLowerCase().replace(/_/g, " "));
  const row = page.locator(`${MODAL} .mapping-setting-field[data-setting-key="${key.toLowerCase()}"]`);
  await expect(row).toBeVisible();
  return row;
}

const feature = (page: Page, key: string) => page.locator(`${FEATURES} [data-key="${key}"]`);

test.describe("19 mapping modal", () => {
  test("Add opens an empty create modal, an empty Save sends nothing and Cancel stays hidden", async ({ page }) => {
    const sent = await stubWrites(page);
    await openDashboard(page);
    await page.locator("#new-mapping-card").click();
    await expect(page.locator(MODAL)).toBeVisible();
    await expect(page.locator("#map_mapping_name")).toHaveValue("");
    await expect(page.locator("#map_original_guild_id")).toHaveValue("");
    await expect(page.locator("#map_cloned_guild_id")).toHaveValue("");
    await expect(page.locator("#map_original_guild_id")).not.toHaveAttribute("readonly", /.*/);
    await expect(page.locator("#mapping-cancel-btn")).toBeHidden();
    await page.locator("#mapping-save-btn").click();
    await expect(page.locator(MODAL)).toBeVisible();
    expect(sent).toHaveLength(0);
  });

  test("create posts the name, both guild IDs and every setting with the CSRF header, then closes", async ({ page }) => {
    const sent = await stubWrites(page);
    await openDashboard(page);
    await page.locator("#new-mapping-card").click();
    await page.locator("#map_mapping_name").fill("  Fresh  ");
    await page.locator("#map_original_guild_id").fill("333");
    await page.locator("#map_cloned_guild_id").fill("444");
    const emoji = await setting(page, "CLONE_EMOJI");
    expect(await isOn(emoji)).toBe(true);
    await flip(emoji);
    expect(await isOn(emoji)).toBe(false);
    await page.locator("#mapping-save-btn").click();
    await expect(page.locator(MODAL)).toHaveCount(0);
    expect(sent).toHaveLength(1);
    const { req, body } = sent[0]!;
    expect(`${req.method()} ${new URL(req.url()).pathname}`).toBe("POST /api/guild-mappings");
    expect(await req.headerValue("x-copycord-csrf")).toBeTruthy();
    expect(body).toMatchObject({ mapping_name: "Fresh", original_guild_id: "333", cloned_guild_id: "444" });
    expect(body.settings.CLONE_EMOJI).toBe(false);
    expect(body.settings.CLONE_MESSAGES).toBe(true);
    expect(body.settings.ANONYMIZE_USERS).toBe(false);
  });

  test("Settings opens the mapping with read-only guild IDs and saves with PATCH", async ({ page }) => {
    const sent = await stubWrites(page);
    await openDashboard(page);
    await card(page).locator(".edit-mapping-btn").click();
    await expect(page.locator(MODAL)).toBeVisible();
    await expect(page.locator("#mapping-id-subtle")).toHaveText("e2e-a");
    await expect(page.locator("#map_mapping_name")).toHaveValue("Stub Alpha");
    await expect(page.locator("#map_original_guild_id")).toHaveValue("111");
    await expect(page.locator("#map_cloned_guild_id")).toHaveValue("222");
    await expect(page.locator("#map_original_guild_id")).toHaveAttribute("readonly", /.*/);
    await expect(page.locator("#map_cloned_guild_id")).toHaveAttribute("readonly", /.*/);
    expect(await isOn(await setting(page, "CLONE_EMOJI"))).toBe(false);
    await page.locator("#map_mapping_name").fill("Renamed");
    await page.locator("#mapping-save-btn").click();
    await expect(page.locator(MODAL)).toHaveCount(0);
    expect(sent).toHaveLength(1);
    const { req, body } = sent[0]!;
    expect(`${req.method()} ${new URL(req.url()).pathname}`).toBe("PATCH /api/guild-mappings/e2e-a");
    expect(body).toMatchObject({ mapping_name: "Renamed", original_guild_id: "111", cloned_guild_id: "222" });
    expect(body.settings.CLONE_EMOJI).toBe(false);
    expect(body.settings.ANONYMIZE_USERS).toBe(true);
  });

  test("Clone starts blank with the settings copied and saves as a new mapping with POST", async ({ page }) => {
    const sent = await stubWrites(page);
    await openDashboard(page);
    await card(page).locator(".clone-mapping-btn").click();
    await expect(page.locator(MODAL)).toBeVisible();
    await expect(page.locator("#map_mapping_name")).toHaveValue("");
    await expect(page.locator("#map_original_guild_id")).toHaveValue("");
    await expect(page.locator("#map_cloned_guild_id")).not.toHaveAttribute("readonly", /.*/);
    await page.locator("#map_mapping_name").fill("Copy");
    await page.locator("#map_original_guild_id").fill("555");
    await page.locator("#map_cloned_guild_id").fill("666");
    await page.locator("#mapping-save-btn").click();
    await expect(page.locator(MODAL)).toHaveCount(0);
    const { req, body } = sent[0]!;
    expect(`${req.method()} ${new URL(req.url()).pathname}`).toBe("POST /api/guild-mappings");
    expect(body.settings.CLONE_EMOJI).toBe(false);
    expect(body.settings.ANONYMIZE_USERS).toBe(true);
  });

  test("a server error keeps the modal open with the entered values", async ({ page }) => {
    const sent = await stubWrites(page, "That host+clone pair already exists.");
    await openDashboard(page);
    await page.locator("#new-mapping-card").click();
    await page.locator("#map_mapping_name").fill("Dup");
    await page.locator("#map_original_guild_id").fill("1");
    await page.locator("#map_cloned_guild_id").fill("2");
    await page.locator("#mapping-save-btn").click();
    await expect.poll(() => sent.length).toBe(1);
    await expect(page.getByText("That host+clone pair already exists.")).toBeVisible();
    await expect(page.locator(MODAL)).toBeVisible();
    await expect(page.locator("#map_mapping_name")).toHaveValue("Dup");
  });

  test("settings search filters the rows and shows a message when nothing matches", async ({ page }) => {
    await stubWrites(page);
    await openDashboard(page);
    await page.locator("#new-mapping-card").click();
    const search = page.locator("#mappingSettingsSearch");
    await search.fill("emoji");
    await expect(page.locator(`${MODAL} .mapping-setting-field[data-setting-key="clone_emoji"]`)).toBeVisible();
    await expect(page.locator(`${MODAL} .mapping-setting-field[data-setting-key="clone_roles"]`)).toBeHidden();
    await search.fill("zzz-no-match");
    await expect(page.locator("#mapping-no-results")).toBeVisible();
    await search.fill("");
    await expect(page.locator("#mapping-no-results")).toBeHidden();
  });

  test("an edit shows Cancel, Cancel reverts it, and closing a dirty form asks before discarding", async ({ page }) => {
    await stubWrites(page);
    await openDashboard(page);
    await card(page).locator(".edit-mapping-btn").click();
    const name = page.locator("#map_mapping_name");
    await name.fill("Changed");
    await expect(page.locator("#mapping-cancel-btn")).toBeVisible();
    await page.locator("#mapping-cancel-btn").click();
    await expect(name).toHaveValue("Stub Alpha");
    await expect(page.locator("#mapping-cancel-btn")).toBeHidden();

    await name.fill("Changed again");
    await page.keyboard.press("Escape");
    const dialog = page.locator(CONFIRM).filter({ hasText: "Discard unsaved mapping changes?" });
    await expect(dialog).toBeVisible();
    await dialog.getByRole("button", { name: "Cancel" }).click();
    await expect(page.locator(MODAL)).toBeVisible();
    await expect(name).toHaveValue("Changed again");

    await page.locator("#mapping-close").click();
    await page.locator(CONFIRM).getByRole("button", { name: "Discard changes" }).click();
    await expect(page.locator(MODAL)).toHaveCount(0);
  });

  test("a clean modal closes on Escape and on the X without asking", async ({ page }) => {
    await stubWrites(page);
    await openDashboard(page);
    await card(page).locator(".edit-mapping-btn").click();
    await expect(page.locator(MODAL)).toBeVisible();
    await page.keyboard.press("Escape");
    await expect(page.locator(MODAL)).toHaveCount(0);
    await page.locator("#new-mapping-card").click();
    await expect(page.locator(MODAL)).toBeVisible();
    await page.locator("#mapping-close").click();
    await expect(page.locator(MODAL)).toHaveCount(0);
  });
});

test.describe("20 message features modal", () => {
  test("opens with the mapping's options, the preview follows the toggles and Escape closes only the sub-modal", async ({ page }) => {
    await stubWrites(page);
    await openDashboard(page);
    await card(page).locator(".edit-mapping-btn").click();
    await page.locator("#msg-features-open").click();
    await expect(page.locator(FEATURES)).toBeVisible();
    expect(await isOn(feature(page, "ANONYMIZE_USERS"))).toBe(true);
    expect(await isOn(feature(page, "APPEND_AUTHOR"))).toBe(true);
    expect(await isOn(feature(page, "TAG_REPLY_MSG"))).toBe(false);
    await expect(page.locator(`${FEATURES} .msg-preview-username`)).toHaveText("SwiftFox123");
    await expect(page.locator("#msg-features-preview-footer")).toContainText("SwiftFox123");
    await expect(page.locator("#msg-features-preview-reply")).toHaveText("");

    await flip(feature(page, "TAG_REPLY_MSG"));
    await expect(page.locator("#msg-features-preview-reply")).toContainText("In reply to");
    await flip(feature(page, "ANONYMIZE_USERS"));
    await expect(page.locator(`${FEATURES} .msg-preview-username`)).toHaveText("Copycord");

    await page.keyboard.press("Escape");
    await expect(page.locator(FEATURES)).toHaveCount(0);
    await expect(page.locator(MODAL)).toBeVisible();
    // The sub-modal toggles are the form fields: the mapping is dirty now.
    await expect(page.locator("#mapping-cancel-btn")).toBeVisible();
  });

  test("the toggled options are saved with the mapping", async ({ page }) => {
    const sent = await stubWrites(page);
    await openDashboard(page);
    await card(page).locator(".edit-mapping-btn").click();
    await page.locator("#msg-features-open").click();
    await flip(feature(page, "DISABLE_EVERYONE_MENTIONS"));
    await flip(feature(page, "APPEND_TIMESTAMP"));
    await flip(feature(page, "ANONYMIZE_USERS"));
    await page.locator("#msg-features-close").click();
    await expect(page.locator(FEATURES)).toHaveCount(0);
    await page.locator("#mapping-save-btn").click();
    await expect(page.locator(MODAL)).toHaveCount(0);
    const settings = sent[0]!.body.settings;
    expect(settings).toMatchObject({
      TAG_REPLY_MSG: false,
      ANONYMIZE_USERS: false,
      DISABLE_EVERYONE_MENTIONS: true,
      DISABLE_ROLE_MENTIONS: false,
      APPEND_TIMESTAMP: true,
      APPEND_AUTHOR: true,
    });
  });

  test("the sub-modal reflects what the form holds when it is reopened", async ({ page }) => {
    await stubWrites(page);
    await openDashboard(page);
    await page.locator("#new-mapping-card").click();
    await page.locator("#msg-features-open").click();
    await flip(feature(page, "APPEND_AUTHOR"));
    await page.locator("#msg-features-close").click();
    await page.locator("#msg-features-open").click();
    expect(await isOn(feature(page, "APPEND_AUTHOR"))).toBe(true);
    await expect(page.locator("#msg-features-preview-footer")).toContainText("Copycord");
  });
});
