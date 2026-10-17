// The mapping Filters modal (#filters-modal / #form-filters) with its category/channel picker
// (#filterObjectsModal) and role picker (#roleBlocksModal).
// Filters, channels and roles are stubbed so the seeded mapping keeps its real (empty) filters for
// the other specs. Chip lists are read from the hidden inputs both UIs keep in sync.
import { expect, test, type Page, type Route } from "@playwright/test";
import { pageUrl } from "../lib/env.ts";
import { MAPPING_FILTERS_BTN, settle } from "../lib/ui.ts";

const MODAL = "#filters-modal.show";
const PICKER = "#filterObjectsModal.show";
const ROLES = "#roleBlocksModal.show";
const CONFIRM = "[role=dialog], #confirm-modal";

const LOADED = {
  wl_categories: ["1001"],
  wl_channels: ["2001", "2002"],
  ex_categories: [],
  ex_channels: ["2003"],
  blocked_words: ["spoiler"],
  channel_name_blacklist: ["ticket-*"],
  blocked_role_ids: ["3002"],
  wl_users: ["4001"],
  bl_users: [],
};

const TREE = {
  ok: true,
  categories: [
    { id: "1001", name: "Text Zone", type: 4, position: 0, parent_id: null },
    { id: "1002", name: "Voice Zone", type: 4, position: 1, parent_id: null },
  ],
  channels: [
    { id: "2001", name: "general", type: 0, position: 0, parent_id: "1001" },
    { id: "2002", name: "random", type: 0, position: 1, parent_id: "1001" },
    { id: "2003", name: "lounge", type: 2, position: 0, parent_id: "1002" },
    { id: "2004", name: "side-thread", type: 11, position: 0, parent_id: "2001", parent_name: "general", is_thread: true },
  ],
};

const ROLE_LIST = {
  ok: true,
  roles: [
    { id: "3001", name: "Admin", position: 3, color: 16711680, color_hex: "#FF0000" },
    { id: "3002", name: "Mod", position: 2, color: 0, color_hex: null },
    { id: "3003", name: "Member", position: 1, color: 0, color_hex: null },
  ],
};

interface Stub {
  saved: Array<Record<string, string>>;
  gets: number;
}

/** Parse a form post, urlencoded (old UI) or multipart (SPA), into name -> value. */
function formFields(body: string, contentType: string): Record<string, string> {
  const out: Record<string, string> = {};
  if (contentType.includes("multipart")) {
    for (const m of body.matchAll(/name="([^"]+)"\r?\n\r?\n([\s\S]*?)\r?\n--/g)) out[m[1]!] = m[2]!;
  } else {
    for (const [k, v] of new URLSearchParams(body)) out[k] = v;
  }
  return out;
}

async function stubFilters(page: Page): Promise<Stub> {
  const stub: Stub = { saved: [], gets: 0 };
  await page.route(/\/filters\/[^/?]+(\/save)?(\?.*)?$/, async (route: Route) => {
    const req = route.request();
    const path = new URL(req.url()).pathname;
    if (path.includes("/api/")) return route.continue();
    if (req.method() === "POST" && path.endsWith("/save")) {
      stub.saved.push(formFields(req.postData() ?? "", (await req.headerValue("content-type")) ?? ""));
      return route.fulfill({ json: { ok: true } });
    }
    stub.gets++;
    return route.fulfill({ json: LOADED });
  });
  return stub;
}

async function stubTree(page: Page, status = 200): Promise<void> {
  await page.route("**/api/mappings/*/channels", (route) =>
    status === 200 ? route.fulfill({ json: TREE }) : route.fulfill({ status, json: { detail: "discord-channels-error" } }),
  );
}

async function stubRoles(page: Page, status = 200): Promise<void> {
  await page.route("**/api/mappings/*/roles", (route) =>
    status === 200 ? route.fulfill({ json: ROLE_LIST }) : route.fulfill({ status, json: { detail: "discord-roles-error" } }),
  );
}

async function openFilters(page: Page): Promise<void> {
  const errors: string[] = [];
  page.on("pageerror", (e) => errors.push(e.message));
  await page.goto(pageUrl("dashboard"));
  await settle(page);
  await page.locator(MAPPING_FILTERS_BTN).first().click();
  await expect(page.locator(MODAL)).toBeVisible();
  await expect.poll(() => page.locator("#wl_channels").inputValue()).toBe("2001,2002");
  expect(errors, "dashboard page errors").toEqual([]);
}

const value = (page: Page, id: string) => page.locator(`#${id}`).inputValue();

async function addChip(page: Page, inputId: string, text: string): Promise<void> {
  const input = page.locator(`#${inputId}`);
  await input.fill(text);
  await input.press("Enter");
}

test.describe("22 filters modal", () => {
  test("loads GET /filters/{id} into every chip list and shows the mapping name", async ({ page }) => {
    const stub = await stubFilters(page);
    await openFilters(page);
    expect(stub.gets).toBeGreaterThan(0);
    await expect(page.locator("#filters-title")).toContainText("E2E Mapping");
    const want: Record<string, string> = {
      wl_categories: "1001",
      wl_channels: "2001,2002",
      ex_categories: "",
      ex_channels: "2003",
      blocked_words: "spoiler",
      channel_name_blacklist: "ticket-*",
      blocked_role_ids: "3002",
      wl_users: "4001",
      bl_users: "",
    };
    for (const [id, v] of Object.entries(want)) expect(await value(page, id), id).toBe(v);
    for (const id of [
      "wl_categories_input", "wl_channels_input", "ex_categories_input", "ex_channels_input", "channel_name_blacklist_input",
      "blocked_role_ids_input", "blocked_words_input", "wl_users_input", "bl_users_input", "filters-save-btn", "filters-role-refresh",
    ]) {
      await expect(page.locator(`#${id}`), id).toBeVisible();
    }
  });

  test("chip inputs add on Enter, keep only IDs for ID lists, drop duplicates, and save posts every list", async ({ page }) => {
    const stub = await stubFilters(page);
    await openFilters(page);
    await addChip(page, "wl_categories_input", "1002 abc 1001");
    await addChip(page, "ex_categories_input", "1001, 1002");
    await addChip(page, "blocked_words_input", "Plot Twist");
    await addChip(page, "blocked_words_input", "spoiler");
    await addChip(page, "channel_name_blacklist_input", "*-log-*");
    await addChip(page, "blocked_role_ids_input", "3001");
    await addChip(page, "bl_users_input", "4002");
    await addChip(page, "wl_users_input", "not-an-id");
    expect(await value(page, "wl_categories")).toBe("1001,1002");
    expect(await value(page, "blocked_words")).toBe("spoiler,Plot Twist");
    expect(await value(page, "wl_users")).toBe("4001");
    await page.locator("#filters-save-btn").click();
    await expect(page.locator(MODAL)).toHaveCount(0);
    expect(stub.saved).toHaveLength(1);
    const f = stub.saved[0]!;
    expect(f.wl_categories).toBe("1001,1002");
    expect(f.wl_channels).toBe("2001,2002");
    expect(f.ex_categories).toBe("1001,1002");
    expect(f.ex_channels).toBe("2003");
    expect(f.blocked_words).toBe("spoiler,Plot Twist");
    expect(f.channel_name_blacklist).toBe("ticket-*,*-log-*");
    expect(f.blocked_role_ids).toBe("3002,3001");
    expect(f.wl_users).toBe("4001");
    expect(f.bl_users).toBe("4002");
  });

  test("an unsaved edit shows Cancel, Cancel reloads, and closing asks before discarding", async ({ page }) => {
    const stub = await stubFilters(page);
    await openFilters(page);
    await expect(page.locator("#btn-cancel-filters")).toBeHidden();
    await addChip(page, "wl_users_input", "4009");
    await expect(page.locator("#btn-cancel-filters")).toBeVisible();
    await page.locator("#btn-cancel-filters").click();
    await expect.poll(() => value(page, "wl_users")).toBe("4001");
    await expect(page.locator("#btn-cancel-filters")).toBeHidden();

    await addChip(page, "wl_users_input", "4009");
    await page.locator("#filters-close").click();
    const dialog = page.locator(CONFIRM).filter({ hasText: "Discard unsaved filter changes?" });
    await expect(dialog).toBeVisible();
    await dialog.getByRole("button", { name: "Cancel" }).click();
    await expect(dialog).toBeHidden();
    await expect(page.locator(MODAL)).toBeVisible();
    expect(await value(page, "wl_users")).toBe("4001,4009");

    await page.locator("#filters-close").click();
    await page.locator(CONFIRM).filter({ hasText: "Discard unsaved filter changes?" }).getByRole("button", { name: "Discard changes" }).click();
    await expect(page.locator(MODAL)).toHaveCount(0);
    expect(stub.saved).toHaveLength(0);
  });

  test("a clean modal closes on X and Escape without asking", async ({ page }) => {
    await stubFilters(page);
    await openFilters(page);
    await page.locator("#filters-close").click();
    await expect(page.locator(MODAL)).toHaveCount(0);
    await page.locator(MAPPING_FILTERS_BTN).first().click();
    await expect(page.locator(MODAL)).toBeVisible();
    await page.keyboard.press("Escape");
    await expect(page.locator(MODAL)).toHaveCount(0);
  });

  test("channel picker lists channels and threads, searches by category, selects all and applies", async ({ page }) => {
    await stubFilters(page);
    await stubTree(page);
    await openFilters(page);
    await page.locator('[data-filter-target="ex_channels"]').click();
    await expect(page.locator(PICKER)).toBeVisible();
    await expect(page.locator("#filterObjectsTitle")).toHaveText("Block Channels & Threads");
    const rows = page.locator("#filterObjectsTableBody tr[data-obj-id]");
    await expect(rows).toHaveCount(4);
    await expect(rows.filter({ hasText: "side-thread" })).toContainText("thread");
    await expect(rows.filter({ hasText: "side-thread" })).toContainText("general");
    await expect(rows.filter({ hasText: "lounge" })).toContainText("Voice Zone");
    await expect(rows.filter({ hasText: "lounge" }).locator("input[type=checkbox]")).toBeChecked();

    await page.locator("#filterObjectsSearch").fill("text zone");
    await expect(rows).toHaveCount(2);
    await page.locator("#filterObjectsSelectAll").check();
    await page.locator("#filterObjectsSearch").fill("");
    await expect(rows).toHaveCount(4);
    await page.locator("#filterObjectsApply").click();
    await expect(page.locator(PICKER)).toHaveCount(0);
    expect(await value(page, "ex_channels")).toBe("2003,2001,2002");
    await expect(page.locator(MODAL)).toBeVisible();
  });

  test("category picker hides the category column, a row click toggles, Cancel keeps the list", async ({ page }) => {
    await stubFilters(page);
    await stubTree(page);
    await openFilters(page);
    await page.locator('[data-filter-target="wl_categories"]').click();
    await expect(page.locator(PICKER)).toBeVisible();
    await expect(page.locator("#filterObjectsTitle")).toHaveText("Allow Categories");
    await expect(page.locator("#filterObjectsCategoryHeader")).toBeHidden();
    const rows = page.locator("#filterObjectsTableBody tr[data-obj-id]");
    await expect(rows).toHaveCount(2);
    await rows.filter({ hasText: "Voice Zone" }).locator("td").nth(1).click();
    await expect(rows.filter({ hasText: "Voice Zone" }).locator("input[type=checkbox]")).toBeChecked();
    await page.locator("#filterObjectsCancel").click();
    await expect(page.locator(PICKER)).toHaveCount(0);
    expect(await value(page, "wl_categories")).toBe("1001");
  });

  test("picker reports a failed Discord fetch and Escape closes only the picker", async ({ page }) => {
    await stubFilters(page);
    await stubTree(page, 502);
    await openFilters(page);
    await page.locator('[data-filter-target="wl_channels"]').click();
    await expect(page.locator("#filterObjectsTableBody")).toContainText("Failed to fetch channels/categories");
    await page.keyboard.press("Escape");
    await expect(page.locator(PICKER)).toHaveCount(0);
    await expect(page.locator(MODAL)).toBeVisible();
  });

  test("role refresh loads roles, marks the blocked one, searches, selects all and applies", async ({ page }) => {
    await stubFilters(page);
    await stubRoles(page);
    await openFilters(page);
    await page.locator("#filters-role-refresh").click();
    await expect(page.locator(ROLES)).toBeVisible();
    const rows = page.locator("#roleBlocksTableBody tr[data-role-id]");
    await expect(rows).toHaveCount(3);
    await expect(rows.filter({ hasText: "Mod" }).locator("input[type=checkbox]")).toBeChecked();
    await expect(rows.filter({ hasText: "Admin" }).locator("input[type=checkbox]")).not.toBeChecked();
    await page.locator("#roleBlocksSearch").fill("admin");
    await expect(rows).toHaveCount(1);
    await page.locator("#roleBlocksSearch").fill("3003");
    await expect(rows).toHaveCount(1);
    await page.locator("#roleBlocksSearch").fill("");
    await page.locator("#roleBlocksSelectAll").check();
    await page.locator("#roleBlocksApply").click();
    await expect(page.locator(ROLES)).toHaveCount(0);
    expect(new Set((await value(page, "blocked_role_ids")).split(","))).toEqual(new Set(["3001", "3002", "3003"]));
  });

  test("role picker Cancel keeps the list and a failed fetch is reported", async ({ page }) => {
    await stubFilters(page);
    await stubRoles(page);
    await openFilters(page);
    await page.locator("#filters-role-refresh").click();
    await page.locator("#roleBlocksTableBody tr[data-role-id]").filter({ hasText: "Admin" }).locator("td").nth(1).click();
    await page.locator("#roleBlocksCancel").click();
    await expect(page.locator(ROLES)).toHaveCount(0);
    expect(await value(page, "blocked_role_ids")).toBe("3002");

    await page.unroute("**/api/mappings/*/roles");
    await stubRoles(page, 502);
    await page.locator("#filters-role-refresh").click();
    await expect(page.locator("#roleBlocksTableBody")).toContainText("Failed to load roles.");
    await expect(page.locator("#filters-role-status")).toContainText("Failed to load roles.");
  });
});
