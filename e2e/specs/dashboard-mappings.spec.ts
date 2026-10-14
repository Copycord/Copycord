// Inventory item 18: Cloning Configuration mapping cards.
// The seeded mapping drives list and search;
// toggle and delete use a stubbed list so the seeded mapping stays intact for the other specs.
import { expect, test, type Page, type Request } from "@playwright/test";
import { pageUrl } from "../lib/env.ts";
import { SEED } from "../lib/seed.ts";
import { settle } from "../lib/ui.ts";

const LIST = "#guild-mapping-list";
const CONFIRM = "[role=dialog], #confirm-modal";

const MAPPINGS = [
  { mapping_id: "e2e-a", mapping_name: "Stub Alpha", original_guild_id: "111", original_guild_name: "Host A", cloned_guild_id: "222", cloned_guild_name: "Clone A", status: "active" },
  { mapping_id: "e2e-b", mapping_name: "Stub Beta", original_guild_id: "333", original_guild_name: "Host B", cloned_guild_id: "444", cloned_guild_name: "Clone B", status: "paused" },
];

async function openDashboard(page: Page): Promise<void> {
  const errors: string[] = [];
  page.on("pageerror", (e) => errors.push(e.message));
  await page.goto(pageUrl("dashboard"));
  await settle(page);
  expect(errors, "dashboard page errors").toEqual([]);
}

/** Serve MAPPINGS on GET, flip status on toggle and drop the row on DELETE; collect the unsafe requests. */
async function stubMappings(page: Page): Promise<Request[]> {
  const rows = MAPPINGS.map((m) => ({ ...m }));
  const sent: Request[] = [];
  await page.route("**/api/guild-mappings**", async (route) => {
    const req = route.request();
    const url = new URL(req.url());
    const id = url.pathname.split("/")[3];
    if (req.method() === "GET") return route.fulfill({ json: { ok: true, mappings: rows } });
    sent.push(req);
    const i = rows.findIndex((r) => r.mapping_id === id);
    const row = rows[i];
    if (req.method() === "DELETE") {
      if (i >= 0) rows.splice(i, 1);
      return route.fulfill({ json: { ok: true } });
    }
    if (url.pathname.endsWith("/toggle-status") && row) {
      row.status = row.status === "paused" ? "active" : "paused";
      return route.fulfill({ json: { ok: true, status: row.status } });
    }
    return route.continue();
  });
  return sent;
}

test.describe("18 mapping cards", () => {
  test("seeded mapping card is listed with its host, search filters and Escape clears", async ({ page }) => {
    await openDashboard(page);
    const list = page.locator(LIST);
    await expect(list).toContainText(SEED.mappingName);
    await expect(list.locator(".guild-card-avatar").first()).toBeAttached();
    const search = page.locator("#mappingSearchInput");
    await search.fill("zzz-no-match");
    await expect(list.getByText(SEED.mappingName)).toBeHidden();
    await search.fill(SEED.guildId);
    await expect(list).toContainText(SEED.mappingName);
    await search.fill("zzz-no-match");
    await search.press("Escape");
    await expect(search).toHaveValue("");
    await expect(list).toContainText(SEED.mappingName);
  });

  test("card has Settings, Filters, status, Clone and Delete buttons and an Add tile", async ({ page }) => {
    await openDashboard(page);
    const card = page.locator(`${LIST} .guild-card[data-id]`).first();
    for (const sel of [".edit-mapping-btn", ".mapping-filters-btn", ".mapping-status-btn", ".clone-mapping-btn", ".delete-mapping-btn"]) {
      await expect(card.locator(sel), sel).toBeVisible();
    }
    await expect(page.locator("#new-mapping-card")).toBeVisible();
  });

  test("status toggle posts toggle-status and flips the card between paused and active", async ({ page }) => {
    const sent = await stubMappings(page);
    await openDashboard(page);
    const alpha = page.locator(`${LIST} .guild-card[data-id="e2e-a"]`);
    await expect(alpha).toHaveAttribute("data-status", "active");
    await alpha.locator(".mapping-status-btn").click();
    await expect(alpha).toHaveAttribute("data-status", "paused");
    await expect(alpha).toHaveClass(/is-paused/);
    await alpha.locator(".mapping-status-btn").click();
    await expect(alpha).toHaveAttribute("data-status", "active");
    expect(sent.map((r) => `${r.method()} ${new URL(r.url()).pathname}`)).toEqual([
      "POST /api/guild-mappings/e2e-a/toggle-status",
      "POST /api/guild-mappings/e2e-a/toggle-status",
    ]);
    expect(await sent[0]?.headerValue("x-copycord-csrf")).toBeTruthy();
  });

  test("delete asks first, cancel sends nothing, confirm sends DELETE and removes the card", async ({ page }) => {
    const sent = await stubMappings(page);
    await openDashboard(page);
    const beta = page.locator(`${LIST} .guild-card[data-id="e2e-b"]`);
    await beta.locator(".delete-mapping-btn").click();
    const dialog = page.locator(CONFIRM).filter({ hasText: "Stub Beta" });
    await expect(dialog).toBeVisible();
    await page.keyboard.press("Escape");
    await expect(dialog).toBeHidden();
    expect(sent).toHaveLength(0);
    await beta.locator(".delete-mapping-btn").click();
    await page.locator(CONFIRM).getByRole("button", { name: "Delete mapping" }).click();
    await expect(beta).toHaveCount(0);
    expect(sent.map((r) => `${r.method()} ${new URL(r.url()).pathname}`)).toEqual(["DELETE /api/guild-mappings/e2e-b"]);
    await expect(page.locator(`${LIST} .guild-card[data-id="e2e-a"]`)).toBeVisible();
  });
});
