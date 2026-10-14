// Parity for the Global Configuration card, token validation, backup tokens and the link cards
// (inventory items 15, 16, 17).
// The save and backup token endpoints are stubbed so the seeded install is never changed.
import { expect, test, type Page } from "@playwright/test";
import { pageUrl } from "../lib/env.ts";
import { settle } from "../lib/ui.ts";

async function open(page: Page): Promise<void> {
  const errors: string[] = [];
  page.on("pageerror", (e) => errors.push(e.message));
  const r = await page.goto(pageUrl("dashboard"));
  expect(r?.status()).toBeLessThan(400);
  await settle(page);
  expect(errors, "page errors").toEqual([]);
}

const SAVE = "#cfg-save-btn";
const CANCEL = "#cfg-cancel-btn";
const toast = (page: Page, text: string | RegExp) => expect(page.getByText(text).first()).toBeVisible();

test.describe("dashboard config", () => {
  test("15 form shows token fields with show and hide, Save and Cancel only while dirty", async ({ page }) => {
    await open(page);
    await expect(page.locator("#cfg-form")).toBeAttached();
    for (const id of ["SERVER_TOKEN", "CLIENT_TOKEN"]) await expect(page.locator(`#${id}`)).toHaveAttribute("type", "password");
    await expect(page.locator(SAVE)).toBeHidden();
    await expect(page.locator(CANCEL)).toBeHidden();

    await page.locator('.reveal-btn[data-target="SERVER_TOKEN"], button[data-target="SERVER_TOKEN"]').first().click();
    await expect(page.locator("#SERVER_TOKEN")).toHaveAttribute("type", "text");
    await page.locator('button[data-target="SERVER_TOKEN"]').first().click();
    await expect(page.locator("#SERVER_TOKEN")).toHaveAttribute("type", "password");

    await page.locator("#LOG_MAX_SIZE_MB").fill("42");
    await expect(page.locator(SAVE)).toBeVisible();
    await expect(page.locator(CANCEL)).toBeVisible();
    const before = await page.locator("#LOG_MAX_SIZE_MB").inputValue();
    await page.locator(CANCEL).click();
    await expect(page.locator("#LOG_MAX_SIZE_MB")).not.toHaveValue(before);
    await expect(page.locator(SAVE)).toBeHidden();
  });

  test("15 command users chips add on Enter, remove on Backspace and feed COMMAND_USERS", async ({ page }) => {
    await open(page);
    const entry = page.locator("#cmd_users_input");
    await entry.fill("123456789012345678");
    await entry.press("Enter");
    await entry.fill("987654321098765432");
    await entry.press("Enter");
    await expect(page.locator("#COMMAND_USERS")).toHaveValue(/123456789012345678,987654321098765432$/);
    await expect(page.locator(SAVE)).toBeVisible();
    await entry.press("Backspace");
    await expect(page.locator("#COMMAND_USERS")).not.toHaveValue(/987654321098765432/);
    await expect(page.locator("#COMMAND_USERS")).toHaveValue(/123456789012345678$/);
  });

  test("15 Save posts the form to /save and reports success", async ({ page }) => {
    const bodies: string[] = [];
    await page.route("**/save", (route) => {
      if (route.request().method() !== "POST") return route.continue();
      bodies.push(route.request().postData() ?? "");
      return route.fulfill({ status: 200, body: "ok" });
    });
    await open(page);
    await page.locator("#LOG_MAX_SIZE_MB").fill("42");
    await page.locator("#cmd_users_input").fill("555");
    await page.locator("#cmd_users_input").press("Enter");
    await page.locator(SAVE).click();
    await expect.poll(() => bodies.length).toBe(1);
    for (const key of ["SERVER_TOKEN", "CLIENT_TOKEN", "COMMAND_USERS", "COPYCORD_AUTOSTART", "LOG_LEVEL", "LOG_MAX_SIZE_MB"]) {
      expect(bodies[0], key).toContain(`name="${key}"`);
    }
    expect(bodies[0]).toMatch(/name="LOG_MAX_SIZE_MB"\r?\n\r?\n42\r?\n/);
    expect(bodies[0]).toMatch(/555/);
    await toast(page, "Configuration saved.");
    await expect(page.locator(SAVE)).toBeHidden();
  });

  test("15 a rejected save shows the server's message and keeps the form dirty", async ({ page }) => {
    await page.route("**/save", (route) =>
      route.request().method() === "POST" ? route.fulfill({ status: 400, body: "Invalid config: Missing SERVER_TOKEN" }) : route.continue(),
    );
    await open(page);
    await page.locator("#LOG_MAX_SIZE_MB").fill("43");
    await page.locator(SAVE).click();
    await toast(page, /Missing SERVER_TOKEN/);
    await expect(page.locator(SAVE)).toBeVisible();
  });

  test("16 token validation flags bad tokens and turns on the invite link", async ({ page }) => {
    await page.route("**/api/validate-tokens", (route) =>
      route.fulfill({ json: { ok: false, has_tokens: true, errors: ["CLIENT_TOKEN is invalid or expired."], bot_client_id: "777" } }),
    );
    await open(page);
    await toast(page, /CLIENT_TOKEN is invalid or expired/);
    await expect(page.locator("#CLIENT_TOKEN")).toHaveAttribute("aria-invalid", "true");
    await expect(page.locator("#SERVER_TOKEN")).not.toHaveClass(/is-invalid/);
    await expect(page.locator("#bot-invite-btn")).toHaveAttribute("href", /client_id=777&permissions=8/);
  });

  test("16 the invite link stays off without a bot client id", async ({ page }) => {
    await page.route("**/api/validate-tokens", (route) => route.fulfill({ json: { ok: true, has_tokens: true, errors: [] } }));
    await open(page);
    await expect(page.locator("#bot-invite-btn")).not.toHaveAttribute("href", /client_id/);
  });

  test("17 backup tokens modal lists, adds with a note, removes after confirm and closes", async ({ page }) => {
    const tokens = [{ token_id: "t1", masked: "abcd…wxyz", note: "alt account", added_at: 1700000000, last_used: null }];
    const adds: string[] = [];
    const deletes: string[] = [];
    await page.route("**/api/backup-tokens", (route) => route.fulfill({ json: { ok: true, tokens } }));
    await page.route("**/api/backup-tokens/add", (route) => {
      const body = route.request().postData() ?? "";
      adds.push(body);
      if (body.includes("bad-token")) return route.fulfill({ status: 400, json: { detail: "Invalid backup token e2e." } });
      tokens.push({ token_id: "t2", masked: "efgh…stuv", note: "spare", added_at: 1700000100, last_used: null });
      return route.fulfill({ json: { ok: true, token_id: "t2" } });
    });
    await page.route("**/api/backup-tokens/delete", (route) => {
      deletes.push(route.request().postData() ?? "");
      tokens.splice(0, 1);
      return route.fulfill({ json: { ok: true } });
    });
    await open(page);

    const list = page.locator("#backupTokensList");
    await page.locator("#backupTokensBtn").click();
    await expect(list).toBeVisible();
    await expect(list).toContainText("abcd…wxyz");
    await expect(list).toContainText("alt account");
    await expect(list).toContainText("Last used: Never");

    await page.locator("#backup-token-add").click();
    await toast(page, "Paste a token first.");
    expect(adds).toEqual([]);

    await page.locator("#backup_token_value").fill("bad-token");
    await page.locator("#backup-token-add").click();
    await toast(page, "Invalid backup token e2e.");

    await page.locator("#backup_token_value").fill("good-token");
    await page.locator("#backup_token_note").fill("spare");
    await page.locator("#backup-token-add").click();
    await expect(list).toContainText("efgh…stuv");
    await expect(page.locator("#backup_token_value")).toHaveValue("");
    expect(adds[1]).toMatch(/name="token_value"\r?\n\r?\ngood-token/);
    expect(adds[1]).toMatch(/name="note"\r?\n\r?\nspare/);

    await list.locator('[aria-label="Remove backup token"]').first().click();
    await page.getByRole("button", { name: "Remove", exact: true }).filter({ visible: true }).click();
    await expect.poll(() => deletes.length).toBe(1);
    expect(deletes[0]).toMatch(/name="token_id"\r?\n\r?\nt1/);
    await expect(list).not.toContainText("abcd…wxyz");

    await page.keyboard.press("Escape");
    await expect(list).toBeHidden();
  });

  test("17 an empty backup token list says so", async ({ page }) => {
    await page.route("**/api/backup-tokens", (route) => route.fulfill({ json: { ok: true, tokens: [] } }));
    await open(page);
    await page.locator("#backupTokensBtn").click();
    await expect(page.locator("#backupTokensList")).toContainText("No backup tokens saved yet.");
  });

  test("links row shows Star on GitHub, Join the Discord and Donate above the Bots card", async ({ page }) => {
    await open(page);
    const cards = page.locator("a.promo-card");
    await expect(cards).toHaveCount(3);
    await expect(cards.nth(0)).toContainText("Star on GitHub");
    await expect(cards.nth(1)).toContainText("Join the Discord");
    await expect(cards.nth(2)).toContainText("Donate");
    for (let i = 0; i < 3; i++) await expect(cards.nth(i)).toHaveAttribute("target", "_blank");
    const top = (await cards.first().boundingBox())!.y;
    const bots = (await page.getByRole("heading", { name: "Bots", exact: true }).boundingBox())!.y;
    expect(top).toBeLessThan(bots);
  });
});
