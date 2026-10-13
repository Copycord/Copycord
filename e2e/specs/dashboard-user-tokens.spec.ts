// Inventory item 21: the Token Message Cloning block of the mapping modal (#user-tokens-section):
// the USE_USER_TOKENS options saved with the mapping, and the per mapping token manager (add, bulk
// add, search, enable, remove, verify, delete all). Runs against the old UI by default;
// E2E_BASE_PATH=/app runs the same checks against the SPA. The mapping list and every token
// endpoint are stubbed with fake tokens, so no Discord check runs and no real data is touched.
import { expect, test, type Locator, type Page, type Request } from "@playwright/test";
import { pageUrl } from "../lib/env.ts";
import { settle } from "../lib/ui.ts";

const MODAL = "#mapping-modal.show";
const SECTION = "#user-tokens-section";
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
    settings: { USE_USER_TOKENS: false, USER_TOKEN_STRATEGY: "round_robin" },
  },
];

interface Tok {
  id: string;
  username: string;
  user_id: string;
  enabled: boolean;
  token_masked: string;
}

const FAKE_ADD = "FAKE.single.token-aaaaaaaaaaaaaaaa";
const FAKE_BULK_OK = "FAKE.bulk.ok-bbbbbbbbbbbbbbbbbbb";
const FAKE_BULK_BAD = "FAKE.bulk.bad-cccccccccccccccccc";

interface Stub {
  tokens: Tok[];
  calls: string[];
  bodies: Array<Record<string, unknown>>;
  saves: Array<{ settings: Record<string, unknown> }>;
}

const tok = (id: string, username: string, masked: string, enabled = true): Tok => ({
  id,
  username,
  user_id: `u-${id}`,
  enabled,
  token_masked: masked,
});

/** Serve the mapping list, record mapping saves and answer every token endpoint from `stub.tokens`. */
async function stubAll(page: Page, tokens: Tok[]): Promise<Stub> {
  const stub: Stub = { tokens: tokens.map((t) => ({ ...t })), calls: [], bodies: [], saves: [] };
  await page.route("**/api/guild-mappings**", async (route) => {
    const req: Request = route.request();
    const path = new URL(req.url()).pathname;
    const method = req.method();
    const m = path.match(/\/api\/guild-mappings\/e2e-a\/user-tokens(\/[^/]+)?$/);
    if (!m) {
      if (method === "GET") return route.fulfill({ json: { ok: true, mappings: ROWS } });
      if (method === "POST" || method === "PATCH") {
        stub.saves.push(req.postDataJSON());
        return route.fulfill({ json: { ok: true, mapping_id: "e2e-new" } });
      }
      return route.continue();
    }
    const tail = m[1] ?? "";
    stub.calls.push(`${method} ${tail || "/"}`);
    const body = method === "GET" || method === "DELETE" ? {} : (req.postDataJSON() ?? {});
    if (method !== "GET") stub.bodies.push(body);
    if (method === "GET" && !tail) return route.fulfill({ json: { ok: true, tokens: stub.tokens } });
    if (method === "POST" && !tail) {
      if (body.token === "FAKE.rejected") {
        return route.fulfill({ status: 400, json: { ok: false, error: "That token is invalid or the account is not a member." } });
      }
      stub.tokens.push(tok(`t${stub.tokens.length + 10}`, "newbie", "FAKE.s…aaaa"));
      return route.fulfill({ json: { ok: true, token: stub.tokens.at(-1) } });
    }
    if (method === "POST" && tail === "/bulk") {
      stub.tokens.push(tok("t90", "bulkuser", "FAKE.b…bbbb"));
      return route.fulfill({
        json: {
          ok: true,
          added: 1,
          total: 2,
          results: [
            { masked: "FAKE.b…bbbb", status: "added", username: "bulkuser" },
            { masked: "FAKE.b…cccc", status: "invalid" },
          ],
        },
      });
    }
    if (method === "POST" && tail === "/verify") {
      return route.fulfill({
        json: {
          ok: true,
          total: stub.tokens.length,
          working: 1,
          broken: 1,
          results: [
            { id: stub.tokens[0]!.id, masked: stub.tokens[0]!.token_masked, username: stub.tokens[0]!.username, ok: true },
            { id: stub.tokens[1]!.id, masked: stub.tokens[1]!.token_masked, username: "", ok: false },
          ],
        },
      });
    }
    if (method === "POST" && tail === "/delete") {
      const ids = body.ids as string[] | undefined;
      const before = stub.tokens.length;
      stub.tokens = body.scope === "all" ? [] : stub.tokens.filter((t) => !ids?.includes(t.id));
      return route.fulfill({ json: { ok: true, deleted: before - stub.tokens.length } });
    }
    const id = tail.slice(1);
    if (method === "PATCH") {
      const t = stub.tokens.find((x) => x.id === id)!;
      t.enabled = Boolean(body.enabled);
      return route.fulfill({ json: { ok: true, token: t } });
    }
    if (method === "DELETE") {
      stub.tokens = stub.tokens.filter((x) => x.id !== id);
      return route.fulfill({ json: { ok: true } });
    }
    return route.fulfill({ status: 404, json: { ok: false } });
  });
  return stub;
}

async function openEdit(page: Page): Promise<void> {
  const errors: string[] = [];
  page.on("pageerror", (e) => errors.push(e.message));
  await page.goto(pageUrl("dashboard"));
  await settle(page);
  await page.locator('#guild-mapping-list .guild-card[data-id="e2e-a"] .edit-mapping-btn').click();
  await expect(page.locator(MODAL)).toBeVisible();
  await page.locator(`${SECTION} > summary`).click();
  expect(errors, "page errors").toEqual([]);
}

async function openCreate(page: Page): Promise<void> {
  await page.goto(pageUrl("dashboard"));
  await settle(page);
  await page.locator("#new-mapping-card").click();
  await expect(page.locator(MODAL)).toBeVisible();
  await page.locator(`${SECTION} > summary`).click();
}

/** Old UI: a hidden checkbox in a label.toggle; SPA: a role=switch button. */
async function isOn(row: Locator): Promise<boolean> {
  return row.locator("input[type=checkbox], [role=switch]").first().evaluate((el) => {
    const input = el as HTMLInputElement;
    return input.type === "checkbox" ? input.checked : input.getAttribute("aria-checked") === "true";
  });
}
const flip = (row: Locator) => row.locator("[role=switch], .toggle-track").first().click();
const optionRow = (page: Page, key: string) => page.locator(`${SECTION} [data-setting-key="${key}"]`);

/** Set the strategy select the same way for the old custom dropdown and the SPA select. */
const pickStrategy = (page: Page, value: string) =>
  page.locator("#ut_strategy").evaluate((el, v) => {
    const sel = el as HTMLSelectElement;
    sel.value = v;
    sel.dispatchEvent(new Event("change", { bubbles: true }));
  }, value);

/** Click a confirm button: the old UI may use a native dialog (auto accepted), the SPA a dialog. */
async function confirmWith(page: Page, name: string): Promise<void> {
  const btn = page.locator(CONFIRM).getByRole("button", { name, exact: true });
  await btn.click({ timeout: 4000 });
}

const rows = (page: Page) => page.locator(`${SECTION} .user-token-row`);

test.describe("21 user tokens block", () => {
  test("a new mapping asks to save first and has no token manager", async ({ page }) => {
    await stubAll(page, []);
    await openCreate(page);
    await expect(page.locator("#user-tokens-save-first")).toBeVisible();
    await expect(page.locator("#user-token-add")).toBeHidden();
    await expect(page.locator("#user_token_value")).toBeHidden();
    await expect(page.locator("#user-token-sticky-options")).toBeHidden();
  });

  test("options show only for the sticky strategy and save as USER_TOKEN_* settings", async ({ page }) => {
    const stub = await stubAll(page, []);
    await openCreate(page);
    expect(await isOn(optionRow(page, "use_user_tokens"))).toBe(false);
    expect(await isOn(optionRow(page, "user_token_fallback_webhook"))).toBe(true);
    await expect(page.locator("#ut_strategy_desc")).toContainText("Cycles through your accounts");
    await expect(page.locator("#user-token-sticky-options")).toBeHidden();
    await pickStrategy(page, "sticky_author");
    await expect(page.locator("#user-token-sticky-options")).toBeVisible();
    await expect(page.locator("#ut_strategy_desc")).toContainText("same account");
    await flip(optionRow(page, "use_user_tokens"));
    await flip(optionRow(page, "user_token_sticky_nickname"));
    await flip(optionRow(page, "user_token_sticky_roles"));
    await flip(optionRow(page, "user_token_fallback_webhook"));
    await flip(optionRow(page, "user_token_typing"));
    await flip(optionRow(page, "user_token_links_only"));
    await flip(optionRow(page, "user_token_use_proxies"));
    await page.locator("#ut_min_delay").fill("1.5");
    await page.locator("#ut_max_delay").fill("4");
    await page.locator("#map_mapping_name").fill("Fresh");
    await page.locator("#map_original_guild_id").fill("333");
    await page.locator("#map_cloned_guild_id").fill("444");
    await page.locator("#mapping-save-btn").click();
    await expect(page.locator(MODAL)).toHaveCount(0);
    expect(stub.saves).toHaveLength(1);
    expect(stub.saves[0]!.settings).toMatchObject({
      USE_USER_TOKENS: true,
      USER_TOKEN_STRATEGY: "sticky_author",
      USER_TOKEN_STICKY_NICKNAME: true,
      USER_TOKEN_STICKY_AVATAR: false,
      USER_TOKEN_STICKY_ROLES: true,
      USER_TOKEN_FALLBACK_WEBHOOK: false,
      USER_TOKEN_TYPING: true,
      USER_TOKEN_LINKS_ONLY: true,
      USER_TOKEN_USE_PROXIES: true,
      USER_TOKEN_MIN_DELAY: 1.5,
      USER_TOKEN_MAX_DELAY: 4,
    });
  });

  test("an edited mapping lists masked tokens, counts and searches them", async ({ page }) => {
    await stubAll(page, [tok("t1", "alice", "FAKE.a…1111"), tok("t2", "bob", "FAKE.b…2222", false)]);
    await openEdit(page);
    await expect(rows(page)).toHaveCount(2);
    await expect(page.locator("#user-tokens-count")).toHaveText("2 tokens");
    await expect(rows(page).first()).toContainText("FAKE.a…1111 · alice");
    expect(await isOn(rows(page).nth(1))).toBe(false);
    await expect(page.locator("#user_token_value")).toHaveAttribute("type", "password");
    await page.locator("#user_tokens_search").fill("bob");
    await expect(rows(page).filter({ visible: true })).toHaveCount(1);
    await page.locator("#user_tokens_search").fill("zzz");
    await expect(rows(page).filter({ visible: true })).toHaveCount(0);
    await expect(page.locator("#user-tokens-no-match")).toBeVisible();
  });

  test("add posts the token, shows the result and never prints the token back", async ({ page }) => {
    const stub = await stubAll(page, [tok("t1", "alice", "FAKE.a…1111")]);
    await openEdit(page);
    await page.locator("#user-token-add").click();
    await expect(page.locator("#user-token-status")).toHaveText("Enter a token.");
    await page.locator("#user_token_value").fill("FAKE.rejected");
    await page.locator("#user-token-add").click();
    await expect(page.locator("#user-token-status")).toContainText("invalid or the account is not a member");
    await expect(rows(page)).toHaveCount(1);
    await page.locator("#user_token_value").fill(FAKE_ADD);
    await page.locator("#user-token-add").click();
    await expect(rows(page)).toHaveCount(2);
    await expect(page.locator("#user-token-status")).toHaveText("Token added.");
    await expect(page.locator("#user_token_value")).toHaveValue("");
    expect(stub.bodies.filter((b) => b.token === FAKE_ADD)).toHaveLength(1);
    await expect(page.locator(SECTION)).not.toContainText(FAKE_ADD);
  });

  test("bulk add posts the pasted lines and lists each result", async ({ page }) => {
    const stub = await stubAll(page, [tok("t1", "alice", "FAKE.a…1111")]);
    await openEdit(page);
    await page.locator("#user-token-bulk-open").click();
    const bulk = page.locator("#user-token-bulk-modal.show");
    await expect(bulk).toBeVisible();
    await page.locator("#user-token-bulk-submit").click();
    await expect(page.locator("#user-token-bulk-status")).toHaveText("Paste at least one token.");
    await page.locator("#user_token_bulk_input").fill(`${FAKE_BULK_OK}\n${FAKE_BULK_BAD}`);
    await page.locator("#user-token-bulk-submit").click();
    await expect(page.locator("#user-token-bulk-status")).toHaveText("Added 1 of 2, 1 skipped.");
    const results = page.locator("#user-token-bulk-results .user-token-bulk-row");
    await expect(results).toHaveCount(2);
    await expect(results.first()).toContainText("Added");
    await expect(results.nth(1)).toContainText("Invalid / not in server");
    expect(stub.bodies.some((b) => typeof b.tokens === "string" && b.tokens.includes(FAKE_BULK_OK))).toBe(true);
    await expect(page.locator(`${SECTION} .user-token-row`)).toHaveCount(2);
    await page.locator("#user-token-bulk-close").click();
    await expect(bulk).toHaveCount(0);
  });

  test("enable switch patches and Remove deletes after a confirm", async ({ page }) => {
    page.on("dialog", (d) => void d.accept());
    const stub = await stubAll(page, [tok("t1", "alice", "FAKE.a…1111"), tok("t2", "bob", "FAKE.b…2222")]);
    await openEdit(page);
    await flip(rows(page).first());
    await expect.poll(() => stub.calls.includes("PATCH /t1")).toBe(true);
    expect(stub.bodies.find((b) => "enabled" in b)).toEqual({ enabled: false });
    await rows(page).nth(1).locator(".user-token-remove").click();
    const hasDialog = await page.locator(CONFIRM).getByRole("button", { name: "Remove", exact: true }).isVisible().catch(() => false);
    if (hasDialog) await confirmWith(page, "Remove");
    await expect.poll(() => stub.calls.includes("DELETE /t2")).toBe(true);
    await expect(rows(page)).toHaveCount(1);
  });

  test("verify offers to delete only the non-working tokens", async ({ page }) => {
    const stub = await stubAll(page, [tok("t1", "alice", "FAKE.a…1111"), tok("t2", "bob", "FAKE.b…2222")]);
    await openEdit(page);
    await page.locator("#user-token-settings-open").click();
    await page.locator("#user-token-verify").click();
    const dialog = page.locator(CONFIRM).filter({ hasText: "Token verification" });
    await expect(dialog).toContainText("1 working");
    await expect(dialog).toContainText("1 not working");
    await expect(dialog.locator(".user-token-bulk-row").first()).toContainText("Not working");
    await confirmWith(page, "Delete 1 non-working");
    await expect.poll(() => stub.calls.includes("POST /delete")).toBe(true);
    expect(stub.bodies.find((b) => "ids" in b)).toEqual({ ids: ["t2"] });
    await expect(rows(page)).toHaveCount(1);
  });

  test("Delete all tokens asks first and posts scope all", async ({ page }) => {
    const stub = await stubAll(page, [tok("t1", "alice", "FAKE.a…1111"), tok("t2", "bob", "FAKE.b…2222")]);
    await openEdit(page);
    await page.locator("#user-token-settings-open").click();
    await page.locator("#user-token-delete-all").click();
    const dialog = page.locator(CONFIRM).filter({ hasText: "Delete all tokens?" });
    await expect(dialog).toBeVisible();
    await dialog.getByRole("button", { name: "Cancel" }).click();
    expect(stub.calls).not.toContain("POST /delete");
    await page.locator("#user-token-settings-open").click();
    await page.locator("#user-token-delete-all").click();
    await confirmWith(page, "Delete all");
    await expect.poll(() => stub.calls.includes("POST /delete")).toBe(true);
    expect(stub.bodies.find((b) => "scope" in b)).toEqual({ scope: "all" });
    await expect(rows(page)).toHaveCount(0);
    await expect(page.locator(`${SECTION} #userTokensList`)).toContainText("No user tokens yet");
  });
});
