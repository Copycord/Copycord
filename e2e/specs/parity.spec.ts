// Parity smoke for inventory items reachable without Discord (numbers refer to copycord-frontend-inventory.md).
import { expect, test, type Page } from "@playwright/test";
import { CSRF_HEADERS, LOGIN_PATH, NOT_FOUND_PATH, PAGES, PASSWORD, pageUrl, type PageName } from "../lib/env.ts";
import { SEED, stubClientGuilds } from "../lib/seed.ts";
import { FWD_CREATE_BTN, MAPPING_EDIT_BTN, MAPPING_FILTERS_BTN, closeX, expectClosed, expectOpen, modal, settle } from "../lib/ui.ts";
import { DETAILS, FIRST, GUILDS, LAST, dialog as guildDialog, guildAction, guildCard, stubGuilds } from "./guilds-fixtures.ts";
import { LOG_LINES, stubLogStream, stubRunning, stubVersion } from "./dashboard-fixtures.ts";
import { CHANNELS, categoryChips, channelCards, stubChannels } from "./channels-fixtures.ts";

async function open(page: Page, name: PageName): Promise<void> {
  const errors: string[] = [];
  page.on("pageerror", (e) => errors.push(e.message));
  const r = await page.goto(pageUrl(name));
  expect(r?.status(), `${name} status`).toBeLessThan(400);
  await settle(page);
  expect(errors, `${name} page errors`).toEqual([]);
}

test.describe("shared", () => {
  test("1 sidebar links reach every page", async ({ page }) => {
    // Eight page loads, each allowed up to 15 s of networkidle in settle().
    test.setTimeout(180_000);
    await open(page, "dashboard");
    for (const name of Object.keys(PAGES) as PageName[]) {
      const href = pageUrl(name);
      const link = page.locator(`a[href="${href}"]`).first();
      await expect(link, `nav link ${href}`).toBeAttached();
    }
    for (const name of Object.keys(PAGES) as PageName[]) await open(page, name);
  });

  test("3 mobile drawer opens and Escape closes", async ({ page }) => {
    await page.setViewportSize({ width: 390, height: 844 });
    await open(page, "dashboard");
    const menu = page.locator("#side-menu");
    await page.locator("#menu-toggle").click();
    await expect(menu).toBeInViewport();
    await page.keyboard.press("Escape");
    await expect(menu).not.toBeInViewport();
  });

  test("4 unsafe API calls need the CSRF header", async ({ page }) => {
    await open(page, "dashboard");
    const bad = await page.request.put("/api/sync/settings", { data: {} });
    expect(bad.status()).toBeGreaterThanOrEqual(400);
  });

  test("5 status endpoint answers", async ({ page }) => {
    const r = await page.request.get("/api/status");
    expect(r.ok()).toBeTruthy();
  });

  test("8 version endpoint answers", async ({ page }) => {
    const r = await page.request.get("/version");
    expect(r.ok()).toBeTruthy();
  });
});

test.describe("auth", () => {
  test.use({ storageState: { cookies: [], origins: [] } });

  test("11 logged out pages redirect to login", async ({ page }) => {
    await page.goto(pageUrl("system"));
    await expect(page).toHaveURL(/\/login/);
  });

  test("10 bad password shows an error, good password logs in, 2 logout", async ({ page }) => {
    await page.goto(LOGIN_PATH);
    await page.locator("#password").fill("wrong-password");
    await page.locator("#login-form button[type=submit]").click();
    await expect(page).toHaveURL(/\/login/);
    await page.locator("#password").fill(PASSWORD);
    await page.locator("#login-form button[type=submit]").click();
    await page.waitForURL((u) => !u.pathname.startsWith("/login"));
    await settle(page);
    const out = page.locator("[data-logout]").first();
    if (!(await out.isVisible())) await page.locator("#menu-toggle").click().catch(() => undefined);
    await out.click();
    await expect(page).toHaveURL(/\/login/);
  });
});

test("12 not found page links home", async ({ page }) => {
  await page.goto(NOT_FOUND_PATH);
  await settle(page);
  await expect(page.locator(`a[href="${pageUrl("dashboard")}"]`).first()).toBeAttached();
});

test.describe("dashboard", () => {
  test("8 update notice shows only when /version reports a newer release", async ({ page }) => {
    await stubVersion(page, true);
    await open(page, "dashboard");
    await expect(page.locator("#update-notice")).toBeVisible();
    await expect(page.locator("#update-notice")).toHaveText(/New Update Available/);
    await page.unrouteAll();
    await stubVersion(page, false);
    await open(page, "dashboard");
    await expect(page.locator("#update-notice")).toBeHidden();
  });

  test("13 bots panel shows stopped bots with Start", async ({ page }) => {
    await open(page, "dashboard");
    await expect(page.locator("#toggle-btn")).toHaveText(/^\s*Start\s*$/);
    await expect(page.locator("#toggle-form")).toHaveAttribute("action", /\/start$/);
    for (const role of ["server", "client"]) await expect(page.locator(`#${role}-state`)).toHaveText(/stopped/);
  });

  test("13 running bots show uptime and Stop posts /stop with CSRF", async ({ page }) => {
    await stubRunning(page);
    const posts: Array<{ method: string; csrf: string | null }> = [];
    await page.route("**/stop", (route) => {
      posts.push({ method: route.request().method(), csrf: route.request().headers()["x-copycord-csrf"] ?? null });
      return route.fulfill({ status: 200, body: "ok" });
    });
    await open(page, "dashboard");
    await expect(page.locator("#toggle-btn")).toHaveText(/^\s*Stop\s*$/);
    await expect(page.locator("#toggle-form")).toHaveAttribute("action", /\/stop$/);
    await expect(page.locator("#server-state")).toHaveText(/running/);
    await expect(page.locator("#client-uptime")).toHaveText(/^1h 1m \d+s$/);
    await expect(page.locator("#client-status")).toHaveText("e2e syncing");
    await expect(page.locator("#server-status")).toHaveText("");
    await page.locator("#toggle-btn").click();
    await expect.poll(() => posts.length).toBe(1);
    expect(posts[0]).toEqual({ method: "POST", csrf: "1" });
  });

  test("14 log modal tails the stream, searches, clears with CSRF and closes", async ({ page }) => {
    await stubLogStream(page);
    const clears: Array<string | null> = [];
    await page.route("**/logs/clear/server", (route) => {
      clears.push(route.request().headers()["x-copycord-csrf"] ?? null);
      return route.fulfill({ status: 200, body: "ok" });
    });
    await open(page, "dashboard");
    const shown = page.locator("#log-modal.show");
    const body = page.locator("#log-body");

    await page.locator('[data-log="server"]').click();
    await expect(shown).toHaveCount(1);
    for (const line of LOG_LINES) await expect(body).toContainText(line);

    await page.locator("#log-search-input").fill("BETA");
    await expect(body).toContainText(LOG_LINES[1]);
    await expect(body).not.toContainText(LOG_LINES[0]);
    await page.locator("#log-search-input").fill("");
    await expect(body).toContainText(LOG_LINES[0]);

    await page.locator("#log-clear-btn").click();
    await page.locator('button:has-text("Clear logs"):not(#log-clear-btn)').filter({ visible: true }).click();
    await expect.poll(() => clears).toEqual(["1"]);
    await expect(body).not.toContainText(LOG_LINES[0]);

    await page.locator("#log-close").click();
    await expect(shown).toHaveCount(0);

    await page.locator('[data-log="client"]').click();
    await expect(shown).toHaveCount(1);
    await page.keyboard.press("Escape");
    await expect(shown).toHaveCount(0);
  });

  test("15 config form loads with save and cancel", async ({ page }) => {
    await open(page, "dashboard");
    await expect(page.locator("#cfg-form")).toBeAttached();
    await expect(page.locator("#cfg-save-btn")).toBeAttached();
    await expect(page.locator("#cfg-cancel-btn")).toBeAttached();
  });

  test("17 backup tokens modal opens and closes", async ({ page }) => {
    await open(page, "dashboard");
    const m = modal(page, "backup-token-modal");
    await page.locator("#backupTokensBtn").click();
    await expectOpen(m);
    await page.keyboard.press("Escape");
    await expectClosed(m);
  });

  test("18 seeded mapping card is listed and search filters it", async ({ page }) => {
    await open(page, "dashboard");
    const list = page.locator("#guild-mapping-list");
    await expect(list).toContainText(SEED.mappingName);
    await page.locator("#mappingSearchInput").fill("zzz-no-match");
    await expect(list.getByText(SEED.mappingName)).toBeHidden();
    await page.locator("#mappingSearchInput").fill("");
    await expect(list).toContainText(SEED.mappingName);
  });

  test("19 mapping settings modal opens with the seeded guild IDs and closes", async ({ page }) => {
    await open(page, "dashboard");
    await page.locator(MAPPING_EDIT_BTN).first().click();
    const m = modal(page, "mapping-modal");
    await expectOpen(m);
    // The modal fills inputs through .value, so check the property, not the attribute.
    await expect(m.locator("#map_original_guild_id")).toHaveValue(SEED.guildId);
    await closeX(m).click();
    await expectClosed(m);
  });

  test("22 filters modal opens and closes", async ({ page }) => {
    await open(page, "dashboard");
    await page.locator(MAPPING_FILTERS_BTN).first().click();
    const m = modal(page, "filters-modal");
    await expectOpen(m);
    await page.keyboard.press("Escape");
    await expectClosed(m);
  });

  test("23 24 notification and sync settings round trip", async ({ page }) => {
    for (const url of ["/api/notifications/settings", "/api/sync/settings"]) {
      const r = await page.request.get(url);
      expect(r.ok(), url).toBeTruthy();
      const put = await page.request.put(url, { data: await r.json(), headers: CSRF_HEADERS });
      expect(put.status(), `${url} put`).toBeLessThan(500);
    }
  });

  test("27 proxy settings and favorites help modals open and close", async ({ page }) => {
    await open(page, "dashboard");
    for (const [btn, id] of [["#srv-proxy-settings-btn", "proxy-settings-modal"], ["#srv-proxy-fav-help-btn", "proxy-fav-help-modal"]] as const) {
      const m = modal(page, id);
      await page.locator(btn).click();
      await expectOpen(m);
      await closeX(m).click();
      await expectClosed(m);
    }
  });
});

test.describe("channels", () => {
  test("29 30 mapping select and search controls", async ({ page }) => {
    await open(page, "channels");
    for (const id of ["mapping-select", "ch-search", "ch-filter", "ch-sort"]) {
      await expect(page.locator(`#${id}`), id).toBeAttached();
    }
  });

  // Row actions, orphans and the verify and backfill modals are lanes 23 and 24; their tests land with them.
  test.describe("with channels", () => {
    const seen: string[] = [];
    test.beforeEach(async ({ page }) => {
      seen.length = 0;
      await stubChannels(page, seen);
    });
    const cats = (page: Page) => page.locator("#channels-root [data-cat-name]").evaluateAll((els) => els.map((e) => e.getAttribute("data-cat-name")));

    test("29 mapping select loads the tree of categories and channels for its mapping", async ({ page }) => {
      await open(page, "channels");
      await expect(channelCards(page)).toHaveCount(CHANNELS.length);
      const mid = await page.locator("#mapping-select").inputValue();
      expect(mid).not.toBe("");
      await expect(page.locator("#mapping-select option:checked")).toHaveText(SEED.mappingName);
      expect(seen.some((u) => new URL(u).searchParams.get("mapping_id") === mid)).toBeTruthy();
      expect(await cats(page)).toEqual(["Information", "Community", "Support", "Uncategorized"]);
      await expect(categoryChips(page).nth(1)).toContainText("Chat Zone");
      await expect(channelCards(page).first()).toHaveAttribute("data-cid", "444444444444444402");
      const memes = page.locator('#channels-root .ch-card[data-cid="444444444444444405"]');
      await expect(memes).toContainText("# funny-stuff");
      await expect(memes).toContainText("Custom");
      await expect(memes).toContainText("→ 555555555555555505");
      await expect(page.locator('#channels-root .ch-card[data-cid="444444444444444407"]')).toContainText("Forum");
    });

    test("30 search, filter and sort", async ({ page }) => {
      await open(page, "channels");
      const cards = channelCards(page);
      await expect(cards).toHaveCount(CHANNELS.length);
      const search = page.locator("#ch-search");
      for (const [q, n] of [["chat zone", 3], ["COMMUNITY", 3], ["#cafe", 1], ["funny", 1], ["555555555555555508", 1]] as const) {
        await search.fill(q);
        await expect(cards, q).toHaveCount(n);
      }
      await search.fill("zzz-nothing");
      await expect(cards).toHaveCount(0);
      await expect(page.locator("#channels-empty")).toBeVisible();
      await search.fill("");
      await expect(cards).toHaveCount(CHANNELS.length);
      await expect(page.locator("#channels-empty")).toBeHidden();

      await page.locator("#ch-filter").selectOption("orphans", { force: true });
      await expect(cards).toHaveCount(0);
      await page.locator("#ch-filter").selectOption("nonorphans", { force: true });
      await expect(cards).toHaveCount(CHANNELS.length);

      await page.locator("#ch-sort").selectOption("category", { force: true });
      await expect.poll(() => cats(page)).toEqual(["Community", "Information", "Support", "Uncategorized"]);
      await page.locator("#ch-sort").selectOption("type", { force: true });
      await expect.poll(() => cats(page)).toEqual(["Information", "Community", "Support", "Uncategorized"]);
      await expect(cards.first()).toHaveAttribute("data-cid", "444444444444444403");
    });
  });
});

test.describe("guilds", () => {
  test("37 search and sort controls, guild list endpoint", async ({ page }) => {
    await open(page, "guilds");
    await expect(page.locator("#g-search")).toBeAttached();
    await expect(page.locator("#g-sortdir")).toBeAttached();
    expect((await page.request.get("/api/guilds")).ok()).toBeTruthy();
  });

  test.describe("with guilds", () => {
    test.beforeEach(async ({ page }) => stubGuilds(page));

    test("37 guild grid lists, search filters, sort direction flips", async ({ page }) => {
      await open(page, "guilds");
      const cards = page.locator("#guilds-root article[data-gid]");
      await expect(cards).toHaveCount(GUILDS.length);
      await expect(guildCard(page, FIRST.id)).toContainText(FIRST.name);
      await page.locator("#g-search").fill("BETA");
      await expect(cards).toHaveCount(1);
      await page.locator("#g-search").fill("");
      await expect(cards).toHaveCount(GUILDS.length);
      const sort = page.locator("#g-sortdir");
      await expect(sort).toContainText("A → Z");
      await expect(cards.first()).toHaveAttribute("data-gid", FIRST.id);
      await sort.click();
      await expect(sort).toContainText("Z → A");
      await expect(cards.first()).toHaveAttribute("data-gid", LAST.id);
    });

    test("38 view details shows the guild and closes with X and Close", async ({ page }) => {
      await open(page, "guilds");
      const d = guildDialog(page, "Guild details");
      for (const which of ["x", "footer"] as const) {
        await guildAction(page, FIRST.id, "View details");
        await expect(d).toBeVisible();
        await expect(d).toContainText("1,234 members");
        await expect(d).toContainText(DETAILS.owner_id);
        await expect(d).toContainText(DETAILS.description);
        const close = d.getByRole("button", { name: "Close" });
        await (which === "x" ? close.first() : close.last()).click();
        await expect(d).toBeHidden();
      }
    });

    test("39 export dialog has every option and posts the download media choice", async ({ page }) => {
      let body: Record<string, unknown> | undefined;
      await page.route("**/api/export/messages", async (r) => {
        body = r.request().postDataJSON() as Record<string, unknown>;
        await r.fulfill({ json: { ok: true, accepted: true } });
      });
      await open(page, "guilds");
      await guildAction(page, FIRST.id, "Export messages");
      const d = guildDialog(page, "Export messages");
      await expect(d).toBeVisible();
      const ids = [
        "ex-channel", "ex-user", "ex-f-hascontent", "ex-f-embeds", "ex-f-attachments", "ex-f-att-tip", "ex-f-links",
        "ex-f-emojis", "ex-f-word-on", "ex-f-word", "ex-f-replies", "ex-f-bots", "ex-f-system", "ex-f-pinned",
        "ex-f-stickers", "ex-f-mentions", "ex-f-threads", "ex-f-minlen", "ex-f-minreacts", "ex-webhook", "ex-after",
        "ex-before", "ex-dl-images", "ex-dl-videos", "ex-dl-audio", "ex-dl-other",
      ];
      for (const id of ids) await expect(page.locator(`#${id}`), id).toBeAttached();
      await page.locator("#ex-channel").fill("123456789012345678");
      await page.locator("#ex-dlwrap > summary").click();
      await page.locator("#ex-dl-images").check();
      await page.locator("#ex-dl-other").check();
      await d.getByRole("button", { name: "Start export" }).click();
      await expect(d).toBeHidden();
      expect(body?.guild_id).toBe(FIRST.id);
      expect(body?.channel_id).toBe("123456789012345678");
      const filters = body?.filters as { download_media?: unknown; attachments?: unknown } | undefined;
      expect(filters?.attachments).toBe(true);
      expect(filters?.download_media).toEqual({ images: true, videos: false, audio: false, other: true });
    });

    test("39 export dialog closes with Cancel without posting", async ({ page }) => {
      let posted = false;
      await page.route("**/api/export/messages", (r) => {
        posted = true;
        return r.fulfill({ json: { ok: true } });
      });
      await open(page, "guilds");
      await guildAction(page, FIRST.id, "Export messages");
      const d = guildDialog(page, "Export messages");
      await expect(d).toBeVisible();
      await d.getByRole("button", { name: "Cancel" }).click();
      await expect(d).toBeHidden();
      expect(posted).toBe(false);
    });
  });
});

test.describe("forwarding", () => {
  test.beforeEach(async ({ page }) => stubClientGuilds(page));

  test("40 seeded rule is listed", async ({ page }) => {
    await open(page, "forwarding");
    await expect(page.locator("#forwarding-list")).toContainText(SEED.ruleLabel);
  });

  test("41 new rule modal opens and closes", async ({ page }) => {
    await open(page, "forwarding");
    const m = modal(page, "fwdModal");
    await page.locator(FWD_CREATE_BTN).filter({ visible: true }).first().click();
    await expectOpen(m);
    await closeX(m).click();
    await expectClosed(m);
  });

  test("41 modal creates a Discord rule and edit prefills it", async ({ page }) => {
    // Saving only stores the rule; nothing is sent to the made up webhook.
    const label = "E2E Modal Rule";
    await open(page, "forwarding");
    const m = modal(page, "fwdModal");
    await page.locator(FWD_CREATE_BTN).filter({ visible: true }).first().click();
    await expectOpen(m);
    await m.locator("#fwd_label").fill(label);
    await m.locator("#fwd_guild_id").selectOption(SEED.guildId);
    await m.locator("#providerSelectBtn").click();
    await m.locator('#providerSelectMenu [role="option"][data-value="discord"]').click();
    const hook = m.locator('[data-chip-input="discord_webhook_urls"] .chip-text-input');
    await hook.fill(SEED.webhook);
    await hook.press("Enter");
    const saved = page.waitForResponse((r) => r.request().method() === "POST" && new URL(r.url()).pathname === "/api/forwarding");
    await m.locator("#fwdSaveBtn").click();
    const r = await saved;
    expect(r.status()).toBe(200);
    const body = r.request().postDataJSON() as { label: string; provider: string; guild_id: string; config: { urls: string[] } };
    expect(body).toMatchObject({ label, provider: "discord", guild_id: SEED.guildId, config: { urls: [SEED.webhook] } });
    const id = ((await r.json()) as { rule_id: string }).rule_id;
    await expectClosed(m);

    const card = page.locator(".fwd-card", { hasText: label });
    await expect(card).toBeVisible();
    await card.locator(".fwd-edit-btn").click();
    await expectOpen(m);
    await expect(m.locator("#fwdModalTitle")).toHaveText("Edit Forwarding Rule");
    await expect(m.locator("#fwd_label")).toHaveValue(label);
    await expect(m.locator("#fwd_provider")).toHaveValue("discord");
    await expect(m.locator("#fwd_guild_id")).toHaveValue(SEED.guildId);
    await expect(m.locator("#provider_discord")).toContainText(SEED.webhook);
    await m.locator("#fwdCancelBtn").click();
    await expectClosed(m);

    expect((await page.request.delete(`/api/forwarding/${encodeURIComponent(id)}`, { headers: CSRF_HEADERS })).ok()).toBeTruthy();
  });

  test("40 total and per rule counts answer", async ({ page }) => {
    await open(page, "forwarding");
    await expect(page.locator("#fwdTotalCount")).toHaveText(/^\d[\d,.\s]*$/);
    const card = page.locator(".fwd-card", { hasText: SEED.ruleLabel });
    await expect(card.locator("[data-rule-count-id]")).toHaveText(/^\d[\d,.\s]*$/);
    expect((await page.request.get("/api/forwarding/count")).ok()).toBeTruthy();
    expect((await page.request.get("/api/forwarding/count/by-rule")).ok()).toBeTruthy();
  });

  test("42 delete asks first, cancel keeps the rule, confirm deletes it", async ({ page }) => {
    // A throwaway rule, so the seeded one stays for the other tests.
    const label = "E2E Delete Me";
    const made = await page.request.post("/api/forwarding", {
      data: { label, guild_id: SEED.guildId, provider: "discord", enabled: true, config: { urls: [SEED.webhook] }, filters: {} },
      headers: CSRF_HEADERS,
    });
    expect(made.ok()).toBeTruthy();
    const id = ((await made.json()) as { rule_id: string }).rule_id;

    await open(page, "forwarding");
    const card = page.locator(".fwd-card", { hasText: label });
    const dialog = page.getByRole("dialog").filter({ hasText: "Delete forwarding rule" });
    await card.locator(".fwd-delete-btn").click();
    await expect(dialog).toBeVisible();
    await dialog.getByRole("button", { name: "Cancel" }).click();
    await expect(dialog).toBeHidden();
    await expect(card).toBeVisible();

    await card.locator(".fwd-delete-btn").click();
    const reply = page.waitForResponse((r) => r.request().method() === "DELETE" && r.url().includes("/api/forwarding/"));
    await dialog.getByRole("button", { name: "Delete" }).click();
    const r = await reply;
    expect(r.status()).toBe(200);
    expect(r.url()).toContain(`/api/forwarding/${encodeURIComponent(id)}`);
    await expect(card).toHaveCount(0);
    expect(await (await page.request.get("/api/forwarding")).text()).not.toContain(label);
    await expect(page.locator("#forwarding-list")).toContainText(SEED.ruleLabel);
  });
});

test.describe("scraper", () => {
  // Made up values; adding a token is stubbed so no Discord call is made.
  const FAKE_TOKEN = "FAKE.e2e-parity-token.abcdefghijklmnop.WXYZ";
  const FAKE_MASKED = "FAKE.e…WXYZ";

  test("43 tokens add with label, list masked, validate and delete", async ({ page }) => {
    let tokens: unknown[] = [];
    const added: { value: string | null; label: string | null }[] = [];
    let validated = 0;
    let deleted = 0;
    await page.route("**/api/scraper/tokens", (r) => r.fulfill({ json: { ok: true, tokens } }));
    await page.route("**/api/scraper/tokens/add", async (r) => {
      const body = r.request().postData() ?? "";
      added.push({
        value: /name="token_value"\r\n\r\n([^\r]*)/.exec(body)?.[1] ?? null,
        label: /name="label"\r\n\r\n([^\r]*)/.exec(body)?.[1] ?? null,
      });
      tokens = [{ token_id: "e2e1", label: "Parity", masked: FAKE_MASKED, is_valid: false, username: null }];
      await r.fulfill({ json: { ok: true, token_id: "e2e1", is_valid: false } });
    });
    await page.route("**/api/scraper/tokens/e2e1/validate", async (r) => {
      validated++;
      await r.fulfill({ json: { ok: true, is_valid: false } });
    });
    await page.route("**/api/scraper/tokens/e2e1", async (r) => {
      if (r.request().method() !== "DELETE") return r.fallback();
      deleted++;
      tokens = [];
      await r.fulfill({ json: { ok: true } });
    });
    await open(page, "scraper");
    await expect(page.locator("#sc-token-count")).toHaveText("0 tokens");
    await page.locator("#sc-token-value").fill(FAKE_TOKEN);
    await page.locator("#sc-token-label").fill("Parity");
    await page.locator("#sc-add-submit").click();
    await expect(page.locator("#sc-token-count")).toHaveText("1 token");
    expect(added).toEqual([{ value: FAKE_TOKEN, label: "Parity" }]);
    const row = page.locator('.sc-token-item[data-id="e2e1"]');
    await expect(row).toContainText("Parity");
    await expect(row).toContainText(FAKE_MASKED);
    await expect(page.locator("#sc-token-value")).toHaveValue("");
    expect(await page.content()).not.toContain(FAKE_TOKEN);

    await row.locator(".sc-token-revalidate").click();
    await expect.poll(() => validated).toBe(1);

    await row.locator(".sc-token-delete").click();
    await page.getByRole("dialog").getByRole("button", { name: "Delete", exact: true }).click();
    await expect.poll(() => deleted).toBe(1);
    await expect(page.locator("#sc-token-empty")).toBeVisible();
  });

  test("44 configuration fields and validate setup", async ({ page }) => {
    await open(page, "scraper");
    for (const id of ["sc-guild-id", "sc-validate-btn", "sc-queue-btn", "sc-start-btn"]) {
      await expect(page.locator(`#${id}`), id).toBeVisible();
    }
    await expect(page.locator("#sc-opt-username")).toBeChecked();
    await expect(page.locator("#sc-opt-avatar")).toBeChecked();
    await expect(page.locator("#sc-opt-roles")).not.toBeChecked();
    await expect(page.locator("#sc-opt-bio")).not.toBeChecked();
    // The old page draws its own box over a hidden input, so the label is what takes the click.
    await page.locator("label:has(#sc-opt-roles)").click();
    await expect(page.locator("#sc-opt-roles")).toBeChecked();

    // No tokens in the e2e home, so the server answers without calling Discord.
    await page.locator("#sc-guild-id").fill("123456789012345678");
    const reply = page.waitForResponse((r) => r.url().endsWith("/api/scraper/validate-setup"));
    await page.locator("#sc-validate-btn").click();
    const r = await reply;
    expect(r.request().postData()).toContain("123456789012345678");
    await expect(page.locator("#sc-validation-result")).toContainText("No valid tokens configured");

    // Start without tokens is refused in the page, before any request.
    let started = 0;
    await page.route("**/api/scraper/start", async (route) => {
      started++;
      await route.fulfill({ json: { ok: false } });
    });
    await page.locator("#sc-start-btn").click();
    await page.waitForTimeout(300);
    expect(started).toBe(0);
  });

  test("45 proxies list reads and saves", async ({ page }) => {
    await open(page, "scraper");
    await expect(page.locator("#sc-proxy-card")).toContainText("Proxies");
    const put = (proxies: string[]) => page.request.put("/api/scraper/proxies", { headers: CSRF_HEADERS, data: { proxies } });
    try {
      expect((await put(["127.0.0.1:9"])).ok()).toBeTruthy();
      const got = (await (await page.request.get("/api/scraper/proxies")).json()) as { proxies: string[] };
      expect(got.proxies).toEqual(["127.0.0.1:9"]);
      await page.reload();
      await settle(page);
      await page.locator("#sc-proxy-toggle").click();
      await expect(page.locator("#sc-proxy-panel")).toContainText("127.0.0.1:9");
      await page.locator("#sc-proxy-input").fill("127.0.0.1:10");
      await page.locator("#sc-proxy-input").press("Enter");
      const saved = page.waitForResponse((res) => res.url().endsWith("/api/scraper/proxies") && res.request().method() === "PUT");
      await page.locator("#sc-proxy-save").click();
      expect((await saved).ok()).toBeTruthy();
      const after = (await (await page.request.get("/api/scraper/proxies")).json()) as { proxies: string[] };
      expect(after.proxies).toEqual(["127.0.0.1:9", "127.0.0.1:10"]);
    } finally {
      await put([]);
    }
  });

  test("46 47 queue list, remove, clear, cancel all and progress card", async ({ page }) => {
    const queue = [
      { id: 7, guild_id: "111111111111111111", status: "running" },
      { id: 8, guild_id: "222222222222222222", status: "pending" },
    ];
    const hits: string[] = [];
    await page.route("**/api/scraper/status", (r) =>
      r.fulfill({ json: { running: true, guild_id: queue[0]!.guild_id, started_at: Date.now() / 1000, queue } }),
    );
    for (const p of ["queue/8", "queue/clear", "cancel-all"]) {
      await page.route(`**/api/scraper/${p}`, async (r) => {
        hits.push(`${r.request().method()} ${p}`);
        await r.fulfill({ json: { ok: true } });
      });
    }
    await open(page, "scraper");
    await expect(page.locator("#sc-queue-card")).toBeVisible();
    await expect(page.locator("#sc-queue-count")).toHaveText("2 items");
    await expect(page.locator(".sc-queue-item")).toHaveCount(2);
    await expect(page.locator("#sc-queue-list")).toContainText("222222222222222222");
    await expect(page.locator(".sc-queue-remove")).toHaveCount(1);
    await expect(page.locator("#sc-progress-card")).toBeVisible();
    for (const id of ["sc-elapsed", "sc-progress-fill", "sc-progress-text", "sc-progress-pct"]) {
      await expect(page.locator(`#${id}`), id).toBeAttached();
    }

    await page.locator('.sc-queue-remove[data-id="8"]').click();
    await expect.poll(() => hits).toContain("DELETE queue/8");
    await page.locator("#sc-queue-clear").click();
    await expect.poll(() => hits).toContain("POST queue/clear");
    await page.locator("#sc-cancel-all-btn").click();
    await page.getByRole("dialog").getByRole("button", { name: "Cancel All", exact: true }).click();
    await expect.poll(() => hits).toContain("POST cancel-all");
  });

  test("48 49 log and past scrapes cards", async ({ page }) => {
    await open(page, "scraper");
    for (const id of ["sc-config-card", "sc-log-card", "sc-log-search", "sc-log-autoclear", "sc-log-body", "sc-scrapes-card", "sc-scrapes-refresh"]) {
      await expect(page.locator(`#${id}`), id).toBeAttached();
    }
    await expect(page.locator("#sc-log-autoclear")).toBeChecked();
    await page.locator("#sc-log-search").fill("nothing-matches-this");
    const cleared = page.waitForResponse((r) => r.url().endsWith("/api/scraper/logs/clear"));
    await page.locator("#sc-log-clear").click();
    expect((await cleared).ok()).toBeTruthy();
    await expect(page.locator("#sc-log-search")).toHaveValue("");
    await expect(page.locator("#sc-log-body")).toHaveText("");
    expect((await page.request.get("/api/scraper/scrapes")).ok()).toBeTruthy();
    await expect(page.locator("#sc-scrapes-empty")).toBeVisible();
  });

  test("49 past scrapes download link and delete with confirm and encoded path", async ({ page }) => {
    const scrape = {
      filename: "scrape_e2e.json",
      path: "123456789/scrape_e2e.json",
      guild_id: "123456789",
      guild_name: "Parity <i>Guild</i>",
      total_count: 3,
      scraped_at: "2026-10-13T09:41:00Z",
      metadata: { method: "gateway" },
      file_size: 2048,
    };
    let scrapes = [scrape];
    const deleted: string[] = [];
    await page.route("**/api/scraper/scrapes", (r) => r.fulfill({ json: { ok: true, scrapes } }));
    await page.route("**/api/scraper/scrapes/*", async (r) => {
      if (r.request().method() !== "DELETE") return r.fallback();
      deleted.push(new URL(r.request().url()).pathname);
      scrapes = [];
      await r.fulfill({ json: { ok: true } });
    });
    await open(page, "scraper");
    const item = page.locator(".sc-scrape-item");
    await expect(item).toHaveCount(1);
    await expect(item).toContainText("Parity <i>Guild</i>");
    await expect(item).toContainText("3 members");
    await expect(item.locator("a[download]")).toHaveAttribute("href", "/api/scraper/scrapes/123456789%2Fscrape_e2e.json");

    await page.locator("#sc-scrapes-refresh").click();
    await expect(item).toHaveCount(1);

    await item.locator(".sc-scrape-delete").click();
    await page.getByRole("dialog").getByRole("button", { name: "Delete", exact: true }).click();
    await expect.poll(() => deleted).toEqual(["/api/scraper/scrapes/123456789%2Fscrape_e2e.json"]);
    await expect(page.locator("#sc-scrapes-empty")).toBeVisible();
    await expect(item).toHaveCount(0);
  });
});

test.describe("system", () => {
  // Restore and delete only touch the throwaway e2e home that global-setup starts.
  async function archiveNames(page: Page): Promise<string[]> {
    const r = await page.request.get("/api/backup/info");
    expect(r.ok()).toBeTruthy();
    return ((await r.json()) as { archives: { name: string }[] }).archives.map((a) => a.name);
  }

  async function makeBackup(page: Page): Promise<string> {
    const r = await page.request.post("/admin/backup-now", { headers: CSRF_HEADERS });
    expect(r.ok()).toBeTruthy();
    return ((await r.json()) as { file: string }).file;
  }

  async function expandDatabase(page: Page): Promise<void> {
    await open(page, "system");
    await page.locator("#adv-collapse-btn").click();
    await expect(page.locator("#adv-backup-now")).toBeVisible();
  }

  test("50 database card with backup now and info", async ({ page }) => {
    await open(page, "system");
    await expect(page.locator("#adv-backup-now")).toBeAttached();
    expect((await page.request.get("/api/backup/info")).ok()).toBeTruthy();
  });

  test("50 collapse toggle shows and hides the backup actions", async ({ page }) => {
    await open(page, "system");
    const btn = page.locator("#adv-collapse-btn");
    await expect(btn).toHaveAttribute("aria-expanded", "false");
    await expect(page.locator("#adv-backup-now")).toBeHidden();
    await btn.click();
    await expect(btn).toHaveAttribute("aria-expanded", "true");
    for (const id of ["adv-backup-now", "adv-download-latest", "adv-archives"]) {
      await expect(page.locator(`#${id}`), id).toBeVisible();
    }
    await expect(page.locator("#adv-restore-file")).toHaveAttribute("accept", ".gz");
    await btn.click();
    await expect(btn).toHaveAttribute("aria-expanded", "false");
    await expect(page.locator("#adv-backup-now")).toBeHidden();
  });

  test("50 backup now lists the archive with working download links", async ({ page }) => {
    await expandDatabase(page);
    const before = await archiveNames(page);
    const reply = page.waitForResponse((r) => r.url().endsWith("/admin/backup-now") && r.request().method() === "POST");
    await page.locator("#adv-backup-now").click();
    const { ok, file } = (await (await reply).json()) as { ok: boolean; file: string };
    expect(ok).toBe(true);
    expect(before).not.toContain(file);

    const del = page.locator(`#adv-archives [data-delete="${file}"]`);
    await expect(del).toBeVisible();
    const href = `/api/backup/download/${encodeURIComponent(file)}`;
    await expect(page.locator("#adv-download-latest")).toHaveAttribute("href", href);
    // has: is resolved inside each row, so it takes the bare attribute selector.
    const row = page.locator("#adv-archives tbody tr", { has: page.locator(`[data-delete="${file}"]`) });
    await expect(row.getByRole("link", { name: "Download" })).toHaveAttribute("href", href);
    const dl = await page.request.get(href);
    expect(dl.ok()).toBeTruthy();
    expect(dl.headers()["content-type"]).toContain("gzip");
  });

  test("50 restore by upload asks first, cancel sends nothing, confirm restores", async ({ page }) => {
    const file = await makeBackup(page);
    const archive = await (await page.request.get(`/api/backup/download/${encodeURIComponent(file)}`)).body();
    const upload = { name: "e2e-restore.tar.gz", mimeType: "application/gzip", buffer: archive };
    await expandDatabase(page);
    const restores: string[] = [];
    page.on("request", (r) => {
      if (r.url().endsWith("/api/backup/restore")) restores.push(r.method());
    });
    const input = page.locator("#adv-restore-file");

    await input.setInputFiles(upload);
    const dialog = page.getByRole("dialog", { name: "Restore database?" });
    await expect(dialog).toContainText("e2e-restore.tar.gz");
    await dialog.getByRole("button", { name: "Cancel" }).click();
    await expect(dialog).toBeHidden();
    expect(restores).toEqual([]);

    await input.setInputFiles(upload);
    await expect(dialog).toBeVisible();
    const reply = page.waitForResponse((r) => r.url().endsWith("/api/backup/restore"));
    await dialog.getByRole("button", { name: "Restore" }).click();
    const r = await reply;
    expect(r.status()).toBe(200);
    expect(((await r.json()) as { ok: boolean }).ok).toBe(true);
    expect(restores).toEqual(["POST"]);
    // The archive was taken after seeding, so the seeded forwarding rule survives the restore.
    expect(await (await page.request.get("/api/forwarding")).text()).toContain(SEED.ruleLabel);
  });

  test("50 delete asks first and removes the archive", async ({ page }) => {
    const file = await makeBackup(page);
    await expandDatabase(page);
    const del = page.locator(`#adv-archives [data-delete="${file}"]`);
    const dialog = page.getByRole("dialog", { name: "Delete backup?" });

    await del.click();
    await expect(dialog).toContainText(file);
    await dialog.getByRole("button", { name: "Cancel" }).click();
    await expect(dialog).toBeHidden();
    await expect(del).toBeVisible();
    expect(await archiveNames(page)).toContain(file);

    await del.click();
    const reply = page.waitForResponse((r) => r.url().endsWith("/api/backup/delete"));
    await dialog.getByRole("button", { name: "Delete" }).click();
    const r = await reply;
    expect(r.status()).toBe(200);
    expect(r.request().postData() ?? "").toContain(file);
    await expect(del).toHaveCount(0);
    expect(await archiveNames(page)).not.toContain(file);
  });
});

test.describe("event logs", () => {
  const LOGS_URL = PAGES["event-logs"];
  const SEEDED: Array<[string, string]> = [
    ["channel_created", "Created channel #general"],
    ["channel_deleted", "Deleted channel #old-news"],
    ["role_created", "Created role Moderator"],
    ["category_created", "Created category Text Channels"],
  ];

  async function openLogs(page: Page): Promise<void> {
    const errors: string[] = [];
    page.on("pageerror", (e) => errors.push(e.message));
    const r = await page.goto(LOGS_URL);
    expect(r?.status(), "event-logs status").toBeLessThan(400);
    await settle(page);
    expect(errors, "event-logs page errors").toEqual([]);
  }

  async function addLog(page: Page, event_type: string, details: string): Promise<string> {
    const r = await page.request.post("/api/event-logs", {
      data: { event_type, details, guild_id: SEED.clonedGuildId, guild_name: "E2E Clone", channel_name: "general" },
      headers: CSRF_HEADERS,
    });
    expect(r.ok(), `add log ${details}`).toBeTruthy();
    return ((await r.json()) as { log_id: string }).log_id;
  }

  async function deleteLogs(page: Page, ids: string[]): Promise<number> {
    const r = await page.request.post("/api/event-logs/delete-bulk", { data: { ids }, headers: CSRF_HEADERS });
    expect(r.ok(), "bulk delete").toBeTruthy();
    return ((await r.json()) as { deleted: number }).deleted;
  }

  /** The SPA confirms every delete; the old page confirms only clear all. */
  async function confirmIfAsked(page: Page, name: string): Promise<void> {
    const btn = page.getByRole("dialog").getByRole("button", { name, exact: true });
    await btn.click({ timeout: 2_000 }).catch(() => undefined);
  }

  test("51 seeded events listed, search filters, clear filters restores", async ({ page }) => {
    await openLogs(page);
    const body = page.locator("#logs-tbody");
    await expect(body).toContainText("Created channel #general");
    await expect(body).toContainText("Created role Moderator");
    await page.locator("#logs-search").fill("Moderator");
    await expect(body).not.toContainText("Created channel #general");
    await page.locator("#logs-clear-filters").click();
    await expect(body).toContainText("Created channel #general");
  });

  test("51 type filter lists the server types and narrows the table", async ({ page }) => {
    await openLogs(page);
    const types = (await (await page.request.get("/api/event-log-types")).json()) as { types: string[] };
    expect(types.types).toEqual(expect.arrayContaining(["role_created", "channel_created"]));
    const select = page.locator("#logs-filter-type");
    await expect(select.locator('option[value="role_created"]')).toHaveCount(1);
    await select.selectOption("role_created", { force: true });
    const body = page.locator("#logs-tbody");
    await expect(body).not.toContainText("Created channel #general");
    await expect(body).toContainText("Created role Moderator");
    await page.locator("#logs-clear-filters").click();
    await expect(body).toContainText("Created channel #general");
  });

  test("51 refresh shows a new event and delete one removes it", async ({ page }) => {
    await openLogs(page);
    const body = page.locator("#logs-tbody");
    await addLog(page, "channel_renamed", "Renamed channel e2e-refresh-probe");
    await expect(body).not.toContainText("e2e-refresh-probe");
    await page.locator("#logs-refresh").click();
    const row = body.locator("tr", { hasText: "e2e-refresh-probe" });
    await expect(row).toHaveCount(1);
    await row.locator('button[aria-label="Delete log"]').click();
    await confirmIfAsked(page, "Delete");
    await expect(body).not.toContainText("e2e-refresh-probe");
    await expect(body).toContainText("Created channel #general");
  });

  test("51 pagination pages 50 at a time", async ({ page }) => {
    test.setTimeout(90_000);
    const ids: string[] = [];
    for (let i = 0; i < 55; i++) ids.push(await addLog(page, "emoji_synced", `Synced emoji e2e-page-${i}`));
    try {
      await openLogs(page);
      await page.locator("#logs-search").fill("e2e-page-");
      const count = page.locator("#logs-count");
      await expect(count).toHaveText("Showing 1-50 of 55");
      const pager = page.locator("#logs-pagination");
      await pager.locator('button[aria-label="Next page"]').click();
      await expect(count).toHaveText("Showing 51-55 of 55");
      await expect(page.locator("#logs-tbody tr[data-log-id]")).toHaveCount(5);
      await pager.locator('button[aria-label="Previous page"]').click();
      await expect(count).toHaveText("Showing 1-50 of 55");
    } finally {
      expect(await deleteLogs(page, ids)).toBe(55);
    }
  });

  test("51 bulk delete endpoint removes the given ids", async ({ page }) => {
    const ids = [await addLog(page, "role_updated", "e2e-bulk-api 1"), await addLog(page, "role_updated", "e2e-bulk-api 2")];
    expect(await deleteLogs(page, ids)).toBe(2);
    const empty = await page.request.post("/api/event-logs/delete-bulk", { data: { ids: [] }, headers: CSRF_HEADERS });
    expect(empty.status()).toBe(400);
  });

  test("51 bulk delete selected rows after confirming", async ({ page }) => {
    for (const n of [1, 2, 3]) await addLog(page, "sticker_synced", `Synced sticker e2e-bulk-ui ${n}`);
    await openLogs(page);
    await page.locator("#logs-search").fill("e2e-bulk-ui");
    const body = page.locator("#logs-tbody");
    await expect(page.locator("#logs-count")).toHaveText("Showing 1-3 of 3");
    for (const n of [1, 3]) {
      await body.locator("tr", { hasText: `e2e-bulk-ui ${n}` }).getByRole("checkbox", { name: "Select log" }).check();
    }
    const bulk = page.locator("#logs-delete-selected");
    await expect(bulk).toHaveText(/Delete selected \(2\)/);
    const posted = page.waitForRequest((r) => r.url().endsWith("/api/event-logs/delete-bulk") && r.method() === "POST");
    await bulk.click();
    await page.getByRole("dialog").getByRole("button", { name: "Delete", exact: true }).click();
    expect(((await posted).postDataJSON() as { ids: string[] }).ids).toHaveLength(2);
    await expect(page.locator("#logs-count")).toHaveText("Showing 1-1 of 1");
    await expect(body).toContainText("e2e-bulk-ui 2");
    await body.locator("tr", { hasText: "e2e-bulk-ui 2" }).locator('button[aria-label="Delete log"]').click();
    await confirmIfAsked(page, "Delete");
    await expect(body.locator("tr[data-log-id]")).toHaveCount(0);
  });

  // Runs last in the file: clears the table, then puts the seeded events back.
  test("51 clear all asks first and Cancel keeps the logs", async ({ page }) => {
    await openLogs(page);
    const body = page.locator("#logs-tbody");
    await expect(body).toContainText("Created channel #general");
    await page.locator("#logs-clear-all").click();
    const dialog = page.getByRole("dialog");
    await expect(dialog).toContainText("Are you sure you want to delete all event logs? This cannot be undone.");
    await dialog.getByRole("button", { name: "Cancel", exact: true }).click();
    await expect(dialog).toBeHidden();
    await expect(body).toContainText("Created channel #general");

    await page.locator("#logs-clear-all").click();
    await page.getByRole("dialog").getByRole("button", { name: "Delete All", exact: true }).click();
    await expect(body.locator("tr[data-log-id]")).toHaveCount(0);
    await expect(page.locator("#logs-empty")).toBeVisible();
    for (const [type, details] of SEEDED) await addLog(page, type, details);
  });
});
