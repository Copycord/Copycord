// Parity smoke for inventory items reachable without Discord (numbers refer to copycord-frontend-inventory.md).
// Runs against the old UI by default; E2E_BASE_PATH=/app runs the same checks against the SPA.
import { expect, test, type Page } from "@playwright/test";
import { BASE_PATH, CSRF_HEADERS, LOGIN_PATH, NOT_FOUND_PATH, PAGES, PASSWORD, pageUrl, type PageName } from "../lib/env.ts";
import { SEED, stubClientGuilds } from "../lib/seed.ts";
import { FWD_CREATE_BTN, MAPPING_EDIT_BTN, MAPPING_FILTERS_BTN, closeX, expectClosed, expectOpen, modal, settle } from "../lib/ui.ts";

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
});

test.describe("guilds", () => {
  test("37 search and sort controls, guild list endpoint", async ({ page }) => {
    await open(page, "guilds");
    await expect(page.locator("#g-search")).toBeAttached();
    await expect(page.locator("#g-sortdir")).toBeAttached();
    expect((await page.request.get("/api/guilds")).ok()).toBeTruthy();
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
});

test.describe("scraper", () => {
  test("48 49 log and past scrapes cards", async ({ page }) => {
    await open(page, "scraper");
    for (const id of ["sc-config-card", "sc-log-card", "sc-log-search", "sc-log-autoclear", "sc-scrapes-card", "sc-scrapes-refresh"]) {
      await expect(page.locator(`#${id}`), id).toBeAttached();
    }
    expect((await page.request.get("/api/scraper/scrapes")).ok()).toBeTruthy();
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
    const row = page.locator("#adv-archives tbody tr", { has: del });
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
  // The SPA serves the old /event-logs page at /app/logs (App.tsx PAGES).
  const LOGS_URL = BASE_PATH ? `${BASE_PATH}/logs` : PAGES["event-logs"];
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
    test.skip(!BASE_PATH, "the old page has no row selection");
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
