// Inventory items 33 to 36: the clone action, the orphan scan, the backfill modal and live backfill progress.
// Endpoints and the /ws/out and /ws/in sockets are stubbed, nothing reaches the real client.
import { expect, test, type Page, type WebSocketRoute } from "@playwright/test";
import { pageUrl } from "../lib/env.ts";
import { settle } from "../lib/ui.ts";
import { CHANNELS, channelCards, stubChannels } from "./channels-fixtures.ts";

const WELCOME = "444444444444444401"; // cloned
const RULES = "444444444444444403"; // not cloned

type Call = { url: string; body: Record<string, unknown> };

interface Stub {
  posts: Call[];
  ins: Array<Record<string, unknown>>;
  out: (payload: unknown, kind?: string) => void;
  resume: { value: Record<string, unknown> | null };
  inflight: { items: Record<string, unknown> };
}

async function stubBackfill(page: Page): Promise<Stub> {
  await stubChannels(page);
  const outs: WebSocketRoute[] = [];
  const stub: Stub = {
    posts: [],
    ins: [],
    out: (payload, kind = "client") => {
      for (const s of outs) s.send(JSON.stringify({ kind, role: "client", payload }));
    },
    resume: { value: null },
    inflight: { items: {} },
  };
  await page.routeWebSocket(/\/ws\/(ui|out|in)$/, (ws) => {
    const path = new URL(ws.url()).pathname;
    if (path.endsWith("/out")) outs.push(ws);
    if (path.endsWith("/in")) {
      ws.onMessage((m) => {
        const frame = JSON.parse(String(m)) as { kind?: string; payload?: Record<string, unknown> };
        stub.ins.push(frame.payload ?? {});
      });
    }
  });
  await page.route(
    (url) => url.pathname === "/api/backfills/inflight",
    (r) => r.fulfill({ json: { ok: true, items: stub.inflight.items } }),
  );
  await page.route(
    (url) => url.pathname === "/api/backfills/queue",
    (r) => r.fulfill({ json: { ok: true, items: [] } }),
  );
  await page.route(
    (url) => url.pathname === "/api/backfills/resume-info",
    (r) => r.fulfill({ json: { ok: true, resume: stub.resume.value } }),
  );
  for (const path of ["/api/backfill/start", "/api/backfill/start-batch"]) {
    await page.route(
      (url) => url.pathname === path,
      (r) => {
        stub.posts.push({ url: new URL(r.request().url()).pathname, body: r.request().postDataJSON() as Record<string, unknown> });
        return r.fulfill({ json: { ok: true } });
      },
    );
  }
  return stub;
}

async function openChannels(page: Page): Promise<void> {
  await page.goto(pageUrl("channels"));
  await settle(page);
  await expect(channelCards(page)).toHaveCount(CHANNELS.length);
}

const card = (page: Page, cid: string) => page.locator(`#channels-root .ch-card[data-cid="${cid}"]`);
const menu = (page: Page) => page.locator("#ch-menu");

async function openCloneDialog(page: Page, cid: string): Promise<void> {
  await card(page, cid).getByRole("button", { name: "Channel menu" }).click();
  await menu(page).locator("#ctx-backfill").click();
  await expect(page.locator("#bf-form")).toBeVisible();
}

test.describe("channels verify and backfill", () => {
  test("33 the clone action is offered for cloned channels only", async ({ page }) => {
    await stubBackfill(page);
    await openChannels(page);
    await card(page, RULES).getByRole("button", { name: "Channel menu" }).click();
    await expect(menu(page)).toBeVisible();
    await expect(menu(page).locator("#ctx-backfill")).toHaveCount(0);
    await page.keyboard.press("Escape");

    await card(page, WELCOME).getByRole("button", { name: "Channel menu" }).click();
    await expect(menu(page).locator("#ctx-backfill")).toHaveText("Clone messages");
    await menu(page).locator("#ctx-backfill").click();
    await expect(page.getByRole("heading", { name: "Clone Channel History" })).toBeVisible();
  });

  test("35 all history posts mode all and ignore cloned adds ignore_cloned", async ({ page }) => {
    const stub = await stubBackfill(page);
    await openChannels(page);
    const mid = await page.locator("#mapping-select").inputValue();

    await openCloneDialog(page, WELCOME);
    await page.locator("#bf-start").click();
    await expect.poll(() => stub.posts.length).toBe(1);
    expect(stub.posts[0]).toEqual({ url: "/api/backfill/start", body: { channel_id: WELCOME, mapping_id: mid, mode: "all" } });
    await expect(page.locator("#bf-form")).toBeHidden();

    // A running clone blocks a second dialog; reset by finishing it.
    stub.out({ type: "backfill_done", task_id: "t1", data: { channel_id: WELCOME, state: "finished" } });
    await expect(card(page, WELCOME).locator(".ch-progress")).toHaveCount(0);

    await openCloneDialog(page, WELCOME);
    await page.locator("#bf-ignore-cloned").check();
    await page.locator("#bf-start").click();
    await expect.poll(() => stub.posts.length).toBe(2);
    expect(stub.posts[1]!.body).toMatchObject({ mode: "all", ignore_cloned: true });
  });

  test("35 since, between and last send their ranges and validate first", async ({ page }) => {
    const stub = await stubBackfill(page);
    await openChannels(page);

    await openCloneDialog(page, WELCOME);
    await page.getByLabel("Since date/time").check();
    await page.locator("#bf-start").click();
    await expect(page.getByRole("alert").filter({ hasText: "Pick a date." })).toBeVisible();
    expect(stub.posts).toHaveLength(0);
    await page.locator("#bf-since").fill("2025-03-04");
    await page.locator("#bf-since-time").fill("13:05");
    await page.locator("#bf-start").click();
    await expect.poll(() => stub.posts.length).toBe(1);
    expect(stub.posts[0]!.body).toMatchObject({ mode: "since", since: "2025-03-04T13:05" });
    stub.out({ type: "backfill_done", task_id: "t1", data: { channel_id: WELCOME, state: "finished" } });

    await openCloneDialog(page, WELCOME);
    await page.getByLabel("Between dates").check();
    await page.locator("#bf-from").fill("2025-03-04");
    await page.locator("#bf-to").fill("2025-03-31");
    await page.locator("#bf-start").click();
    await expect.poll(() => stub.posts.length).toBe(2);
    expect(stub.posts[1]!.body).toMatchObject({ mode: "between", since: "2025-03-04T00:00", before_iso: "2025-04-01T00:00" });
    stub.out({ type: "backfill_done", task_id: "t2", data: { channel_id: WELCOME, state: "finished" } });

    await openCloneDialog(page, WELCOME);
    await page.getByLabel("Last N messages").check();
    await page.locator("#bf-lastn").fill("0");
    await page.locator("#bf-start").click();
    await expect(page.getByRole("alert").filter({ hasText: "Enter a valid number." })).toBeVisible();
    await page.locator("#bf-lastn").fill("250");
    await page.locator("#bf-start").click();
    await expect.poll(() => stub.posts.length).toBe(3);
    expect(stub.posts[2]!.body).toMatchObject({ mode: "last", last_n: 250 });
  });

  test("35 an unfinished backfill offers resume and posts the checkpoint", async ({ page }) => {
    const stub = await stubBackfill(page);
    stub.resume.value = { available: true, run_id: "run-9", delivered: 40, expected_total: 100, checkpoint: { last_orig_timestamp: "2025-01-01T00:00:00Z" } };
    await openChannels(page);
    const mid = await page.locator("#mapping-select").inputValue();

    await card(page, WELCOME).getByRole("button", { name: "Channel menu" }).click();
    await menu(page).locator("#ctx-backfill").click();
    const dlg = page.getByRole("dialog", { name: "Resume previous backfill?" });
    await expect(dlg).toContainText("run-9");
    await dlg.getByRole("button", { name: "Start Over" }).click();
    await expect(page.locator("#bf-form")).toBeVisible();
    await page.keyboard.press("Escape");

    await card(page, WELCOME).getByRole("button", { name: "Channel menu" }).click();
    await menu(page).locator("#ctx-backfill").click();
    await page.getByRole("dialog", { name: "Resume previous backfill?" }).getByRole("button", { name: "Continue" }).click();
    await expect.poll(() => stub.posts.length).toBe(1);
    expect(stub.posts[0]).toEqual({
      url: "/api/backfill/start",
      body: { channel_id: WELCOME, mapping_id: mid, resume: true, run_id: "run-9", checkpoint: { last_orig_timestamp: "2025-01-01T00:00:00Z" } },
    });
  });

  test("36 progress events drive the card pill and bar, and done shows a persisted toast", async ({ page }) => {
    const stub = await stubBackfill(page);
    await openChannels(page);
    await openCloneDialog(page, WELCOME);
    await page.locator("#bf-start").click();
    await expect.poll(() => stub.posts.length).toBe(1);
    await expect(card(page, WELCOME).locator(".ch-status")).toHaveText("Starting…");

    stub.out({ type: "backfill_started", task_id: "t1", data: { channel_id: WELCOME } });
    await expect(page.getByText("Clone started…")).toBeVisible();
    await expect(card(page, WELCOME).locator(".ch-status")).toHaveText("Cloning");

    stub.out({ type: "backfill_progress", task_id: "t1", data: { delivered: 25, expected_total: 100 } });
    await expect(card(page, WELCOME).locator(".ch-status")).toHaveText("Cloning (25/100)");
    await expect(card(page, WELCOME).locator(".ch-progress")).toHaveAttribute("aria-valuenow", "25");

    stub.out({ type: "backfill_progress", task_id: "t1", data: { delivered: 10, expected_total: 100 } });
    await expect(card(page, WELCOME).locator(".ch-status")).toHaveText("Cloning (25/100)");

    stub.out({ type: "backfill_cleanup", task_id: "t1", data: { state: "starting" } });
    await expect(card(page, WELCOME).locator(".ch-status")).toHaveText("Cleaning up");

    stub.out({ type: "backfill_cleanup", task_id: "t1", data: { state: "finished" } });
    await expect(page.getByText("Clone completed for #welcome.")).toBeVisible();
    await expect(card(page, WELCOME).locator(".ch-progress")).toHaveCount(0);
    const keys = await page.evaluate(() => Object.keys(localStorage).filter((k) => k.startsWith("toast:persist:bf:done-task:")));
    expect(keys).toEqual(["toast:persist:bf:done-task:t1"]);

    // A repeat of the same finished event does not toast again.
    stub.out({ type: "backfill_cleanup", task_id: "t1", data: { state: "finished" } });
    await page.waitForTimeout(300);
    await expect(page.getByText("Clone completed for #welcome.")).toHaveCount(1);
  });

  test("36 an inflight clone found on load shows its progress and blocks a second start", async ({ page }) => {
    const stub = await stubBackfill(page);
    stub.inflight.items = { [WELCOME]: { delivered: 5, expected_total: 20 } };
    await openChannels(page);
    await expect(card(page, WELCOME).locator(".ch-status")).toHaveText("Cloning (5/20)");
    await card(page, WELCOME).getByRole("button", { name: "Channel menu" }).click();
    await menu(page).locator("#ctx-backfill").click();
    await expect(page.getByText("A clone for this channel is already in progress.")).toBeVisible();
    await expect(page.locator("#bf-form")).toHaveCount(0);
  });

  test("36 a cancelled clone clears the card and toasts the reason", async ({ page }) => {
    const stub = await stubBackfill(page);
    stub.inflight.items = { [WELCOME]: { delivered: 1, expected_total: 5 } };
    await openChannels(page);
    await expect(card(page, WELCOME).locator(".ch-status")).toBeVisible();
    stub.out({ type: "backfill_cancelled", data: { channel_id: WELCOME, reason: "server_shutdown" } });
    await expect(page.getByText("Clone cancelled: server is shutting down.")).toBeVisible();
    await expect(card(page, WELCOME).locator(".ch-status")).toHaveCount(0);
  });

  test("34 the page asks for the orphan list on load and marks the orphans in the tree", async ({ page }) => {
    const stub = await stubBackfill(page);
    await openChannels(page);
    await expect.poll(() => stub.ins.filter((p) => p.action === "list").length).toBeGreaterThan(0);
    const mid = await page.locator("#mapping-select").inputValue();
    expect(stub.ins.find((p) => p.action === "list")).toMatchObject({ action: "list", mapping_id: mid });
    await expect(page.locator("#verify-btn")).toHaveCount(0);

    // An event for another mapping is ignored.
    stub.out({ type: "orphans", mapping_id: "other", categories: [], channels: [{ id: "911", name: "stray" }] }, "verify");
    await page.waitForTimeout(200);
    await expect(page.locator("#orph-delall")).toBeDisabled();

    stub.out(
      {
        type: "orphans",
        mapping_id: mid,
        categories: [{ id: "910", name: "Old <b>cat</b>" }],
        channels: [{ id: "911", name: "stray", category_name: "Old" }],
      },
      "verify",
    );
    await expect(card(page, "911")).toHaveAttribute("data-orphan", "1");
    await expect(page.locator("#orph-delall")).toBeEnabled();
    await expect(page.locator("#orph-delall")).toHaveText("Delete 2 orphans");

    stub.out({ type: "orphans", mapping_id: mid, categories: [], channels: [] }, "verify");
    await expect(page.locator("#orph-delall")).toBeDisabled();
  });

  test("34 delete all orphans confirms and sends delete_all with every orphan id", async ({ page }) => {
    const stub = await stubBackfill(page);
    await openChannels(page);
    const mid = await page.locator("#mapping-select").inputValue();
    stub.out({ type: "orphans", mapping_id: mid, categories: [{ id: "910", name: "Old" }], channels: [{ id: "911", name: "stray" }] }, "verify");
    await expect(page.locator("#orph-delall")).toBeEnabled();
    await page.locator("#orph-delall").click();
    await page.getByRole("dialog", { name: "Delete all orphans?" }).getByRole("button", { name: "Delete all" }).click();
    await expect.poll(() => stub.ins.some((p) => p.action === "delete_all")).toBe(true);
    expect(stub.ins.find((p) => p.action === "delete_all")).toMatchObject({ mapping_id: mid, ids: ["910", "911"] });
  });
});
