import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { ToastProvider } from "../../../components/Toast";
import { ConfirmProvider } from "../../../components/ui";
import { Channels } from "../../Channels";
import { FakeWs } from "../../../test/fakeSockets";

const ROWS = [
  {
    original_channel_id: "10",
    original_channel_name: "general",
    cloned_channel_id: "110",
    clone_channel_name: "",
    channel_type: 0,
    category_name: "Lobby",
    original_category_name: "Lobby",
    original_category_id: "900",
    cloned_category_id: "901",
    original_guild_id: "1",
    cloned_guild_id: "2",
  },
  {
    original_channel_id: "11",
    original_channel_name: "pending",
    cloned_channel_id: "",
    clone_channel_name: "",
    channel_type: 0,
    category_name: "Lobby",
    original_category_name: "Lobby",
    original_guild_id: "1",
    cloned_guild_id: "2",
  },
];

const json = (body: unknown) =>
  new Response(JSON.stringify(body), { status: 200, headers: { "content-type": "application/json" } });

function serve() {
  const posts: Array<{ url: string; body: unknown }> = [];
  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input);
      if (init?.method === "POST") {
        posts.push({ url, body: JSON.parse(String(init.body)) });
        return json({ ok: true });
      }
      if (url === "/api/status") return json({ server: { ready: true }, client: { ready: true } });
      if (url === "/api/guild-mappings")
        return json({
          ok: true,
          mappings: [{ mapping_id: "m1", mapping_name: "Main", original_guild_id: "1", original_guild_name: "S", cloned_guild_id: "2", cloned_guild_name: "D" }],
        });
      return json({ items: ROWS });
    }),
  );
  return posts;
}

const page = () =>
  render(
    <ToastProvider>
      <ConfirmProvider>
        <Channels />
      </ConfirmProvider>
    </ToastProvider>,
  );

const cardMenu = async (cid: string) => {
  const card = await waitFor(() => {
    const a = document.querySelector<HTMLElement>(`article[data-cid="${cid}"]`);
    expect(a).not.toBeNull();
    return a!;
  });
  await userEvent.click(within(card).getByRole("button", { name: "Channel menu" }));
};

beforeEach(() => {
  sessionStorage.clear();
});

describe("row actions", () => {
  it("shows Customize only for cloned channels", async () => {
    serve();
    page();
    await cardMenu("11");
    expect(screen.queryByRole("menuitem", { name: "Customize" })).toBeNull();
    expect(screen.queryByRole("menuitem", { name: "Copy clone channel ID" })).toBeNull();
    expect(screen.getByRole("menuitem", { name: "Add channel to blacklist" })).toBeInTheDocument();
  });

  it("saves a channel customization with the trimmed name", async () => {
    const posts = serve();
    page();
    await cardMenu("10");
    await userEvent.click(screen.getByRole("menuitem", { name: "Customize" }));
    const dlg = await screen.findByRole("dialog", { name: "Customize channel" });
    await userEvent.type(within(dlg).getByLabelText("Custom channel name"), "  funny ");
    await userEvent.click(within(dlg).getByRole("button", { name: "Save" }));
    await waitFor(() => expect(posts).toHaveLength(1));
    expect(posts[0]).toEqual({
      url: "/api/channels/customize",
      body: { original_channel_id: "10", cloned_guild_id: "2", clone_channel_name: "funny" },
    });
    await waitFor(() => expect(screen.queryByRole("dialog")).toBeNull());
  });

  it("saves a category customization", async () => {
    const posts = serve();
    page();
    await waitFor(() => expect(document.querySelector("#channels-root article")).not.toBeNull());
    await userEvent.click(screen.getByRole("button", { name: "Category menu" }));
    await userEvent.click(screen.getByRole("menuitem", { name: "Customize category" }));
    const dlg = await screen.findByRole("dialog", { name: "Customize category" });
    await userEvent.type(within(dlg).getByLabelText("Custom category name"), "Hall");
    await userEvent.click(within(dlg).getByRole("button", { name: "Save" }));
    await waitFor(() => expect(posts).toHaveLength(1));
    expect(posts[0]).toEqual({
      url: "/api/categories/customize",
      body: { original_category_id: "900", cloned_guild_id: "2", custom_category_name: "Hall" },
    });
  });

  it("blacklists a channel after confirm and not on cancel", async () => {
    const posts = serve();
    page();
    await cardMenu("10");
    await userEvent.click(screen.getByRole("menuitem", { name: "Add channel to blacklist" }));
    await userEvent.click(within(await screen.findByRole("dialog")).getByRole("button", { name: "Cancel" }));
    expect(posts).toHaveLength(0);

    await cardMenu("10");
    await userEvent.click(screen.getByRole("menuitem", { name: "Add channel to blacklist" }));
    await userEvent.click(within(await screen.findByRole("dialog")).getByRole("button", { name: "Add to blacklist" }));
    await waitFor(() => expect(posts).toHaveLength(1));
    expect(posts[0]).toEqual({
      url: "/api/filters/blacklist",
      body: { scope: "channel", obj_id: "10", mapping_id: "m1", original_guild_id: "1", cloned_guild_id: "2" },
    });
  });

  it("blacklists a category by its original id", async () => {
    const posts = serve();
    page();
    await waitFor(() => expect(document.querySelector("#channels-root article")).not.toBeNull());
    await userEvent.click(screen.getByRole("button", { name: "Category menu" }));
    await userEvent.click(screen.getByRole("menuitem", { name: "Add category to blacklist" }));
    await userEvent.click(within(await screen.findByRole("dialog")).getByRole("button", { name: "Add to blacklist" }));
    await waitFor(() => expect(posts).toHaveLength(1));
    expect(posts[0]!.body).toMatchObject({ scope: "category", obj_id: "900", mapping_id: "m1" });
  });

  it("copies the original and clone ids", async () => {
    serve();
    const writeText = vi.fn().mockResolvedValue(undefined);
    Object.defineProperty(navigator, "clipboard", { value: { writeText }, configurable: true });
    page();
    await cardMenu("10");
    await userEvent.click(screen.getByRole("menuitem", { name: "Copy original channel ID" }));
    await cardMenu("10");
    await userEvent.click(screen.getByRole("menuitem", { name: "Copy clone channel ID" }));
    expect(writeText.mock.calls.map((c) => c[0])).toEqual(["10", "110"]);
  });

  it("keeps Delete all orphans disabled with no orphans", async () => {
    serve();
    page();
    await waitFor(() => expect(document.querySelector("#channels-root article")).not.toBeNull());
    const del = document.getElementById("orph-delall")!;
    expect(del).toBeDisabled();
    expect(del).toHaveTextContent("Delete 0 orphans");
    // A danger outline at the right end of the controls, not an accent control among the filters.
    expect(del.className).toMatch(/\bml-auto\b/);
    expect(del.className).toMatch(/\btext-danger\b/);
    expect(del.className).toMatch(/\bborder-danger\b/);
    // Disabled drops the red for the neutral border and muted text, with a reason on hover.
    expect(del.className).toMatch(/\bdisabled:border-border\b/);
    expect(del.className).toMatch(/\bdisabled:text-muted\b/);
    expect(del).toHaveAttribute("aria-disabled", "true");
    expect(del).toHaveAttribute("title", "No orphans to delete");
    expect(del.className).not.toMatch(/\btext-accent\b/);
  });

  it("toasts each result of Delete all orphans from the server's verify frame", async () => {
    serve();
    FakeWs.all = [];
    vi.stubGlobal("WebSocket", FakeWs);
    page();
    await waitFor(() => expect(document.querySelector("#channels-root article")).not.toBeNull());
    // Frames as VerifyController._publish sends them through AdminBus.publish("verify", ...).
    FakeWs.emit("/ws/out", {
      kind: "verify",
      role: "server",
      payload: {
        type: "orphans",
        req_id: "a1",
        categories: [],
        channels: [
          { id: "1521898853337403511", name: "old-news" },
          { id: "1521898853337403512", name: "12367686" },
        ],
      },
    });
    await waitFor(() => expect(document.getElementById("orph-delall")).toBeEnabled());
    expect(document.getElementById("orph-delall")).toHaveTextContent("Delete 2 orphans");
    await userEvent.click(document.getElementById("orph-delall")!);
    await userEvent.click(await screen.findByRole("button", { name: "Delete all" }));
    FakeWs.emit("/ws/out", {
      kind: "verify",
      role: "server",
      payload: {
        type: "deleted",
        req_id: "841c7bf2",
        ok: true,
        results: [
          { id: "1521898853337403511", kind: "channel", name: "old-news", deleted: true },
          { id: "1521898853337403512", kind: "channel", name: "12367686", deleted: false, reason: "protected" },
        ],
      },
    });
    expect(await screen.findByText('Deleted "old-news"')).toBeInTheDocument();
    expect(screen.getByText('"12367686" can\'t be deleted. Manual action required.')).toBeInTheDocument();
    vi.unstubAllGlobals();
  });

  it("offers Convert to orphan only on a mapped cloned channel", async () => {
    serve();
    page();
    await cardMenu("11");
    expect(screen.queryByRole("menuitem", { name: "Convert to orphan" })).toBeNull();
    await userEvent.keyboard("{Escape}");
    await cardMenu("10");
    expect(screen.getByRole("menuitem", { name: "Convert to orphan" })).toBeInTheDocument();
  });

  describe("Convert to orphan", () => {
    // Every frame the page sends on /ws/in, parsed.
    let frames: Array<{ url: string; payload: Record<string, unknown> }> = [];
    class SpyWs extends FakeWs {
      send(data?: unknown) {
        frames.push({ url: this.url, payload: JSON.parse(String(data)).payload });
      }
    }
    const unlinks = () => frames.filter((f) => f.url.endsWith("/ws/in") && f.payload.action === "unlink_channel");
    const lists = () => frames.filter((f) => f.payload.action === "list").length;
    const channelLoads = () =>
      vi.mocked(fetch).mock.calls.filter(([u]) => String(u).startsWith("/api/channels")).length;
    const reply = (payload: Record<string, unknown>) =>
      FakeWs.emit("/ws/out", { kind: "verify", role: "server", payload: { type: "unlinked", req_id: "u1", mapping_id: "m1", ...payload } });

    const openConfirm = async () => {
      await cardMenu("10");
      await userEvent.click(screen.getByRole("menuitem", { name: "Convert to orphan" }));
      return screen.findByRole("dialog");
    };

    beforeEach(() => {
      frames = [];
      FakeWs.all = [];
      vi.stubGlobal("WebSocket", SpyWs);
    });

    it("explains the result, sends nothing on cancel, and unlinks on confirm", async () => {
      serve();
      page();
      let dlg = await openConfirm();
      expect(within(dlg).getByText("Convert to orphan?")).toBeInTheDocument();
      expect(dlg).toHaveTextContent("#general (110) stays in the clone server as it is");
      expect(dlg).toHaveTextContent("On the next sync the source channel is cloned again as a new channel.");
      await userEvent.click(within(dlg).getByRole("button", { name: "Cancel" }));
      await waitFor(() => expect(screen.queryByRole("dialog")).toBeNull());
      expect(unlinks()).toHaveLength(0);

      dlg = await openConfirm();
      await userEvent.click(within(dlg).getByRole("button", { name: "Convert to orphan" }));
      await waitFor(() => expect(unlinks()).toHaveLength(1));
      expect(unlinks()[0]!.payload).toEqual({ mapping_id: "m1", action: "unlink_channel", original_channel_id: "10" });

      const loads = channelLoads();
      const listed = lists();
      reply({ id: "10", ok: true, cloned_channel_id: "110", name: "general" });
      expect(await screen.findByText('Converted "general" to an orphan.')).toBeInTheDocument();
      // Refreshes like delete orphan: the channel list and the orphan scan.
      await waitFor(() => expect(channelLoads()).toBeGreaterThan(loads));
      expect(lists()).toBeGreaterThan(listed);
      vi.unstubAllGlobals();
    });

    it("toasts why an unlink was refused and ignores replies it did not ask for", async () => {
      serve();
      page();
      await waitFor(() => expect(document.querySelector("#channels-root article")).not.toBeNull());
      await userEvent.click(within(await openConfirm()).getByRole("button", { name: "Convert to orphan" }));
      await waitFor(() => expect(unlinks()).toHaveLength(1));
      reply({ id: "11", ok: true });
      reply({ id: "10", ok: true, mapping_id: "other" });
      expect(screen.queryByText(/to an orphan\.$/)).toBeNull();
      reply({ id: "10", ok: false, reason: "backfilling" });
      expect(await screen.findByText('"general" is being cloned by a task. Try again when it ends.')).toBeInTheDocument();
      vi.unstubAllGlobals();
    });
  });
});
