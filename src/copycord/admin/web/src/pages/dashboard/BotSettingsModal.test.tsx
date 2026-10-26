import { fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { BotsPanel } from "./BotsPanel";
import { savedMessage } from "./botSettings";

class FakeSocket {
  static OPEN = 1;
  readyState = 0;
  onopen: (() => void) | null = null;
  onclose: (() => void) | null = null;
  onmessage: ((ev: MessageEvent) => void) | null = null;
  send() {}
  close() {}
}
const WsImpl = FakeSocket as unknown as typeof WebSocket;

function json(body: unknown) {
  return new Response(JSON.stringify(body), { status: 200, headers: { "content-type": "application/json" } });
}

function stub(live = { server: false, client: false }) {
  const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
    const url = String(input);
    if (url === "/api/status") return json({ server: { running: false }, client: { running: false } });
    if (url === "/api/bot-status" && (init?.method ?? "GET") === "GET") return json({ SERVER_STATUS: "online", CLIENT_STATUS: "idle" });
    if (url === "/api/bot-status") return json({ ok: true, ...JSON.parse(String(init?.body)), live });
    return new Response("not found", { status: 404 });
  });
  vi.stubGlobal("fetch", fetchMock);
  return fetchMock;
}

afterEach(() => {
  vi.unstubAllGlobals();
});

async function openModal() {
  render(<BotsPanel onViewLogs={() => {}} WebSocketImpl={WsImpl} />);
  fireEvent.click(screen.getByRole("button", { name: "Bot settings" }));
  const dialog = await screen.findByRole("dialog");
  await waitFor(() => expect(within(dialog).getAllByRole("combobox", { name: "Status" })).toHaveLength(2));
  return dialog;
}

describe("BotSettingsModal", () => {
  it("opens from the gear with both sections and their Status selects", async () => {
    stub();
    const dialog = await openModal();
    expect(within(dialog).getByText("Bot settings")).toBeTruthy();
    expect(within(dialog).queryByText("How each bot appears on Discord")).toBeNull();
    expect(within(dialog).queryByText(/Applies right away/)).toBeNull();
    const server = within(dialog).getByRole("region", { name: "Server bot" });
    const client = within(dialog).getByRole("region", { name: "Client bot" });
    await waitFor(() => expect(within(server).getByRole("combobox", { name: "Status" }).textContent).toContain("Online"));
    expect(within(client).getByRole("combobox", { name: "Status" }).textContent).toContain("Idle");
  });

  it("keeps Save off until a change, then saves only the two keys", async () => {
    const fetchMock = stub();
    const dialog = await openModal();
    const save = within(dialog).getByRole("button", { name: "Save" }) as HTMLButtonElement;
    const server = within(dialog).getByRole("region", { name: "Server bot" });
    const combo = within(server).getByRole("combobox", { name: "Status" });
    await waitFor(() => expect(combo.textContent).toContain("Online"));
    expect(save.disabled).toBe(true);

    fireEvent.click(combo);
    const list = await screen.findByRole("listbox");
    expect(within(list).getAllByRole("option").map((o) => o.textContent?.replace("✓", "").trim())).toEqual([
      "Online",
      "Idle",
      "Busy",
      "Invisible",
    ]);
    fireEvent.click(within(list).getByRole("option", { name: /Busy/ }));
    expect(save.disabled).toBe(false);

    fireEvent.click(save);
    await waitFor(() => expect(screen.queryByRole("dialog")).toBeNull());
    const posts = fetchMock.mock.calls.filter(([, init]) => init?.method === "POST");
    expect(posts).toHaveLength(1);
    expect(String(posts[0]![0])).toBe("/api/bot-status");
    expect(JSON.parse(String(posts[0]![1]!.body))).toEqual({ SERVER_STATUS: "dnd", CLIENT_STATUS: "idle" });
  });

  it("words the toast by whether a bot took it live", () => {
    expect(savedMessage({ ok: true, SERVER_STATUS: "idle", CLIENT_STATUS: "online", live: { server: true, client: false } })).toBe(
      "Status updated",
    );
    expect(savedMessage({ ok: true, SERVER_STATUS: "idle", CLIENT_STATUS: "online", live: { server: false, client: false } })).toBe(
      "Saved, applies at next start",
    );
  });
});
