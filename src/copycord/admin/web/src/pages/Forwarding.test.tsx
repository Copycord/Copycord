import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { ToastProvider } from "../components/Toast";
import { ConfirmProvider } from "../components/ui/ConfirmDialog";
import { CSRF_HEADER } from "../lib/api";
import type { ForwardingRule } from "../types/forwarding";
import { Forwarding } from "./Forwarding";
import type { OpenRuleEditor } from "./forwarding/editor";
import { filterRules, toList } from "./forwarding/forwardingApi";

const RULES: ForwardingRule[] = [
  {
    rule_id: "r1",
    label: "Support <b>pings</b>",
    provider: "discord",
    enabled: true,
    filters: { keywords_any: ["urgent"] },
  },
  { rule_id: "r/2", label: "Phone alerts", provider: "pushover", enabled: false, filters: {} },
];

function jsonResponse(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json" } });
}

type Handler = (init: RequestInit) => Response;

function server(routes: Record<string, Handler>) {
  const fetchMock = vi.fn(async (input: RequestInfo | URL, init: RequestInit = {}) => {
    const handler = routes[`${init.method ?? "GET"} ${String(input)}`];
    return handler ? handler(init) : new Response("not found", { status: 404 });
  });
  vi.stubGlobal("fetch", fetchMock);
  return fetchMock;
}

function listRoutes(items: ForwardingRule[] = RULES): Record<string, Handler> {
  return {
    "GET /api/forwarding": () => jsonResponse({ ok: true, items }),
    "GET /api/forwarding/count": () => jsonResponse({ ok: true, count: 12345, by_provider: {} }),
    "GET /api/forwarding/count/by-rule": () => jsonResponse({ ok: true, by_rule: { r1: 7, "r/2": "1200" } }),
  };
}

function renderPage(openRuleEditor?: OpenRuleEditor) {
  return render(
    <ToastProvider>
      <ConfirmProvider>
        <Forwarding openRuleEditor={openRuleEditor} />
      </ConfirmProvider>
    </ToastProvider>,
  );
}

function card(id: string): HTMLElement {
  return document.querySelector<HTMLElement>(`.fwd-card[data-id="${id}"]`)!;
}

describe("Forwarding page", () => {
  it("lists the rules as cards with provider, status and label as plain text", async () => {
    server(listRoutes());
    renderPage();
    expect(screen.getByRole("heading", { level: 1, name: "Forwarding" })).toBeInTheDocument();
    expect(screen.getAllByTestId("fwd-skeleton")).toHaveLength(3);

    await waitFor(() => expect(document.querySelectorAll(".fwd-card")).toHaveLength(2));
    expect(screen.queryByTestId("fwd-skeleton")).toBeNull();
    expect(document.getElementById("forwarding-empty")).not.toBeVisible();

    const first = card("r1");
    // The label is React text: the markup shows up literally, no <b> element.
    expect(within(first).getByRole("heading", { level: 3 })).toHaveTextContent("Support <b>pings</b>");
    expect(first.querySelector("b")).toBeNull();
    expect(first).toHaveTextContent("Discord");
    expect(first).toHaveTextContent("Enabled");
    expect(within(first).getByRole("button", { name: "Disable" })).toBeInTheDocument();

    const second = card("r/2");
    expect(second).toHaveTextContent("Pushover");
    expect(second).toHaveTextContent("Disabled");
    expect(within(second).getByRole("button", { name: "Enable" })).toBeInTheDocument();
  });

  it("shows the total and the per rule counts", async () => {
    const fetchMock = server(listRoutes());
    renderPage();
    await waitFor(() => expect(document.getElementById("fwdTotalCount")).toHaveTextContent((12345).toLocaleString()));
    expect(card("r1").querySelector('[data-rule-count-id="r1"]')).toHaveTextContent("7");
    expect(card("r/2").querySelector('[data-rule-count-id="r/2"]')).toHaveTextContent((1200).toLocaleString());
    for (const url of ["/api/forwarding", "/api/forwarding/count", "/api/forwarding/count/by-rule"]) {
      expect(fetchMock.mock.calls.some(([u]) => String(u) === url), url).toBe(true);
    }
  });

  it("falls back to a dash and zero when the counts fail", async () => {
    server({
      "GET /api/forwarding": () => jsonResponse({ ok: true, items: RULES }),
      "GET /api/forwarding/count": () => jsonResponse({ ok: false, error: "db-error" }, 500),
      "GET /api/forwarding/count/by-rule": () => jsonResponse({ ok: false, error: "db-error" }, 500),
    });
    renderPage();
    await waitFor(() => expect(document.querySelectorAll(".fwd-card")).toHaveLength(2));
    expect(document.getElementById("fwdTotalCount")).toHaveTextContent("—");
    expect(card("r1").querySelector('[data-rule-count-id="r1"]')).toHaveTextContent("0");
  });

  it("shows the empty state when there are no rules", async () => {
    server(listRoutes([]));
    const open = vi.fn();
    const user = userEvent.setup();
    renderPage(open);
    await waitFor(() => expect(document.getElementById("forwarding-empty")).toBeVisible());
    expect(screen.getByText("No forwarding rules yet")).toBeInTheDocument();
    expect(document.querySelectorAll(".fwd-card")).toHaveLength(0);
    expect(document.getElementById("fwdSearchInput")).toBeNull();
    await user.click(document.getElementById("fwdCreateBtn")!);
    expect(open).toHaveBeenCalledWith({ mode: "create" });
  });

  it("deletes a rule only after the confirm, with a CSRF DELETE to its encoded url", async () => {
    const fetchMock = server({
      ...listRoutes(),
      "DELETE /api/forwarding/r%2F2": () => jsonResponse({ ok: true, deleted: "r/2" }),
    });
    const user = userEvent.setup();
    renderPage();
    await waitFor(() => expect(document.querySelectorAll(".fwd-card")).toHaveLength(2));
    const deletes = () => fetchMock.mock.calls.filter(([, init]) => init?.method === "DELETE");

    await user.click(within(card("r/2")).getByRole("button", { name: "Delete" }));
    let dialog = await screen.findByRole("dialog");
    expect(dialog).toHaveTextContent("Delete forwarding rule");
    expect(dialog).toHaveTextContent("Are you sure you want to delete this forwarding rule?");
    await user.click(within(dialog).getByRole("button", { name: "Cancel" }));
    await waitFor(() => expect(screen.queryByRole("dialog")).toBeNull());
    expect(deletes()).toHaveLength(0);
    expect(card("r/2")).not.toBeNull();

    await user.click(within(card("r/2")).getByRole("button", { name: "Delete" }));
    dialog = await screen.findByRole("dialog");
    await user.click(within(dialog).getByRole("button", { name: "Delete" }));
    await waitFor(() => expect(document.querySelectorAll(".fwd-card")).toHaveLength(1));
    expect(deletes()).toHaveLength(1);
    const [url, init] = deletes()[0]!;
    expect(String(url)).toBe("/api/forwarding/r%2F2");
    expect(init?.body).toBeUndefined();
    expect(new Headers(init?.headers).get(CSRF_HEADER)).toBe("1");
    expect(await screen.findByText("Forwarding rule deleted.")).toBeInTheDocument();
  });

  it("keeps the rule and toasts when the delete fails", async () => {
    server({
      ...listRoutes(),
      "DELETE /api/forwarding/r1": () => jsonResponse({ detail: "fwd-not-found" }, 404),
    });
    const user = userEvent.setup();
    renderPage();
    await waitFor(() => expect(document.querySelectorAll(".fwd-card")).toHaveLength(2));
    await user.click(within(card("r1")).getByRole("button", { name: "Delete" }));
    await user.click(within(await screen.findByRole("dialog")).getByRole("button", { name: "Delete" }));
    expect(await screen.findByText("Failed to delete forwarding rule (404)")).toBeInTheDocument();
    expect(document.querySelectorAll(".fwd-card")).toHaveLength(2);
  });

  it("toggles a rule by posting it back with enabled flipped, then reloads the list", async () => {
    let items = RULES;
    const fetchMock = server({
      ...listRoutes(),
      "GET /api/forwarding": () => jsonResponse({ ok: true, items }),
      // The server answers with the id only, like app.py api_save_forwarding.
      "POST /api/forwarding": (init) => {
        const saved = JSON.parse(String(init.body)) as ForwardingRule;
        items = items.map((r) => (r.rule_id === saved.rule_id ? saved : r));
        return jsonResponse({ ok: true, rule_id: saved.rule_id, guild_id: "" });
      },
    });
    const user = userEvent.setup();
    renderPage();
    await waitFor(() => expect(document.querySelectorAll(".fwd-card")).toHaveLength(2));
    await user.click(within(card("r1")).getByRole("button", { name: "Disable" }));
    await waitFor(() => expect(card("r1")).toHaveTextContent("Disabled"));
    const post = fetchMock.mock.calls.find(([, init]) => init?.method === "POST")!;
    expect(JSON.parse(String(post[1]?.body))).toMatchObject({ rule_id: "r1", label: "Support <b>pings</b>", enabled: false });
  });

  it("filters by search, shows no match, and Escape clears", async () => {
    server(listRoutes());
    const user = userEvent.setup();
    renderPage();
    await waitFor(() => expect(document.querySelectorAll(".fwd-card")).toHaveLength(2));
    const search = screen.getByLabelText("Search forwarding rules");
    await user.type(search, "urgent");
    expect(document.querySelectorAll(".fwd-card")).toHaveLength(1);
    await user.clear(search);
    await user.type(search, "nothing here");
    expect(screen.getByText("No rules matched")).toBeInTheDocument();
    await user.keyboard("{Escape}");
    expect(search).toHaveValue("");
    expect(document.querySelectorAll(".fwd-card")).toHaveLength(2);
  });

  it("routes New Forward, Edit and Duplicate to the lane 10 editor hook", async () => {
    server(listRoutes());
    const open = vi.fn();
    const user = userEvent.setup();
    renderPage(open);
    await waitFor(() => expect(document.querySelectorAll(".fwd-card")).toHaveLength(2));
    await user.click(screen.getByRole("button", { name: "New Forward" }));
    await user.click(within(card("r1")).getByRole("button", { name: "Edit" }));
    await user.click(within(card("r/2")).getByRole("button", { name: "Duplicate" }));
    expect(open.mock.calls.map(([req]) => req)).toEqual([
      { mode: "create" },
      { mode: "edit", rule: RULES[0] },
      { mode: "duplicate", rule: RULES[1] },
    ]);
  });
});

describe("forwarding helpers", () => {
  it("reads keyword lists from arrays or CSV and searches them", () => {
    expect(toList("a, b")).toEqual(["a", "b"]);
    expect(toList(undefined)).toEqual([]);
    const rules: ForwardingRule[] = [
      { rule_id: "x", label: "X", provider: "telegram", enabled: true, filters: { keywords_all: "alpha,beta" } },
    ];
    expect(filterRules(rules, "BETA")).toHaveLength(1);
    expect(filterRules(rules, "telegram")).toHaveLength(1);
    expect(filterRules(rules, "gamma")).toHaveLength(0);
  });
});
