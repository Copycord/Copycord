import { render, screen, waitFor } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { ToastProvider } from "../../components/Toast";
import { ConfirmProvider } from "../../components/ui/ConfirmDialog";
import { holdFetch } from "../../test/holdFetch";
import { cells, shared, structure } from "../../test/structure";
import type { ForwardingRule } from "../../types/forwarding";
import { Forwarding } from "../Forwarding";
import { ForwardingSkeleton, RULES_COUNT_KEY } from "./ForwardingSkeleton";

const RULE: ForwardingRule = { rule_id: "r1", label: "E2E Forward", provider: "discord", enabled: true, filters: {} };

const json = (body: unknown) => new Response(JSON.stringify(body), { headers: { "content-type": "application/json" } });

function serve(items: ForwardingRule[]) {
  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input);
      if (url === "/api/forwarding") return json({ ok: true, items });
      if (url === "/api/forwarding/count") return json({ ok: true, count: 42, by_provider: {} });
      if (url === "/api/forwarding/count/by-rule") return json({ ok: true, by_rule: { r1: 42 } });
      if (url === "/api/config") return json({ CLIENT_TOKEN: "tok" });
      return new Response("{}", { status: 404 });
    }),
  );
}

const page = () =>
  render(
    <ToastProvider>
      <ConfirmProvider>
        <Forwarding openRuleEditor={() => {}} />
      </ConfirmProvider>
    </ToastProvider>,
  );

// The rule tiles of both states; the toolbar is compared as chrome. The skeleton's toolbar is aria-hidden
// and inert but has the same controls.
const ROWS = ".fwd-card, [data-skeleton-row]";
const TILE_HOOKS = ["fwd-card", "fwd-card-title", "fwd-edit-btn", "fwd-toggle-btn", "fwd-duplicate-btn", "fwd-delete-btn"];

beforeEach(() => serve([RULE]));

describe("Forwarding skeleton", () => {
  it("has the loaded page's title, toolbar and one tile per rule", async () => {
    const skel = render(<ForwardingSkeleton />);
    const skeleton = structure(skel.container, ROWS);
    expect(skel.container.querySelectorAll("[data-skeleton-row]")).toHaveLength(1);
    skel.unmount();

    const release = holdFetch("/api/forwarding");
    const view = page();
    expect(structure(view.container, ROWS)).toEqual(skeleton);
    release();
    await waitFor(() => expect(view.container.querySelectorAll(".fwd-card")).toHaveLength(1));
    const loaded = structure(view.container, ROWS);

    expect(loaded).toEqual(skeleton);
    expect(loaded).toMatchObject({
      headings: ["h2 Forwarding", "h1 Forwarding"],
      controls: ["input:text Search forwarding rules", "button New Forward"],
    });
  });

  it("draws a loading tile with the boxes of a rule tile", async () => {
    const skel = render(<ForwardingSkeleton />);
    const tile = skel.container.querySelector("[data-skeleton-row]")!;
    const skelShape = {
      tile: shared(tile),
      parts: cells(tile),
      head: cells(tile.children[0]!),
      foot: cells(tile.children[1]!),
      badges: tile.children[0]!.children[1]!.children.length,
    };
    skel.unmount();

    const view = page();
    await waitFor(() => expect(view.container.querySelectorAll(".fwd-card")).toHaveLength(1));
    const real = view.container.querySelector(".fwd-card")!;
    expect(shared(real, TILE_HOOKS)).toBe(skelShape.tile);
    expect(cells(real)).toEqual(skelShape.parts);
    expect(cells(real.children[0]!)).toEqual(skelShape.head);
    expect(cells(real.children[1]!, TILE_HOOKS)).toEqual(skelShape.foot);
    // The provider badge and the status pill.
    expect(real.children[0]!.children[1]!.children).toHaveLength(skelShape.badges);
  });

  it("draws as many tiles as the last load had, and the empty block after an empty one", async () => {
    localStorage.setItem(`skel:${RULES_COUNT_KEY}`, "3");
    const three = render(<ForwardingSkeleton />);
    expect(three.container.querySelectorAll("[data-skeleton-row]")).toHaveLength(3);
    three.unmount();

    serve([]);
    const view = page();
    await waitFor(() => expect(document.getElementById("forwarding-empty")).toBeVisible());
    // The count is stored in an effect after the commit that shows the empty block.
    await waitFor(() => expect(localStorage.getItem(`skel:${RULES_COUNT_KEY}`)).toBe("0"));
    view.unmount();
    const none = render(<ForwardingSkeleton />);
    expect(none.container.querySelectorAll("[data-skeleton-row]")).toHaveLength(0);
    expect(screen.getByRole("button", { name: "+ Create your first rule", hidden: true })).toBeInTheDocument();
  });
});
