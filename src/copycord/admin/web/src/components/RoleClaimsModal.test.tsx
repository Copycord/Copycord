import { act, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { useState } from "react";
import { afterEach, beforeEach, describe, expect, it, vi, type MockInstance } from "vitest";
import { BODY, CODE_MAX, formatCode, RoleClaimsModal, VIEW, VIEWS, type RoleClaimsConfig } from "./RoleClaimsModal";
import { DISCORD } from "../pages/dashboard/config/PromoCards";
import { PROMO_LINKS } from "../pages/dashboard/config/model";
import { ToastProvider } from "./Toast";
import gift from "../assets/gift.json";

// jsdom cannot run lottie: the player is a mock that draws an empty svg into its container, as the real one does.
const lottie = vi.hoisted(() => {
  const destroy = vi.fn();
  const goToAndStop = vi.fn();
  const loadAnimation = vi.fn(({ container }: { container: Element }) => {
    container.appendChild(document.createElementNS("http://www.w3.org/2000/svg", "svg"));
    return { destroy, goToAndStop };
  });
  return { loadAnimation, destroy, goToAndStop };
});
vi.mock("lottie-web/build/player/lottie_light", () => ({ default: { loadAnimation: lottie.loadAnimation } }));

const CONFIG: RoleClaimsConfig = { configured: true };
const CLAIMED = { status: "claimed", message: "Role claimed", reward: "role" };
const INTRO = "Join the Copycord Community Discord server to claim codes that unlock exclusive rewards.";

const json = (body: unknown, status = 200) =>
  new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json" } });

/** Answers the config GET with `config` and the start POST with `start`; returns the fetch mock. */
function mockApi(config: RoleClaimsConfig | Response = CONFIG, start: () => Response | Promise<Response> = () => json(CLAIMED)) {
  const fn = vi.fn(async (input: RequestInfo | URL, _init?: RequestInit) => {
    const url = String(input);
    if (url === "/api/redeem/config") return config instanceof Response ? config : json(config);
    if (url === "/api/redeem/start") return start();
    return json({ detail: "unexpected" }, 404);
  });
  vi.stubGlobal("fetch", fn);
  return fn;
}

/** A start POST that stays out until answer() is called. */
function heldStart() {
  let answer!: (r: Response) => void;
  const start = () => new Promise<Response>((r) => (answer = r));
  return { start, answer: (r: Response) => act(async () => answer(r)) };
}

// There is no login window any more: window.open must never be called.
let open: MockInstance<typeof window.open>;
beforeEach(() => {
  open = vi.spyOn(window, "open").mockReturnValue(null);
});
afterEach(() => {
  expect(open).not.toHaveBeenCalled();
  open.mockRestore();
});

/** The modal with its own open state, as the side menu holds it, plus a button to open it again. */
function Harness({ onClose }: { onClose: () => void }) {
  const [isOpen, setOpen] = useState(true);
  return (
    <ToastProvider>
      <button type="button" onClick={() => setOpen(true)}>
        Reopen
      </button>
      <RoleClaimsModal
        open={isOpen}
        onClose={() => {
          onClose();
          setOpen(false);
        }}
      />
    </ToastProvider>
  );
}

function setup() {
  const user = userEvent.setup();
  const onClose = vi.fn();
  render(<Harness onClose={onClose} />);
  return { user, onClose };
}

const toasts = () => within(document.getElementById("toast-root")!);
const dialog = () => screen.queryByRole("dialog", { name: "Redeem code" });
const submitButton = () => screen.getByRole("button", { name: /^Redeem/ });

async function submitCode(code = "  abcd-2345  ") {
  const ctx = setup();
  await ctx.user.type(await screen.findByPlaceholderText("Enter code"), code);
  await ctx.user.click(submitButton());
  return ctx;
}

const verifying = () => screen.queryByText("Verifying...");

const view = (name: string) => dialog()!.querySelector<HTMLElement>(`[data-role-claims-view="${name}"]`)!;

/** Waits for the result view to show `title`, checks it is the only visible view, and returns it. */
async function resultView(title: string, tone: string) {
  const heading = await screen.findByRole("heading", { name: title }, { timeout: 2000 });
  const result = view("result");
  expect(result).toContainElement(heading);
  expect(result).toHaveClass("opacity-100");
  expect(result).not.toHaveAttribute("aria-hidden");
  // The claimed gift draws its svg once the lazy player has loaded.
  await waitFor(() => expect(result.querySelector(`[data-role-claims-icon="${tone}"] svg`)).toBeInTheDocument());
  expect(view("form")).toHaveClass("opacity-0");
  expect(view("form")).toHaveAttribute("inert");
  expect(view("waiting")).toBeEmptyDOMElement();
  expect(toasts().queryAllByRole("alert")).toEqual([]);
  expect(document.querySelectorAll("#toast-root .toast")).toHaveLength(0);
  return result;
}

const LOGO_LABEL = "Join the Copycord Discord server";

describe("RoleClaimsModal", () => {
  it("shows the /redeem pitch under the Discord logo, which is the invite link in a new tab, and no Join button", async () => {
    mockApi();
    setup();
    await screen.findByPlaceholderText("Enter code");
    const modal = dialog()!;
    const logo = within(modal).getByRole("link", { name: LOGO_LABEL });
    expect(logo).toHaveAttribute("data-role-claims-logo");
    expect(logo).toHaveClass("text-discord", "hover:scale-[1.05]", "duration-150", "motion-reduce:hover:scale-100");
    expect(logo).not.toHaveClass("rounded-full");
    expect(logo.className).not.toMatch(/\bbg-/);
    expect(logo.className).toMatch(/focus-visible:ring-2/);
    expect(logo.querySelector("svg path")).toHaveAttribute("d", DISCORD);
    expect(logo).toHaveAttribute("href", PROMO_LINKS.discord);
    expect(logo).toHaveAttribute("href", "https://discord.gg/ArFdqrJHBj");
    expect(logo).toHaveAttribute("target", "_blank");
    expect(logo).toHaveAttribute("rel", "noopener noreferrer");
    expect(logo).toHaveAttribute("aria-label", LOGO_LABEL);
    expect(within(modal).getByRole("heading", { name: "Unlock exclusive rewards" })).toBeInTheDocument();
    expect(within(modal).getByText(INTRO)).toHaveClass("text-sm", "text-muted");
    expect(within(modal).getAllByRole("link")).toEqual([logo]);
    expect(within(modal).queryByText("Join the Discord")).not.toBeInTheDocument();
    expect(within(modal).queryByRole("button", { name: /Join/ })).not.toBeInTheDocument();
  });

  it("keeps the intro and the logo link when not configured", async () => {
    mockApi({ configured: false });
    setup();
    await screen.findByText("Redeeming codes is not available on this install.");
    expect(screen.getByText(INTRO)).toBeInTheDocument();
    expect(screen.getByRole("link", { name: LOGO_LABEL })).toHaveAttribute("href", PROMO_LINKS.discord);
  });

  it("is a title, one code box and Redeem, and nothing else", async () => {
    mockApi();
    setup();
    expect(screen.getByRole("status", { name: "Loading" })).toBeInTheDocument();
    const input = await screen.findByPlaceholderText("Enter code");
    const modal = dialog()!;
    expect(within(modal).getByRole("heading", { name: "Redeem code" })).toBeInTheDocument();
    expect(input).toHaveAttribute("maxlength", "19");
    expect(input).toHaveAttribute("autocomplete", "off");
    expect(within(modal).getAllByRole("textbox")).toEqual([input]);
    const buttons = within(modal)
      .getAllByRole("button")
      .map((b) => b.getAttribute("aria-label") ?? b.textContent);
    expect(buttons.filter((b) => b !== "Close")).toEqual(["Redeem"]);
    expect(modal.querySelector("[data-role-claims-body]")?.textContent).toBe("Redeem");
  });

  it("shows muted text in place of the form when not configured", async () => {
    const fetch = mockApi({ configured: false });
    setup();
    const text = await screen.findByText("Redeeming codes is not available on this install.");
    expect(text).toHaveClass("text-muted");
    expect(screen.queryByPlaceholderText("Enter code")).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Redeem" })).not.toBeInTheDocument();
    expect(fetch).toHaveBeenCalledTimes(1);
  });

  it("requires a code: a blank or spaces only code never calls start", async () => {
    const fetch = mockApi();
    await submitCode("   ");
    expect(await toasts().findByRole("alert")).toHaveTextContent("Enter a code");
    expect(screen.getByPlaceholderText("Enter code")).toHaveAttribute("aria-invalid", "true");
    expect(fetch.mock.calls.map(([u]) => String(u))).toEqual(["/api/redeem/config"]);
  });

  it("posts the cleaned code (uppercase, base32 only, grouped in fours) once", async () => {
    const fetch = mockApi();
    await submitCode();
    await resultView("Role claimed", "good");
    const calls = fetch.mock.calls.filter(([u]) => String(u) === "/api/redeem/start");
    expect(calls).toHaveLength(1);
    const init = calls[0]![1] as RequestInit;
    expect(init.method).toBe("POST");
    expect(JSON.parse(String(init.body))).toEqual({ code: "ABCD-2345" });
  });

  it("while start is out: only the purple Spinner and Verifying..., no Cancel, the form hidden with its code", async () => {
    const held = heldStart();
    const fetch = mockApi(CONFIG, held.start);
    await submitCode("ABCD");
    await screen.findByText("Verifying...");
    expect(fetch.mock.calls.map(([u]) => String(u))).toEqual(["/api/redeem/config", "/api/redeem/start"]);
    const modal = dialog()!;
    const waiting = view("waiting");
    const form = view("form");
    const spinner = waiting.querySelector("[data-spinner]")!;
    expect(spinner).toHaveClass("animate-spin", "border-t-accent", "size-14");
    expect(within(waiting).getByRole("status")).toHaveTextContent("Verifying...");
    expect(waiting.textContent).toBe("Verifying...");
    expect(within(waiting).queryAllByRole("button")).toEqual([]);
    expect(within(waiting).queryAllByRole("link")).toEqual([]);
    expect(screen.queryByText("Cancel")).not.toBeInTheDocument();
    expect(waiting).toHaveClass("opacity-100");
    expect(waiting).not.toHaveAttribute("aria-hidden");
    expect(form).toHaveClass("opacity-0");
    expect(form).toHaveAttribute("aria-hidden", "true");
    expect(form).toHaveAttribute("inert");
    expect(within(modal).queryByRole("heading", { name: "Unlock exclusive rewards" })).not.toBeInTheDocument();
    expect(screen.getByPlaceholderText("Enter code")).toHaveValue("ABCD");
    expect(screen.getByPlaceholderText("Enter code")).toHaveAttribute("readonly");
    expect(waiting).toHaveFocus();
    await held.answer(json(CLAIMED));
    await resultView("Role claimed", "good");
    expect(verifying()).not.toBeInTheDocument();
  });

  it("keeps the modal the same height across the form, waiting and result views", async () => {
    const held = heldStart();
    mockApi(CONFIG, held.start);
    const { user } = setup();
    await user.type(await screen.findByPlaceholderText("Enter code"), "ABCD");
    const modal = dialog()!;
    const views = modal.querySelector<HTMLElement>("[data-role-claims-views]")!;
    const body = views.querySelector<HTMLElement>("[data-role-claims-body]")!;
    // All three views sit in the one grid cell, so the form, mounted in every state, always sets the height.
    const shape = () => ({
      views: views.className,
      kids: Array.from(views.children).map((el) => [
        el.getAttribute("data-role-claims-view"),
        el.className.split(" ").filter((c) => VIEW.split(" ").includes(c)).join(" "),
      ]),
      formKids: Array.from(views.children[0]!.children).map((el) => el.className),
      body: body.className,
      transitions: Array.from(views.children).every((el) =>
        ["transition-opacity", "motion-reduce:transition-none"].every((c) => el.classList.contains(c)),
      ),
    });
    const idle = shape();
    expect(idle.views).toBe(VIEWS);
    expect(VIEWS).toBe("grid");
    expect(idle.kids).toEqual([
      ["form", VIEW],
      ["waiting", VIEW],
      ["result", VIEW],
    ]);
    expect(VIEW).toContain("[grid-area:1/1]");
    expect(idle.body).toBe(BODY);
    expect(idle.transitions).toBe(true);
    expect(view("waiting")).toBeEmptyDOMElement();
    expect(view("result")).toBeEmptyDOMElement();
    await user.click(submitButton());
    await screen.findByText("Verifying...");
    expect(shape()).toEqual(idle);
    expect(views.querySelector("form")).toBeInTheDocument();
    await held.answer(json(CLAIMED));
    await resultView("Role claimed", "good");
    expect(shape()).toEqual(idle);
    expect(views.querySelector("form")).toBeInTheDocument();
  });

  it("claimed: the gift plays and Done closes the modal; reopening starts empty", async () => {
    mockApi();
    lottie.loadAnimation.mockClear();
    lottie.destroy.mockClear();
    lottie.goToAndStop.mockClear();
    const { user, onClose } = await submitCode("ABCD-EFGH-2345-67XY");
    const result = await resultView("Role claimed", "good");
    expect(result.querySelector("[data-role-claims-icon]")).toHaveClass("text-good");
    // The helper's message equals the title, so it is not repeated.
    expect(within(result).getAllByText("Role claimed")).toHaveLength(1);
    const box = result.querySelector<HTMLElement>('[data-role-claims-icon="good"] [data-gift-animation]')!;
    expect(box).toHaveAttribute("aria-hidden", "true");
    expect(box).toHaveClass("size-16", "-m-2");
    expect(lottie.loadAnimation).toHaveBeenCalledTimes(1);
    expect(lottie.loadAnimation).toHaveBeenCalledWith({
      container: box,
      renderer: "svg",
      loop: false,
      autoplay: true,
      animationData: gift,
    });
    expect(lottie.goToAndStop).not.toHaveBeenCalled();
    expect(lottie.destroy).not.toHaveBeenCalled();
    const done = within(result).getByRole("button", { name: "Done" });
    await waitFor(() => expect(done).toHaveFocus());
    expect(within(result).queryByRole("button", { name: "Try again" })).not.toBeInTheDocument();
    expect(onClose).not.toHaveBeenCalled();
    await user.click(done);
    expect(onClose).toHaveBeenCalledTimes(1);
    await waitFor(() => expect(dialog()).not.toBeInTheDocument());
    await waitFor(() => expect(lottie.destroy).toHaveBeenCalledTimes(1));
    await user.click(screen.getByRole("button", { name: "Reopen" }));
    expect(await screen.findByPlaceholderText("Enter code")).toHaveValue("");
    expect(view("result")).toBeEmptyDOMElement();
  });

  it("claimed: a helper message other than the title shows under it", async () => {
    mockApi(CONFIG, () => json({ ...CLAIMED, message: "Beta Tester added" }));
    await submitCode("ABCD");
    const result = await resultView("Role claimed", "good");
    expect(within(result).getByText("Beta Tester added")).toHaveClass("text-muted");
  });

  it("claimed under prefers-reduced-motion: the gift still plays", async () => {
    mockApi();
    lottie.loadAnimation.mockClear();
    lottie.goToAndStop.mockClear();
    Object.defineProperty(window, "matchMedia", {
      configurable: true,
      value: (query: string) => ({ matches: query === "(prefers-reduced-motion: reduce)" }),
    });
    try {
      await submitCode("ABCD");
      await resultView("Role claimed", "good");
      expect(lottie.loadAnimation).toHaveBeenCalledWith(expect.objectContaining({ autoplay: true, loop: false }));
      expect(lottie.goToAndStop).not.toHaveBeenCalled();
    } finally {
      delete (window as { matchMedia?: unknown }).matchMedia;
    }
  });

  it("claimed: the check icon stands in when the gift cannot load", async () => {
    mockApi();
    lottie.loadAnimation.mockImplementationOnce(() => {
      throw new Error("no lottie");
    });
    await submitCode("ABCD");
    const result = await resultView("Role claimed", "good");
    const icon = result.querySelector('[data-role-claims-icon="good"]')!;
    expect(icon.querySelector("[data-gift-animation]")).not.toBeInTheDocument();
    expect(icon.querySelector("svg path")?.getAttribute("d")).toMatch(/^M1 12C1 5\.925/);
  });

  it.each([
    ["with status already", { detail: "Role already claimed", status: "already" }],
    ["without status", { detail: "Conflict" }],
  ])("a 409 %s shows the already view, its icon, no gift, and Done closes", async (_case, body) => {
    mockApi(CONFIG, () => json(body, 409));
    lottie.loadAnimation.mockClear();
    const { user, onClose } = await submitCode("ABCD");
    const result = await resultView("Role already claimed", "info");
    expect(result.querySelector("[data-role-claims-icon]")).toHaveClass("text-info");
    expect(result.querySelector("[data-gift-animation]")).not.toBeInTheDocument();
    expect(lottie.loadAnimation).not.toHaveBeenCalled();
    expect(within(result).queryByRole("button", { name: "Try again" })).not.toBeInTheDocument();
    expect(onClose).not.toHaveBeenCalled();
    await user.click(within(result).getByRole("button", { name: "Done" }));
    expect(onClose).toHaveBeenCalledTimes(1);
  });

  it("an invalid code: the error view with the detail, and Try again goes back to the form with the code", async () => {
    mockApi(CONFIG, () => json({ detail: "Invalid code" }, 400));
    lottie.loadAnimation.mockClear();
    const { user, onClose } = await submitCode("WRONG");
    const result = await resultView("Invalid code", "danger");
    expect(result.querySelector("[data-role-claims-icon]")).toHaveClass("text-danger");
    expect(within(result).getByRole("alert")).toHaveTextContent("Invalid code");
    expect(lottie.loadAnimation).not.toHaveBeenCalled();
    expect(within(result).queryByRole("button", { name: "Done" })).not.toBeInTheDocument();
    const again = within(result).getByRole("button", { name: "Try again" });
    await waitFor(() => expect(again).toHaveFocus());
    expect(onClose).not.toHaveBeenCalled();
    await user.click(again);
    expect(screen.getByPlaceholderText("Enter code")).toHaveValue("WRON-G");
    expect(submitButton()).toBeEnabled();
    expect(view("form")).toHaveClass("opacity-100");
    expect(view("result")).toHaveClass("opacity-0");
    expect(view("result")).toHaveAttribute("inert");
    await waitFor(() => expect(screen.getByPlaceholderText("Enter code")).toHaveFocus());
  });

  it.each([
    ["not a member", 400, "Join the Copycord Discord server first"],
    ["a failed role add", 502, "Could not add the role, try again later"],
    ["a down server", 502, "Server is down"],
    ["rate limiting", 429, "Too many attempts, try again later"],
  ])("%s: the error view with the detail and Try again", async (_case, status, detail) => {
    mockApi(CONFIG, () => json({ detail }, status));
    const { user, onClose } = await submitCode("ABCD");
    const result = await resultView(detail, "danger");
    expect(within(result).getByRole("alert")).toHaveTextContent(detail);
    expect(within(result).queryByRole("button", { name: "Done" })).not.toBeInTheDocument();
    await user.click(within(result).getByRole("button", { name: "Try again" }));
    expect(screen.getByPlaceholderText("Enter code")).toHaveValue("ABCD");
    expect(onClose).not.toHaveBeenCalled();
  });

  it("a late answer after the modal closed is dropped", async () => {
    const held = heldStart();
    mockApi(CONFIG, held.start);
    const { user } = await submitCode("ABCD");
    await screen.findByText("Verifying...");
    await user.keyboard("{Escape}");
    await waitFor(() => expect(dialog()).not.toBeInTheDocument());
    await held.answer(json(CLAIMED));
    await user.click(screen.getByRole("button", { name: "Reopen" }));
    expect(await screen.findByPlaceholderText("Enter code")).toHaveValue("");
    expect(view("form")).toHaveClass("opacity-100");
    expect(view("result")).toBeEmptyDOMElement();
  });

  it("never logs the code", async () => {
    mockApi(CONFIG, () => json({ detail: "Invalid code" }, 400));
    const spies = (["log", "info", "warn", "error", "debug"] as const).map((k) => vi.spyOn(console, k));
    const setItem = vi.spyOn(Storage.prototype, "setItem");
    await submitCode("SECRETCODE234567");
    await screen.findByRole("heading", { name: "Invalid code" });
    for (const spy of spies) {
      expect(spy.mock.calls.flat().map(String).join(" ")).not.toContain("SECRETCODE234567");
      expect(spy.mock.calls.flat().map(String).join(" ")).not.toContain("SECR-ETCO-DE23-4567");
    }
    expect(setItem).not.toHaveBeenCalled();
  });

  it("formatCode uppercases, keeps base32 only, caps at 16 characters and groups in fours", () => {
    expect(formatCode(" abcd efgh-2345 67xy zz ")).toBe("ABCD-EFGH-2345-67XY");
    expect(formatCode("ab01-89cd")).toBe("ABCD");
    expect(formatCode("")).toBe("");
    expect(CODE_MAX).toBe(19);
  });

  it("typing stops at a full code", async () => {
    mockApi();
    const { user } = setup();
    const input = await screen.findByPlaceholderText("Enter code");
    expect(input).toHaveAttribute("maxlength", "19");
    await user.type(input, "ABCDEFGHJKLMNPQRSTUV");
    expect(input).toHaveValue("ABCD-EFGH-JKLM-NPQR");
  });

  it("pasting a full code redeems it at once: Verifying..., then claimed", async () => {
    const held = heldStart();
    const fetch = mockApi(CONFIG, held.start);
    const { user } = setup();
    const input = await screen.findByPlaceholderText("Enter code");
    await user.click(input);
    await user.paste("  abcd-efgh-2345-67xy\n");
    expect(input).toHaveValue("ABCD-EFGH-2345-67XY");
    await screen.findByText("Verifying...");
    const call = fetch.mock.calls.find(([u]) => String(u) === "/api/redeem/start")!;
    expect(JSON.parse(String((call[1] as RequestInit).body))).toEqual({ code: "ABCD-EFGH-2345-67XY" });
    await held.answer(json(CLAIMED));
    await resultView("Role claimed", "good");
  });

  it("pasting a partial code only fills the box", async () => {
    const fetch = mockApi();
    const { user } = setup();
    const input = await screen.findByPlaceholderText("Enter code");
    await user.click(input);
    await user.paste("abcd-efgh");
    expect(input).toHaveValue("ABCD-EFGH");
    expect(fetch.mock.calls.map(([u]) => String(u))).toEqual(["/api/redeem/config"]);
  });
});
