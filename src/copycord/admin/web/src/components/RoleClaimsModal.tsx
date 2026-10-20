// "Redeem code": the Discord logo (the invite link) and a short pitch over one box for a personal code, which a
// member gets from the /redeem slash command in the Copycord Discord. Redeem (or pasting a full code) posts it to
// /api/redeem/start (contract in src/copycord/admin/role_claims.py) and the modal cross-fades to a spinner and
// "Verifying..." while it is out; the helper grants the role there and then, so there is no login window. The answer
// cross-fades to a result view: claimed (the gift), already claimed, or an error, each with an arrow Back to the form.
// Nothing here says what a code redeems. The code lives in this component's state only: never logged, never stored.

import { useCallback, useEffect, useRef, useState, type FormEvent, type ReactNode } from "react";
import { api, ApiError, errorText } from "../lib/api";
import { GiftAnimation } from "./GiftAnimation";
import { Modal } from "./Modal";
import { useToast } from "./Toast";
import { DISCORD } from "../pages/dashboard/config/PromoCards";
import { PROMO_LINKS } from "../pages/dashboard/config/model";
import { Button, Input, Skeleton, Spinner, useReveal } from "./ui";

export interface RoleClaimsConfig {
  configured: boolean;
}

/** A successful /api/redeem/start answer. */
export interface RedeemAnswer {
  status: string;
  message: string;
  reward: string;
}

/** Helper codes are 16 base32 characters shown as XXXX-XXXX-XXXX-XXXX. */
export const CODE_CHARS = 16;
export const CODE_MAX = CODE_CHARS + 3;

/** Uppercases, keeps only base32 characters, caps at CODE_CHARS and regroups in fours. */
export function formatCode(raw: string): string {
  const chars = raw.toUpperCase().replace(/[^A-Z2-7]/g, "").slice(0, CODE_CHARS);
  return chars.match(/.{1,4}/g)?.join("-") ?? "";
}

/** True when the code holds every character a helper code has. */
export function isCompleteCode(code: string): boolean {
  return code.replace(/-/g, "").length === CODE_CHARS;
}

type Load = { kind: "loading" } | { kind: "error"; message: string } | { kind: "ready"; data: RoleClaimsConfig };

/** The redeem in progress: waiting while the start POST is out, result once there is an outcome to show. */
type Flow = { kind: "idle" } | { kind: "waiting" } | { kind: "result" };

/** good: claimed; info: already claimed; danger: an error. */
export type ResultTone = "good" | "info" | "danger";

export interface Result {
  tone: ResultTone;
  title: string;
  /** A muted line under the title, when there is more to say. */
  detail?: string;
}

/** A claim (or already claimed) shows bigger and its Back returns to an empty form; an error's Back keeps the code. */
const isDone = (r: Result) => r.tone === "good" || r.tone === "info";

const TONE_TEXT: Record<ResultTone, string> = {
  good: "text-good",
  info: "text-info",
  danger: "text-danger",
};
/** GitHub Octicons 24 px, filled (MIT, primer/octicons): check-circle-fill for already claimed (and for claimed
 *  when the gift animation cannot load), x-circle-fill for errors. Claimed shows the gift animation, 96 px with
 *  -8 px margins. */
const CHECK_CIRCLE_FILL =
  "M1 12C1 5.925 5.925 1 12 1s11 4.925 11 11-4.925 11-11 11S1 18.075 1 12Zm16.28-2.72a.751.751 0 0 0-.018-1.042.751.751 0 0 0-1.042-.018l-5.97 5.97-2.47-2.47a.751.751 0 0 0-1.042.018.751.751 0 0 0-.018 1.042l3 3a.75.75 0 0 0 1.06 0Z";
const TONE_PATH: Record<ResultTone, string> = {
  good: CHECK_CIRCLE_FILL,
  info: CHECK_CIRCLE_FILL,
  danger:
    "M1 12C1 5.925 5.925 1 12 1s11 4.925 11 11-4.925 11-11 11S1 18.075 1 12Zm8.036-4.024a.751.751 0 0 0-1.042.018.751.751 0 0 0-.018 1.042L10.939 12l-2.963 2.963a.749.749 0 0 0 .326 1.275.749.749 0 0 0 .734-.215L12 13.06l2.963 2.964a.75.75 0 0 0 1.061-1.06L13.061 12l2.963-2.964a.749.749 0 0 0-.326-1.275.749.749 0 0 0-.734.215L12 10.939Z",
};

/** Octicons arrow-left 16 px (MIT, primer/octicons) on Back; it nudges left on hover (not under reduced motion). */
const ARROW_LEFT =
  "M7.78 12.53a.75.75 0 0 1-1.06 0L2.47 8.28a.75.75 0 0 1 0-1.06l4.25-4.25a.751.751 0 0 1 1.042.018.751.751 0 0 1 .018 1.042L4.81 7h7.44a.75.75 0 0 1 0 1.5H4.81l2.97 2.97a.75.75 0 0 1 0 1.06Z";

function BackArrow() {
  return (
    <svg
      data-role-claims-back-arrow
      viewBox="0 0 16 16"
      width="16"
      height="16"
      fill="currentColor"
      aria-hidden="true"
      focusable="false"
      className="transition-transform duration-150 ease-out group-hover:-translate-x-0.5 motion-reduce:transition-none motion-reduce:group-hover:translate-x-0"
    >
      <path d={ARROW_LEFT} />
    </svg>
  );
}

function ToneIcon({ tone }: { tone: ResultTone }) {
  return (
    <svg viewBox="0 0 24 24" width="48" height="48" fill="currentColor" aria-hidden="true" focusable="false">
      <path d={TONE_PATH[tone]} />
    </svg>
  );
}

const ALREADY: Result = { tone: "info", title: "Role already claimed" };
const CLAIMED = "Role claimed";

/** A start answer as a result: claimed, with the helper's message under the title when it says more. */
export function answerResult(answer: Partial<RedeemAnswer> | null | undefined): Result {
  if (answer?.status === "already") return ALREADY;
  const message = typeof answer?.message === "string" ? answer.message.trim() : "";
  return { tone: "good", title: CLAIMED, detail: message && message !== CLAIMED ? message : undefined };
}

/** A start error as a result: a 409 (or status "already") is already claimed, anything else an error. */
export function startErrorResult(err: unknown): Result {
  if (err instanceof ApiError) {
    const body = err.body && typeof err.body === "object" ? (err.body as { status?: unknown }) : {};
    if (err.status === 409 || body.status === "already") return ALREADY;
  }
  return { tone: "danger", title: errorText(err, "Could not redeem the code") };
}

type ToastApi = ReturnType<typeof useToast>;

/** The shared toast stack, or null when there is no provider (the shell tests render the menu without one). */
function useOptionalToast(): ToastApi | null {
  try {
    return useToast();
  } catch {
    return null;
  }
}

/**
 * The area every state renders into under the intro: the code box and Redeem, so swapping states in place
 * never moves the modal's edges.
 */
export const BODY = "h-[100px]";
/**
 * The form, waiting and result views share one grid cell, so the cell is always the form's height and the
 * modal keeps its size; the swap is an opacity cross-fade (180 ms, none under prefers-reduced-motion).
 */
export const VIEWS = "grid";
export const VIEW = "[grid-area:1/1] transition-opacity duration-[180ms] ease-out motion-reduce:transition-none";
/** Input plus its 2 px borders; Redeem matches it. */
const ROW = "h-[46px]";
/** The logo is the invite link: a 150 ms scale and soft blurple glow on hover (no scale under reduced motion), a focus ring.
 *  The glow stays small and the intro has top padding so the scrolling modal body never clips it. The intro has no
 *  side padding so its two line pitch keeps the modal's full width. */
const LOGO =
  "mb-3 inline-grid cursor-pointer place-items-center rounded-control text-discord outline-none transition duration-150 ease-out " +
  "hover:scale-[1.05] hover:drop-shadow-[0_0_5px_color-mix(in_srgb,var(--color-discord-glow-mid)_55%,transparent)] " +
  "focus-visible:ring-2 focus-visible:ring-discord/60 focus-visible:ring-offset-4 focus-visible:ring-offset-surface-deep " +
  "motion-reduce:transition-none motion-reduce:hover:scale-100";
/** Non form states sit centred in the body. */
const CENTRED = "flex h-full items-center justify-center";

export function RoleClaimsModal({ open, onClose }: { open: boolean; onClose: () => void }) {
  const toast = useOptionalToast();
  const [load, setLoad] = useState<Load>({ kind: "loading" });
  const [flow, setFlow] = useState<Flow>({ kind: "idle" });
  const [code, setCode] = useState("");
  const [invalid, setInvalid] = useState(false);
  /** The last outcome; kept after Back so the result view fades out with its content. */
  const [result, setResult] = useState<Result | null>(null);
  const inputRef = useRef<HTMLInputElement | null>(null);
  const waitingRef = useRef<HTMLDivElement | null>(null);
  const actionRef = useRef<HTMLButtonElement | null>(null);
  const flowRef = useRef(flow);
  flowRef.current = flow;
  /** Bumped on every redeem and on close, so a late start answer after the modal closed is dropped. */
  const attemptRef = useRef(0);

  const reveal = useReveal(load.kind !== "loading");

  const fetchConfig = useCallback(async (signal?: AbortSignal) => {
    setLoad({ kind: "loading" });
    try {
      const data = await api.get<RoleClaimsConfig>("/api/redeem/config", { signal });
      setLoad({ kind: "ready", data });
    } catch (e) {
      if (signal?.aborted) return;
      setLoad({ kind: "error", message: errorText(e, "Could not load") });
    }
  }, []);

  // Load on open; forget the code and drop any answer still out on close.
  useEffect(() => {
    if (!open) return;
    const ctl = new AbortController();
    void fetchConfig(ctl.signal);
    return () => {
      ctl.abort();
      attemptRef.current += 1;
      flowRef.current = { kind: "idle" };
      setFlow({ kind: "idle" });
      setResult(null);
      setCode("");
      setInvalid(false);
    };
  }, [open, fetchConfig]);

  const showResult = (next: Result) => {
    setResult(next);
    flowRef.current = { kind: "result" };
    setFlow({ kind: "result" });
  };

  const waiting = flow.kind === "waiting";
  const showing = flow.kind === "result";
  const formHidden = waiting || showing;

  // Focus follows the view: the waiting view while waiting, the result's button on a result, the code box once
  // the form is back.
  const wasAway = useRef(false);
  useEffect(() => {
    if (waiting) waitingRef.current?.focus();
    else if (showing) actionRef.current?.focus();
    else if (flow.kind === "idle" && wasAway.current) inputRef.current?.focus();
    wasAway.current = flow.kind !== "idle";
  }, [flow.kind, waiting, showing]);

  // Back goes to the form, the code kept after an error (cleared after a claim).
  const back = () => {
    if (result && isDone(result)) setCode("");
    flowRef.current = { kind: "idle" };
    setFlow({ kind: "idle" });
  };

  const submit = (e: FormEvent) => {
    e.preventDefault();
    void redeem(code);
  };

  const redeem = async (value: string) => {
    if (flowRef.current.kind !== "idle") return;
    const trimmed = formatCode(value);
    if (!trimmed) {
      setInvalid(true);
      toast?.show("Enter a code", { kind: "error" });
      inputRef.current?.focus();
      return;
    }
    setInvalid(false);
    const attempt = ++attemptRef.current;
    flowRef.current = { kind: "waiting" };
    setFlow({ kind: "waiting" });
    let answer: RedeemAnswer;
    try {
      answer = await api.post<RedeemAnswer>("/api/redeem/start", { code: trimmed });
    } catch (err) {
      if (attemptRef.current !== attempt) return;
      showResult(startErrorResult(err));
      return;
    }
    if (attemptRef.current !== attempt) return;
    const next = answerResult(answer);
    // A personal code is single use: a claimed (or already claimed) one is gone, so the box starts empty next time.
    if (isDone(next)) setCode("");
    showResult(next);
  };

  let body: ReactNode;
  if (load.kind === "loading") {
    body = (
      <div role="status" aria-label="Loading" aria-busy="true" className="flex flex-col gap-2">
        <Skeleton.Block width="100%" height={46} rounded="rounded-control" />
        <Skeleton.Block width="100%" height={46} rounded="rounded-control" />
      </div>
    );
  } else if (load.kind === "error") {
    body = (
      <div className={CENTRED}>
        <div role="alert" className="flex w-full items-center justify-between gap-3 text-sm text-danger">
          <span>{load.message}</span>
          <Button size="md" onClick={() => void fetchConfig()}>
            Retry
          </Button>
        </div>
      </div>
    );
  } else if (!load.data.configured) {
    body = (
      <div className={CENTRED}>
        <p className="m-0 text-center text-sm text-muted">Redeeming codes is not available on this install.</p>
      </div>
    );
  } else {
    body = (
      <form noValidate onSubmit={submit} aria-label="Redeem code" className="flex flex-col gap-2">
        <div className={`flex ${ROW}`}>
          <Input
            ref={inputRef}
            value={code}
            placeholder="Enter code"
            aria-label="Code"
            aria-invalid={invalid || undefined}
            maxLength={CODE_MAX}
            required
            aria-required="true"
            autoComplete="off"
            spellCheck={false}
            autoFocus
            readOnly={waiting}
            className="min-w-0 flex-1"
            onChange={(e) => {
              setCode(formatCode(e.target.value));
              if (invalid) setInvalid(false);
            }}
            onPaste={(e) => {
              // A pasted full code redeems at once; a partial one just fills the box.
              e.preventDefault();
              const pasted = formatCode(e.clipboardData.getData("text"));
              setCode(pasted);
              if (invalid) setInvalid(false);
              if (isCompleteCode(pasted)) void redeem(pasted);
            }}
          />
        </div>
        <Button type="submit" variant="primary" loading={waiting} className={`w-full ${ROW}`}>
          Redeem
        </Button>
      </form>
    );
  }

  return (
    <Modal open={open} onClose={onClose} title="Redeem code" id="role-claims-modal" className="w-[min(460px,92vw)]">
      <div data-role-claims-views className={VIEWS}>
        <div
          data-role-claims-view="form"
          aria-hidden={formHidden || undefined}
          inert={formHidden || undefined}
          className={`${VIEW} ${formHidden ? "opacity-0" : "opacity-100"}`}
        >
          <div data-role-claims-intro className="flex flex-col items-center pt-3 pb-5 text-center">
            <a
              data-role-claims-logo
              className={LOGO}
              href={PROMO_LINKS.discord}
              target="_blank"
              rel="noopener noreferrer"
              aria-label="Join the Copycord Discord server"
            >
              <svg viewBox="0 0 24 24" width="48" height="48" fill="currentColor" aria-hidden="true">
                <path d={DISCORD} />
              </svg>
            </a>
            <h3 className="m-0 text-lg font-bold text-text">Unlock exclusive rewards</h3>
            <p className="mt-1.5 mb-0 text-sm leading-relaxed text-muted">
              Join the Copycord Community Discord server to claim codes that unlock exclusive rewards.
            </p>
          </div>
          <div data-role-claims-body className={BODY}>
            <div className={`h-full ${reveal ?? ""}`}>{body}</div>
          </div>
        </div>
        <div
          ref={waitingRef}
          data-role-claims-view="waiting"
          tabIndex={-1}
          aria-hidden={!waiting || undefined}
          inert={!waiting || undefined}
          className={`${VIEW} flex flex-col items-center justify-center gap-4 text-center outline-none ${waiting ? "opacity-100" : "pointer-events-none opacity-0"}`}
        >
          {waiting ? (
            <>
              <Spinner size="xl" label={null} />
              <p role="status" className="m-0 text-base font-semibold text-text">
                Verifying...
              </p>
            </>
          ) : null}
        </div>
        <div
          data-role-claims-view="result"
          aria-hidden={!showing || undefined}
          inert={!showing || undefined}
          className={`${VIEW} relative flex flex-col items-center justify-center px-4 text-center ${result ? "pt-8" : ""} ${showing ? "opacity-100" : "pointer-events-none opacity-0"}`}
        >
          {result ? (
            <>
              <button
                ref={actionRef}
                type="button"
                className="group absolute left-0 top-0 inline-flex cursor-pointer items-center gap-1.5 rounded-md px-2 py-1 text-sm font-medium text-muted outline-none transition-colors duration-150 hover:bg-white/5 hover:text-text focus-visible:ring-2 focus-visible:ring-accent/60 motion-reduce:transition-none"
                onClick={back}
              >
                <BackArrow />
                Back
              </button>
              <span
                data-role-claims-icon={result.tone}
                className={`mb-4 grid place-items-center ${TONE_TEXT[result.tone]} transition-transform duration-[180ms] ease-out motion-reduce:transition-none ${showing ? "scale-100" : "scale-90 motion-reduce:scale-100"}`}
              >
                {result.tone === "good" ? (
                  <GiftAnimation className="size-24! -m-2" fallback={<ToneIcon tone="good" />} />
                ) : (
                  <ToneIcon tone={result.tone} />
                )}
              </span>
              <div role={result.tone === "danger" ? "alert" : "status"} className="flex flex-col items-center">
                <h3
                  className={`m-0 line-clamp-2 ${isDone(result) ? "text-2xl" : "text-lg"} font-bold break-words text-text`}
                >
                  {result.title}
                </h3>
                {result.detail ? (
                  <p
                    className={`mt-1.5 mb-0 line-clamp-2 ${isDone(result) ? "text-base" : "text-sm"} leading-relaxed break-words text-muted`}
                  >
                    {result.detail}
                  </p>
                ) : null}
              </div>
            </>
          ) : null}
        </div>
      </div>
    </Modal>
  );
}
