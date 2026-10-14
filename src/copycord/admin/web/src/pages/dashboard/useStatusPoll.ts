// The /api/status poll from app.js (startStatusPoll and burstStatusPoll):
// every 4 s, 8 s while a role reports an error, doubling up to 15 s while the
// fetch fails, and every 800 ms for 15 s after Start or Stop. It stops while
// the tab is hidden and polls at once when the tab comes back.

import { useCallback, useEffect, useRef } from "react";

export const POLL_MS = 4000;
export const DEGRADED_POLL_MS = 8000;
export const MAX_POLL_MS = 15000;
export const BURST_POLL_MS = 800;
export const BURST_FOR_MS = 15000;

/** What one poll found: both roles fine, a role reporting an error, or no answer. */
export type PollResult = "ok" | "degraded" | "failed";

export function nextPollDelay(prev: number, result: PollResult): number {
  if (result === "failed") return Math.min(prev * 2, MAX_POLL_MS);
  return result === "degraded" ? DEGRADED_POLL_MS : POLL_MS;
}

/** Polls with `poll` on the app.js schedule; returns a function that starts the fast burst. */
export function useStatusPoll(poll: () => Promise<PollResult>): () => void {
  const timer = useRef<ReturnType<typeof setTimeout> | undefined>(undefined);
  const delay = useRef(POLL_MS);
  const burstUntil = useRef(0);
  const arm = useRef<() => void>(() => {});

  useEffect(() => {
    let alive = true;
    const schedule = () => {
      clearTimeout(timer.current);
      if (!alive || document.hidden) return;
      timer.current = setTimeout(tick, Date.now() < burstUntil.current ? BURST_POLL_MS : delay.current);
    };
    const tick = async () => {
      const result = await poll();
      delay.current = nextPollDelay(delay.current, result);
      schedule();
    };
    const onVisibility = () => {
      if (document.hidden) clearTimeout(timer.current);
      else void tick();
    };
    arm.current = schedule;
    document.addEventListener("visibilitychange", onVisibility);
    schedule();
    return () => {
      alive = false;
      arm.current = () => {};
      clearTimeout(timer.current);
      document.removeEventListener("visibilitychange", onVisibility);
    };
  }, [poll]);

  return useCallback(() => {
    burstUntil.current = Date.now() + BURST_FOR_MS;
    arm.current();
  }, []);
}
