// Polls /api/status and gates a page
// until the bot is connected to Discord.
//  - a recent "up" (sessionStorage bot:lastUpAt, 20 s) skips the gate on load
//  - otherwise the gate appears after 350 ms, so a fast "up" never flashes it
//  - while down or unreachable it re-checks every 5 s, while up every 10 s
//  - require: "either" (default), "both" or "server"
//  - onUp / onDown fire on transitions

import { useEffect, useRef, useState } from "react";

export type GateRequire = "either" | "both" | "server";

export interface StatusGateOptions {
  require?: GateRequire;
  gateDelayMs?: number;
  readyCacheKey?: string;
  readyTtlMs?: number;
  pollUpMs?: number;
  pollDownMs?: number;
  onUp?: (status: unknown) => void;
  onDown?: (status: unknown) => void;
  enabled?: boolean;
}

export interface StatusGateState {
  /** True while the page should be covered. */
  gated: boolean;
  /** True once a check has seen the bot up. */
  ready: boolean;
  serverReady: boolean;
  clientReady: boolean;
  message: string;
}

type Side = {
  ready?: unknown;
  status?: unknown;
  state?: unknown;
  discord?: { ready?: unknown; connected?: unknown; online?: unknown };
};

export function sideReady(side: Side | undefined | null): boolean {
  if (!side) return false;
  const status = String(side.status ?? side.state ?? "").toLowerCase();
  return (
    side.ready === true ||
    side.discord?.ready === true ||
    side.discord?.connected === true ||
    side.discord?.online === true ||
    status === "ready" ||
    status.includes("logged in as")
  );
}

export function evaluateStatus(json: unknown, require: GateRequire = "either") {
  const j = (json ?? {}) as { server?: Side; client?: Side };
  const serverReady = sideReady(j.server);
  const clientReady = sideReady(j.client);
  const isUp =
    require === "both" ? serverReady && clientReady : require === "server" ? serverReady : serverReady || clientReady;
  let message = "";
  if (!isUp) {
    if (require === "both") {
      if (!serverReady && !clientReady) message = "Waiting for Server and Client connection...";
      else if (!serverReady) message = "Waiting for Server to connect to Discord gateway...";
      else message = "Waiting for Client to connect to Discord gateway...";
    } else {
      message = "Bot is not connected to Discord yet...";
    }
  }
  return { isUp, serverReady, clientReady, message };
}

export const UNREACHABLE_MESSAGE = "Make sure Server and Client are started and connected to Discord.";

function lastUpIsFresh(key: string, ttl: number): boolean {
  try {
    const t = Number(sessionStorage.getItem(key) || 0);
    return t > 0 && Date.now() - t < ttl;
  } catch {
    return false;
  }
}

async function fetchStatus(): Promise<unknown | null> {
  try {
    const r = await fetch("/api/status", { cache: "no-store", credentials: "same-origin" });
    if (!r.ok) return null;
    return (await r.json()) as unknown;
  } catch {
    return null;
  }
}

export function useStatusGate(opts: StatusGateOptions = {}): StatusGateState {
  const {
    require = "either",
    gateDelayMs = 350,
    readyCacheKey = "bot:lastUpAt",
    readyTtlMs = 20000,
    pollUpMs = 10000,
    pollDownMs = 5000,
    enabled = true,
  } = opts;
  const cbs = useRef({ onUp: opts.onUp, onDown: opts.onDown });
  cbs.current = { onUp: opts.onUp, onDown: opts.onDown };

  const [state, setState] = useState<StatusGateState>({
    gated: false,
    ready: false,
    serverReady: false,
    clientReady: false,
    message: "Checking bot connection...",
  });

  useEffect(() => {
    if (!enabled) return;
    let stopped = false;
    let lastUp: boolean | null = null;
    let poll: ReturnType<typeof setTimeout> | undefined;
    const showTimer = lastUpIsFresh(readyCacheKey, readyTtlMs)
      ? undefined
      : setTimeout(() => setState((s) => (s.ready ? s : { ...s, gated: true })), gateDelayMs);

    const tick = async () => {
      const json = await fetchStatus();
      if (stopped) return;
      clearTimeout(showTimer);
      let up: boolean;
      if (json === null) {
        up = false;
        setState((s) => ({ ...s, gated: true, ready: false, message: UNREACHABLE_MESSAGE }));
      } else {
        const st = evaluateStatus(json, require);
        up = st.isUp;
        if (up) {
          try {
            sessionStorage.setItem(readyCacheKey, String(Date.now()));
          } catch {
            // Storage blocked: the gate just shows on the next load.
          }
        }
        setState({
          gated: !up,
          ready: up,
          serverReady: st.serverReady,
          clientReady: st.clientReady,
          message: up ? "" : st.message,
        });
      }
      if (lastUp !== up) {
        if (up) cbs.current.onUp?.(json);
        else cbs.current.onDown?.(json);
      }
      lastUp = up;
      poll = setTimeout(tick, up ? pollUpMs : pollDownMs);
    };
    void tick();

    return () => {
      stopped = true;
      clearTimeout(showTimer);
      clearTimeout(poll);
    };
  }, [enabled, require, gateDelayMs, readyCacheKey, readyTtlMs, pollUpMs, pollDownMs]);

  return state;
}
