// The server's warm token pool as /api/status reports it, on the Dashboard status poll schedule.
// Stale while revalidate: a failed or slow poll keeps the last answer on screen. `pool` is null
// while the pool has not started, `loaded` turns true with the first answer.
//   const { pool, loaded, refresh } = useTokenPool(Boolean(mappingId));

import { useCallback, useEffect, useRef, useState } from "react";
import { api } from "../../../../lib/api";
import type { StatusResponse, TokenPoolEntry, TokenPoolStatus } from "../../../../types/dashboard";
import { STATUS_URL } from "../../model";
import { useStatusPoll } from "../../useStatusPoll";
import type { PollResult } from "../../useStatusPoll";

export type TokenDot = "online" | "busy" | "invisible" | "connecting" | "offline" | null;

/** The chip dot of one token: none for a dead token or one the pool does not hold. */
export function tokenDot(entry: TokenPoolEntry | undefined): TokenDot {
  if (!entry || entry.dead) return null;
  if (entry.state === "connecting") return "connecting";
  if (entry.state !== "online") return "offline";
  if (entry.presence === "dnd") return "busy";
  return entry.presence === "invisible" ? "invisible" : "online";
}

export function poolFrom(res: StatusResponse | null | undefined): TokenPoolStatus | null {
  const tp = res?.server?.token_pool;
  return tp && typeof tp === "object" && tp.tokens && typeof tp.tokens === "object" ? tp : null;
}

/** Tokens of this mapping the pool reports online. */
export function onlineCount(pool: TokenPoolStatus | null, ids: readonly string[]): number {
  if (!pool) return 0;
  return ids.filter((id) => pool.tokens[id]?.state === "online").length;
}

export interface TokenPoolState {
  pool: TokenPoolStatus | null;
  loaded: boolean;
  /** Re-polls now and fast for a short while, after a token change. */
  refresh: () => void;
}

export function useTokenPool(enabled: boolean): TokenPoolState {
  const [pool, setPool] = useState<TokenPoolStatus | null>(null);
  const [loaded, setLoaded] = useState(false);
  const on = useRef(enabled);
  on.current = enabled;

  const poll = useCallback(async (): Promise<PollResult> => {
    if (!on.current) return "ok";
    try {
      const res = await api.get<StatusResponse>(STATUS_URL);
      if (!on.current) return "ok";
      setPool(poolFrom(res));
      setLoaded(true);
      return "ok";
    } catch {
      // No answer: keep the last pool until the next poll.
      return "failed";
    }
  }, []);

  const burst = useStatusPoll(poll);

  useEffect(() => {
    if (enabled) void poll();
  }, [enabled, poll]);

  const refresh = useCallback(() => {
    void poll();
    burst();
  }, [poll, burst]);

  return { pool, loaded, refresh };
}
