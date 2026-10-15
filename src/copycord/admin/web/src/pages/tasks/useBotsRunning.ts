// Whether the server and client bots are both running, for the task types that need them
// (requires_bots). Reads /api/status on mount and every 4 s like the Dashboard Bots card, and again
// at once when /ws/ui pushes a bot status. null until the first answer, and while disabled.
//   const botsUp = useBotsRunning(Boolean(type.requires_bots));

import { useCallback, useEffect, useState } from "react";
import { useWsMessages } from "../../hooks/useWsMessages";
import { api } from "../../lib/api";
import type { StatusResponse } from "../../types/dashboard";
import { STATUS_URL, statusFromWs } from "../dashboard/model";
import { POLL_MS } from "../dashboard/useStatusPoll";

/** The server's own answer (both_running), or both roles reporting running. */
export function bothRunning(res: StatusResponse | null | undefined): boolean {
  if (typeof res?.both_running === "boolean") return res.both_running;
  return Boolean(res?.server?.running && res?.client?.running);
}

export function useBotsRunning(enabled: boolean): boolean | null {
  const [running, setRunning] = useState<boolean | null>(null);

  const check = useCallback(async () => {
    try {
      setRunning(bothRunning(await api.get<StatusResponse>(STATUS_URL)));
    } catch {
      // No answer: keep the last state until the next check.
    }
  }, []);

  useEffect(() => {
    if (!enabled) return;
    void check();
    const t = window.setInterval(() => {
      if (!document.hidden) void check();
    }, POLL_MS);
    return () => window.clearInterval(t);
  }, [enabled, check]);

  useWsMessages(
    "/ws/ui",
    (msg) => {
      if (statusFromWs(msg)) void check();
    },
    { enabled },
  );

  return enabled ? running : null;
}
