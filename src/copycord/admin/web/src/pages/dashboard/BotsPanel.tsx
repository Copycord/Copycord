// The Bots card from index.html: server and client state, uptime and status
// note, Start/Stop, and the per bot log buttons. State comes from /api/status
// on load and after Start/Stop, then from the status pushes on /ws/ui.

import { useCallback, useEffect, useState } from "react";
import { Spinner, cx } from "../../components/ui";
import { useWsMessages } from "../../hooks/useWsMessages";
import type { UseWsOptions } from "../../hooks/useWsMessages";
import { api } from "../../lib/api";
import type { LogStream } from "../../types/api";
import type { BotRole, BotStatuses, StatusResponse } from "../../types/dashboard";
import { useNotify } from "../system/providers";
import { GHOST, GHOST_DISABLED } from "./parts";
import {
  anyRunning,
  displayStatus,
  EMPTY_STATUSES,
  formatUptime,
  startBots,
  STATUS_URL,
  statusesFrom,
  statusFromWs,
  stopBots,
} from "./model";

const ROLES: Array<{ role: BotRole; label: string }> = [
  { role: "server", label: "Server" },
  { role: "client", label: "Client" },
];

// The old .runtime-table: fixed layout, 110px State and 120px Uptime columns,
// muted 0.9rem headers, a rule under every row, centered cells after the first.
const HEAD = "border-b border-b-border px-3 py-2 align-middle text-[0.9rem] font-medium whitespace-nowrap text-muted";
const CELL = "border-b border-b-border px-3 py-2 align-middle text-[0.95rem] whitespace-nowrap";

// The old .runtime-table .badge with .badge-ok / .badge-stop.
const STATE =
  "inline-block rounded-[12px] border border-border px-2 py-0.5 font-[system-ui,sans-serif] text-[12px] leading-[1.8] font-semibold";

export interface BotsPanelProps {
  onViewLogs: (which: LogStream) => void;
  /** Fires with whether any bot is running, so the mapping cards can lock. */
  onRunningChange?: (running: boolean) => void;
  /** For tests. */
  WebSocketImpl?: UseWsOptions["WebSocketImpl"];
  /** Why Start is off (missing or rejected tokens), like the old updateStartButtonOnly; null when it may start. */
  startBlocked?: string | null;
}

export function BotsPanel({ onViewLogs, onRunningChange, startBlocked = null, WebSocketImpl }: BotsPanelProps) {
  const notify = useNotify();
  const [statuses, setStatuses] = useState<BotStatuses>(EMPTY_STATUSES);
  const [busy, setBusy] = useState(false);
  const [expanded, setExpanded] = useState<Record<BotRole, boolean>>({ server: false, client: false });
  // When each row's uptime was reported, so it counts up between reports like the old page.
  const [stamps, setStamps] = useState<Record<BotRole, number>>({ server: 0, client: 0 });
  const [now, setNow] = useState(() => Date.now());

  const refresh = useCallback(async () => {
    try {
      setStatuses(statusesFrom(await api.get<StatusResponse>(STATUS_URL)));
      setStamps({ server: Date.now(), client: Date.now() });
      setNow(Date.now());
    } catch {
      // The table keeps its last state; the websocket fills it in.
    }
  }, []);

  useEffect(() => {
    void refresh();
  }, [refresh]);

  useWsMessages(
    "/ws/ui",
    (msg) => {
      const update = statusFromWs(msg);
      if (!update) return;
      setStatuses((s) => ({ ...s, [update.role]: update.status }));
      setStamps((t) => ({ ...t, [update.role]: Date.now() }));
      setNow(Date.now());
    },
    { WebSocketImpl },
  );

  const running = anyRunning(statuses);

  useEffect(() => {
    onRunningChange?.(running);
  }, [running, onRunningChange]);

  useEffect(() => {
    if (!running) return;
    const id = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(id);
  }, [running]);

  const toggle = async () => {
    setBusy(true);
    try {
      await (running ? stopBots() : startBots());
      notify(running ? "Stopped." : "Started.", "success");
      await refresh();
    } catch (e) {
      notify(e instanceof Error ? e.message : String(e), "error");
    } finally {
      setBusy(false);
    }
  };

  return (
    <section className="mt-[18px] rounded-card border border-border bg-(image:--card-gradient) p-[18px] text-text">
      <header>
        <h2 className="my-[0.83em] text-[24px] leading-[normal] font-bold">Bots</h2>
      </header>
      <div className="w-full overflow-x-hidden">
        <table className="mt-2 w-full table-fixed border-collapse border-[#808080] text-left [&_tr]:border-[#808080]">
          <thead>
            <tr>
              <th className={cx(HEAD, "text-left")}>Component</th>
              <th className={cx(HEAD, "w-[110px] text-center")}>State</th>
              <th className={cx(HEAD, "w-[120px] text-center")}>Uptime</th>
              <th className={cx(HEAD, "text-center")}>Status</th>
            </tr>
          </thead>
          <tbody id="runtime-body">
            {ROLES.map(({ role, label }) => {
              const s = statuses[role];
              const note = displayStatus(s.status);
              return (
                <tr key={role}>
                  <td className={cx(CELL, "text-left")}>{label}</td>
                  <td className={cx(CELL, "text-center")}>
                    <span
                      id={`${role}-state`}
                      className={cx(STATE, s.running ? "bg-[#183a2e] text-[#baf7d0]" : "bg-[#3a1c20] text-[#ffc5cf]")}
                    >
                      {s.running ? "running" : "stopped"}
                    </span>
                  </td>
                  <td id={`${role}-uptime`} className={cx(CELL, "text-center")}>
                    {s.running && s.uptime_sec != null
                      ? formatUptime(s.uptime_sec + Math.max(0, Math.floor((now - stamps[role]) / 1000)))
                      : ""}
                  </td>
                  <td className={cx(CELL, "text-center")}>
                    <span
                      id={`${role}-status`}
                      title={note}
                      role="button"
                      tabIndex={note ? 0 : -1}
                      onClick={() => setExpanded((x) => ({ ...x, [role]: !x[role] }))}
                      className={cx(
                        "inline-block max-w-[200px] cursor-pointer overflow-hidden rounded-[20px] bg-[rgba(255,255,255,0.06)] px-2.5 py-[3px] align-middle text-[11px] font-medium text-ellipsis text-muted empty:hidden",
                        expanded[role] ? "whitespace-normal" : "whitespace-nowrap",
                      )}
                    >
                      {note}
                    </span>
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>

      <div className="mt-3 flex flex-wrap gap-2.5 max-sm:grid max-sm:grid-cols-1">
        <form
          id="toggle-form"
          className="inline max-sm:block"
          action={running ? "/stop" : "/start"}
          onSubmit={(e) => {
            e.preventDefault();
            void toggle();
          }}
        >
          <button
            id="toggle-btn"
            type="submit"
            disabled={busy || (!running && !!startBlocked)}
            aria-busy={busy || undefined}
            title={running ? undefined : (startBlocked ?? undefined)}
            className={cx(GHOST, GHOST_DISABLED, "inline-flex max-sm:w-full")}
          >
            {busy ? <Spinner size="sm" label="Working" /> : null}
            {running ? "Stop" : "Start"}
          </button>
        </form>
        <span className="flex-1 max-sm:hidden" />
        <button type="button" data-log="server" onClick={() => onViewLogs("server")} className={cx(GHOST, "flex max-sm:w-full")}>
          View server logs
        </button>
        <button type="button" data-log="client" onClick={() => onViewLogs("client")} className={cx(GHOST, "flex max-sm:w-full")}>
          View client logs
        </button>
      </div>
    </section>
  );
}
