// The Bots card from index.html: server and client state, uptime and status
// note, Start/Stop, and the per bot log buttons. State comes from /api/status
// on load and after Start/Stop, then from the status pushes on /ws/ui.

import { useCallback, useEffect, useState } from "react";
import { Badge, cx } from "../../components/ui";
import { useWsMessages } from "../../hooks/useWsMessages";
import type { UseWsOptions } from "../../hooks/useWsMessages";
import { api } from "../../lib/api";
import type { LogStream } from "../../types/api";
import type { BotRole, BotStatuses, StatusResponse } from "../../types/dashboard";
import { GhostButton } from "../system/buttons";
import { useNotify } from "../system/providers";
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

const HEAD = "px-2 py-2 text-left font-semibold text-accent";
const CELL = "border-t border-border px-2 py-2 align-middle";

export interface BotsPanelProps {
  onViewLogs: (which: LogStream) => void;
  /** Fires with whether any bot is running, so the mapping cards can lock. */
  onRunningChange?: (running: boolean) => void;
  /** For tests. */
  WebSocketImpl?: UseWsOptions["WebSocketImpl"];
}

export function BotsPanel({ onViewLogs, onRunningChange, WebSocketImpl }: BotsPanelProps) {
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
    <section className="rounded-card border border-border bg-linear-to-b from-card to-[rgba(20,10,40,0.45)] p-[18px] text-text">
      <header className="mb-3">
        <h2 className="text-base font-semibold">Bots</h2>
      </header>
      <div className="overflow-x-auto">
        <table className="w-full border-collapse text-sm">
          <thead>
            <tr>
              <th className={HEAD}>Component</th>
              <th className={HEAD}>State</th>
              <th className={HEAD}>Uptime</th>
              <th className={cx(HEAD, "w-full")}>Status</th>
            </tr>
          </thead>
          <tbody id="runtime-body">
            {ROLES.map(({ role, label }) => {
              const s = statuses[role];
              const note = displayStatus(s.status);
              return (
                <tr key={role}>
                  <td className={CELL}>{label}</td>
                  <td className={CELL}>
                    <span id={`${role}-state`}>
                      <Badge tone={s.running ? "good" : "bad"}>{s.running ? "running" : "stopped"}</Badge>
                    </span>
                  </td>
                  <td id={`${role}-uptime`} className={cx(CELL, "whitespace-nowrap")}>
                    {s.running && s.uptime_sec != null
                      ? formatUptime(s.uptime_sec + Math.max(0, Math.floor((now - stamps[role]) / 1000)))
                      : ""}
                  </td>
                  <td className={cx(CELL, "max-w-0")}>
                    <span
                      id={`${role}-status`}
                      title={note}
                      role="button"
                      tabIndex={note ? 0 : -1}
                      onClick={() => setExpanded((x) => ({ ...x, [role]: !x[role] }))}
                      className={cx(
                        "block cursor-pointer overflow-hidden text-ellipsis text-muted",
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

      <div className="mt-4 flex flex-wrap items-center gap-2">
        <form
          id="toggle-form"
          action={running ? "/stop" : "/start"}
          onSubmit={(e) => {
            e.preventDefault();
            void toggle();
          }}
        >
          <GhostButton id="toggle-btn" type="submit" loading={busy}>
            {running ? "Stop" : "Start"}
          </GhostButton>
        </form>
        <span className="flex-1" />
        <GhostButton data-log="server" onClick={() => onViewLogs("server")}>
          View server logs
        </GhostButton>
        <GhostButton data-log="client" onClick={() => onViewLogs("client")}>
          View client logs
        </GhostButton>
      </div>
    </section>
  );
}
