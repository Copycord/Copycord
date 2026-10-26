// The Bots card: server and client state, uptime and status
// note, Start/Stop, and the per bot log buttons. State comes from /api/status
// on load, on the useStatusPoll schedule and after Start/Stop, and from the
// status pushes on /ws/ui in between. Uptime counts from each bot's start time,
// which a report only moves when it is more than 2 s off. From a Stop click the
// chips say stopping, and Start/Stop is off, until /api/status reports stopped.

import { useCallback, useEffect, useRef, useState } from "react";
import { BUSY_HOST, BusyContent, cx, useReveal } from "../../components/ui";
import { useWsMessages } from "../../hooks/useWsMessages";
import type { UseWsOptions } from "../../hooks/useWsMessages";
import { api, errorText } from "../../lib/api";
import type { LogStream } from "../../types/api";
import type { BotRole, BotState, BotStatus, BotStatuses, StatusResponse } from "../../types/dashboard";
import { useNotify } from "../system/providers";
import { BOT_ROLES, BotStatePending, BOTS_CARD, BOTS_CELL as CELL, BOTS_COL_COMPONENT, BOTS_COL_STATE, BOTS_COL_STATUS, BOTS_COL_UPTIME, BOTS_HEAD as HEAD, BOTS_STATE as STATE, BOTS_STATE_LABEL as STATE_LABEL, BOTS_TABLE, BOTS_TITLE, GHOST, GHOST_DISABLED } from "./parts";
import {
  anyRunning,
  stateOf,
  displayStatus,
  EMPTY_STATUSES,
  formatUptime,
  mergeStatus,
  startBots,
  STATUS_URL,
  statusesFrom,
  statusFromWs,
  statusHealthy,
  stopBots,
} from "./model";
import type { BotStatusPatch } from "./model";
import { BotSettingsModal } from "./BotSettingsModal";
import { MAPPING_ICON_BTN } from "./DashboardSkeleton";
import { SettingsIcon } from "./mappings/icons";
import type { PollResult } from "./useStatusPoll";
import { useStatusPoll } from "./useStatusPoll";

const ROLES: Array<{ role: BotRole; label: string }> = BOT_ROLES;

const STATE_TONE: Record<BotState, string> = {
  running: "bg-run-bg text-run-text",
  stopping: "bg-stopping-bg text-stopping-text",
  stopped: "bg-stop-bg text-stop-text",
};

/** A report moves the held start only when it is further off than this (a restart, real drift). */
export const UPTIME_DRIFT_MS = 2000;

/** How long after a Stop click a "running" answer cannot undo the stopping chip: the server's
 *  stop budget (token pool close cap plus margin) and some slack. */
export const STOP_HOLD_MS = 60000;

/**
 * When a bot started (ms, wall clock), from the uptime_sec it reported at `at`. Polls and pushes
 * each read the uptime at a slightly different moment, so the held start stays unless the report
 * puts it more than UPTIME_DRIFT_MS away; the counter then never jumps on a report.
 */
export function uptimeStart(held: number | null, uptimeSec: number, at: number): number {
  const start = at - uptimeSec * 1000;
  return held != null && Math.abs(start - held) <= UPTIME_DRIFT_MS ? held : start;
}

/** The whole seconds since `start`, drawn again right as each one passes: one step a second. */
function Uptime({ start }: { start: number }) {
  const [, redraw] = useState(0);
  const elapsed = Date.now() - start;
  useEffect(() => {
    const into = (((Date.now() - start) % 1000) + 1000) % 1000;
    const id = setTimeout(() => redraw((n) => n + 1), 1000 - into);
    return () => clearTimeout(id);
  });
  return <>{formatUptime(Math.max(0, Math.floor(elapsed / 1000)))}</>;
}

export interface BotsPanelProps {
  onViewLogs: (which: LogStream) => void;
  /** Fires with whether any bot is running, so the mapping cards can lock. */
  onRunningChange?: (running: boolean) => void;
  /** For tests. */
  WebSocketImpl?: UseWsOptions["WebSocketImpl"];
  /** Why Start is off (missing or rejected tokens); null when it may start. */
  startBlocked?: string | null;
}

export function BotsPanel({ onViewLogs, onRunningChange, startBlocked = null, WebSocketImpl }: BotsPanelProps) {
  const notify = useNotify();
  const [statuses, setStatuses] = useState<BotStatuses>(EMPTY_STATUSES);
  // The state badges are bars, like the page skeleton's, until the first status answer or push.
  const [loaded, setLoaded] = useState(false);
  const reveal = useReveal(loaded);
  const [busy, setBusy] = useState(false);
  const [settingsOpen, setSettingsOpen] = useState(false);
  const [expanded, setExpanded] = useState<Record<BotRole, boolean>>({ server: false, client: false });
  // When each bot started, from its reported uptime, so the row counts up between reports.
  const [starts, setStarts] = useState<Record<BotRole, number | null>>({ server: null, client: null });

  // When Stop was clicked (ms). For STOP_HOLD_MS after it a "running" answer, one read before the
  // stop reached the bots or from a server that does not say stopping, keeps a stopping chip.
  const stopAt = useRef(0);

  const refresh = useCallback(async (): Promise<PollResult> => {
    try {
      const res = await api.get<StatusResponse>(STATUS_URL);
      const fresh = statusesFrom(res);
      const holding = Date.now() - stopAt.current < STOP_HOLD_MS;
      // The answer is the truth for the state and the note; an uptime it leaves out (the control
      // socket timed out and the hub filled in running) keeps the last one and its stamp.
      const next = (role: BotRole, prev: BotStatus): BotStatus => {
        const r = fresh[role];
        if (holding && stateOf(prev) === "stopping" && r.state === "running") return prev;
        const patch: BotStatusPatch = r.uptime_sec == null ? { running: r.running, state: r.state, status: r.status } : r;
        return mergeStatus(prev, patch);
      };
      setStatuses((s) => ({ server: next("server", s.server), client: next("client", s.client) }));
      const t = Date.now();
      const start = (role: BotRole, held: number | null) => {
        const up = fresh[role].uptime_sec;
        return up == null ? held : uptimeStart(held, up, t);
      };
      setStarts((old) => ({ server: start("server", old.server), client: start("client", old.client) }));
      return statusHealthy(res) ? "ok" : "degraded";
    } catch {
      // The table keeps its last state; the websocket fills it in.
      return "failed";
    } finally {
      setLoaded(true);
    }
  }, []);

  useEffect(() => {
    void refresh();
  }, [refresh]);

  const burstPoll = useStatusPoll(refresh);

  useWsMessages(
    "/ws/ui",
    (msg) => {
      const update = statusFromWs(msg);
      if (!update) return;
      // Pushes carry only some fields (the bots' status note has no uptime): merge, never replace.
      setStatuses((s) => ({ ...s, [update.role]: mergeStatus(s[update.role], update.status) }));
      setLoaded(true);
      const up = update.status.uptime_sec;
      if (up != null) setStarts((t) => ({ ...t, [update.role]: uptimeStart(t[update.role], up, Date.now()) }));
    },
    { WebSocketImpl },
  );

  // Running while stopping too: the mapping cards stay locked until the bots have exited.
  const running = anyRunning(statuses);
  const stopping = ROLES.some(({ role }) => stateOf(statuses[role]) === "stopping");

  useEffect(() => {
    onRunningChange?.(running);
  }, [running, onRunningChange]);

  const toggle = async () => {
    const stop = running;
    setBusy(true);
    if (stop) {
      // Stopping from the click on; only /api/status says when a bot has stopped.
      stopAt.current = Date.now();
      const mark = (s: BotStatus) => (stateOf(s) === "stopped" ? s : mergeStatus(s, { state: "stopping" }));
      setStatuses((s) => ({ server: mark(s.server), client: mark(s.client) }));
    }
    try {
      await (stop ? stopBots() : startBots());
      notify(stop ? "Stopping." : "Started.", "success");
      await refresh();
      burstPoll();
    } catch (e) {
      notify(errorText(e, stop ? "Failed to stop" : "Failed to start"), "error");
      // The stop did not go through: the next answer may show the bots running again.
      if (stop) {
        stopAt.current = 0;
        await refresh();
      }
    } finally {
      setBusy(false);
    }
  };

  return (
    <section className={BOTS_CARD}>
      <header className="flex items-center justify-between gap-3">
        <h2 className={BOTS_TITLE}>Bots</h2>
        <button
          type="button"
          id="bot-settings-btn"
          aria-label="Bot settings"
          title="Bot settings"
          onClick={() => setSettingsOpen(true)}
          className={cx(MAPPING_ICON_BTN, "size-8 min-h-8")}
        >
          <SettingsIcon />
        </button>
      </header>
      <BotSettingsModal open={settingsOpen} onClose={() => setSettingsOpen(false)} />
      <div className="w-full overflow-x-hidden">
        <table className={BOTS_TABLE}>
          <thead>
            <tr>
              <th className={cx(HEAD, BOTS_COL_COMPONENT)}>Component</th>
              <th className={cx(HEAD, BOTS_COL_STATE)}>State</th>
              <th className={cx(HEAD, BOTS_COL_UPTIME)}>Uptime</th>
              <th className={cx(HEAD, BOTS_COL_STATUS)}>Status</th>
            </tr>
          </thead>
          <tbody id="runtime-body">
            {ROLES.map(({ role, label }) => {
              const s = statuses[role];
              const state = stateOf(s);
              const start = starts[role];
              const note = displayStatus(s.status);
              return (
                <tr key={role}>
                  <td className={cx(CELL, BOTS_COL_COMPONENT)}>{label}</td>
                  <td className={cx(CELL, BOTS_COL_STATE)}>
                    {loaded ? (
                      <span id={`${role}-state`} className={cx(STATE, STATE_TONE[state], reveal)}>
                        <span className={STATE_LABEL}>{state}</span>
                      </span>
                    ) : (
                      <BotStatePending />
                    )}
                  </td>
                  {/* Only a running bot counts up; a stopping one is on its way out, so the cell is blank. */}
                  <td id={`${role}-uptime`} className={cx(CELL, BOTS_COL_UPTIME)}>
                    {state === "running" && s.uptime_sec != null && start != null ? <Uptime start={start} /> : ""}
                  </td>
                  <td className={cx(CELL, BOTS_COL_STATUS)}>
                    {/* Muted plain text, cut with an ellipsis; a click shows the whole note. */}
                    <span
                      id={`${role}-status`}
                      title={note}
                      role="button"
                      tabIndex={note ? 0 : -1}
                      onClick={() => setExpanded((x) => ({ ...x, [role]: !x[role] }))}
                      className={cx(
                        "block max-w-full cursor-pointer overflow-hidden text-ellipsis text-muted",
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
            disabled={busy || stopping || (!running && !!startBlocked)}
            aria-busy={busy || undefined}
            title={running ? undefined : (startBlocked ?? undefined)}
            className={cx(GHOST, GHOST_DISABLED, "inline-flex max-sm:w-full", busy && BUSY_HOST)}
          >
            <BusyContent busy={busy}>{stopping ? "Stopping..." : running ? "Stop" : "Start"}</BusyContent>
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
