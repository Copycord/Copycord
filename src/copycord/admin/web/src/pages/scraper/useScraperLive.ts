// Live scraper state for the page: the queue, the running flag, the status
// pill and the progress card, seeded from GET /api/scraper/status and then
// driven by the progress, complete and queue_update messages on /ws/out, as
// enterRunningState, onProgress, onComplete and onQueueUpdate in the old
// scraper.js did.

import { useCallback, useEffect, useRef, useState } from "react";
import { useWsMessages } from "../../hooks/useWsMessages";
import type { UseWsOptions } from "../../hooks/useWsMessages";
import type { ScraperEvent, ScraperQueueItem, ScraperStatus, ScraperWsMessage } from "../../types/scraper";
import type { Notify } from "../system/providers";
import { fmtSec } from "./scraperApi";

export type PillStatus = "idle" | "running" | "done" | "error";

export interface ProgressState {
  visible: boolean;
  /** ms epoch the elapsed clock counts from. */
  startedAt: number | null;
  /** Set when the run ends, so the clock stops there. */
  endedAt: number | null;
  /** 0 to 100, or null for the old page's dash after a failure. */
  pct: number | null;
  text: string;
  members: string;
  eta: string;
}

/** What the log card should do with its lines: clear on start, clear and empty the file after a run. */
export interface LogSignal {
  kind: "start" | "done";
  seq: number;
}

/** How long the finished progress card stays up before it hides and the logs autoclear. */
export const DONE_HIDE_MS = 3000;

const IDLE_PROGRESS: ProgressState = {
  visible: false,
  startedAt: null,
  endedAt: null,
  pct: 0,
  text: "Initializing…",
  members: "",
  eta: "",
};

export interface ScraperLive {
  queue: ScraperQueueItem[];
  running: boolean;
  pill: PillStatus;
  pillLabel: string;
  progress: ProgressState;
  logSignal: LogSignal | null;
  /** The page started a scrape or the queue. */
  start: () => void;
}

export interface ScraperLiveOptions {
  notify: Notify;
  /** A finished scrape saved members; the past scrapes list should reload. */
  onSaved: () => void;
  ws?: Pick<UseWsOptions, "WebSocketImpl">;
}

export function useScraperLive(status: ScraperStatus | undefined, opts: ScraperLiveOptions): ScraperLive {
  const [queue, setQueue] = useState<ScraperQueueItem[]>([]);
  const [running, setRunning] = useState(false);
  const [pill, setPill] = useState<PillStatus>("idle");
  const [pillLabel, setPillLabel] = useState("Idle");
  const [progress, setProgress] = useState<ProgressState>(IDLE_PROGRESS);
  const [logSignal, setLogSignal] = useState<LogSignal | null>(null);

  // The ws handler and timers read the latest values through these.
  const runningRef = useRef(running);
  runningRef.current = running;
  const queueRef = useRef(queue);
  queueRef.current = queue;
  const optsRef = useRef(opts);
  optsRef.current = opts;
  const hideTimer = useRef<ReturnType<typeof setTimeout> | undefined>(undefined);

  useEffect(() => () => clearTimeout(hideTimer.current), []);

  const signal = useCallback((kind: LogSignal["kind"]) => {
    setLogSignal((p) => ({ kind, seq: (p?.seq ?? 0) + 1 }));
  }, []);

  const enterRunning = useCallback(
    (restoredStart?: number | null) => {
      clearTimeout(hideTimer.current);
      runningRef.current = true;
      setRunning(true);
      setPill("running");
      setPillLabel("Running");
      setProgress({ ...IDLE_PROGRESS, visible: true, startedAt: restoredStart || Date.now() });
      if (!restoredStart) signal("start");
    },
    [signal],
  );

  const exitRunning = useCallback(() => {
    runningRef.current = false;
    setRunning(false);
  }, []);

  // Seed from the status reply, as checkStatus did on load. The page reloads
  // status after Cancel, so an idle reply also ends a run the page started.
  useEffect(() => {
    if (!status) return;
    if (status.queue && status.queue.length > 0) setQueue(status.queue);
    if (status.running && !runningRef.current) enterRunning(status.started_at ? status.started_at * 1000 : null);
    else if (!status.running && runningRef.current) {
      exitRunning();
      setPill("idle");
      setPillLabel("Idle");
    }
  }, [status, enterRunning, exitRunning]);

  const onProgress = useCallback((current = 0, total = 0, message?: string) => {
    setProgress((p) => {
      const next = { ...p, text: message || `${current} found` };
      if (total > 0) {
        const pct = Math.min(100, Math.round((current / total) * 100));
        next.pct = pct;
        next.members = `${total.toLocaleString()} members`;
        if (current > 0 && p.startedAt) {
          const rate = current / ((Date.now() - p.startedAt) / 1000);
          const remaining = total - current;
          next.eta = rate > 0 && remaining > 0 ? `~${fmtSec(Math.round(remaining / rate))} left` : "";
        }
      }
      return next;
    });
  }, []);

  const onComplete = useCallback(
    (data: Extract<ScraperEvent, { type: "complete" }>) => {
      if (!runningRef.current) return;
      const { notify, onSaved } = optsRef.current;
      const count = data.total_count ?? 0;
      if (data.success) {
        const gid = data.guild_id ? ` (${data.guild_id})` : "";
        notify(`Scrape complete${gid}: ${count} members in ${fmtSec(data.elapsed_seconds)}`, "success");
      } else {
        notify(data.error || "Scrape failed", "error");
      }
      const saved = Boolean(data.success) && count > 0;

      if (queueRef.current.some((q) => q.status === "pending")) {
        setProgress({ ...IDLE_PROGRESS, visible: true, startedAt: Date.now(), text: "Starting next in queue…" });
        setPill("running");
        setPillLabel("Running Queue");
        if (saved) onSaved();
        return;
      }

      exitRunning();
      setPill(data.success ? "done" : "error");
      setPillLabel(data.success ? "Complete" : "Error");
      setProgress((p) => ({
        ...p,
        endedAt: Date.now(),
        text: data.success ? `Done: ${count} members` : `Failed: ${data.error || "unknown"}`,
        pct: data.success ? 100 : null,
        eta: "",
      }));
      if (saved) onSaved();
      clearTimeout(hideTimer.current);
      hideTimer.current = setTimeout(() => {
        signal("done");
        setProgress((p) => ({ ...p, visible: false }));
      }, DONE_HIDE_MS);
    },
    [exitRunning, signal],
  );

  const onQueueUpdate = useCallback(
    (data: Extract<ScraperEvent, { type: "queue_update" }>) => {
      const next = data.queue ?? [];
      queueRef.current = next;
      setQueue(next);
      if (!data.running && runningRef.current) {
        exitRunning();
        setPill("idle");
        setPillLabel("Idle");
      }
      if (data.running && !runningRef.current) enterRunning();
    },
    [enterRunning, exitRunning],
  );

  useWsMessages<ScraperWsMessage>(
    "/ws/out",
    (msg) => {
      if (msg.kind !== "scraper" || !msg.payload) return;
      const p = msg.payload;
      if (p.type === "progress") onProgress(p.current, p.total, p.message);
      else if (p.type === "complete") onComplete(p);
      else if (p.type === "queue_update") onQueueUpdate(p);
    },
    { WebSocketImpl: opts.ws?.WebSocketImpl },
  );

  const start = useCallback(() => enterRunning(), [enterRunning]);

  return { queue, running, pill, pillLabel, progress, logSignal, start };
}
