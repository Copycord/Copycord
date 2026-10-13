// Progress card: bar, percent, message, elapsed clock, ETA and the target
// member count of the running scrape. Shown from start until a few seconds
// after the run ends; the state comes from useScraperLive.

import { useEffect, useState } from "react";
import { Card } from "../../components/ui";
import { TITLE } from "./TokensCard";
import type { ProgressState } from "./useScraperLive";

/** "12s" or "3m 7s", as updateElapsed did. */
export function fmtElapsed(ms: number): string {
  const s = Math.max(0, Math.round(ms / 1000));
  const m = Math.floor(s / 60);
  return m > 0 ? `${m}m ${s % 60}s` : `${s}s`;
}

export function ProgressCard({ progress }: { progress: ProgressState }) {
  const { visible, startedAt, endedAt, pct, text, members, eta } = progress;
  const [now, setNow] = useState(() => Date.now());

  useEffect(() => {
    setNow(Date.now());
    if (!visible || !startedAt || endedAt) return;
    const t = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(t);
  }, [visible, startedAt, endedAt]);

  const elapsed = startedAt ? fmtElapsed((endedAt ?? now) - startedAt) : "0s";

  return (
    <Card id="sc-progress-card" aria-label="Progress" hidden={!visible}>
      <div className="mb-3 flex flex-wrap items-center justify-between gap-3">
        <h3 className={TITLE}>Progress</h3>
        <div className="flex items-center gap-3 text-[13px] text-text-1">
          <span id="sc-elapsed" className="font-mono">
            {elapsed}
          </span>
          <span id="sc-eta">{eta}</span>
          <span id="sc-member-count" className="font-semibold text-text">
            {members}
          </span>
        </div>
      </div>
      <div
        className="h-2.5 overflow-hidden rounded-full bg-white/8"
        role="progressbar"
        aria-label="Scrape progress"
        aria-valuemin={0}
        aria-valuemax={100}
        aria-valuenow={pct ?? undefined}
      >
        <div
          id="sc-progress-fill"
          className="h-full rounded-full bg-linear-to-r from-primary to-accent transition-[width] duration-300"
          style={{ width: `${pct ?? 0}%` }}
        />
      </div>
      <div className="mt-2 flex items-center justify-between gap-3 text-sm">
        <span id="sc-progress-text" className="min-w-0 truncate text-text-1">
          {text}
        </span>
        <span id="sc-progress-pct" className="font-mono font-semibold">
          {pct == null ? "\u2014" : `${pct}%`}
        </span>
      </div>
    </Card>
  );
}
