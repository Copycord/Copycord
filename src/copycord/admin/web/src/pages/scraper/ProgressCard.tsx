// Progress card: bar, percent, message, elapsed clock, ETA and the target
// member count of the running scrape. Shown from start until a few seconds
// after the run ends; the state comes from useScraperLive.

import { useEffect, useState } from "react";
import { Card } from "../../components/ui";
import { SC_CARD, TITLE, TITLEBAR } from "./TokensCard";
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
    <Card id="sc-progress-card" aria-label="Progress" className={SC_CARD} hidden={!visible}>
      <div className={TITLEBAR}>
        <h3 className={TITLE}>Progress</h3>
        <div className="flex shrink-0 items-center gap-2">
          <span
            id="sc-elapsed"
            className="rounded-md border border-border bg-[rgba(20,11,38,0.5)] px-2 py-0.5 font-mono text-xs text-muted"
          >
            {elapsed}
          </span>
          <span
            id="sc-eta"
            className="rounded-md border border-[rgba(52,211,153,0.25)] bg-[rgba(52,211,153,0.1)] px-2 py-0.5 font-mono text-xs text-[#34d399] empty:hidden"
          >
            {eta}
          </span>
          <span id="sc-member-count" className="text-xs font-semibold text-primary empty:hidden">
            {members}
          </span>
        </div>
      </div>
      <div className="mt-3">
        <div
          className="h-2 w-full overflow-hidden rounded-lg border border-border bg-[rgba(20,11,38,0.8)]"
          role="progressbar"
          aria-label="Scrape progress"
          aria-valuemin={0}
          aria-valuemax={100}
          aria-valuenow={pct ?? undefined}
        >
          <div
            id="sc-progress-fill"
            className="h-full rounded-lg bg-linear-to-r from-primary to-[#34d399] transition-[width] duration-[400ms]"
            style={{ width: `${pct ?? 0}%` }}
          />
        </div>
        <div className="mt-2 flex items-center justify-between gap-3 text-[13px] text-muted">
          <span id="sc-progress-text" className="min-w-0 truncate">
            {text}
          </span>
          <span id="sc-progress-pct">{pct == null ? "—" : `${pct}%`}</span>
        </div>
      </div>
    </Card>
  );
}
