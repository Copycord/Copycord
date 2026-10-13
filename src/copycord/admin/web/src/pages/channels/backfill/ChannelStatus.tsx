// The status pill and progress bar at the bottom of a channel card (.ch-status, .ch-progress).

import { cx } from "../../../components/ui";
import type { CloneStore } from "./cloneStore";
import { useCloneState, useSynced } from "./cloneStore";
import { progressLabel, progressPct } from "./logic";

export function ChannelStatus({ store, cid }: { store: CloneStore; cid: string }) {
  const s = useCloneState(store, cid);
  const synced = useSynced(store, cid);
  if (!s) {
    return synced ? <span className="ch-status w-fit text-xs font-semibold text-[#7ee2b8]">Synced ✓</span> : null;
  }
  const pct = progressPct(s);
  return (
    <div className="flex flex-col gap-1.5" aria-busy="true">
      <span className="ch-status w-fit rounded-full border border-border bg-white/6 px-2.5 py-0.5 text-xs font-semibold text-accent">
        {progressLabel(s)}
      </span>
      <div
        className={cx("ch-progress h-1.5 overflow-hidden rounded-full bg-white/10", pct === null && "indeterminate")}
        role="progressbar"
        aria-label="Clone progress"
        aria-valuemin={0}
        aria-valuemax={100}
        aria-valuenow={pct ?? 0}
        aria-busy={pct === null ? true : undefined}
      >
        <div
          className={cx("bar h-full rounded-full bg-accent", pct === null && "animate-pulse")}
          style={{ width: pct === null ? "30%" : `${pct}%` }}
        />
      </div>
    </div>
  );
}
