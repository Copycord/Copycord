// The tree of category sections (.ch-section) and channel cards (.ch-card)
// inside #channels-root. Names are plain React text. Cards are memoized so
// a search only re-renders the cards whose data changed.

import { memo } from "react";
import type { ReactNode } from "react";
import { cx } from "../../components/ui";
import type { ChannelTree as Tree, TreeChannel } from "./model";
import type { ChannelTreeSlots } from "./slots";

const CHIP = "inline-flex items-center gap-1.5 rounded-full border px-2.5 py-0.5 text-xs font-semibold";
const CHIP_GOOD = "border-[#2e7d5b] bg-[#0f2a1f] text-[#7ee2b8]";
const CHIP_CUSTOM = "border-[#7c5cff] bg-[#21154a] text-[#c9b8ff]";
const CHIP_ORPHAN = "border-[#a33a4a] bg-[#2a0f17] text-[#ff9aa9]";
const CHIP_MUTED = "border-border bg-white/4 text-muted";

export interface ChannelTreeProps extends ChannelTreeSlots {
  tree: Tree;
}

export function ChannelTree({ tree, channelActions, categoryActions, channelStatus }: ChannelTreeProps) {
  return (
    <div id="channels-root" aria-live="polite" className="mt-3 flex flex-col gap-5">
      {tree.sections.map((s) => (
        <section key={s.key} className="ch-section flex flex-col gap-2.5">
          <div className="flex items-center">
            <h3 className="m-0 text-sm">
              <span
                data-cat-name={s.originalName}
                title={s.tooltip || undefined}
                data-orphan-cat-id={s.orphanCategoryId ?? undefined}
                className={cx(CHIP, s.orphanCategory ? CHIP_ORPHAN : s.customized ? CHIP_CUSTOM : CHIP_GOOD)}
              >
                {s.displayName}
                {!s.uncategorized ? categoryActions?.(s) : null}
              </span>
            </h3>
          </div>
          <div className="grid grid-cols-[repeat(auto-fill,minmax(240px,1fr))] gap-3">
            {s.channels.map((c) => (
              <ChannelCard
                key={c.id}
                channel={c}
                actions={channelActions?.(c, s)}
                status={channelStatus?.(c)}
              />
            ))}
          </div>
        </section>
      ))}
    </div>
  );
}

interface ChannelCardProps {
  channel: TreeChannel;
  actions: ReactNode;
  status: ReactNode;
}

const ChannelCard = memo(function ChannelCard({ channel: c, actions, status }: ChannelCardProps) {
  return (
    <article
      data-cid={c.id}
      data-orphan={c.orphan ? "1" : undefined}
      className={cx(
        "ch-card flex min-w-0 flex-col gap-2 rounded-panel border bg-linear-to-b from-[#1f1237] to-[#160b2a] p-3",
        c.orphan ? "border-[#a33a4a]" : "border-border",
      )}
    >
      <div className="flex items-start gap-2">
        <div className="min-w-0 flex-1">
          <span
            title={c.title}
            className={cx("block truncate font-semibold", c.customized ? "text-[#c9b8ff]" : "text-text")}
          >
            # {c.displayName}
          </span>
        </div>
        {actions ? <div className="shrink-0">{actions}</div> : null}
      </div>
      <div className="flex flex-wrap items-center gap-1.5">
        <span title="Channel type" className={cx(CHIP, CHIP_MUTED)}>
          {c.typeLabel}
        </span>
        {c.orphan ? (
          <span className={cx(CHIP, CHIP_ORPHAN)}>Orphan</span>
        ) : c.clonedId ? (
          <>
            <span title="Part of the host servers structure" className={cx(CHIP, CHIP_GOOD)}>
              Clone
            </span>
            {c.customized ? (
              <span title="Customized channel" className={cx(CHIP, CHIP_CUSTOM)}>
                Custom
              </span>
            ) : null}
          </>
        ) : null}
      </div>
      <div className="flex flex-wrap gap-x-2 font-mono text-xs text-muted">
        <span title="Original channel ID">{c.id}</span>
        {c.clonedId ? (
          <span title="Cloned channel ID" className="opacity-80">
            → {c.clonedId}
          </span>
        ) : null}
      </div>
      {status}
    </article>
  );
});

