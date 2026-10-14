// The tree of category sections (.ch-section) and channel cards (.ch-card)
// inside #channels-root. Names are plain React text. Cards are memoized so
// a search only re-renders the cards whose data changed.

import { memo } from "react";
import type { ReactNode } from "react";
import { cx } from "../../components/ui";
import type { ChannelTree as Tree, TreeChannel } from "./model";
import type { ChannelTreeSlots } from "./slots";

// .ch-section-title .cat-chip: 12px system-ui pill with the old good / custom / orphan inks.
const CAT_CHIP =
  "inline-flex min-h-6 items-center justify-center rounded-full border px-2.5 py-1 text-center " +
  "font-[system-ui,sans-serif] text-[12px] font-normal leading-[1.2]";
const CAT_GOOD = "border-[rgba(16,185,129,0.35)] bg-[rgba(16,185,129,0.12)] text-good";
// .badge.badge-custom
const CUSTOM =
  "border-[color-mix(in_srgb,#facc15_45%,rgba(255,255,255,0.12))] " +
  "bg-[color-mix(in_srgb,#facc15_12%,rgba(255,255,255,0.045))] text-custom-ink";
const CAT_ORPHAN = "border-[rgba(239,68,68,0.35)] bg-[#3a1c20] text-[#ffc5cf]";

// .badge inside a card: 600 12px/1.8 system-ui, 2px 8px.
const BADGE =
  "inline-block rounded-full border px-2 py-0.5 font-[system-ui,sans-serif] text-[12px] font-semibold leading-[1.8]";
const BADGE_MUTED = "border-border bg-bg2 text-[#e6e1ff]";
const BADGE_GOOD = "border-[rgba(16,185,129,0.35)] bg-[rgba(16,185,129,0.12)] text-good";
const BADGE_ORPHAN = "border-border bg-[#3a1c20] text-[#ffc5cf]";

export interface ChannelTreeProps extends ChannelTreeSlots {
  tree: Tree;
  /** Hidden until the status gate first opens, as the old page did. */
  hidden?: boolean;
}

export function ChannelTree({ tree, hidden, channelActions, categoryActions, channelStatus }: ChannelTreeProps) {
  return (
    <div id="channels-root" aria-live="polite" hidden={hidden} className="grid gap-6 xl:gap-7">
      {tree.sections.map((s) => (
        <section key={s.key} className="ch-section grid gap-3">
          <div className="flex items-center">
            <h3 className="m-0 gap-[0.5ch] font-[system-ui,sans-serif] text-[12px] leading-[1.8] font-semibold text-muted">
              <span
                data-cat-name={s.originalName}
                title={s.tooltip || undefined}
                data-orphan-cat-id={s.orphanCategoryId ?? undefined}
                className={cx(CAT_CHIP, s.orphanCategory ? CAT_ORPHAN : s.customized ? CUSTOM : CAT_GOOD)}
              >
                {s.displayName}
                {!s.uncategorized ? categoryActions?.(s) : null}
              </span>
            </h3>
          </div>
          <div
            className={
              "grid grid-cols-[repeat(auto-fill,minmax(320px,1fr))] gap-3 xl:gap-4 " +
              "max-sm:grid-cols-[repeat(auto-fill,minmax(200px,1fr))] " +
              "max-[520px]:grid-cols-[repeat(auto-fill,minmax(180px,1fr))] max-[520px]:gap-2.5 " +
              "max-[360px]:grid-cols-1"
            }
          >
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
        "ch-card block min-w-0 rounded-panel border bg-[linear-gradient(#1f1237,#160b2a)] p-3 max-[520px]:p-2.5",
        c.orphan ? "border-white/12" : "border-border",
      )}
    >
      {/* .ch-head: the name (.ch-name) and the menu button (.ch-top-right) */}
      <div className="flex items-center justify-between">
        <span className="flex min-w-0 flex-1 items-center gap-1.5 font-semibold text-text max-[520px]:text-[13px]">
          <bdi title={c.title} className={cx("min-w-0 truncate", c.customized && "text-custom-ink")}>
            # {c.displayName}
          </bdi>
        </span>
        {actions ? <div className="relative ml-2 flex shrink-0 items-center gap-1.5">{actions}</div> : null}
      </div>
      <div className="mt-2 flex gap-1.5">
        <span title="Channel type" className={cx(BADGE, BADGE_MUTED)}>
          {c.typeLabel}
        </span>
        {c.orphan ? (
          <span className={cx(BADGE, BADGE_ORPHAN)}>Orphan</span>
        ) : c.clonedId ? (
          <>
            <span title="Part of the host servers structure" className={cx(BADGE, BADGE_GOOD)}>
              Clone
            </span>
            {c.customized ? (
              <span title="Customized channel" className={cx(BADGE, CUSTOM)}>
                Custom
              </span>
            ) : null}
          </>
        ) : null}
      </div>
      <div
        className={
          "mt-1.5 flex gap-1.5 text-[12px] text-muted max-sm:flex-wrap max-[520px]:gap-1 " +
          "max-sm:*:min-w-0 max-sm:*:[overflow-wrap:anywhere] max-[520px]:*:max-w-full max-[520px]:*:flex-[1_1_100%] max-[520px]:*:truncate"
        }
      >
        <span title="Original channel ID">{c.id}</span>
        {c.clonedId ? <span title="Cloned channel ID">→ {c.clonedId}</span> : null}
      </div>
      {status}
    </article>
  );
});
