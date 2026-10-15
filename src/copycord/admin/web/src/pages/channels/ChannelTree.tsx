// The tree of category sections (.ch-section) and channel cards (.ch-card)
// inside #channels-root. Names are plain React text. Cards are memoized so
// a search only re-renders the cards whose data changed.

import { memo } from "react";
import type { ReactNode } from "react";
import { cx } from "../../components/ui";
import type { ChannelTree as Tree, TreeChannel } from "./model";
import type { ChannelTreeSlots } from "./slots";
import {
  BADGE,
  BADGE_GOOD,
  BADGE_MUTED,
  BADGE_ORPHAN,
  BADGE_ROW,
  CARD,
  CARD_ACTIONS,
  CARD_GRID,
  CARD_HEAD,
  CARD_NAME,
  CAT_CHIP,
  CAT_GOOD,
  CAT_ORPHAN,
  CUSTOM,
  ID_ROW,
  SECTION,
  SECTION_HEAD,
  SECTION_TITLE,
  TREE,
} from "./treeClasses";

export interface ChannelTreeProps extends ChannelTreeSlots {
  tree: Tree;
  /** Hidden until the status gate first opens, as the old page did. */
  hidden?: boolean;
  /** A refetch is running: the rows stay, marked busy. */
  busy?: boolean;
}

export function ChannelTree({ tree, hidden, busy, channelActions, categoryActions }: ChannelTreeProps) {
  return (
    <div id="channels-root" aria-live="polite" aria-busy={busy || undefined} hidden={hidden} className={TREE}>
      {tree.sections.map((s) => (
        <section key={s.key} className={cx("ch-section", SECTION)}>
          <div className={SECTION_HEAD}>
            <h3 className={SECTION_TITLE}>
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
          <div className={CARD_GRID}>
            {s.channels.map((c) => (
              <ChannelCard key={c.id} channel={c} actions={channelActions?.(c, s)} />
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
}

const ChannelCard = memo(function ChannelCard({ channel: c, actions }: ChannelCardProps) {
  return (
    <article
      data-cid={c.id}
      data-orphan={c.orphan ? "1" : undefined}
      className={cx("ch-card", CARD, c.orphan ? "border-white/12" : "border-border")}
    >
      <div className={CARD_HEAD}>
        <span className={CARD_NAME}>
          <bdi title={c.title} className={cx("min-w-0 truncate", c.customized && "text-custom-ink")}>
            # {c.displayName}
          </bdi>
        </span>
        {actions ? <div className={CARD_ACTIONS}>{actions}</div> : null}
      </div>
      <div className={BADGE_ROW}>
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
      <div className={ID_ROW}>
        <span title="Original channel ID">{c.id}</span>
        {/* .muted: 0.9em at 80% opacity, as the old clone id. */}
        {c.clonedId ? (
          <span title="Cloned channel ID" className="text-[0.9em] opacity-80">
            → {c.clonedId}
          </span>
        ) : null}
      </div>
    </article>
  );
});
