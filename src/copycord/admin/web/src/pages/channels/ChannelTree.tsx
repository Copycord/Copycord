// The tree of category sections (.ch-section) and channel cards (.ch-card)
// inside #channels-root. Names are plain React text. Cards are memoized so
// a search only re-renders the cards whose data changed.

import { memo } from "react";
import type { ReactNode } from "react";
import { ChannelTypeIcon } from "../../components/icons";
import { cx } from "../../components/ui";
import { channelBadges, chTypeIcon } from "./model";
import type { ChannelBadge, ChannelTree as Tree, TreeChannel } from "./model";
import type { ChannelTreeSlots } from "./slots";
import {
  BADGE,
  BADGE_NOT_CLONED,
  BADGE_ORPHAN,
  BADGE_ROW,
  CARD,
  CARD_ACTIONS,
  CARD_GRID,
  CARD_HEAD,
  CARD_NAME,
  CAT_COUNT,
  CAT_DOT,
  CAT_DOT_CUSTOM,
  CAT_DOT_ORPHAN,
  CAT_LABEL,
  CUSTOM,
  ID_ROW,
  SECTION,
  SECTION_HEAD,
  SECTION_TITLE,
  TREE,
  TYPE_ICON,
} from "./treeClasses";

export interface ChannelTreeProps extends ChannelTreeSlots {
  tree: Tree;
  /** Hidden until the status gate first opens, as the old page did. */
  hidden?: boolean;
  /** A refetch is running: the rows stay, marked busy. */
  busy?: boolean;
  /** The fade in as the tree replaces the loading tree (useReveal). */
  className?: string;
}

export function ChannelTree({ tree, hidden, busy, className, channelActions, categoryActions }: ChannelTreeProps) {
  return (
    <div id="channels-root" aria-live="polite" aria-busy={busy || undefined} hidden={hidden} className={cx(TREE, className)}>
      {tree.sections.map((s) => (
        <section key={s.key} className={cx("ch-section", SECTION)}>
          <div className={SECTION_HEAD}>
            <h3 className={SECTION_TITLE}>
              {s.orphanCategory || s.customized ? (
                <span
                  role="img"
                  aria-label={s.orphanCategory ? "Orphan category" : "Customized category"}
                  title={s.orphanCategory ? "Orphan category" : "Customized category"}
                  className={cx(CAT_DOT, s.orphanCategory ? CAT_DOT_ORPHAN : CAT_DOT_CUSTOM)}
                />
              ) : null}
              <span
                data-cat-name={s.originalName}
                title={s.tooltip || undefined}
                data-orphan-cat-id={s.orphanCategoryId ?? undefined}
                className={CAT_LABEL}
              >
                {s.displayName}
                <span title={`${s.channels.length} ${s.channels.length === 1 ? "channel" : "channels"}`} className={CAT_COUNT}>
                  {s.channels.length}
                </span>
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

const BADGE_VIEW: Record<ChannelBadge, { label: string; title: string; className: string }> = {
  orphan: { label: "Orphan", title: "In the clone but no longer in the host server", className: BADGE_ORPHAN },
  notcloned: { label: "Not cloned", title: "No cloned channel yet", className: BADGE_NOT_CLONED },
  custom: { label: "Custom", title: "Customized channel", className: CUSTOM },
};

// The card is not clickable (only its ⋯ menu is), so it has no hover state.
const ChannelCard = memo(function ChannelCard({ channel: c, actions }: ChannelCardProps) {
  const badges = channelBadges(c);
  return (
    <article
      data-cid={c.id}
      data-orphan={c.orphan ? "1" : undefined}
      className={cx("ch-card", CARD, c.orphan ? "border-white/12" : "border-border")}
    >
      <div className={CARD_HEAD}>
        <span className={CARD_NAME}>
          <ChannelTypeIcon kind={chTypeIcon(c.type)} label={c.typeLabel} className={TYPE_ICON} />
          <bdi title={c.title} className="min-w-0 truncate">
            {c.displayName}
          </bdi>
          {badges.length ? (
            <span className={BADGE_ROW}>
              {badges.map((b) => (
                <span key={b} data-badge={b} title={BADGE_VIEW[b].title} className={cx(BADGE, BADGE_VIEW[b].className)}>
                  {BADGE_VIEW[b].label}
                </span>
              ))}
            </span>
          ) : null}
        </span>
        {actions ? <div className={CARD_ACTIONS}>{actions}</div> : null}
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
