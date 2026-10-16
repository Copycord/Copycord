// The tree of category sections (.ch-section) and channel cards (.ch-card)
// inside #channels-root. Names are plain React text. Cards are memoized so
// a search only re-renders the cards whose data changed.

import { memo } from "react";
import type { ReactNode } from "react";
import { ChannelTypeIcon } from "../../components/icons";
import { CARD_HOVER, cx } from "../../components/ui";
import { categoryBadge, channelBadges, chTypeIcon, idSides } from "./model";
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
  CAT_BADGE,
  CAT_COUNT,
  CAT_LABEL,
  CUSTOM,
  ID_DASH,
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
      {tree.sections.map((s) => {
        const badge = categoryBadge(s);
        return (
          <section key={s.key} className={cx("ch-section", SECTION)}>
            <div className={SECTION_HEAD}>
              <h3 className={SECTION_TITLE}>
                <span
                  data-cat-name={s.originalName}
                  title={s.tooltip || undefined}
                  data-orphan-cat-id={s.orphanCategoryId ?? undefined}
                  className={CAT_LABEL}
                >
                  {s.displayName}
                  {badge ? (
                    <span
                      data-cat-badge={badge}
                      title={CAT_BADGE_TITLE[badge]}
                      className={cx(BADGE, BADGE_VIEW[badge].className, CAT_BADGE)}
                    >
                      {BADGE_VIEW[badge].label}
                    </span>
                  ) : null}
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
        );
      })}
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

const CAT_BADGE_TITLE: Record<"orphan" | "custom", string> = {
  orphan: "Category in the clone but no longer in the host server",
  custom: "Category renamed in the clone",
};

// The side of the id row a channel does not have: a muted dash, so the arrow always reads host → clone.
function NoId({ title }: { title: string }) {
  return (
    <span data-id-dash="" title={title} className={ID_DASH}>
      —
    </span>
  );
}

// Same hover as the Guilds tiles (CARD_HOVER); only the ⋯ menu is clickable.
const ChannelCard = memo(function ChannelCard({ channel: c, actions }: ChannelCardProps) {
  const badges = channelBadges(c);
  const ids = idSides(c);
  return (
    <article
      data-cid={c.id}
      data-orphan={c.orphan ? "1" : undefined}
      className={cx("ch-card", CARD, CARD_HOVER, c.orphan ? "border-white/12" : "border-border")}
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
        <span title="Original channel ID">{ids.original ?? <NoId title="No channel in the host server" />}</span>
        {/* .muted: 0.9em at 80% opacity, as the old clone id. */}
        <span title="Cloned channel ID" className="text-[0.9em] opacity-80">
          → {ids.cloned ?? <NoId title="Not cloned yet" />}
        </span>
      </div>
    </article>
  );
});
