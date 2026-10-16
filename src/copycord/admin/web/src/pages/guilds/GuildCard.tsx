// One tile of the guild grid (.guild-card): icon in a glow frame and the
// name on one line with an ellipsis. The body is a button whose hit area covers
// the whole tile, and it opens the action menu where it was clicked.

import { useState } from "react";
import type { MouseEvent } from "react";
import { CARD_HOVER, cx } from "../../components/ui";
import type { GuildSummary } from "../../types/guilds";
import { GUILD_BODY, GUILD_CARD, GUILD_NAME, ICON_FRAME } from "./GuildsSkeleton";
import { FALLBACK_ICON } from "./model";

export interface GuildCardProps {
  guild: GuildSummary;
  menuOpen: boolean;
  onOpenMenu: (guild: GuildSummary, at: { x: number; y: number }, anchor: HTMLElement) => void;
}

export function GuildCard({ guild, menuOpen, onOpenMenu }: GuildCardProps) {
  const [broken, setBroken] = useState(false);
  const icon = !guild.icon_url || broken ? FALLBACK_ICON : guild.icon_url;

  const open = (e: MouseEvent<HTMLButtonElement>) => {
    const el = e.currentTarget;
    // Keyboard activation reports no pointer position: anchor on the tile instead.
    let at = { x: e.clientX, y: e.clientY };
    if (e.detail === 0) {
      const r = el.getBoundingClientRect();
      at = { x: r.left + r.width / 2, y: r.top + r.height / 2 };
    }
    onOpenMenu(guild, at, el);
  };

  return (
    // .guild-card: 12px card with the old lift on hover; the body is the button that opens the menu.
    <article
      data-gid={guild.id}
      className={cx(GUILD_CARD, CARD_HOVER)}
    >
      <button
        type="button"
        aria-haspopup="menu"
        aria-expanded={menuOpen}
        onClick={open}
        className={cx(
          GUILD_BODY,
          "cursor-pointer after:absolute after:inset-0 after:content-[''] outline-none focus-visible:ring-2 focus-visible:ring-accent/60",
        )}
      >
        <span
          className={cx(
            ICON_FRAME,
            "bg-[radial-gradient(120px_120px_at_10%_0,var(--color-accent-a18),transparent_60%),radial-gradient(120px_120px_at_90%_100%,var(--color-good-a18),transparent_60%)]",
          )}
        >
          <img
            src={icon}
            alt=""
            loading="lazy"
            draggable={false}
            onError={() => setBroken(true)}
            className="block size-22 rounded-[18px] object-cover"
          />
        </span>
        <span title={guild.name} className={GUILD_NAME}>
          {guild.name}
        </span>
      </button>
    </article>
  );
}
