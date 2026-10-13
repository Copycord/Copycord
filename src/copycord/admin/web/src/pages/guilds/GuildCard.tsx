// One tile of the guild grid (.guild-card): icon in a glow frame and the
// name clamped to two lines. The whole tile is a button that opens the
// action menu where it was clicked.

import { useState } from "react";
import type { MouseEvent } from "react";
import type { GuildSummary } from "../../types/guilds";
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
    <article data-gid={guild.id} className="relative min-w-0">
      <button
        type="button"
        aria-haspopup="menu"
        aria-expanded={menuOpen}
        onClick={open}
        className={
          "flex w-full cursor-pointer flex-col items-center rounded-panel border border-border " +
          "bg-linear-to-b from-[#1f1237] to-[#160b2a] px-3 pt-4 pb-3.5 text-center text-text outline-none " +
          "hover:shadow-[inset_0_0_0_2px_rgba(167,139,250,0.18),0_4px_12px_rgba(0,0,0,0.25)] hover:outline hover:outline-accent/35 " +
          "focus-visible:ring-2 focus-visible:ring-accent/60 aria-expanded:outline aria-expanded:outline-accent/35"
        }
      >
        <span
          className={
            "mx-auto mt-1.5 mb-2.5 grid size-24 place-items-center rounded-[20px] border border-white/6 " +
            "bg-[radial-gradient(120px_120px_at_10%_0,rgba(167,139,250,0.18),transparent_60%),radial-gradient(120px_120px_at_90%_100%,rgba(16,185,129,0.18),transparent_60%)]"
          }
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
        <span title={guild.name} className="mb-2.5 line-clamp-2 max-w-full font-semibold break-words">
          {guild.name}
        </span>
      </button>
    </article>
  );
}
