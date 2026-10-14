// One tile of the guild grid (.guild-card): icon in a glow frame and the
// name on one line with an ellipsis. The body is a button whose hit area covers
// the whole tile, and it opens the action menu where it was clicked.

import { useState } from "react";
import type { MouseEvent } from "react";
import { cx } from "../../components/ui";
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
    // .guild-card: 12px card with the old lift on hover; the body is the button that opens the menu.
    <article
      data-gid={guild.id}
      className={cx(
        "relative flex min-w-0 flex-col overflow-hidden rounded-[12px] border border-border bg-card p-3 text-left text-text",
        "[box-shadow:0_4px_12px_rgba(0,0,0,0.4)] transition-[border-color,box-shadow,transform,background-color] duration-150",
        "hover:-translate-y-0.5 hover:border-accent hover:bg-[rgba(27,16,48,0.6)] hover:[box-shadow:0_8px_20px_rgba(0,0,0,0.6)]",
      )}
    >
      <button
        type="button"
        aria-haspopup="menu"
        aria-expanded={menuOpen}
        onClick={open}
        className="block w-full min-w-0 cursor-pointer after:absolute after:inset-0 after:content-[''] border-0 bg-transparent p-0 text-left font-sans text-base text-text outline-none focus-visible:ring-2 focus-visible:ring-accent/60"
      >
        <span
          className={
            "mx-auto mt-1.5 mb-2.5 grid size-24 place-items-center rounded-[20px] border border-[rgba(255,255,255,0.06)] " +
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
        <span title={guild.name} className="mb-2.5 flow-root max-w-full truncate font-semibold">
          {guild.name}
        </span>
      </button>
    </article>
  );
}
