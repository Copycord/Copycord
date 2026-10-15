// The Guilds page while its chunk downloads: the same card, title and control row (search and sort) as
// the page, as inert chrome, and the guild tiles it shows while the list loads. Kept out of Guilds.tsx
// so the app shell can show it without the page chunk; the page and GuildCard take their shared classes
// and the loading tiles from here.

import { Card, Skeleton, cx } from "../../components/ui";
import { readLastCount } from "../../components/ui/lastCount";
import { StatusGate } from "../../components/ui/StatusGate";
import { useStatusGate } from "../../hooks/useStatusGate";
import { CONTROLS, SEARCH_INPUT } from "../channels/ChannelsSkeleton";
import { sortLabel } from "./model";

export { CONTROLS, SEARCH_INPUT };

/** .btn.btn-select */
export const SORT_BTN =
  "flex min-h-[34px] cursor-pointer items-center justify-center rounded-btn border border-border bg-control px-2.5 py-2 " +
  "text-[13px] leading-none font-semibold text-text outline-none hover:border-[#7c5cff] hover:shadow-[0_0_0_0.25px_#7c5cff] " +
  "focus-visible:border-accent focus-visible:shadow-[0_0_0_4px_rgba(167,139,250,0.18)]";

export const GUILD_GRID = "grid grid-cols-[repeat(auto-fill,minmax(180px,1fr))] gap-4";

// One tile (.guild-card): the 96 px icon frame, then the name line. GuildCard and the loading tiles share these.
export const GUILD_CARD =
  "relative flex min-w-0 flex-col overflow-hidden rounded-[12px] border border-border bg-card p-3 text-left text-text " +
  "[box-shadow:0_4px_12px_rgba(0,0,0,0.4)]";
export const GUILD_BODY = "block w-full min-w-0 border-0 bg-transparent p-0 text-left font-sans text-base text-text";
export const ICON_FRAME =
  "mx-auto mt-1.5 mb-2.5 grid size-24 place-items-center rounded-[20px] border border-[rgba(255,255,255,0.06)]";
export const GUILD_NAME = "mb-2.5 flow-root max-w-full truncate font-semibold";

/** The old visible h2, still the page's top heading. */
export function GuildsTitle() {
  return (
    <h2 aria-level={1} className="my-[0.83em] text-[24px] font-bold">
      Guilds
    </h2>
  );
}

// As many tiles as the grid had last time, eight on a first visit.
export const GUILDS_COUNT_KEY = "guilds.list";
const NAME_WIDTHS = [96, 120, 76, 108];

/** Guild tiles while the list loads, shared with the page: the tile's boxes, a block on the icon, a bar on the name. */
export function GuildTileSkeletons({ rows = readLastCount(GUILDS_COUNT_KEY, 8, 24) }: { rows?: number }) {
  return (
    <div role="status" aria-label="Loading guilds" aria-busy="true" className={GUILD_GRID}>
      {Array.from({ length: rows }, (_, i) => (
        <article key={i} aria-hidden="true" data-skeleton-row="" className={GUILD_CARD}>
          <span className={GUILD_BODY}>
            <span data-skeleton="" className={cx(ICON_FRAME, "cc-skeleton border-transparent")} />
            <span className={GUILD_NAME}>
              <Skeleton.Text width={NAME_WIDTHS[i % NAME_WIDTHS.length]!} />
            </span>
          </span>
        </article>
      ))}
    </div>
  );
}

export function GuildsSkeleton() {
  // The page gates on both sides the same way, so the skeleton shows the same cover while the bot is down.
  const gate = useStatusGate({ require: "both" });
  return (
    <StatusGate gate={gate}>
      <Card className="mt-[18px]" data-testid="card-skeleton">
        <header className="mb-1.5 block gap-3">
          <GuildsTitle />
          <span inert className={CONTROLS}>
            <input type="text" placeholder="Search guilds…" aria-label="Search guilds" readOnly className={SEARCH_INPUT} />
            <button type="button" aria-label="Toggle sort direction" aria-pressed={false} className={SORT_BTN}>
              {sortLabel("asc")}
            </button>
          </span>
        </header>
        <GuildTileSkeletons />
      </Card>
    </StatusGate>
  );
}
