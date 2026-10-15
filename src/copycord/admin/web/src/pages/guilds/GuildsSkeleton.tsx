// The Guilds page while its chunk downloads: the same card, title and control row as the page at first
// paint (the search and the sort button wait for the gate), as inert chrome, and the guild tiles it shows
// while the list loads. Kept out of Guilds.tsx so the app shell can show it without the page chunk; the
// page takes its shared classes and the loading tiles from here.

import { Card, Skeleton } from "../../components/ui";
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

/** The old visible h2, still the page's top heading. */
export function GuildsTitle() {
  return (
    <h2 aria-level={1} className="my-[0.83em] text-[24px] font-bold">
      Guilds
    </h2>
  );
}

/** Guild tiles while the list loads, shared with the page. */
export function GuildTileSkeletons() {
  return (
    <div role="status" aria-label="Loading guilds" aria-busy="true" className="grid grid-cols-[repeat(auto-fill,minmax(180px,1fr))] gap-4">
      {Array.from({ length: 8 }, (_, i) => (
        // The .guild-card box: 96 px icon tile, then the name line.
        <div key={i} aria-hidden="true" className="flex flex-col items-center rounded-[12px] border border-border bg-card p-3">
          <Skeleton.Block width={96} height={96} rounded="rounded-[20px]" className="mt-1.5 mb-2.5" />
          <Skeleton.Line width="60%" height={14} className="mt-[3px] mb-[12px]" />
        </div>
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
            <input type="text" placeholder="Search guilds…" aria-label="Search guilds" hidden className={SEARCH_INPUT} />
            <button type="button" hidden className={SORT_BTN}>
              {sortLabel("asc")}
            </button>
          </span>
        </header>
      </Card>
    </StatusGate>
  );
}
