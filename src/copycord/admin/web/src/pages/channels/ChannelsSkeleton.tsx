// The Channels page while its chunk downloads: the same status gate, card, title and control row as the
// page at first paint, as inert chrome; the mapping dropdown, the one control fed by the API, has a bar
// where its label goes. Kept out of Channels.tsx so the app shell can show it without the page chunk;
// the page takes its shared classes and the loading rows from here.

import { Card, SkeletonSection } from "../../components/ui";
import { StatusGate } from "../../components/ui/StatusGate";
import { useStatusGate } from "../../hooks/useStatusGate";
import { FILTER_OPTIONS, SORT_OPTIONS } from "./model";
import { ToolbarSelect } from "./ToolbarSelect";

/** The old .container.wide sat 20px in from the body padding at every width; the shell gives 12px below sm. */
export const CHANNELS_CARD = "mt-[18px] max-sm:mx-2";
export const CONTROLS = "flex flex-wrap items-center gap-2.5";

export const SEARCH_INPUT =
  "w-auto max-w-[260px] min-w-[220px] flex-none rounded-input border border-[#3b365a] bg-[#1c1830] px-2.5 py-1.5 text-[14px] text-[#e6e1ff] " +
  "caret-[#7c5cff] outline-none transition-[border-color,box-shadow,background-color] duration-150 " +
  "placeholder:text-[color-mix(in_srgb,#e6e1ff_55%,transparent)] placeholder:opacity-75 " +
  "hover:border-[#7c5cff] hover:[box-shadow:0_0_0_1px_#7c5cff] focus:border-[#7c5cff] focus:[box-shadow:0_0_0_1px_#7c5cff]";

/** .btn.btn-ghost-red */
export const DELETE_ALL_BTN =
  "flex h-[34px] cursor-pointer items-center justify-center rounded-input border border-border bg-[rgba(255,255,255,0.04)] px-2.5 " +
  "text-[13px] leading-8 font-semibold text-accent transition-colors duration-150 " +
  "hover:border-[color-mix(in_oklab,#2c1d4a_20%,#ef4444_80%)] hover:bg-[rgba(255,255,255,0.08)] hover:text-danger active:bg-[rgba(255,255,255,0.1)] " +
  "focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-white/28 " +
  "disabled:cursor-not-allowed disabled:opacity-60 disabled:hover:border-border disabled:hover:bg-[rgba(255,255,255,0.04)] disabled:hover:text-accent";

/** .card-titlebar: the h2, then one wrapping row of controls (.titlebar-actions). The old visible h2 is the page's top heading. */
export function ChannelsTitle() {
  return (
    <h2 aria-level={1} className="my-[0.83em] text-[24px] font-bold">
      Channels
    </h2>
  );
}

/** Channel rows while the list loads, shared with the page. */
export function ChannelRowsSkeleton() {
  return <SkeletonSection rows={6} label="Loading channels" className="px-2.5 py-2" />;
}

const noop = () => {};

export function ChannelsSkeleton() {
  // The page gates on both sides the same way, so the skeleton shows the same cover while the bot is down.
  const gate = useStatusGate({ require: "both" });
  return (
    <StatusGate gate={gate}>
      <Card className={CHANNELS_CARD} data-testid="card-skeleton">
        <header className="mb-1.5 block gap-3">
          <ChannelsTitle />
          {/* As at the page's first paint: the mapping list is still empty (disabled), the search box waits for the gate. */}
          <span inert className={CONTROLS}>
            <ToolbarSelect id="mapping-select-skel" nativeHidden pending label="Guild mapping" value="" disabled options={[]} onChange={noop} />
            <input type="text" placeholder="Search channels…" aria-label="Search channels" hidden className={SEARCH_INPUT} />
            <ToolbarSelect id="ch-filter-skel" nativeHidden label="Filter" value="all" options={FILTER_OPTIONS} onChange={noop} />
            <ToolbarSelect id="ch-sort-skel" nativeHidden label="Sort" value="name" options={SORT_OPTIONS} onChange={noop} />
            <button type="button" disabled className={DELETE_ALL_BTN}>
              Delete all orphans
            </button>
          </span>
        </header>
      </Card>
    </StatusGate>
  );
}
