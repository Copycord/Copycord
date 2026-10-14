// The Channels page while its chunk downloads: the same status gate, card, title and control row as the
// page at first paint, then the rows it shows while the channel list loads. Kept out of Channels.tsx so
// the app shell can show it without the page chunk.

import { Card, SkeletonSection } from "../../components/ui";
import { StatusGate } from "../../components/ui/StatusGate";
import { useStatusGate } from "../../hooks/useStatusGate";
import { FILTER_OPTIONS, SORT_OPTIONS } from "./model";
import { ToolbarSelect } from "./ToolbarSelect";

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
      <Card className="mt-[18px] max-sm:mx-2" data-testid="card-skeleton">
        <header className="mb-1.5 block gap-3">
          <h2 aria-level={1} className="my-[0.83em] text-[24px] font-bold">
            Channels
          </h2>
          <span className="flex flex-wrap items-center gap-2.5">
            <ToolbarSelect id="mapping-select-skel" nativeHidden pending label="Guild mapping" value="" disabled options={[]} onChange={noop} />
            <ToolbarSelect id="ch-filter-skel" nativeHidden label="Filter" value="all" disabled options={FILTER_OPTIONS} onChange={noop} />
            <ToolbarSelect id="ch-sort-skel" nativeHidden label="Sort" value="name" disabled options={SORT_OPTIONS} onChange={noop} />
            <button
              type="button"
              disabled
              className="flex h-[34px] items-center justify-center rounded-input border border-border bg-[rgba(255,255,255,0.04)] px-2.5 text-[13px] leading-8 font-semibold text-accent disabled:opacity-60"
            >
              Delete all orphans
            </button>
          </span>
        </header>
      </Card>
    </StatusGate>
  );
}
