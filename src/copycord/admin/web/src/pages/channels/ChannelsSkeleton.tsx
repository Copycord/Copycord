// The Channels page while its chunk downloads: the same card, title and control row as the page, then the
// rows it shows while the channel list loads. Kept out of Channels.tsx so the app shell can show it
// without the page chunk.

import { Card, Skeleton, SkeletonSection } from "../../components/ui";

/** Channel rows while the list loads, shared with the page. */
export function ChannelRowsSkeleton() {
  return <SkeletonSection rows={6} label="Loading channels" className="px-2.5 py-2" />;
}

export function ChannelsSkeleton() {
  return (
    <div className="relative">
      <div>
        <Card className="mt-[18px] max-sm:mx-2" data-testid="card-skeleton">
          <header className="mb-1.5 block gap-3">
            <h2 aria-level={1} className="my-[0.83em] text-[24px] font-bold">
              Channels
            </h2>
            <span className="flex flex-wrap items-center gap-2.5">
              <Skeleton.Block width={216} height={34} rounded="rounded-input" />
              <button
                type="button"
                disabled
                className="flex h-[34px] items-center justify-center rounded-input border border-border bg-[rgba(255,255,255,0.04)] px-2.5 text-[13px] leading-8 font-semibold text-accent disabled:opacity-60"
              >
                Delete all orphans
              </button>
            </span>
          </header>
          <ChannelRowsSkeleton />
        </Card>
      </div>
    </div>
  );
}
