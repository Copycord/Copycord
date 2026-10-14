// The Guilds page while its chunk downloads: the same card and title as the page with the guild tiles it
// shows while the list loads. Kept out of Guilds.tsx so the app shell can show it without the page chunk.

import { Card, Skeleton } from "../../components/ui";

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
  return (
    <div className="relative">
      <div>
        <Card className="mt-[18px]" data-testid="card-skeleton">
          <header className="mb-1.5 block gap-3">
            <h2 aria-level={1} className="my-[0.83em] text-[24px] font-bold">
              Guilds
            </h2>
            <span className="flex flex-wrap items-center gap-2.5" />
          </header>
        </Card>
      </div>
    </div>
  );
}
