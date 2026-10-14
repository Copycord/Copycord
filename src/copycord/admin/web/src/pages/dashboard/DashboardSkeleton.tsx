// The Configuration page while its chunk downloads: the same cards in the same order with their real
// titles, buttons and table heads, shimmer only where a value goes. Kept out of Dashboard.tsx so the app
// shell can show it without pulling the page chunk in. The sections share the loading bodies below so the
// page's own loading state and this one are the same boxes.

import { Skeleton, SkeletonSection, cx } from "../../components/ui";
import { PromoCards } from "./config/PromoCards";
import { BOT_ROLES, BOTS_CELL, BOTS_HEAD, BOTS_STATE, CARD, Chevron, GHOST, SUMMARY, SUMMARY_TITLE } from "./parts";

/** Mapping tiles shown while the list loads; three is the common count. */
export const MAPPING_SKELETONS = 3;

/** Global Configuration body while the config loads. */
export function GlobalConfigLoading() {
  return <SkeletonSection rows={6} rowHeight={34} label="Loading configuration" />;
}

/** One mapping tile while the list loads. */
export function MappingTileSkeleton() {
  return <Skeleton.Card height="auto" testId="mapping-skeleton" className="min-h-[150px]" />;
}

function DocIcon() {
  return <span aria-hidden="true" className="inline-flex size-3.5 opacity-50" />;
}

function Summary({ title }: { title: string }) {
  return (
    <summary className={SUMMARY} onClick={(e) => e.preventDefault()}>
      <h3 className={SUMMARY_TITLE}>{title}</h3>
      <DocIcon />
      <Chevron />
    </summary>
  );
}

export function DashboardSkeleton() {
  return (
    <div className="flex flex-col" role="status" aria-busy="true" aria-label="Loading page" data-testid="dashboard-skeleton">
      <h1 className="sr-only">Configuration</h1>
      <PromoCards />
      <section data-testid="card-skeleton" className="mt-[18px] rounded-card border border-border bg-(image:--card-gradient) p-[18px] text-text">
        <header>
          <h2 className="my-[0.83em] text-[24px] leading-[normal] font-bold">Bots</h2>
        </header>
        <div className="w-full overflow-x-hidden">
          <table className="mt-2 w-full table-fixed border-collapse text-left">
            <thead>
              <tr>
                <th className={cx(BOTS_HEAD, "text-left")}>Component</th>
                <th className={cx(BOTS_HEAD, "w-[110px] text-center")}>State</th>
                <th className={cx(BOTS_HEAD, "w-[120px] text-center")}>Uptime</th>
                <th className={cx(BOTS_HEAD, "text-center")}>Status</th>
              </tr>
            </thead>
            <tbody>
              {BOT_ROLES.map(({ role, label }) => (
                <tr key={role}>
                  <td className={cx(BOTS_CELL, "text-left")}>{label}</td>
                  <td className={cx(BOTS_CELL, "text-center")}>
                    <span data-skeleton="" className={cx(BOTS_STATE, "cc-skeleton border-transparent text-transparent")}>
                      stopped
                    </span>
                  </td>
                  <td className={cx(BOTS_CELL, "text-center")} />
                  <td className={cx(BOTS_CELL, "text-center")} />
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        <div className="mt-3 flex flex-wrap gap-2.5 max-sm:grid max-sm:grid-cols-1">
          <span className="inline max-sm:block">
            <button type="button" disabled className={cx(GHOST, "inline-flex max-sm:w-full")}>
              Start
            </button>
          </span>
          <span className="flex-1 max-sm:hidden" />
          <button type="button" disabled className={cx(GHOST, "flex max-sm:w-full")}>
            View server logs
          </button>
          <button type="button" disabled className={cx(GHOST, "flex max-sm:w-full")}>
            View client logs
          </button>
        </div>
      </section>
      <details open data-testid="card-skeleton" className={CARD}>
        <Summary title="Global Configuration" />
        <div className="mt-3.5">
          <GlobalConfigLoading />
        </div>
      </details>
      <details open data-testid="card-skeleton" className={CARD}>
        <Summary title="Cloning Configuration" />
        <div className="mt-3.5">
          <div className="flex flex-wrap items-center justify-between gap-3">
            <p className="m-0 max-w-[480px] min-w-[200px] flex-[1_1_auto] text-xs leading-[1.4] text-muted opacity-90">
              Map a source server to a target server to enable cloning for that pair
            </p>
            <div className="ml-auto">
              <input
                type="text"
                disabled
                placeholder="Search mappings…"
                aria-label="Search mappings"
                className="ml-0 inline-block min-w-[220px] rounded-lg border border-[#3b365a] bg-[#1c1830] px-2.5 py-1.5 text-[13.3333px] text-[#e6e1ff] outline-none placeholder:text-[#e6e1ff]/55"
              />
            </div>
          </div>
          <div className="mt-4 grid max-h-80 min-h-[180px] grid-cols-[repeat(auto-fill,minmax(220px,1fr))] gap-4 overflow-y-auto py-2 pr-1 max-[600px]:grid-cols-[repeat(auto-fill,minmax(180px,1fr))] max-[600px]:gap-3">
            <span className="flex rounded-xl border border-dashed border-border bg-[rgba(255,255,255,0.02)] p-4 max-[600px]:min-h-[120px]" />
            {Array.from({ length: MAPPING_SKELETONS }, (_, i) => (
              <MappingTileSkeleton key={i} />
            ))}
          </div>
        </div>
      </details>
      {["Notifications", "Sync Settings", "Proxy"].map((title) => (
        <details key={title} data-testid="card-skeleton" className={CARD}>
          <Summary title={title} />
        </details>
      ))}
    </div>
  );
}
