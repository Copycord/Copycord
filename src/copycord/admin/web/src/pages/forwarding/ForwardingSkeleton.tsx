// The Forwarding page while its chunk downloads: the same card, title and rule grid as the page, with the
// rule tiles it shows while the list loads. Kept out of Forwarding.tsx so the app shell can show it
// without pulling the page chunk in.

import { Button, Card, Skeleton, cx } from "../../components/ui";

/** The rule grid, shared with the page so both lay the tiles out the same way. */
export const FWD_LIST =
  "fwd-list grid grid-cols-[1fr] items-start gap-3.5 min-[701px]:grid-cols-[repeat(2,minmax(260px,1fr))] " +
  "min-[1101px]:grid-cols-[repeat(3,minmax(260px,1fr))] min-[1501px]:grid-cols-[repeat(4,minmax(260px,1fr))]";

/** The toolbar over the rules (search, Total Forwarded, New Forward) and its search box, shared with the page. */
export const FWD_TOOLBAR = "col-span-full mb-2 flex items-start justify-between gap-3";
export const FWD_SEARCH = cx(
  "w-[min(420px,100%)] min-w-[220px] rounded-input border border-[#3b365a] bg-[#1c1830] px-2.5 py-1.5",
  "text-[13px] text-[#e6e1ff] caret-[#7c5cff] outline-none transition-colors",
  "placeholder:text-[#e6e1ff]/55 placeholder:opacity-75 focus:border-[#7c5cff]",
);

/** The search and count toolbar over the rules as inert chrome, a bar on the total, then the rule tiles, while the list loads. */
export function RuleSkeletons() {
  return (
    <>
      <div aria-hidden="true" inert className={FWD_TOOLBAR}>
        <div className="flex flex-col">
          <div className="flex min-w-0 flex-auto items-center gap-2.5">
            <input
              type="text"
              placeholder="Search rules…"
              aria-label="Search forwarding rules"
              className={FWD_SEARCH}
            />
          </div>
          <div className="mt-2 mb-3">
            <span className="opacity-60">Total Forwarded:</span>{" "}
            <span data-skeleton="" className="cc-skeleton inline-block rounded-md align-middle text-transparent">
              0000
            </span>
          </div>
        </div>
        <div className="flex items-center">
          <Button variant="primary" className="fwd-add-btn">
            New Forward
          </Button>
        </div>
      </div>
      {/* One tile: the rule count is unknown until the list answers, and one rule is the common case. */}
      <Skeleton.Card height={116} testId="fwd-skeleton" />
    </>
  );
}

export function ForwardingSkeleton() {
  return (
    <div className="forwarding-page w-full" role="status" aria-busy="true" aria-label="Loading page">
      <div className="relative">
        <div>
          <Card className="mt-[18px]" data-testid="card-skeleton">
            <header>
              <h2 className="mb-1 text-[18px] font-bold">Forwarding</h2>
            </header>
            <div className="mt-1.5">
              <div className={FWD_LIST}>
                <RuleSkeletons />
              </div>
            </div>
          </Card>
        </div>
      </div>
      <h1 className="sr-only">Forwarding</h1>
    </div>
  );
}
