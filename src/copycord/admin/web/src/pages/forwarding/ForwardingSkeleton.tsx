// The Forwarding page while its chunk downloads: the same card, title and rule grid as the page, with the
// rule tiles it shows while the list loads. Kept out of Forwarding.tsx so the app shell can show it
// without pulling the page chunk in.

import { Card, Skeleton, cx } from "../../components/ui";

/** The rule grid, shared with the page so both lay the tiles out the same way. */
export const FWD_LIST =
  "fwd-list grid grid-cols-[1fr] items-start gap-3.5 min-[701px]:grid-cols-[repeat(2,minmax(260px,1fr))] " +
  "min-[1101px]:grid-cols-[repeat(3,minmax(260px,1fr))] min-[1501px]:grid-cols-[repeat(4,minmax(260px,1fr))]";

/** Rule tiles shown while the list loads. */
export function RuleSkeletons() {
  return (
    <>
      {[0, 1, 2].map((i) => (
        <Skeleton.Card key={i} height={118} testId="fwd-skeleton" />
      ))}
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
              <div className={cx(FWD_LIST)}>
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
