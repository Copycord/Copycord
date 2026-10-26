// The mappings section's shared pieces: the header, the scrolling list, the card box and its three line
// boxes, the chip and the loading cards. The page section and the chunk skeleton both build from these,
// so a loading card has the height and lines of a loaded one. Card surface and chips as the senders section.

import type { ReactNode } from "react";
import { Skeleton, cx } from "../../components/ui";
import { readLastCount } from "../../components/ui/lastCount";
import { COUNT } from "../logs/LogsSkeleton";

export const MAPPINGS_SECTION = "mt-[18px]";
// Cards in a grid that scrolls inside about three rows of cards, so many mappings never push the
// sending accounts off screen.
export const MAPPINGS_LIST = "cc-hscroll m-0 grid max-h-[268px] list-none grid-cols-1 gap-2.5 overflow-auto p-0 sm:grid-cols-2 xl:grid-cols-3";
export const MAPPING_CARD = cx(
  "flex min-w-0 flex-col gap-1.5 rounded-xl border border-border px-4 py-3",
  "bg-linear-to-b from-card-a50 to-well-a60",
);
/** The name and chip line, the status line and the bar: fixed line boxes, so changing counts never move a card. */
export const MAPPING_TOP = "flex h-6 min-w-0 items-center justify-between gap-3";
export const MAPPING_NAME = "min-w-0 truncate text-[14px] leading-6 font-semibold text-text";
// One line high: counts that do not fit wrap onto a hidden second line whole, never cut mid word.
export const MAPPING_LINE = "flex h-5 min-w-0 flex-wrap items-center gap-x-3 overflow-hidden text-[13px] leading-5 whitespace-nowrap";
export const MAPPING_BAR = "flex h-1.5 w-full gap-px overflow-hidden rounded-full bg-wash-6";
export const MAPPINGS_NOTE = "m-0 rounded-xl border border-border px-4 py-6 text-center text-[13px] text-muted";

export const MAPPINGS_COUNT_KEY = "system.mappings";

// The Active or Paused chip, in the senders chip's shape. Both labels share one grid cell, the other
// one invisible, so the chip keeps its width when a mapping is paused or resumed.
const CHIP = "inline-grid shrink-0 rounded-[20px] px-2.5 py-1 text-center text-[11px] leading-none font-semibold";
const CHIP_CELL = "[grid-area:1/1]";

function ChipLabels({ paused }: { paused: boolean }) {
  return (
    <>
      <span data-chip-label="" className={CHIP_CELL}>
        {paused ? "Paused" : "Active"}
      </span>
      <span aria-hidden="true" className={cx(CHIP_CELL, "invisible")}>
        {paused ? "Active" : "Paused"}
      </span>
    </>
  );
}

export function MappingChip({ paused }: { paused: boolean }) {
  return (
    <span data-mapping-chip={paused ? "paused" : "active"} className={cx(CHIP, paused ? "bg-warn-a12 text-warn" : "bg-ok-a12 text-ok")}>
      <ChipLabels paused={paused} />
    </span>
  );
}

/** Title left, the mapping count right, as the senders header. */
export function MappingsHeader({ count }: { count: ReactNode }) {
  return (
    <div className="mb-3 flex flex-wrap items-end justify-between gap-x-4 gap-y-1">
      <h2 id="system-mappings-title" className="m-0 text-[22px] font-bold text-text">
        Mappings
      </h2>
      <span id="system-mappings-count" className={COUNT} aria-live="polite">
        {count}
      </span>
    </div>
  );
}

/** The count until the first answer: a bar in the line box of "4 mappings". */
export function MappingsCountBar() {
  return <Skeleton.Text className={COUNT} width={72} />;
}

const NAME_BAR = [120, 96, 144];
const LINE_BAR = [150, 110, 128];

/** Cards while the first answer loads: as many as last time (up to the list's height), two on a first visit. */
export function MappingsLoadingCards({ cards = readLastCount(MAPPINGS_COUNT_KEY, 2, 9) }: { cards?: number }) {
  return (
    <>
      {Array.from({ length: cards }, (_, i) => (
        <li key={i} data-skeleton-card="" className={MAPPING_CARD}>
          {i === 0 ? <span role="status" aria-label="Loading mappings" className="sr-only" /> : null}
          <div className={MAPPING_TOP}>
            <Skeleton.Text className={MAPPING_NAME} width={NAME_BAR[i % NAME_BAR.length]} />
            <Skeleton.Box className={CHIP}>
              <ChipLabels paused={false} />
            </Skeleton.Box>
          </div>
          <div className={MAPPING_LINE}>
            <Skeleton.Text className="leading-5" width={LINE_BAR[i % LINE_BAR.length]} />
          </div>
          <div className={MAPPING_BAR} />
        </li>
      ))}
    </>
  );
}

/** The section while the page chunk downloads: the real header and loading cards. */
export function MappingsSectionSkeleton() {
  return (
    <section aria-labelledby="system-mappings-title" className={MAPPINGS_SECTION}>
      <MappingsHeader count={<MappingsCountBar />} />
      <ul inert aria-label="Mappings" className={MAPPINGS_LIST}>
        <MappingsLoadingCards />
      </ul>
    </section>
  );
}
