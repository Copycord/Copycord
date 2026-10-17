// The Channels page while its chunk downloads: the same status gate, card, title and control row as the
// page at first paint, as inert chrome; the mapping dropdown, the one control fed by the API, has a bar
// where its label goes. Kept out of Channels.tsx so the app shell can show it without the page chunk;
// the page takes its shared classes and the loading rows from here.

import { Skeleton, cx } from "../../components/ui";
import { readLastShape } from "../../components/ui/lastCount";
import { StatusGate } from "../../components/ui/StatusGate";
import { useStatusGate } from "../../hooks/useStatusGate";
import { deleteOrphansLabel, FILTER_OPTIONS, SORT_OPTIONS } from "./model";
import { ToolbarSelect } from "./ToolbarSelect";
import {
  CARD,
  CARD_ACTIONS,
  CARD_GRID,
  CARD_HEAD,
  CARD_NAME,
  CAT_COUNT,
  CAT_LABEL,
  CATEGORY_MENU_BTN,
  CHANNEL_MENU_BTN,
  ID_ROW,
  SECTION,
  SECTION_HEAD,
  SECTION_TITLE,
  TREE,
  TYPE_ICON,
} from "./treeClasses";

/** No panel: the title, the controls and the tree sit on the page with no side inset, on the container edge like every
 *  other page's title and first card. */
export const CHANNELS_CARD = "mt-[18px] border-y border-transparent py-[18px] text-text";
export const CONTROLS = "flex flex-wrap items-center gap-2.5";
/** The Guild mapping select: its toggle sizes to its label, so both rows give it one floor that the loading bar
 *  and a usual mapping name fit under; the search box after it then stands in the same place. */
export const MAPPING_SELECT = "min-w-[180px]";

export const SEARCH_INPUT =
  "w-auto max-w-[260px] min-w-[220px] flex-none rounded-input border border-log-input-edge bg-log-input-bg px-2.5 py-1.5 text-[14px] text-text-soft " +
  "caret-log-input-focus outline-none transition-[border-color,box-shadow,background-color] duration-150 " +
  "placeholder:text-[color-mix(in_srgb,var(--color-text-soft)_55%,transparent)] placeholder:opacity-75 " +
  "hover:border-log-input-focus hover:[box-shadow:0_0_0_1px_var(--color-log-input-focus)] focus:border-log-input-focus focus:[box-shadow:0_0_0_1px_var(--color-log-input-focus)]";

/** Delete orphans: a danger outline (danger text and border, no fill, the soft danger fill on hover) at the right end
 *  of the control row, apart from the filters. While there are none it drops the red (neutral border, muted text,
 *  not-allowed) so it reads as disabled. */
export const DELETE_ALL_BTN =
  "ml-auto flex h-[34px] cursor-pointer items-center justify-center rounded-input border border-danger bg-transparent px-2.5 " +
  "text-[13px] leading-8 font-semibold text-danger transition-colors duration-150 " +
  "enabled:hover:bg-danger-a10 enabled:active:bg-danger/18 " +
  "focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-white/28 " +
  "disabled:cursor-not-allowed disabled:border-border disabled:text-muted";

/** .card-titlebar: the h2, then one wrapping row of controls (.titlebar-actions). The visible h2 is the page's top heading. */
export function ChannelsTitle() {
  return (
    <h2 aria-level={1} className="my-[0.83em] text-[24px] font-bold">
      Channels
    </h2>
  );
}

// The tree draws the shape it had on the last load (channels per category), two categories on a first visit.
export const TREE_SHAPE_KEY = "channels.tree";
const DEFAULT_SHAPE = [4, 3];
const NAME_WIDTHS = [118, 92, 140, 104, 80, 126];

/** One channel card while the list loads: the card's boxes and menu button, the type icon, bars on the name and ids. */
function ChannelCardSkeleton({ index }: { index: number }) {
  return (
    <article data-skeleton-row="" className={cx(CARD, "border-border")}>
      <div className={CARD_HEAD}>
        <span className={CARD_NAME}>
          <Skeleton.Box className={TYPE_ICON} />
          <Skeleton.Text width={NAME_WIDTHS[index % NAME_WIDTHS.length]!} />
        </span>
        <div className={CARD_ACTIONS}>
          <button type="button" tabIndex={-1} className={CHANNEL_MENU_BTN}>
            ⋯
          </button>
        </div>
      </div>
      <div className={ID_ROW}>
        <Skeleton.Text width={140} />
      </div>
    </article>
  );
}

/** The channel tree while it loads: category sections with their chip and cards, shared with the page. */
export function ChannelRowsSkeleton({ shape = readLastShape(TREE_SHAPE_KEY, DEFAULT_SHAPE, 6, 8) }: { shape?: number[] }) {
  let n = 0;
  return (
    <div role="status" aria-label="Loading channels" aria-busy="true" className={TREE}>
      {shape.map((count, i) => (
        <section key={i} aria-hidden="true" className={SECTION}>
          <div className={SECTION_HEAD}>
            <h3 className={SECTION_TITLE}>
              <span className={CAT_LABEL}>
                <Skeleton.Text width={72 + (i % 3) * 18} />
                <span className={CAT_COUNT}>
                  <Skeleton.Text width={10} />
                </span>
                <span className={CATEGORY_MENU_BTN}>⋯</span>
              </span>
            </h3>
          </div>
          <div className={CARD_GRID}>
            {Array.from({ length: count }, () => (
              <ChannelCardSkeleton key={n} index={n++} />
            ))}
          </div>
        </section>
      ))}
    </div>
  );
}

const noop = () => {};

export function ChannelsSkeleton() {
  // The page gates on both sides the same way, so the skeleton shows the same cover while the bot is down.
  const gate = useStatusGate({ require: "both" });
  return (
    <StatusGate gate={gate}>
      <section className={CHANNELS_CARD} data-testid="card-skeleton">
        <header className="mb-4 block gap-3">
          <ChannelsTitle />
          {/* As at the page's first paint: the mapping list is still empty (disabled). */}
          <span inert className={CONTROLS}>
            <ToolbarSelect
              id="mapping-select-skel"
              nativeHidden
              pending
              label="Guild mapping"
              value=""
              disabled
              options={[]}
              onChange={noop}
              className={MAPPING_SELECT}
            />
            <input type="text" placeholder="Search channels…" aria-label="Search channels" readOnly className={SEARCH_INPUT} />
            <ToolbarSelect id="ch-filter-skel" nativeHidden label="Filter" value="all" options={FILTER_OPTIONS} onChange={noop} />
            <ToolbarSelect id="ch-sort-skel" nativeHidden label="Sort" value="name" options={SORT_OPTIONS} onChange={noop} />
            {/* The page opens with no orphans until the verify scan reports some. */}
            <button type="button" disabled aria-disabled title="No orphans to delete" className={DELETE_ALL_BTN}>
              {deleteOrphansLabel(0)}
            </button>
          </span>
        </header>
        <ChannelRowsSkeleton />
      </section>
    </StatusGate>
  );
}
