// The classes of the channel tree: category sections, their chips, the channel cards and the menu
// buttons. ChannelTree, RowMenu and the loading tree (ChannelsSkeleton) all build from these, so a
// loading card has the boxes of a loaded one.

export const TREE = "grid gap-6 xl:gap-7";
export const SECTION = "grid gap-3";
export const SECTION_HEAD = "flex items-center";
export const SECTION_TITLE = "m-0 gap-[0.5ch] font-[system-ui,sans-serif] text-[12px] leading-[1.8] font-semibold text-muted";

// .ch-section-title .cat-chip: 12px system-ui pill with the old good / custom / orphan inks.
export const CAT_CHIP =
  "inline-flex min-h-6 items-center justify-center rounded-full border px-2.5 py-1 text-center " +
  "font-[system-ui,sans-serif] text-[12px] font-normal leading-[1.2]";
export const CAT_GOOD = "border-[rgba(16,185,129,0.35)] bg-[rgba(16,185,129,0.12)] text-good";
// .badge.badge-custom
export const CUSTOM =
  "border-[color-mix(in_srgb,#facc15_45%,rgba(255,255,255,0.12))] " +
  "bg-[color-mix(in_srgb,#facc15_12%,rgba(255,255,255,0.045))] text-custom-ink";
export const CAT_ORPHAN = "border-[rgba(239,68,68,0.35)] bg-[#3a1c20] text-[#ffc5cf]";

export const CARD_GRID =
  "grid grid-cols-[repeat(auto-fill,minmax(320px,1fr))] gap-3 xl:gap-4 " +
  "max-sm:grid-cols-[repeat(auto-fill,minmax(200px,1fr))] " +
  "max-[520px]:grid-cols-[repeat(auto-fill,minmax(180px,1fr))] max-[520px]:gap-2.5 " +
  "max-[360px]:grid-cols-1";
export const CARD = "block min-w-0 rounded-panel border bg-[linear-gradient(#1f1237,#160b2a)] p-3 max-[520px]:p-2.5";
// .ch-head: the name (.ch-name) and the menu button (.ch-top-right)
export const CARD_HEAD = "flex items-center justify-between";
export const CARD_NAME = "flex min-w-0 flex-1 items-center gap-1.5 font-semibold text-text max-[520px]:text-[13px]";
export const CARD_ACTIONS = "relative ml-2 flex shrink-0 items-center gap-1.5";
export const BADGE_ROW = "mt-2 flex gap-1.5";
export const ID_ROW =
  "mt-1.5 flex gap-1.5 text-[12px] text-muted max-sm:flex-wrap max-[520px]:gap-1 " +
  "max-sm:*:min-w-0 max-sm:*:[overflow-wrap:anywhere] max-[520px]:*:max-w-full max-[520px]:*:flex-[1_1_100%] max-[520px]:*:truncate";

// .badge inside a card: 600 12px/1.8 system-ui, 2px 8px.
export const BADGE =
  "inline-block rounded-full border px-2 py-0.5 font-[system-ui,sans-serif] text-[12px] font-semibold leading-[1.8]";
// .badge.muted: the .muted ink at 80% opacity (its 0.9em loses to the later 12px).
export const BADGE_MUTED = "border-border bg-bg2 text-muted opacity-80";
export const BADGE_GOOD = "border-[rgba(16,185,129,0.35)] bg-[rgba(16,185,129,0.12)] text-good";
export const BADGE_ORPHAN = "border-border bg-[#3a1c20] text-[#ffc5cf]";

// .icon-btn.ch-menu-btn and .cat-menu-trigger
export const CHANNEL_MENU_BTN = "rounded-btn border border-border bg-control px-2 py-0.5 text-text";
export const CATEGORY_MENU_BTN =
  "ml-1.5 border-0 bg-transparent px-1 text-[16px] leading-none text-inherit hover:text-[#8b8b8b] focus:text-[#8b8b8b]";
