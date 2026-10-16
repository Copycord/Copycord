// The classes of the channel tree: category sections, their chips, the channel cards and the menu
// buttons. ChannelTree, RowMenu and the loading tree (ChannelsSkeleton) all build from these, so a
// loading card has the boxes of a loaded one.

export const TREE = "grid gap-6 xl:gap-7";
export const SECTION = "grid gap-3";
// The category header: one neutral treatment for every category (label, channel count, the ⋯ menu) over
// a divider. Only an orphan or customized category adds a dot before its label.
export const SECTION_HEAD = "flex items-center border-b border-border pb-1.5";
export const SECTION_TITLE =
  "m-0 flex min-h-6 items-center gap-2 font-[system-ui,sans-serif] text-[12px] leading-[1.8] font-semibold tracking-[0.04em] text-muted uppercase";
export const CAT_LABEL = "inline-flex items-center gap-2";
export const CAT_COUNT = "font-normal tabular-nums opacity-80";
export const CAT_DOT = "inline-block size-2 shrink-0 rounded-full";
export const CAT_DOT_ORPHAN = "bg-stop-text";
export const CAT_DOT_CUSTOM = "bg-custom-ink";
// .badge.badge-custom
export const CUSTOM =
  "bg-[color-mix(in_srgb,var(--color-custom-ink)_12%,var(--color-wash-4-5))] text-custom-ink";

export const CARD_GRID =
  "grid grid-cols-[repeat(auto-fill,minmax(min(320px,100%),1fr))] gap-3 xl:gap-4 max-[520px]:gap-2.5";
// The card surface of the dashboard mapping cards (bg-card).
export const CARD = "block min-w-0 rounded-panel border bg-card p-3 max-[520px]:p-2.5";
// .ch-head: the name (.ch-name) and the menu button (.ch-top-right)
export const CARD_HEAD = "flex items-center justify-between";
export const CARD_NAME = "flex min-w-0 flex-1 items-center gap-1.5 font-semibold text-text max-[520px]:text-[13px]";
export const CARD_ACTIONS = "relative ml-2 flex shrink-0 items-center gap-1.5";
// The badges sit in the name row after the name: only the exceptions (Orphan, Not cloned, Custom) have one.
// -my-1 keeps a badged name row as tall as a plain one (the 25.6px badge over the 24px name line).
export const BADGE_ROW = "-my-1 ml-1 flex shrink-0 gap-1.5";
export const TYPE_ICON = "size-4 shrink-0 text-muted";
export const ID_ROW =
  "mt-1.5 flex gap-1.5 text-[12px] text-muted max-sm:flex-wrap max-[520px]:gap-1 " +
  "max-sm:*:min-w-0 max-sm:*:[overflow-wrap:anywhere] max-[520px]:*:max-w-full max-[520px]:*:flex-[1_1_100%] max-[520px]:*:truncate";

// .badge inside a card: 600 12px/1.8 system-ui, 2px 8px; fill and text only, the border stays transparent.
export const BADGE =
  "inline-block rounded-full border border-transparent px-2 py-0.5 font-[system-ui,sans-serif] text-[12px] font-semibold leading-[1.8]";
// Not cloned: the channel has no clone target, in the info blue so it stands apart from Orphan and Custom.
export const BADGE_NOT_CLONED = "bg-info-a12 text-info";
export const BADGE_ORPHAN = "bg-stop-bg text-stop-text";

// .icon-btn.ch-menu-btn and .cat-menu-trigger
export const CHANNEL_MENU_BTN = "rounded-btn border border-border bg-control px-2 py-0.5 text-text";
export const CATEGORY_MENU_BTN =
  "ml-1.5 border-0 bg-transparent px-1 text-[16px] leading-none text-inherit hover:text-menu-hover focus:text-menu-hover";
