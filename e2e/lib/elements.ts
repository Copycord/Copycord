// Element probes for the old Jinja UI (f4211c1) and the SPA, used by scripts/elemdump.ts.
// A probe is a Playwright selector, the same on both sides unless `old` or `new` overrides it.
// elemdump marks the content column with [data-uicmp-root] (old .container or main.page-main, SPA <main>)
// and the open dialog box with [data-uicmp-dialog], so probes can be scoped without class names.

export type Flavour = "old" | "new";

/**
 * How a group's matches are paired across the two sides:
 * text = visible text (or aria-label, title, placeholder), id = id (or name, placeholder),
 * heading = first heading inside, label = first label inside, attr:<name> = that attribute, index = position.
 */
export type Pair = "index" | "text" | "id" | "heading" | "label" | `attr:${string}`;

export interface Probe {
  key: string;
  sel?: string;
  old?: string;
  new?: string;
  /** Up to this many visible matches, paired by `pair`. Without it the probe is a single element. */
  max?: number;
  pair?: Pair;
  /** Single element: take the first visible match (default first match, visible or not), or the last one. */
  pick?: "first" | "visible" | "last";
}

export function selectorFor(p: Probe, ui: Flavour): string | undefined {
  return (ui === "old" ? p.old : p.new) ?? p.sel;
}

export const STYLE_PROPS = [
  "font-family", "font-size", "font-weight", "line-height", "color",
  "background-color", "background-image", "border-color", "border-width", "border-radius",
  "padding-top", "padding-right", "padding-bottom", "padding-left",
  "margin-top", "margin-right", "margin-bottom", "margin-left",
  "gap", "display", "position", "width", "max-width",
  "box-shadow", "text-align", "text-transform", "opacity",
] as const;

const R = "[data-uicmp-root]";
const D = "[data-uicmp-dialog]";
const TEXT_INPUTS = "input:not([type=hidden]):not([type=checkbox]):not([type=radio]), textarea";

// A. Header, drawer, page footer, content column.
export const SHELL: Probe[] = [
  { key: "body", sel: "body" },
  { key: "header", sel: "header:has(#menu-toggle)" },
  { key: "hamburger", sel: "#menu-toggle" },
  { key: "header.logo", sel: "header:has(#menu-toggle) img" },
  { key: "header.wordmark", old: "header:has(#menu-toggle) h1", new: "header:has(#menu-toggle) span.font-bold" },
  { key: "header.docs", sel: 'header a[href*="copycord.github.io"]' },
  { key: "sidemenu", sel: "#side-menu" },
  { key: "sidemenu.list", sel: "#side-menu nav ul" },
  { key: "sidemenu.item", sel: "#side-menu nav li > a", max: 12, pair: "text" },
  { key: "sidemenu.item.label", old: "#side-menu nav li > a .mi-label", new: "#side-menu nav li > a span:last-child", max: 12, pair: "text" },
  { key: "sidemenu.footer", sel: "#side-menu footer" },
  { key: "sidemenu.footer.link", sel: "#side-menu footer a", max: 4, pair: "text" },
  { key: "sidemenu.version", old: "#side-menu .sm-version", new: "#side-menu footer span:last-child" },
  { key: "backdrop", sel: "#menu-backdrop" },
  { key: "footer", sel: "#fixed-footer" },
  { key: "footer.inner", sel: "#fixed-footer > *", pick: "visible" },
  { key: "footer.brand", old: "#fixed-footer .brand-name", new: "#fixed-footer span.font-bold" },
  { key: "footer.version", sel: "#footer-version-link, #footer-version-text", pick: "visible" },
  { key: "footer.link", sel: "#fixed-footer a", max: 6, pair: "index" },
  { key: "container", sel: R },
];

// Content controls common to every page, scoped to the content column.
export const GENERIC: Probe[] = [
  { key: "card", sel: `${R} :is(.card, [class~="rounded-card"], section[id$="-card"], details[id$="-card"])`, max: 24, pair: "heading" },
  { key: "heading", sel: `${R} :is(h1, h2, h3, h4)`, max: 30, pair: "text" },
  { key: "button", sel: `${R} :is(button, a.btn, [role=button])`, max: 40, pair: "text" },
  { key: "input", sel: `${R} :is(${TEXT_INPUTS})`, max: 30, pair: "id" },
  { key: "select", sel: `${R} select`, max: 12, pair: "id" },
  { key: "checkbox", sel: `${R} input[type=checkbox]`, max: 20, pair: "id" },
  { key: "label", sel: `${R} label`, max: 30, pair: "text" },
  { key: "th", sel: `${R} thead th`, max: 16, pair: "text" },
  { key: "tr", sel: `${R} tbody tr`, max: 10, pair: "index" },
];

// B. Dashboard.
export const DASHBOARD: Probe[] = [
  { key: "promo.row", old: `${R} .promo-cards`, new: `${R} section[aria-label="Quick actions"]` },
  { key: "promo.card", sel: ".promo-card", max: 3, pair: "index" },
  { key: "promo.icon", sel: ".promo-icon", max: 3, pair: "index" },
  { key: "promo.badge", old: ".promo-card .promo-badge", new: ".promo-icon > span", max: 3, pair: "index" },
  { key: "promo.title", old: ".promo-title", new: ".promo-card span.font-semibold", max: 3, pair: "text" },
  { key: "promo.sub", old: ".promo-sub", new: ".promo-card span.opacity-80", max: 3, pair: "text" },
  { key: "bots.card", old: `${R} .card:has(#runtime-body)`, new: `${R} section:has(#runtime-body)` },
  { key: "bots.title", old: `${R} .card:has(#runtime-body) h2`, new: `${R} section:has(#runtime-body) h2` },
  { key: "bots.table", sel: "table:has(#runtime-body)" },
  { key: "bots.th", sel: "table:has(#runtime-body) thead th", max: 4, pair: "text" },
  { key: "bots.tr", sel: "#runtime-body tr", max: 2, pair: "index" },
  { key: "bots.td", sel: "#runtime-body tr:first-child td", max: 4, pair: "index" },
  { key: "bots.toggle", sel: "#toggle-btn" },
  { key: "bots.logs.server", sel: '[data-log="server"]' },
  { key: "bots.logs.client", sel: '[data-log="client"]' },
  { key: "cfg.card", sel: "#global-config-card" },
  { key: "cfg.summary", sel: "#global-config-card :is(summary, header)", pick: "visible" },
  { key: "cfg.title", sel: "#global-config-card h3, #global-config-card h2", pick: "visible" },
  { key: "cfg.grid", old: "#cfg-form > .grid", new: "#cfg-form > div", pick: "visible" },
  { key: "cfg.field", old: "#global-config-card .field", new: "#global-config-card div:has(> label)", max: 10, pair: "label" },
  { key: "cfg.label", sel: "#global-config-card label", max: 10, pair: "text" },
  { key: "cfg.hint", old: "#global-config-card :is(.hint, .help, .gc-hint, small, .muted)", new: "#global-config-card :is([id$='-help'], p.text-muted, p.text-xs, small)", max: 10, pair: "text" },
  { key: "cfg.server_token", sel: "#SERVER_TOKEN" },
  { key: "cfg.client_token", sel: "#CLIENT_TOKEN" },
  { key: "cfg.backup_btn", sel: "#backupTokensBtn" },
  { key: "cfg.cmd_users", sel: "#cmd_users_input" },
  { key: "cfg.autostart", sel: "#gc-autostart-toggle" },
  { key: "cfg.log_level", sel: "#LOG_LEVEL" },
  { key: "cfg.log_size", sel: "#LOG_MAX_SIZE_MB" },
  { key: "cfg.save", sel: "#cfg-save-btn" },
  { key: "cfg.cancel", sel: "#cfg-cancel-btn" },
  { key: "cfg.invite", sel: "#bot-invite-btn" },
  { key: "section.card", sel: ":is(#guild-config-card, #notifications-card, #sync-settings-card, #srv-proxy-card)", max: 4, pair: "id" },
  { key: "section.head", sel: ":is(#guild-config-card, #notifications-card, #sync-settings-card, #srv-proxy-card) > :is(summary, header, button)", max: 4, pair: "heading" },
  { key: "section.title", sel: ":is(#guild-config-card, #notifications-card, #sync-settings-card, #srv-proxy-card) :is(h2, h3)", max: 4, pair: "text" },
  { key: "section.chevron", old: ":is(#guild-config-card, #notifications-card, #sync-settings-card, #srv-proxy-card) .card-chev", new: ":is(#guild-config-card, #notifications-card, #sync-settings-card, #srv-proxy-card) > :is(summary, header, button) svg:last-child", max: 4, pair: "index" },
  { key: "section.doclink", old: ".card-doc-link", new: `${R} a[title="View docs"], ${R} a:text-is("Docs")`, max: 6, pair: "index" },
  { key: "proxy.status", sel: "#srv-proxy-status" },
  { key: "proxy.settings_btn", sel: "#srv-proxy-settings-btn" },
  { key: "proxy.fav_help_btn", sel: "#srv-proxy-fav-help-btn" },
  { key: "proxy.toggle", sel: "#srv-proxy-toggle" },
  { key: "mapping.search", sel: "#mappingSearchInput" },
  { key: "mapping.list", sel: "#guild-mapping-list" },
  { key: "mapping.add", old: "#guild-mapping-list .guild-card--new", new: "#new-mapping-card" },
  { key: "mapping.card", sel: "#guild-mapping-list .guild-card:not(.guild-card--new):not(#new-mapping-card)", max: 4, pair: "index" },
  { key: "mapping.name", sel: "#guild-mapping-list .guild-card-name-title", max: 4, pair: "text" },
  { key: "mapping.route", sel: "#guild-mapping-list .guild-card-route", max: 4, pair: "text" },
  { key: "mapping.avatar", sel: "#guild-mapping-list .guild-card-avatar", max: 4, pair: "index" },
  { key: "mapping.actions", sel: "#guild-mapping-list .guild-card-actions", max: 4, pair: "index" },
  { key: "mapping.action", sel: "#guild-mapping-list .guild-card-actions button", max: 6, pair: "index" },
];

export const BOTS_RUNNING: Probe[] = DASHBOARD.filter((p) => p.key.startsWith("bots."));

// C. Channels.
export const CHANNELS: Probe[] = [
  { key: "titlebar", old: `${R} .card-titlebar`, new: `${R} :is(header, div):has(> #mapping-select, > * #mapping-select)`, pick: "last" },
  { key: "title", sel: `${R} h2`, pick: "visible" },
  { key: "mapping_select", sel: "#mapping-select" },
  { key: "search", sel: "#ch-search" },
  { key: "filter", sel: "#ch-filter" },
  { key: "sort", sel: "#ch-sort" },
  { key: "delete_orphans", sel: "#orph-delall" },
  { key: "verify_btn", sel: "#verify-btn" },
  { key: "root", sel: "#channels-root" },
  { key: "empty", sel: "#channels-empty" },
  { key: "gate", old: `${R} :is(.status-gate, .gate, [class*="gate"])`, new: `${R} [class*="gate"], ${R} [role=status]`, pick: "visible" },
  { key: "section", sel: "#channels-root .ch-section", max: 6, pair: "heading" },
  { key: "chip", sel: "#channels-root .ch-section h3", max: 6, pair: "text" },
  { key: "card", sel: "#channels-root .ch-card[data-cid]", max: 12, pair: "attr:data-cid" },
  { key: "card.name", old: "#channels-root .ch-card[data-cid] .ch-name", new: "#channels-root .ch-card[data-cid] > div:first-child span", max: 12, pair: "text" },
  { key: "card.ids", old: "#channels-root .ch-card[data-cid] .ch-ids", new: "#channels-root .ch-card[data-cid] > div:last-child", max: 12, pair: "text" },
  { key: "card.menu", old: "#channels-root .ch-card[data-cid] .ch-menu-btn", new: "#channels-root .ch-card[data-cid] button", max: 12, pair: "index" },
];

// D. Guilds.
export const GUILDS: Probe[] = [
  { key: "titlebar", old: `${R} .card-titlebar`, new: `${R} :is(header, div):has(> #g-search, > * #g-search)`, pick: "last" },
  { key: "title", sel: `${R} h2`, pick: "visible" },
  { key: "search", sel: "#g-search" },
  { key: "sortdir", sel: "#g-sortdir" },
  { key: "root", sel: "#guilds-root" },
  { key: "empty", sel: "#guilds-empty" },
  { key: "card", sel: "#guilds-root article[data-gid]", max: 6, pair: "attr:data-gid" },
  { key: "card.body", old: "#guilds-root article[data-gid] .guild-card-body", new: "#guilds-root article[data-gid] > :first-child", max: 6, pair: "index" },
  { key: "card.icon", old: "#guilds-root article[data-gid] .guild-icon-wrap", new: "#guilds-root article[data-gid] > * > span:first-child", max: 6, pair: "index" },
  { key: "card.name", old: "#guilds-root article[data-gid] .guild-name", new: "#guilds-root article[data-gid] span[title]", max: 6, pair: "text" },
];

export const MENU: Probe[] = [
  { key: "menu", sel: "[role=menu]", pick: "visible" },
  { key: "menuitem", sel: "[role=menuitem]", max: 6, pair: "text" },
];

// E. Forwarding page with the seeded rule.
export const FORWARDING: Probe[] = [
  { key: "root", sel: "#fwd-root" },
  { key: "page.card", old: "#fwd-root > .card", new: `${R} [class~="rounded-card"]`, pick: "visible" },
  { key: "title", sel: "#fwd-root h2, main h2", pick: "visible" },
  { key: "search", sel: "#fwdSearchInput, .fwd-search-wrap input", pick: "visible" },
  { key: "add_btn", sel: ".fwd-add-btn, #fwdCreateBtn", pick: "visible" },
  { key: "count", old: ".fwd-count", new: "#fwdTotalCount, #forwarding-count", pick: "visible" },
  { key: "list", sel: "#forwarding-list" },
  { key: "rule", sel: ".fwd-card", max: 4, pair: "index" },
  { key: "rule.title", sel: ".fwd-card-title", max: 4, pair: "text" },
  { key: "rule.footer", old: ".fwd-card-footer", new: ".fwd-card > footer", max: 4, pair: "index" },
  { key: "rule.edit", sel: ".fwd-edit-btn", max: 4, pair: "index" },
  { key: "rule.toggle", sel: ".fwd-toggle-btn", max: 4, pair: "index" },
  { key: "rule.duplicate", sel: ".fwd-duplicate-btn", max: 4, pair: "index" },
  { key: "rule.delete", sel: ".fwd-delete-btn", max: 4, pair: "index" },
  { key: "rule.stat", sel: ".fwd-card-stat-value", max: 4, pair: "index" },
  { key: "rule.provider", old: ".fwd-card .badge-provider", new: ".fwd-card header span", max: 4, pair: "index" },
];

// F. Scraper.
export const SCRAPER: Probe[] = [
  { key: "root", sel: "#scraper-root" },
  { key: "header", old: ".scraper-header", new: "#scraper-root > div:has(#sc-status-pill)", pick: "last" },
  { key: "title", sel: "#scraper-root h2", pick: "visible" },
  { key: "subtitle", old: ".scraper-subtitle", new: "#scraper-root h2 + p", pick: "visible" },
  { key: "status_pill", sel: "#sc-status-pill" },
  { key: "grid", old: ".scraper-grid", new: "#scraper-root div.grid", pick: "first" },
  { key: "tokens_card", sel: "#sc-tokens-card" },
  { key: "config_card", sel: "#sc-config-card" },
  { key: "proxy_card", sel: "#sc-proxy-card" },
  { key: "queue_card", sel: "#sc-queue-card" },
  { key: "progress_card", sel: "#sc-progress-card" },
  { key: "scrapes_card", sel: "#sc-scrapes-card" },
  { key: "log_card", sel: "#sc-log-card" },
  { key: "card.title", sel: "#scraper-root section h3", max: 10, pair: "text" },
  { key: "token_count", sel: "#sc-token-count" },
  { key: "token_value", sel: "#sc-token-value" },
  { key: "token_label", sel: "#sc-token-label" },
  { key: "add_submit", sel: "#sc-add-submit" },
  { key: "guild_id", sel: "#sc-guild-id" },
  { key: "option", sel: "#sc-config-card label:has(input[type=checkbox])", max: 4, pair: "text" },
  { key: "option.box", old: "#sc-config-card .sc-check-box", new: "#sc-config-card label input[type=checkbox]", max: 4, pair: "index" },
  { key: "validate", sel: "#sc-validate-btn" },
  { key: "queue_btn", sel: "#sc-queue-btn" },
  { key: "start", sel: "#sc-start-btn" },
  { key: "log_autoclear", sel: "#sc-log-autoclear" },
  { key: "log_search", sel: "#sc-log-search" },
  { key: "log_clear", sel: "#sc-log-clear" },
  { key: "log_body", sel: "#sc-log-body" },
];

// G. System.
export const SYSTEM: Probe[] = [
  { key: "root", sel: "#adv-root" },
  { key: "data_card", sel: "#adv-data-card" },
  { key: "collapse_header", sel: "#adv-collapse-header" },
  { key: "title", sel: "#adv-data-card h3, #adv-data-card h2", pick: "visible" },
  { key: "collapse_btn", sel: "#adv-collapse-btn" },
  { key: "last_wrap", sel: "#adv-last-wrap" },
  { key: "panel", sel: "#adv-collapse-panel", pick: "first" },
  { key: "backup_now", sel: "#adv-backup-now" },
  { key: "download_latest", sel: "#adv-download-latest" },
  { key: "archives", sel: "#adv-archives" },
];

// H. Logs.
export const LOGS: Probe[] = [
  { key: "root", sel: "#logs-root" },
  { key: "title", sel: `${R} h2`, pick: "visible" },
  { key: "toolbar", old: ".logs-filters", new: "div:has(> * #logs-search):has(#logs-count)", pick: "last" },
  { key: "search", sel: "#logs-search" },
  { key: "filter_type", sel: "#logs-filter-type" },
  { key: "count", sel: "#logs-count" },
  { key: "refresh", sel: "#logs-refresh" },
  { key: "clear_all", sel: "#logs-clear-all" },
  { key: "table_wrap", old: ".logs-table-wrap", new: "div:has(> #logs-table)" },
  { key: "table", sel: "#logs-table" },
  { key: "th", sel: "#logs-table thead th", max: 8, pair: "index" },
  { key: "tr", sel: "#logs-tbody tr", max: 6, pair: "index" },
  { key: "td", sel: "#logs-tbody tr:first-child td", max: 8, pair: "index" },
  { key: "row_checkbox", sel: "#logs-table input[type=checkbox]", max: 6, pair: "index" },
  { key: "pagination", sel: "#logs-pagination" },
];

// I. Login (logged out, no shell).
export const LOGIN: Probe[] = [
  { key: "body", sel: "body" },
  { key: "page", old: "main.page-main", new: "body > #root > div, body > div > div:has(#login-form)", pick: "first" },
  { key: "wrapper", old: ".login-wrapper", new: "div:has(> section #login-form)", pick: "last" },
  { key: "logo", sel: 'img[src*="logo"]', pick: "visible" },
  { key: "card", old: ".login-card", new: "section:has(#login-form)" },
  { key: "title", sel: ":is(.login-card, section:has(#login-form)) h1" },
  { key: "subtitle", sel: ":is(.login-card, section:has(#login-form)) p", pick: "visible" },
  { key: "form", sel: "#login-form" },
  { key: "label", sel: "label[for=password]" },
  { key: "input", sel: "#password" },
  { key: "reveal", sel: "#login-form button[type=button]" },
  { key: "submit", sel: "#login-form button[type=submit]" },
  { key: "footer", sel: "#fixed-footer" },
];

// J. 404.
export const NOT_FOUND: Probe[] = [
  { key: "card", old: "main.page-main .card", new: "main > section" },
  { key: "title", sel: ":is(main.page-main .card, main > section) h2" },
  { key: "logo", sel: ":is(main.page-main .card, main > section) img" },
  { key: "back", sel: ':is(main.page-main .card, main > section) a[href="/"]' },
];

// K and E2. The open dialog box and what is inside it.
export const DIALOG: Probe[] = [
  { key: "dialog", sel: D },
  { key: "dialog.header", sel: `${D} :is(.modal-header, .modal-head, header)`, pick: "visible" },
  { key: "dialog.title", sel: `${D} :is(h1, h2, h3, h4, .modal-title-main)`, pick: "visible" },
  { key: "dialog.subtitle", sel: `${D} :is(.modal-title-subtle, header p)`, pick: "visible" },
  { key: "dialog.close", sel: `${D} :is(.verify-close, button[aria-label="Close"], button[aria-label="close"])`, pick: "visible" },
  { key: "dialog.section", sel: `${D} :is(h5, summary, legend, fieldset, .form-section-title, .mapping-section-header, .proxy-settings-group-title)`, max: 24, pair: "text" },
  { key: "dialog.label", sel: `${D} label`, max: 40, pair: "text" },
  { key: "dialog.input", sel: `${D} :is(${TEXT_INPUTS})`, max: 30, pair: "id" },
  { key: "dialog.select", sel: `${D} select`, max: 12, pair: "id" },
  { key: "dialog.checkbox", sel: `${D} input[type=checkbox]`, max: 30, pair: "id" },
  { key: "dialog.button", sel: `${D} :is(button, a.btn)`, max: 40, pair: "text" },
  { key: "dialog.footer", sel: `${D} :is(footer, .modal-footer, .form-actions, .btns)`, pick: "last" },
  { key: "dialog.footer.button", sel: `${D} :is(footer, .modal-footer, .form-actions, .btns) button`, max: 6, pair: "text" },
];

export const LOG_MODAL: Probe[] = [
  { key: "log.title", sel: "#log-title" },
  { key: "log.search", sel: "#log-search-input" },
  { key: "log.body", sel: "#log-body" },
  { key: "log.clear", sel: "#log-clear-btn" },
  { key: "log.close", sel: "#log-close" },
];

export const BACKUP_TOKENS: Probe[] = [
  { key: "bt.value", sel: "#backup_token_value" },
  { key: "bt.note", sel: "#backup_token_note" },
  { key: "bt.add", sel: "#backup-token-add" },
  { key: "bt.list", sel: "#backupTokensList" },
  { key: "bt.item", sel: "#backupTokensList > *", max: 4, pair: "index" },
];

export const FWD_MODAL: Probe[] = [
  { key: "fwd.title", sel: "#fwdModalTitle" },
  { key: "fwd.form", sel: "#forwarding-form" },
  { key: "fwd.section_title", sel: `${D} .form-section-title, ${D} h4`, max: 6, pair: "text" },
  { key: "fwd.label", sel: "#fwd_label" },
  { key: "fwd.guild", sel: "#fwd_guild_id" },
  { key: "fwd.provider_btn", sel: "#providerSelectBtn" },
  { key: "fwd.enabled", sel: "#fwd_enabled" },
  { key: "fwd.cancel", sel: "#fwdCancelBtn" },
  { key: "fwd.save", sel: "#fwdSaveBtn" },
];

export const MAPPING_MODAL: Probe[] = [
  { key: "map.name", sel: "#map_mapping_name" },
  { key: "map.host", sel: "#map_original_guild_id" },
  { key: "map.clone", sel: "#map_cloned_guild_id" },
  { key: "map.search", sel: "#mappingSettingsSearch" },
  { key: "map.features", sel: "#msg-features-open" },
  { key: "map.save", sel: "#mapping-save-btn" },
  { key: "map.cancel", sel: "#mapping-cancel-btn" },
  { key: "map.tip", old: `${D} :is(.tip, .tip-btn, .info-tip, [class*="tip-trigger"])`, new: `${D} [aria-describedby^="tip"], ${D} [class*="tooltip"]`, max: 6, pair: "index" },
];

export const FILTERS_MODAL: Probe[] = [
  { key: "flt.group", sel: `${D} :is(fieldset, .filters-group, section)`, max: 10, pair: "heading" },
  { key: "flt.chip_input", sel: `${D} .chip-input, ${D} input[id$="_input"]`, max: 12, pair: "id" },
  { key: "flt.role_refresh", sel: "#filters-role-refresh" },
  { key: "flt.save", sel: "#filters-save-btn" },
  { key: "flt.cancel", sel: "#btn-cancel-filters" },
];

export const PROXY_SETTINGS: Probe[] = [
  { key: "ps.suspend", sel: "#ps-suspend-duration" },
  { key: "ps.batch", sel: "#ps-test-batch" },
  { key: "ps.slow", sel: "#ps-slow-threshold" },
  { key: "ps.reset", sel: "#ps-reset-defaults" },
];

export const GUILD_DIALOG: Probe[] = [
  { key: "gd.name", sel: "#gd-name" },
  { key: "gd.sub", sel: "#gd-sub" },
  { key: "gd.extra", sel: "#gd-extra" },
];
