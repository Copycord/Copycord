// The Scraper page while its chunk downloads: the same header, card grid and cards as the page, their
// titles, inputs, checkboxes, buttons and icons as inert chrome, with bars only where the API fills in
// (the token count, the token list and the saved scrapes). Kept out of Scraper.tsx so the app shell can
// show it without pulling the page chunk in; the cards take their shared classes and pieces from here.

import { Button, Card, Input, Skeleton, cx } from "../../components/ui";
import { readLastCount } from "../../components/ui/lastCount";
import { DownloadIcon, PlayIcon, QueueIcon, RefreshIcon, TrashIcon, UsersIcon } from "./icons";

// .scraper-card .card-titlebar h3 and the .card-titlebar row; every scraper card sits 18px below the last.
export const TITLE = "m-0 text-[15px] font-bold whitespace-nowrap text-accent";
export const TITLEBAR = "mb-1.5 flex items-center justify-between gap-2.5";
export const SC_CARD = "mt-[18px] max-w-full";

// .sc-field label and .sc-opts legend.
export const SC_LABEL = "text-xs font-medium tracking-[0.04em] text-muted uppercase";
// .sc-check: the label row; the checkbox itself draws the old .sc-check-box (18px, purple outline and
// a 10px purple square when checked).
export const SC_CHECK = "flex cursor-pointer items-center gap-2 text-[13px] text-text select-none";
export const CHECK_BOX =
  "m-0 flex size-[18px] shrink-0 cursor-pointer appearance-none items-center justify-center rounded-[5px] " +
  "border-[1.5px] border-border bg-[rgba(20,11,38,0.5)] bg-center bg-no-repeat [background-size:10px_10px] [font-family:inherit] " +
  "transition-[background,border-color] duration-150 outline-none focus-visible:outline-2 focus-visible:outline-offset-2 " +
  "focus-visible:outline-primary checked:border-primary checked:bg-[rgba(139,92,246,0.18)] " +
  "checked:bg-[image:linear-gradient(var(--color-primary),var(--color-primary))]";
// The same box as a span after a visually hidden peer checkbox, as the old markup drew it.
export const CHECK_SPAN =
  "inline-flex size-[18px] shrink-0 items-center justify-center rounded-[5px] border-[1.5px] border-border " +
  "bg-[rgba(20,11,38,0.5)] transition-[background,border-color] duration-150 after:block after:size-2.5 " +
  "after:rounded-[2px] after:content-[''] peer-checked:border-primary peer-checked:bg-[rgba(139,92,246,0.18)] " +
  "peer-checked:after:bg-primary peer-focus-visible:outline-2 peer-focus-visible:outline-offset-2 peer-focus-visible:outline-primary";

// .scraper-status-pill and its idle look.
export const PILL_BOX =
  "mt-1 inline-flex shrink-0 items-center gap-[7px] rounded-[20px] border px-3.5 py-1.5 text-xs font-semibold " +
  "tracking-[0.03em] whitespace-nowrap transition-all duration-[250ms]";
export const IDLE_PILL = { box: "border-border bg-[rgba(255,255,255,0.04)] text-muted", dot: "bg-muted" };

/** The status pill until the first status answer: the pill's box with a bar over it. */
export function PillPending() {
  return (
    <Skeleton.Box className={PILL_BOX}>
      <span aria-hidden="true" className="size-2 rounded-full" />
      <span>Idle</span>
    </Skeleton.Box>
  );
}

/** The 26px magnifier before the page title. */
export function SearchIcon({ className }: { className?: string }) {
  return (
    <svg width={26} height={26} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={1.5} aria-hidden="true" className={className}>
      <path strokeLinecap="round" strokeLinejoin="round" d="m21 21-5.197-5.197m0 0A7.5 7.5 0 1 0 5.196 5.196a7.5 7.5 0 0 0 10.607 10.607Z" />
    </svg>
  );
}

/** The title and subtitle left of the status pill. */
export function ScraperTitle() {
  return (
    <div className="min-w-0">
      <h2 className="m-0 flex items-center gap-2.5 text-[22px] font-bold whitespace-nowrap text-accent">
        <SearchIcon className="shrink-0 text-primary" />
        Member Scraper
      </h2>
      <p className="m-0 mt-1 text-[13px] text-muted">Scrape Discord server members</p>
    </div>
  );
}

// .btn.btn-ghost.btn-xs holding a 14px icon.
export const ICON_BUTTON =
  "inline-flex min-h-7 cursor-pointer items-center justify-center rounded-btn border border-border bg-[rgba(255,255,255,0.04)] " +
  "px-2.5 py-1.5 leading-none " +
  "text-accent outline-none transition duration-200 focus-visible:ring-2 focus-visible:ring-accent/60 " +
  "enabled:hover:-translate-y-0.5 enabled:hover:border-accent/30 enabled:hover:bg-white/8 disabled:cursor-not-allowed disabled:opacity-55";
export const DELETE_BUTTON = cx(ICON_BUTTON, "enabled:hover:border-danger enabled:hover:text-danger");

// One token row (.sc-token-item): the validity badge, name and masked value, then Re-validate and Delete.
// TokensCard and the loading rows build from these, so both have the same boxes.
export const TOKEN_ROW =
  "mb-1.5 flex items-center justify-between gap-2.5 rounded-btn border border-border bg-[rgba(20,11,38,0.4)] px-3 py-2.5 " +
  "transition-colors duration-200 last:mb-0 hover:border-[rgba(139,92,246,0.25)] hover:bg-[rgba(20,11,38,0.7)]";
export const TOKEN_MAIN = "flex min-w-0 items-center gap-2.5";
export const TOKEN_BADGE = "inline-flex size-7 shrink-0 items-center justify-center rounded-lg border";
export const TOKEN_TEXT = "flex min-w-0 flex-col gap-0.5";
export const TOKEN_NAME = "truncate text-[13px] font-semibold text-text";
export const TOKEN_MASK = "truncate font-mono text-[11px] text-muted";
export const TOKEN_ACTIONS = "flex shrink-0 gap-1";

// The token and scrape lists draw as many loading rows as they had last time (none on a first visit).
export const TOKENS_COUNT_KEY = "scraper.tokens";
export const SCRAPES_COUNT_KEY = "scraper.scrapes";
const MAX_ROWS = 6;
const NAME_WIDTHS = [96, 128, 84, 112];

/** The token list while it loads: as many token rows as last time, or a bar for each line of the empty state. */
export function TokenListPlaceholder({ rows = readLastCount(TOKENS_COUNT_KEY, 0, MAX_ROWS) }: { rows?: number }) {
  if (rows > 0)
    return (
      <div role="status" aria-label="Loading tokens" aria-busy="true">
        {Array.from({ length: rows }, (_, i) => (
          <div key={i} aria-hidden="true" data-skeleton-row="" className={TOKEN_ROW}>
            <div className={TOKEN_MAIN}>
              <Skeleton.Box className={TOKEN_BADGE} />
              <div className={TOKEN_TEXT}>
                <Skeleton.Text className={TOKEN_NAME} width={NAME_WIDTHS[i % NAME_WIDTHS.length]!} />
                <Skeleton.Text className={TOKEN_MASK} width={170} />
              </div>
            </div>
            <div className={TOKEN_ACTIONS}>
              <button type="button" tabIndex={-1} className={ICON_BUTTON}>
                <RefreshIcon />
              </button>
              <button type="button" tabIndex={-1} className={DELETE_BUTTON}>
                <TrashIcon />
              </button>
            </div>
          </div>
        ))}
      </div>
    );
  return (
    <div role="status" aria-label="Loading tokens" aria-busy="true" className="flex flex-col items-center gap-1 px-4 py-8 text-center">
      <Skeleton.Block width={32} height={32} className="mb-1.5" />
      <p aria-hidden="true" className="m-0 text-sm">
        <Skeleton.Text>No tokens configured</Skeleton.Text>
      </p>
      <small aria-hidden="true" className="text-xs">
        <Skeleton.Text>Add Discord tokens to start scraping members</Skeleton.Text>
      </small>
    </div>
  );
}

// One saved scrape row (.sc-scrape-item): guild, member count line and date, then Download and Delete.
export const SCRAPE_LIST = "flex flex-col gap-1.5";
export const SCRAPE_ROW =
  "flex items-center justify-between gap-3 rounded-btn border border-border bg-[rgba(20,11,38,0.4)] px-3 py-2.5 " +
  "transition-colors duration-150 hover:border-[rgba(139,92,246,0.35)]";
export const SCRAPE_TEXT = "flex min-w-0 flex-col gap-0.5";
export const SCRAPE_GUILD = "flex min-w-0 items-center gap-1.5 font-mono text-[13px] font-semibold text-text";
export const SCRAPE_META = "text-xs text-muted";
export const SCRAPE_DATE = "text-[11px] text-muted opacity-70";
export const SCRAPE_ACTIONS = "flex shrink-0 items-center gap-1.5";
export const DOWNLOAD_LINK = cx(
  "inline-flex min-h-7 items-center justify-center rounded-btn border border-border px-2.5 py-1.5",
  "bg-linear-to-b from-primary to-primary-700 text-white transition duration-200 hover:-translate-y-0.5",
);

/** The saved scrapes list while it loads, the same way. */
export function ScrapesPlaceholder({ rows = readLastCount(SCRAPES_COUNT_KEY, 0, MAX_ROWS) }: { rows?: number }) {
  if (rows > 0)
    return (
      <div role="status" aria-label="Loading saved scrapes" aria-busy="true" className={SCRAPE_LIST}>
        {Array.from({ length: rows }, (_, i) => (
          <div key={i} aria-hidden="true" data-skeleton-row="" className={SCRAPE_ROW}>
            <div className={SCRAPE_TEXT}>
              <span className={SCRAPE_GUILD}>
                <UsersIcon className="shrink-0 text-accent" />
                <Skeleton.Text className="truncate" width={NAME_WIDTHS[i % NAME_WIDTHS.length]!} />
              </span>
              <Skeleton.Text className={SCRAPE_META} width={190} />
              <Skeleton.Text className={SCRAPE_DATE} width={110} />
            </div>
            <div className={SCRAPE_ACTIONS}>
              <span className={DOWNLOAD_LINK}>
                <DownloadIcon />
              </span>
              <button type="button" tabIndex={-1} className={DELETE_BUTTON}>
                <TrashIcon />
              </button>
            </div>
          </div>
        ))}
      </div>
    );
  return (
    <div role="status" aria-label="Loading saved scrapes" aria-busy="true" className="px-4 py-6 text-center">
      <p aria-hidden="true" className="m-0 mb-1 text-sm font-semibold">
        <Skeleton.Text>No saved scrapes yet</Skeleton.Text>
      </p>
      <small aria-hidden="true" className="text-xs">
        <Skeleton.Text>Completed scrapes are automatically saved here</Skeleton.Text>
      </small>
    </div>
  );
}

// The old .btn had no gap between the icon and the label.
export const OLD_BTN = "has-[svg]:gap-0!";

export interface IncludeOpts {
  username: boolean;
  avatar: boolean;
  roles: boolean;
  bio: boolean;
}

export const DEFAULT_OPTS: IncludeOpts = { username: true, avatar: true, roles: false, bio: false };

/** The Include fields checkboxes, shared by the Configuration card and the skeleton. */
export const INCLUDE_OPTS: Array<{ key: keyof IncludeOpts; id: string; label: string }> = [
  { key: "username", id: "sc-opt-username", label: "Username" },
  { key: "avatar", id: "sc-opt-avatar", label: "Avatar URL" },
  { key: "roles", id: "sc-opt-roles", label: "Roles" },
  { key: "bio", id: "sc-opt-bio", label: "Bio" },
];

const COUNT_BOX = "rounded-xl border px-2.5 py-[3px] text-[11px] font-semibold whitespace-nowrap";

/** The token count badge; a bar the badge's size until the token list answers. */
export function TokenCount({ text, className }: { text: string | null; className?: string }) {
  return text === null ? (
    <Skeleton.Box id="sc-token-count" className={COUNT_BOX}>
      0 tokens
    </Skeleton.Box>
  ) : (
    <span id="sc-token-count" className={cx(COUNT_BOX, "border-[rgba(139,92,246,0.25)] bg-[rgba(139,92,246,0.12)] text-primary", className)}>
      {text}
    </span>
  );
}

/** The Proxies card's expand button look. */
export const PROXY_TOGGLE = cx(
  "flex size-8 shrink-0 cursor-pointer items-center justify-center gap-1.5 rounded-btn border border-border text-sm leading-[14px] font-semibold",
  "bg-[rgba(255,255,255,0.04)] text-accent outline-none transition duration-200 focus-visible:ring-2 focus-visible:ring-accent/60",
  "hover:-translate-y-0.5 hover:border-accent/30 hover:bg-white/8",
);

/** The Proxies card's note pointing at the Configuration page. */
export function ProxyNote() {
  return (
    <p className="m-0 text-xs leading-normal text-muted">
      The scraper uses the shared proxy list from the{" "}
      <a href="/" className="text-accent underline">
        Configuration
      </a>{" "}
      page. Add or manage proxies in the <strong>Proxy</strong> section.
    </p>
  );
}

export const AUTOCLEAR_KEY = "scraper_log_autoclear";

/** The log card's Auto-clear choice, kept in the browser; on unless turned off. */
export function readAutoclear(): boolean {
  try {
    return localStorage.getItem(AUTOCLEAR_KEY) !== "false";
  } catch {
    return true;
  }
}

/** The log card's hidden checkbox and the box drawn after it. */
export const AUTOCLEAR_INPUT = "peer pointer-events-none absolute my-[3px] mr-[3px] ml-1 size-0 opacity-0";
export const LOG_BODY =
  "m-0 mt-3 h-[420px] overflow-y-auto overscroll-contain whitespace-pre-wrap rounded-b-2xl bg-[#0e0720] px-4 py-3 font-mono text-xs leading-normal text-[#e6e1ff]";

const noop = () => undefined;

export function ScraperSkeleton() {
  return (
    <div id="scraper-root" className="min-w-fit" role="status" aria-busy="true" aria-label="Loading page">
      <h1 className="sr-only">Scraper</h1>
      <div className="mt-7 mb-1 flex flex-wrap items-start justify-between gap-4">
        <ScraperTitle />
        <PillPending />
      </div>

      <div inert className="grid grid-cols-[1fr] items-start gap-[18px] min-[961px]:grid-cols-[1fr_1fr]">
        <Card aria-label="Tokens" className={cx(SC_CARD, "min-w-0")}>
          <div className={TITLEBAR}>
            <h3 className={TITLE}>Tokens</h3>
            <TokenCount text={null} />
          </div>
          <div className="mt-3.5 mb-2 rounded-control border border-border bg-[rgba(20,11,38,0.6)] p-3.5">
            <div className="flex items-center gap-2 max-[600px]:flex-col">
              <Input type="text" aria-label="Discord token" placeholder="Paste Discord token…" className="min-w-0 flex-[2]" />
              <Input aria-label="Token label" placeholder="Label (optional)" className="min-w-0 flex-1" />
              <Button type="button" variant="primary" size="sm">
                Add
              </Button>
            </div>
          </div>
          <div className="mt-3 max-h-[340px] overflow-y-auto">
            <TokenListPlaceholder />
          </div>
        </Card>

        <Card aria-label="Configuration" className={cx(SC_CARD, "min-w-0")}>
          <div className={TITLEBAR}>
            <h3 className={TITLE}>Configuration</h3>
          </div>
          <div className="mt-3.5 flex flex-col gap-4">
            <div className="flex flex-col gap-1.5">
              <span className={SC_LABEL}>Server (Guild) ID</span>
              <Input placeholder="e.g. 123456789012345678" autoComplete="off" inputMode="numeric" />
            </div>
            <fieldset className="m-0 rounded-control border border-border bg-[rgba(20,11,38,0.35)] px-4 py-3.5">
              <legend className={cx(SC_LABEL, "px-1.5")}>Include fields</legend>
              <div className="mt-2 grid grid-cols-2 gap-2.5">
                {INCLUDE_OPTS.map((o) => (
                  <label key={o.key} className={SC_CHECK}>
                    <input type="checkbox" className={CHECK_BOX} defaultChecked={DEFAULT_OPTS[o.key]} />
                    <span>{o.label}</span>
                  </label>
                ))}
              </div>
              <p className="m-0 mt-2.5 text-xs text-muted opacity-70">
                Enabling <b>Bio</b> requires extra API calls and will be slower.
              </p>
            </fieldset>
            <div className="flex items-center gap-2.5 pt-1">
              <Button variant="ghost">Validate</Button>
              <div className="ml-auto flex gap-2.5">
                <Button variant="outline" title="Add to queue" className={OLD_BTN}>
                  <QueueIcon />
                  Add to Queue
                </Button>
                <Button variant="primary" className={OLD_BTN}>
                  <PlayIcon />
                  Start Scrape
                </Button>
              </div>
            </div>
          </div>
        </Card>
      </div>

      <Card inert aria-label="Proxies" className={SC_CARD}>
        <div className="mb-2 flex items-center justify-between gap-2.5">
          <h3 className={TITLE}>Proxies</h3>
          <button type="button" aria-expanded={false} aria-label="Expand proxies" className={PROXY_TOGGLE}>
            <span aria-hidden="true">▾</span>
          </button>
        </div>
        <ProxyNote />
      </Card>

      <Card inert aria-label="Scraper Logs" className={cx(SC_CARD, "sc-log-card")}>
        <div className="mb-1.5 flex flex-nowrap items-center justify-between gap-2.5">
          <h3 className={TITLE}>Scraper Logs</h3>
          <div className="ml-auto flex min-w-0 shrink flex-nowrap items-center gap-2">
            <label className="flex cursor-pointer items-center gap-[5px] text-[11px] whitespace-nowrap text-muted select-none">
              <input type="checkbox" defaultChecked={readAutoclear()} onChange={noop} className={AUTOCLEAR_INPUT} />
              <span aria-hidden="true" className={CHECK_SPAN} />
              <span>Auto-clear</span>
            </label>
            <Input type="text" aria-label="Search logs" placeholder="Search logs…" className="block min-w-[60px] shrink" />
            <Button size="sm" variant="ghost" className="shrink-0">
              Clear
            </Button>
          </div>
        </div>
        <pre className={LOG_BODY} />
      </Card>

      <Card inert aria-label="Past Scrapes" className={SC_CARD}>
        <div className={TITLEBAR}>
          <h3 className={TITLE}>Past Scrapes</h3>
          <Button size="sm" variant="ghost" title="Refresh" aria-label="Refresh">
            <RefreshIcon />
          </Button>
        </div>
        <div className="mt-3">
          <ScrapesPlaceholder />
        </div>
      </Card>
    </div>
  );
}
