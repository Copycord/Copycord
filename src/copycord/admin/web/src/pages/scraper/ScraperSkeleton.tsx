// The Scraper page while its chunk downloads: the same header, card grid and cards as the page, with
// the token and saved scrape placeholders the cards show while their lists load. Kept out of
// Scraper.tsx so the app shell can show it without pulling the page chunk in; the cards take their
// shared classes and placeholders from here.

import { Button, Card, Input, Skeleton, cx } from "../../components/ui";

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

// .scraper-status-pill at idle, the state the page opens in.
export const PILL_BOX =
  "mt-1 inline-flex shrink-0 items-center gap-[7px] rounded-[20px] border px-3.5 py-1.5 text-xs font-semibold " +
  "tracking-[0.03em] whitespace-nowrap transition-all duration-[250ms]";
export const IDLE_PILL = { box: "border-border bg-[rgba(255,255,255,0.04)] text-muted", dot: "bg-muted" };

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

// Text kept in place but drawn as a bar, so the placeholder takes the empty state's exact line boxes.
const TEXT_BAR = "cc-skeleton rounded-md text-transparent motion-reduce:animate-none";

/** The token list while it loads: a bar for each line of the empty state, in its place. */
export function TokenListPlaceholder() {
  return (
    <div role="status" aria-label="Loading tokens" aria-busy="true" className="flex flex-col items-center gap-1 px-4 py-8 text-center">
      <Skeleton.Block width={32} height={32} className="mb-1.5" />
      <p aria-hidden="true" className="m-0 text-sm">
        <span data-skeleton="" className={TEXT_BAR}>No tokens configured</span>
      </p>
      <small aria-hidden="true" className="text-xs">
        <span data-skeleton="" className={TEXT_BAR}>Add Discord tokens to start scraping members</span>
      </small>
    </div>
  );
}

/** The saved scrapes list while it loads, the same way. */
export function ScrapesPlaceholder() {
  return (
    <div role="status" aria-label="Loading saved scrapes" aria-busy="true" className="px-4 py-6 text-center">
      <p aria-hidden="true" className="m-0 mb-1 text-sm font-semibold">
        <span data-skeleton="" className={TEXT_BAR}>No saved scrapes yet</span>
      </p>
      <small aria-hidden="true" className="text-xs">
        <span data-skeleton="" className={TEXT_BAR}>Completed scrapes are automatically saved here</span>
      </small>
    </div>
  );
}

const OPTS = ["Username", "Avatar URL", "Roles", "Bio"];

export function ScraperSkeleton() {
  return (
    <div id="scraper-root" className="min-w-fit" role="status" aria-busy="true" aria-label="Loading page">
      <h1 className="sr-only">Scraper</h1>
      <div className="mt-7 mb-1 flex flex-wrap items-start justify-between gap-4">
        <ScraperTitle />
        <span className={cx(PILL_BOX, IDLE_PILL.box)}>
          <span aria-hidden="true" className={cx("size-2 rounded-full transition-all duration-[250ms]", IDLE_PILL.dot)} />
          <span>Idle</span>
        </span>
      </div>

      <div inert className="grid grid-cols-[1fr] items-start gap-[18px] min-[961px]:grid-cols-[1fr_1fr]">
        <Card aria-label="Tokens" className={cx(SC_CARD, "min-w-0")}>
          <div className={TITLEBAR}>
            <h3 className={TITLE}>Tokens</h3>
            <span className="rounded-xl border border-[rgba(139,92,246,0.25)] bg-[rgba(139,92,246,0.12)] px-2.5 py-[3px] text-[11px] font-semibold whitespace-nowrap text-primary">
              0 tokens
            </span>
          </div>
          <div className="mt-3.5 mb-2 rounded-control border border-border bg-[rgba(20,11,38,0.6)] p-3.5">
            <div className="flex items-center gap-2 max-[600px]:flex-col">
              <Input type="text" disabled aria-label="Discord token" placeholder="Paste Discord token…" className="min-w-0 flex-[2]" />
              <Input disabled aria-label="Token label" placeholder="Label (optional)" className="min-w-0 flex-1" />
              <Button type="button" variant="primary" size="sm" disabled>
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
              <Input disabled aria-label="Server (Guild) ID" placeholder="e.g. 123456789012345678" />
            </div>
            <fieldset disabled className="m-0 rounded-control border border-border bg-[rgba(20,11,38,0.35)] px-4 py-3.5">
              <legend className={cx(SC_LABEL, "px-1.5")}>Include fields</legend>
              <div className="mt-2 grid grid-cols-2 gap-2.5">
                {OPTS.map((label, i) => (
                  <label key={label} className={SC_CHECK}>
                    <input type="checkbox" className={CHECK_BOX} defaultChecked={i < 2} />
                    <span>{label}</span>
                  </label>
                ))}
              </div>
              <p className="m-0 mt-2.5 text-xs text-muted opacity-70">
                Enabling <b>Bio</b> requires extra API calls and will be slower.
              </p>
            </fieldset>
            <div className="flex items-center gap-2.5 pt-1">
              <Button variant="ghost" disabled>
                Validate
              </Button>
              <div className="ml-auto flex gap-2.5">
                <Button variant="outline" className="has-[svg]:gap-0!" disabled>
                  <svg width={14} height={14} aria-hidden="true" />
                  Add to Queue
                </Button>
                <Button variant="primary" className="has-[svg]:gap-0!" disabled>
                  <svg width={14} height={14} aria-hidden="true" />
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
          <span className="flex size-8 shrink-0 items-center justify-center rounded-btn border border-border bg-[rgba(255,255,255,0.04)] text-sm leading-[14px] font-semibold text-accent">
            <span aria-hidden="true">▾</span>
          </span>
        </div>
        <p className="m-0 text-xs leading-normal text-muted">
          The scraper uses the shared proxy list from the <span className="text-accent underline">Configuration</span> page. Add
          or manage proxies in the <strong>Proxy</strong> section.
        </p>
      </Card>

      <Card inert aria-label="Scraper Logs" className={cx(SC_CARD, "sc-log-card")}>
        <div className="mb-1.5 flex flex-nowrap items-center justify-between gap-2.5">
          <h3 className={TITLE}>Scraper Logs</h3>
          <div className="ml-auto flex min-w-0 shrink flex-nowrap items-center gap-2">
            <label className="flex items-center gap-[5px] text-[11px] whitespace-nowrap text-muted select-none">
              <input
                type="checkbox"
                disabled
                defaultChecked
                className="peer pointer-events-none absolute my-[3px] mr-[3px] ml-1 size-0 opacity-0"
              />
              <span aria-hidden="true" className={CHECK_SPAN} />
              <span>Auto-clear</span>
            </label>
            <Input type="text" disabled aria-label="Search logs" placeholder="Search logs…" className="block min-w-[60px] shrink" />
            <Button size="sm" variant="ghost" className="shrink-0" disabled>
              Clear
            </Button>
          </div>
        </div>
        <pre className="m-0 mt-3 h-[420px] overflow-y-auto overscroll-contain whitespace-pre-wrap rounded-b-2xl bg-[#0e0720] px-4 py-3 font-mono text-xs leading-normal text-[#e6e1ff]" />
      </Card>

      <Card inert aria-label="Past Scrapes" className={SC_CARD}>
        <div className={TITLEBAR}>
          <h3 className={TITLE}>Past Scrapes</h3>
          <Button size="sm" variant="ghost" aria-label="Refresh" disabled>
            <svg width={14} height={14} aria-hidden="true" />
          </Button>
        </div>
        <div className="mt-3">
          <ScrapesPlaceholder />
        </div>
      </Card>
    </div>
  );
}
