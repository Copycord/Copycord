// The Forwarding page while its chunk downloads: the same card, title and rule grid as the page, with the
// rule tiles it shows while the list loads. Kept out of Forwarding.tsx so the app shell can show it
// without pulling the page chunk in; RuleCard takes its classes from here, so a loading tile and a rule
// tile have the same boxes.

import type { ButtonHTMLAttributes } from "react";
import { Button, Card, Skeleton, cx } from "../../components/ui";
import { readLastCount } from "../../components/ui/lastCount";

/** The rule grid, shared with the page so both lay the tiles out the same way. */
export const FWD_LIST =
  "fwd-list grid grid-cols-[1fr] items-start gap-3.5 min-[701px]:grid-cols-[repeat(2,minmax(260px,1fr))] " +
  "min-[1101px]:grid-cols-[repeat(3,minmax(260px,1fr))] min-[1501px]:grid-cols-[repeat(4,minmax(260px,1fr))]";

/** The toolbar over the rules (search, Total Forwarded, New Forward) and its search box, shared with the page. */
export const FWD_TOOLBAR = "col-span-full mb-2 flex items-start justify-between gap-3";
export const FWD_SEARCH = cx(
  "w-[min(420px,100%)] min-w-[220px] rounded-input border border-log-input-edge bg-log-input-bg px-2.5 py-1.5",
  "text-[13px] text-text-soft caret-log-input-focus outline-none transition-colors",
  "placeholder:text-text-soft/55 placeholder:opacity-75 focus:border-log-input-focus",
);

/** The "no rules" block, shared with the page. */
export const FWD_EMPTY = "flex flex-col items-center justify-center gap-2 px-6 py-10 text-center";

// One rule tile (.fwd-card): the title, provider badge and status pill, then the footer buttons and count.
export const RULE_CARD = cx(
  "w-full self-start rounded-[14px] border border-border bg-[linear-gradient(180deg,var(--color-secondary-top),var(--color-secondary-bottom))]",
  "px-[18px] pt-4 pb-3.5 [box-shadow:0_2px_8px_var(--color-shadow-20)] transition duration-200 max-[480px]:p-3.5",
);
export const RULE_HEADER = "flex items-start justify-between gap-3 max-[640px]:flex-col max-[640px]:gap-2.5";
export const RULE_TITLE_BOX = "flex min-h-7 min-w-0 flex-1 items-center";
export const RULE_TITLE = "m-0 line-clamp-1 text-[15px] leading-[1.3] font-semibold text-text";
export const RULE_BADGES = "flex shrink-0 items-center gap-2 max-[640px]:w-full max-[640px]:justify-start";
export const PROVIDER_BADGE =
  "inline-flex items-center gap-[5px] rounded-[999px] border px-2.5 py-[3px] text-[11px] font-medium tracking-[0.03em] uppercase";
export const STATUS_PILL = "inline-flex items-center rounded-[999px] border px-2 py-0.5 text-[11px] font-medium tracking-[0.02em] uppercase";
export const PROVIDER_ICON = "block size-3.5 shrink-0";
export const RULE_FOOTER = "mt-3 flex items-center gap-2 pt-3 max-[640px]:flex-wrap max-[480px]:flex-col";
export const RULE_STAT = "ml-auto flex items-center gap-2 text-xs leading-[1.2] opacity-60";

// The old .fwd-card-footer .btn: a 32px ghost button with a 12px label; it grows to fill the
// wrapped row under 640px and takes the full width under 480px.
const FOOTER_BUTTON =
  "inline-flex min-h-8 cursor-pointer items-center justify-center rounded-control border border-border bg-wash-4 " +
  "px-3 py-1.5 text-xs font-semibold leading-none select-none outline-none transition duration-200 " +
  "focus-visible:ring-2 focus-visible:ring-accent/60 disabled:cursor-not-allowed disabled:opacity-55 " +
  "max-[640px]:flex-auto max-[480px]:w-full";

export function FooterButton({
  danger = false,
  className,
  type = "button",
  ...rest
}: ButtonHTMLAttributes<HTMLButtonElement> & { danger?: boolean }) {
  return (
    <button
      type={type}
      className={cx(
        FOOTER_BUTTON,
        danger
          ? "text-accent enabled:hover:border-danger enabled:hover:bg-line-1 enabled:hover:text-danger"
          : "text-accent enabled:hover:-translate-y-0.5 enabled:hover:border-accent/30 enabled:hover:bg-line-1 enabled:hover:shadow-soft",
        className,
      )}
      {...rest}
    />
  );
}

// The rule grid draws as many loading tiles as it had last time; one on a first visit, the common case.
export const RULES_COUNT_KEY = "forwarding.rules";
const MAX_TILES = 12;
const TITLE_WIDTHS = [150, 118, 176, 132];

/** A rule tile while the list loads: RuleCard's boxes, the buttons as chrome, bars on the rule's values. */
export function RuleTileSkeleton({ index = 0 }: { index?: number }) {
  return (
    <article aria-hidden="true" data-testid="fwd-skeleton" data-skeleton-row="" className={RULE_CARD}>
      <header className={RULE_HEADER}>
        <div className={RULE_TITLE_BOX}>
          <h3 className={RULE_TITLE}>
            <Skeleton.Text width={TITLE_WIDTHS[index % TITLE_WIDTHS.length]!} />
          </h3>
        </div>
        <div className={RULE_BADGES}>
          <Skeleton.Box className={PROVIDER_BADGE}>
            <span className={PROVIDER_ICON} />
            <span>Discord</span>
          </Skeleton.Box>
          <Skeleton.Box className={STATUS_PILL}>Enabled</Skeleton.Box>
        </div>
      </header>
      <footer className={RULE_FOOTER}>
        <FooterButton tabIndex={-1}>Edit</FooterButton>
        <FooterButton tabIndex={-1}>
          <Skeleton.Text width={40} />
        </FooterButton>
        <FooterButton tabIndex={-1}>Duplicate</FooterButton>
        <FooterButton danger tabIndex={-1}>
          Delete
        </FooterButton>
        <div className={RULE_STAT}>
          <span>Forwarded:</span>
          <Skeleton.Text width={14} />
        </div>
      </footer>
    </article>
  );
}

/** The rule list while it loads, as many tiles as last time: the toolbar as inert chrome with a bar on the total, then the tiles. */
export function RuleSkeletons({ rows = readLastCount(RULES_COUNT_KEY, 1, MAX_TILES) }: { rows?: number }) {
  if (rows === 0)
    return (
      <div aria-hidden="true" className={cx(FWD_EMPTY, "col-span-full")}>
        <Skeleton.Block width={48} height={48} />
        <Skeleton.Text className="text-base font-semibold" width={190} />
        <Skeleton.Text className="text-sm" width={330} />
        <Button variant="primary" className="mt-2" tabIndex={-1}>
          + Create your first rule
        </Button>
      </div>
    );
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
            <Skeleton.Text className="align-middle">0000</Skeleton.Text>
          </div>
        </div>
        <div className="flex items-center">
          <Button variant="primary" className="fwd-add-btn">
            New Forward
          </Button>
        </div>
      </div>
      {Array.from({ length: rows }, (_, i) => (
        <RuleTileSkeleton key={i} index={i} />
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
            <div inert className="mt-1.5">
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
