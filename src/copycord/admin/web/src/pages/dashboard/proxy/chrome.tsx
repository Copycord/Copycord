// The Proxy card's static pieces, shared by the card and the dashboard skeleton: the summary tools
// (status chip, Settings, favorites help), the rotation and favorite re-check rows and their hints.

import type { ReactNode } from "react";
import { cx, Skeleton } from "../../../components/ui";
import type { IntervalUnit } from "../../../types/dashboard-proxy";
import { ToolbarSelect } from "../../channels/ToolbarSelect";
import { CHECK, CHECK_LABEL, PROXY_HINT } from "../parts";

/** .srv-proxy-hint.srv-proxy-rotation-hint */
export const ROTATION_HINT = "m-0 mt-1.5 text-xs leading-normal text-muted";
/** .input.srv-proxy-interval-input: the full width .input look at 34px, so it shrinks on phones. */
export const NUM =
  "h-[34px] w-full rounded-control border border-border-input bg-(image:--control-gradient) px-3.5 py-3 text-center text-sm text-text outline-0 transition-[border-color] [appearance:textfield] " +
  "hover:border-accent/30 hover:bg-[linear-gradient(180deg,var(--color-secondary-top)_0%,var(--color-secondary-bottom)_100%)] focus:border-accent focus:bg-[linear-gradient(180deg,var(--color-secondary-top)_0%,var(--color-secondary-bottom)_100%)] disabled:opacity-50 " +
  "[&::-webkit-inner-spin-button]:appearance-none [&::-webkit-outer-spin-button]:appearance-none";
/** .srv-proxy-rotation-section */
export const ROTATION_SECTION = "mt-4 border-t border-t-border pt-3.5";
/** The rotation and re-check sections while proxies are off. */
export const LOCKED = "is-locked opacity-40";

export const ROTATION_TEXT =
  "Automatically switches to the next proxy after the set duration. When off, one proxy is used until it fails.";
export const RECHECK_TEXT =
  "While the client is on a proxy you have not starred, re-test the favorites on this interval and switch back as soon as one works again.";

const UNITS = [
  { value: "60", label: "minutes" },
  { value: "3600", label: "hours" },
];

/** The interval dropdown; `pending` puts a bar on the label. */
export function UnitSelect({
  id,
  value,
  disabled,
  pending,
  onChange,
}: {
  id: string;
  value: string;
  disabled: boolean;
  pending?: boolean;
  onChange: (v: IntervalUnit) => void;
}) {
  return (
    <ToolbarSelect
      id={id}
      label="Interval unit"
      value={value}
      options={UNITS}
      disabled={disabled}
      pending={pending}
      pendingLabel="minutes"
      onChange={(v) => onChange(v as IntervalUnit)}
    />
  );
}

/** A rotation-style section: the checkbox row with its interval controls, then the hint. */
export function IntervalSection({ id, locked, check, controls, hint }: { id?: string; locked: boolean; check: ReactNode; controls: ReactNode; hint: string }) {
  return (
    <div
      id={id}
      className={cx(ROTATION_SECTION, locked && LOCKED)}
      aria-disabled={locked}
      title={locked ? "Enable proxies for client to use this" : undefined}
    >
      <div className="srv-proxy-rotation-row flex items-center gap-2.5">
        {check}
        {controls}
      </div>
      <p className={ROTATION_HINT}>{hint}</p>
    </div>
  );
}

/** The rotation interval box and unit, shown only while rotation is on. */
export function RotationControls({ on, children }: { on: boolean; children?: ReactNode }) {
  return (
    <div id="srv-proxy-rotation-controls" className={cx("srv-proxy-interval-inline flex items-center gap-1.5", !on && "hidden")}>
      {children}
    </div>
  );
}

/** The favorite re-check interval box and unit, faded while re-check is off. */
export function RecheckControls({ off, children }: { off: boolean; children: ReactNode }) {
  return (
    <div
      id="srv-proxy-fav-recheck-controls"
      className={cx("srv-proxy-interval-inline flex items-center gap-1.5", off && "pointer-events-none opacity-45")}
    >
      {children}
    </div>
  );
}

/** The checkbox label of the enable row and the interval rows. */
export function CheckRow({ id, label, children }: { id?: string; label: string; children: ReactNode }) {
  return (
    <label className={CHECK_LABEL} htmlFor={id}>
      {children}
      <span>{label}</span>
    </label>
  );
}

/** The 18px checkbox box of the skeleton, empty until the setting lands. */
export function CheckPending() {
  return <span className={cx(CHECK, "inline-block")} />;
}

/** .srv-proxy-chip and its .is-on / .is-warn colors. */
const CHIP_TONE = {
  on: "bg-ok-a12 text-ok",
  warn: "bg-warn-a12 text-warn",
  off: "bg-wash-6 text-muted",
} as const;
const CHIP = "srv-proxy-chip rounded-[20px] px-2.5 py-1 text-[11px] leading-none font-semibold empty:hidden";

const SETTINGS_BTN =
  "srv-proxy-settings-btn ml-1.5 inline-flex cursor-pointer items-center gap-[5px] rounded-lg border border-border bg-wash-4 px-2.5 py-1 text-[11px] font-medium text-muted transition-colors duration-150 hover:border-wash-18 hover:bg-line-1 hover:text-text";
const FAV_HELP_BTN =
  "srv-proxy-fav-help-btn inline-flex size-[26px] cursor-pointer items-center justify-center rounded-md border border-wash-10 bg-transparent p-0 text-muted transition-colors duration-100 hover:border-favorite-a40 hover:bg-favorite-a10 hover:text-favorite";

const GEAR =
  "M9.594 3.94c.09-.542.56-.94 1.11-.94h2.593c.55 0 1.02.398 1.11.94l.213 1.281c.063.374.313.686.645.87.074.04.147.083.22.127.325.196.72.257 1.075.124l1.217-.456a1.125 1.125 0 0 1 1.37.49l1.296 2.247a1.125 1.125 0 0 1-.26 1.431l-1.003.827c-.293.241-.438.613-.43.992a7.723 7.723 0 0 1 0 .255c-.008.378.137.75.43.991l1.004.827c.424.35.534.955.26 1.43l-1.298 2.247a1.125 1.125 0 0 1-1.369.491l-1.217-.456c-.355-.133-.75-.072-1.076.124a6.47 6.47 0 0 1-.22.128c-.331.183-.581.495-.644.869l-.213 1.281c-.09.543-.56.94-1.11.94h-2.594c-.55 0-1.019-.398-1.11-.94l-.213-1.281c-.062-.374-.312-.686-.644-.87a6.52 6.52 0 0 1-.22-.127c-.325-.196-.72-.257-1.076-.124l-1.217.456a1.125 1.125 0 0 1-1.369-.49l-1.297-2.247a1.125 1.125 0 0 1 .26-1.431l1.004-.827c.292-.24.437-.613.43-.991a6.932 6.932 0 0 1 0-.255c.007-.38-.138-.751-.43-.992l-1.004-.827a1.125 1.125 0 0 1-.26-1.43l1.297-2.247a1.125 1.125 0 0 1 1.37-.491l1.216.456c.356.133.751.072 1.076-.124.072-.044.146-.086.22-.128.332-.183.582-.495.644-.869l.214-1.28Z";

export interface SummaryToolsProps {
  /** The status chip, or null while the proxy config loads (a bar the chip's size). */
  chip: { text: string; tone: keyof typeof CHIP_TONE } | null;
  onSettings?: () => void;
  onHelp?: () => void;
}

/** The status chip, Settings and the favorites help button in the card summary. */
export function SummaryTools({ chip, onSettings, onHelp }: SummaryToolsProps) {
  return (
    <>
      {chip ? (
        <span id="srv-proxy-status" className={cx(CHIP, CHIP_TONE[chip.tone])}>
          {chip.text}
        </span>
      ) : (
        <Skeleton.Box className={CHIP}>Disabled</Skeleton.Box>
      )}
      <button
        type="button"
        id="srv-proxy-settings-btn"
        title="Proxy Settings"
        className={SETTINGS_BTN}
        onClick={(e) => {
          e.preventDefault();
          e.stopPropagation();
          onSettings?.();
        }}
      >
        <svg xmlns="http://www.w3.org/2000/svg" width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
          <path d={GEAR} />
          <path d="M15 12a3 3 0 1 1-6 0 3 3 0 0 1 6 0Z" />
        </svg>
        Settings
      </button>
      <button
        type="button"
        id="srv-proxy-fav-help-btn"
        title="What do favorites do?"
        aria-label="What do favorites do?"
        className={FAV_HELP_BTN}
        onClick={(e) => {
          // The header is a <summary>; without this the click also toggles the card shut.
          e.preventDefault();
          e.stopPropagation();
          onHelp?.();
        }}
      >
        <svg xmlns="http://www.w3.org/2000/svg" width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
          <polygon points="12 2 15.09 8.26 22 9.27 17 14.14 18.18 21.02 12 17.77 5.82 21.02 7 14.14 2 9.27 8.91 8.26 12 2" />
        </svg>
      </button>
    </>
  );
}

/** The format line above the proxy list, with the Test All and Clear buttons beside it. */
export function HintRow({ children }: { children: ReactNode }) {
  return (
    <div className="srv-proxy-hint-row mb-2.5 flex items-start justify-between gap-3">
      <p className={PROXY_HINT}>
        Paste one or more proxies &mdash; <code>host:port</code> &middot; <code>host:port:user:pass</code>{" "}
        &middot; <code>user:pass@host:port</code>
      </p>
      <div className="srv-proxy-hint-actions flex shrink-0 gap-1.5">{children}</div>
    </div>
  );
}
