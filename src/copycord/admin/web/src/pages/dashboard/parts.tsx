// Pieces the old dashboard cards share, with the old main.css values: the
// ghost button, the collapsible card summary (title, docs link, round
// chevron), the label.toggle switch and the small settings row controls.

import type { ReactNode } from "react";
import { cx } from "../../components/ui";

/** The old .btn.btn-ghost: 42px tall, rgba fills so the values read as the old ones. */
export const GHOST =
  "min-h-[42px] cursor-pointer items-center justify-center rounded-control border border-border bg-[rgba(255,255,255,0.04)] " +
  "px-4 py-2.5 text-center text-sm leading-none font-semibold text-accent no-underline outline-none select-none " +
  "transition duration-200 focus-visible:border-accent " +
  "enabled:hover:-translate-y-0.5 enabled:hover:border-[rgba(167,139,250,0.3)] enabled:hover:bg-[rgba(255,255,255,0.08)] " +
  "enabled:hover:[box-shadow:0_4px_12px_rgba(0,0,0,0.2)] enabled:active:translate-y-0 enabled:active:scale-[0.97] " +
  "disabled:cursor-not-allowed";
/** The old .btn[disabled]; left off where a hidden button must keep opacity 1 like the old one. */
export const GHOST_DISABLED = "disabled:opacity-60";

/** details.card.srv-proxy-card; the summary chevron turns with group-open. */
export const CARD = "group mt-[18px] rounded-card border border-border bg-(image:--card-gradient) p-[18px] text-text";

/** .srv-proxy-summary */
export const SUMMARY =
  "group/sum flex cursor-pointer list-none items-center gap-2.5 p-0 [&::-webkit-details-marker]:hidden";

/** .srv-proxy-summary h3 */
export const SUMMARY_TITLE = "m-0 cursor-text text-[15px] font-bold select-text";

/** .proxy-settings-label and its sublabel */
export const SETTING_LABEL = "flex min-w-0 flex-col gap-px";
export const SETTING_SUB = "text-[11px] text-muted opacity-70";

/** .gc-setting-row / .proxy-settings-field */
export const SETTING_ROW =
  "flex items-center justify-between gap-4 rounded-lg border border-border bg-[rgba(255,255,255,0.02)] px-2.5 py-2 " +
  "transition-colors duration-100 hover:bg-[rgba(255,255,255,0.04)]";

/** .ps-input, the small centered number box */
export const PS_INPUT =
  "h-8 w-16 rounded-lg border border-border bg-[#140b26] px-2 text-center text-[13px] text-text outline-none " +
  "transition-colors [appearance:textfield] focus:border-accent disabled:opacity-50 " +
  "[&::-webkit-inner-spin-button]:appearance-none [&::-webkit-outer-spin-button]:appearance-none";

/** .proxy-settings-unit */
export const PS_UNIT = "text-[11px] text-muted";

const BOOK =
  "M21 5c-1.11-.35-2.33-.5-3.5-.5-1.95 0-4.05.4-5.5 1.5-1.45-1.1-3.55-1.5-5.5-1.5S2.45 4.9 1 6v14.65c0 .25.25.5.5.5.1 0 .15-.05.25-.05C3.1 20.45 5.05 20 6.5 20c1.95 0 4.05.4 5.5 1.5 1.35-.85 3.8-1.5 5.5-1.5 1.65 0 3.35.3 4.75 1.05.1.05.15.05.25.05.25 0 .5-.25.5-.5V6c-.6-.45-1.25-.75-2-1zm0 13.5c-1.1-.35-2.3-.5-3.5-.5-1.7 0-4.15.65-5.5 1.5V8c1.35-.85 3.8-1.5 5.5-1.5 1.2 0 2.4.15 3.5.5v11.5z";

/** .card-doc-link: the faded book icon next to a card title. */
export function DocLink({ href, label = "View docs" }: { href: string; label?: string }) {
  return (
    <a
      href={href}
      target="_blank"
      rel="noopener noreferrer"
      title="View docs"
      aria-label={label}
      onClick={(e) => e.stopPropagation()}
      className="card-doc-link inline-flex items-center justify-center text-muted no-underline opacity-50 transition hover:text-accent hover:opacity-100"
    >
      <svg viewBox="0 0 24 24" width="14" height="14" fill="currentColor" aria-hidden="true">
        <path d={BOOK} />
      </svg>
    </a>
  );
}

/** .card-chev: the round 28px chevron at the right of a summary, pointing right while closed. Keep it last. */
export function Chevron() {
  return (
    <span
      aria-hidden="true"
      className={cx(
        "card-chev order-99 ml-auto inline-flex min-h-7 w-7 shrink-0 -rotate-90 items-center justify-center rounded-[10px] border border-border bg-[rgba(255,255,255,0.04)] p-0",
        "text-[13px] text-accent transition duration-200 group-open:rotate-0",
        "group-hover/sum:border-[rgba(167,139,250,0.3)] group-hover/sum:bg-[rgba(255,255,255,0.08)]",
      )}
    >
      &#9662;
    </span>
  );
}

export interface SwitchProps {
  id?: string;
  label: string;
  checked: boolean;
  onChange: (checked: boolean) => void;
  disabled?: boolean;
  className?: string;
  /** Extra attributes for the switch, e.g. data-event. */
  data?: Record<`data-${string}`, string>;
}

/** label.toggle with its 38x22 track, as a role=switch button. */
export function Switch({ id, label, checked, onChange, disabled, className, data }: SwitchProps) {
  return (
    <label className={cx("toggle relative inline-flex cursor-pointer items-center gap-2.5 select-none", className)}>
      <button
        id={id}
        type="button"
        role="switch"
        aria-checked={checked}
        aria-label={label}
        disabled={disabled}
        onClick={() => onChange(!checked)}
        {...data}
        className={cx(
          "relative block h-[22px] w-[38px] shrink-0 cursor-pointer rounded-[999px] border p-0 outline-none transition",
          "focus-visible:ring-2 focus-visible:ring-accent/60 disabled:cursor-not-allowed disabled:opacity-50",
          checked
            ? "border-transparent bg-[linear-gradient(135deg,var(--color-primary)_0%,var(--color-primary-700)_100%)]"
            : "border-[rgba(255,255,255,0.12)] bg-[rgba(255,255,255,0.08)] hover:border-[rgba(255,255,255,0.2)]",
        )}
      >
        <span
          aria-hidden="true"
          className={cx(
            "absolute top-0.5 left-0.5 size-4 rounded-[50%] transition-transform duration-200",
            checked ? "translate-x-4 bg-white" : "bg-muted",
          )}
        />
      </button>
    </label>
  );
}

/** One .gc-setting-row: label and sublabel on the left, the control on the right. */
export function SettingRow({ label, hint, children, className }: { label: string; hint: ReactNode; children: ReactNode; className?: string }) {
  return (
    <div className={cx(SETTING_ROW, className)}>
      <div className={cx(SETTING_LABEL, "mr-auto flex-1")}>
        <span className="text-[13px] font-medium">{label}</span>
        <span className={SETTING_SUB}>{hint}</span>
      </div>
      {children}
    </div>
  );
}
