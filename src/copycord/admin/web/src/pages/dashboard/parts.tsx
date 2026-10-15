// Pieces the old dashboard cards share, with the old main.css values: the
// ghost button, the collapsible card summary (title, docs link, round
// chevron), the label.toggle switch and the small settings row controls.

import type { ReactNode } from "react";
import { cx, Skeleton } from "../../components/ui";
import {
  GHOST_GREEN_HOVER,
  GHOST_RED_HOVER,
  GHOST_TINT_FILL,
  GHOST_TINT_FOCUS,
} from "../../components/ui/Button";

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

/** The Bots table rows, shared with the dashboard skeleton. */
export const BOT_ROLES: Array<{ role: "server" | "client"; label: string }> = [
  { role: "server", label: "Server" },
  { role: "client", label: "Client" },
];

/** The Bots card and its table, shared with the dashboard skeleton. */
export const BOTS_CARD = "mt-[18px] rounded-card border border-border bg-(image:--card-gradient) p-[18px] text-text";
export const BOTS_TITLE = "my-[0.83em] text-[24px] leading-[normal] font-bold";
export const BOTS_TABLE = "mt-2 w-full table-fixed border-collapse border-[#808080] text-left [&_tr]:border-[#808080]";

// The old .runtime-table: fixed layout, 110px State and 120px Uptime columns,
// muted 0.9rem headers, a rule under every row, centered cells after the first.
export const BOTS_HEAD = "border-b border-b-border px-3 py-2 align-middle text-[0.9rem] font-medium whitespace-nowrap text-muted";
export const BOTS_CELL = "border-b border-b-border px-3 py-2 align-middle text-[0.95rem] whitespace-nowrap";

// The old .runtime-table .badge with .badge-ok / .badge-stop.
export const BOTS_STATE =
  "inline-block rounded-[12px] border border-border px-2 py-0.5 font-[system-ui,sans-serif] text-[12px] leading-[1.8] font-semibold";

/** The Bots state badge until the first status lands: the badge box with a bar over it. */
export function BotStatePending() {
  return <Skeleton.Box className={BOTS_STATE}>stopped</Skeleton.Box>;
}

/** The label.toggle switch until its value lands: a bar the size of the 38x22 track. */
export function SwitchPending({ className }: { className?: string }) {
  return <Skeleton.Box className={cx("block h-[22px] w-[38px] shrink-0 rounded-[999px]", className)} />;
}

/** details.card.srv-proxy-card; the summary chevron turns with group-open. */
export const CARD = "group mt-[18px] rounded-card border border-border bg-(image:--card-gradient) p-[18px] text-text";

/** .srv-proxy-summary */
export const SUMMARY =
  "group/sum flex cursor-pointer list-none items-center gap-2.5 p-0 [&::-webkit-details-marker]:hidden";

/** .srv-proxy-summary h3 */
export const SUMMARY_TITLE = "m-0 cursor-text text-[15px] font-bold select-text";

/** .field.field-token, its label and .gc-field-hint, shared with the Global Configuration loading body. */
export const GC_FIELD =
  "relative block rounded-lg border border-border bg-[rgba(255,255,255,0.02)] px-2.5 py-2 transition-colors duration-100 hover:bg-[rgba(255,255,255,0.04)]";
export const GC_LABEL = "mb-1.5 ml-0.5 block text-xs text-muted";
export const GC_HINT = "mb-1 block text-[11px] leading-[1.4] text-muted opacity-70";

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

/** .input.ps-input: .input padding, gradient and 14px text, .ps-input height and centered value. */
export const PS_FIELD_INPUT =
  "h-8 w-full rounded-control border border-border bg-(image:--control-gradient) px-3.5 py-3 text-center text-sm text-text outline-0 transition-[border-color] [appearance:textfield] hover:border-accent/30 hover:bg-[linear-gradient(180deg,#1f1237_0%,#160b2a_100%)] focus:border-accent focus:bg-[linear-gradient(180deg,#1f1237_0%,#160b2a_100%)] [&::-webkit-inner-spin-button]:appearance-none [&::-webkit-outer-spin-button]:appearance-none";

/** .proxy-settings-field: a borderless 13px row, label on the left and the control on the right. */
export const PS_FIELD =
  "flex items-center justify-between gap-4 rounded-input bg-white/[0.02] px-2.5 py-1.5 text-[13px] text-text transition-colors hover:bg-white/[0.04]";

/** .srv-proxy-hint */
export const SECTION_HINT = "m-0 mb-2.5 text-xs leading-normal text-muted";

/** .notif-webhook-input: 13px monospace on the dark control fill. */
export const WEBHOOK_INPUT =
  "flex-1 rounded-input border border-border bg-control px-3 py-[7px] font-[ui-monospace,SFMono-Regular,Menlo,Consolas,monospace] text-[13px] text-text " +
  "outline-0 transition-colors duration-150 placeholder:text-muted/40 focus:border-accent";

/** .notif-event-row */
export const EVENT_ROW =
  "flex items-center justify-between gap-3.5 rounded-input border border-border bg-[rgba(255,255,255,0.02)] px-2.5 py-2 " +
  "transition-colors duration-100 hover:bg-[rgba(255,255,255,0.04)]";

/** .srv-proxy-check and its 18px box. */
export const CHECK_LABEL = "srv-proxy-check inline-flex cursor-pointer items-center gap-2 text-[13px] font-medium text-text select-none";
export const CHECK =
  "relative m-0 size-[18px] shrink-0 cursor-pointer appearance-none rounded-[5px] border-[1.5px] border-border bg-[rgba(255,255,255,0.04)] " +
  "transition hover:border-accent focus-visible:shadow-[0_0_0_3px_rgba(167,139,250,0.18)] focus-visible:outline-0 checked:border-primary checked:bg-primary " +
  "checked:bg-[url(\"data:image/svg+xml,%3Csvg_xmlns='http://www.w3.org/2000/svg'_viewBox='0_0_24_24'_fill='none'_stroke='white'_stroke-width='3'_stroke-linecap='round'_stroke-linejoin='round'%3E%3Cpath_d='M20_6_9_17l-5-5'/%3E%3C/svg%3E\")] checked:bg-size-[12px] checked:bg-center checked:bg-no-repeat disabled:cursor-not-allowed";

/** .srv-proxy-hint: the format line above the proxy list, with inline code chips. */
export const PROXY_HINT =
  "m-0 text-xs leading-normal text-muted [&_code]:rounded-[4px] [&_code]:bg-white/6 [&_code]:px-[5px] [&_code]:py-px [&_code]:font-[monospace] [&_code]:text-[11px]";

/** .srv-proxy-chips, the bordered proxy list box. */
export const PROXY_CHIPS =
  "srv-proxy-chips flex max-h-[220px] min-h-[44px] flex-col gap-1 overflow-y-auto rounded-btn border border-border bg-control px-2.5 py-2 transition-[border-color,box-shadow] duration-150 focus-within:border-accent focus-within:shadow-[0_0_0_3px_rgba(167,139,250,0.12)]";

/** .btn.btn-xs with .btn-ghost-green or .btn-ghost-red: 28px, 12px accent label that turns green or red on hover. */
const GHOST_XS =
  "inline-flex min-h-7 cursor-pointer items-center justify-center rounded-btn border border-border bg-[rgba(255,255,255,0.04)] px-2.5 py-1.5 " +
  "text-xs leading-none font-semibold text-accent outline-none transition-colors duration-150 " +
  `${GHOST_TINT_FILL} ${GHOST_TINT_FOCUS} disabled:cursor-not-allowed`;
export const GHOST_XS_GREEN = `${GHOST_XS} ${GHOST_GREEN_HOVER}`;
export const GHOST_XS_RED = `${GHOST_XS} ${GHOST_RED_HOVER}`;

const BOOK =
  "M21 5c-1.11-.35-2.33-.5-3.5-.5-1.95 0-4.05.4-5.5 1.5-1.45-1.1-3.55-1.5-5.5-1.5S2.45 4.9 1 6v14.65c0 .25.25.5.5.5.1 0 .15-.05.25-.05C3.1 20.45 5.05 20 6.5 20c1.95 0 4.05.4 5.5 1.5 1.35-.85 3.8-1.5 5.5-1.5 1.65 0 3.35.3 4.75 1.05.1.05.15.05.25.05.25 0 .5-.25.5-.5V6c-.6-.45-1.25-.75-2-1zm0 13.5c-1.1-.35-2.3-.5-3.5-.5-1.7 0-4.15.65-5.5 1.5V8c1.35-.85 3.8-1.5 5.5-1.5 1.2 0 2.4.15 3.5.5v11.5z";

/** Where each card's docs link points, shared with the dashboard skeleton. */
export const CARD_DOCS = {
  global: "https://copycord.github.io/Copycord/docs/getting-started/first-run",
  mappings: "https://copycord.github.io/Copycord/docs/configuration/cloning-options",
  notifications: "https://copycord.github.io/Copycord/docs/configuration/advanced#notifications",
  sync: "https://copycord.github.io/Copycord/docs/configuration/advanced#sync-timing",
  proxy: "https://copycord.github.io/Copycord/docs/configuration/advanced#proxy-support",
} as const;

/** The localStorage keys the two CollapseCards keep their open state under, the old ones (Proxy has PROXY_CARD_OPEN_KEY). */
export const CARD_OPEN_KEYS = {
  notifications: "cpc.notifications-card-open",
  sync: "cpc.sync-card-open",
} as const;

/** Whether a collapsible card was left open; the skeleton reads it too so the card does not jump. */
export function readCardOpen(key: string): boolean {
  try {
    return localStorage.getItem(key) === "1";
  } catch {
    return false;
  }
}

/** .card-doc-link: the faded book icon next to a card title. `inert` for the skeleton copy. */
export function DocLink({ href, label = "View docs", inert }: { href: string; label?: string; inert?: boolean }) {
  return (
    <a
      href={href}
      target="_blank"
      rel="noopener noreferrer"
      title="View docs"
      aria-label={label}
      inert={inert}
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
