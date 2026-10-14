// Same look as the old .btn, .btn-primary, .btn-danger, .btn-outline,
// .btn-ghost, .btn-ghost-red, .btn-ghost-purple and .btn-ghost-green classes
// in main.css.

import type { ButtonHTMLAttributes, Ref } from "react";
import { cx } from "./cx";
import { Spinner } from "./Spinner";

export type ButtonVariant =
  | "primary"
  | "secondary"
  | "danger"
  | "ghost"
  | "outline"
  | "ghost-red"
  | "ghost-purple"
  | "ghost-green";
export type ButtonSize = "sm" | "md" | "lg" | "confirm";

export interface ButtonProps extends ButtonHTMLAttributes<HTMLButtonElement> {
  variant?: ButtonVariant;
  size?: ButtonSize;
  loading?: boolean;
  wide?: boolean;
  /** No focus ring and no shadow, as the old #confirm-modal .btns .btn. */
  flat?: boolean;
  ref?: Ref<HTMLButtonElement>;
}

// The old .btn-ghost-red and .btn-ghost-green: no lift, the accent label and
// border turn red or green on hover, a white outline on keyboard focus.
// pages/system/buttons.tsx and pages/dashboard/parts.tsx build on these.
export const GHOST_TINT_FILL =
  "enabled:hover:bg-[rgba(255,255,255,0.08)] enabled:active:bg-[rgba(255,255,255,0.1)] disabled:opacity-60";
export const GHOST_TINT_FOCUS =
  "focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-white/28";
export const GHOST_RED_HOVER = "enabled:hover:border-danger enabled:hover:text-danger";
export const GHOST_GREEN_HOVER = "enabled:hover:border-good enabled:hover:text-good";

// The old .btn had no gap; only buttons with an icon get one.
const BASE =
  "inline-flex items-center justify-center has-[svg]:gap-2 border font-semibold leading-none " +
  "cursor-pointer select-none outline-none disabled:cursor-not-allowed";

const LIFT =
  "transition duration-200 focus-visible:ring-2 focus-visible:ring-accent/60 " +
  "enabled:hover:-translate-y-0.5 enabled:hover:shadow-card enabled:active:translate-y-0 enabled:active:scale-[0.97] " +
  "disabled:opacity-55";

const GHOST_TINT =
  "border-border bg-[rgba(255,255,255,0.04)] text-accent transition-[background-color,border-color,color,outline-color] " +
  "duration-150 motion-reduce:transition-none";

const VARIANT: Record<ButtonVariant, string> = {
  primary:
    "border-border bg-[linear-gradient(180deg,var(--color-primary),var(--color-primary-700))] text-white " +
    "enabled:hover:bg-[linear-gradient(180deg,#9366f9,var(--color-primary))] enabled:hover:border-[#4c2d7a]",
  secondary: "border-border bg-[linear-gradient(180deg,#1f1237,#160b2a)] text-text",
  danger:
    "border-[#7f1d1d] bg-[linear-gradient(180deg,#ef4444,#b91c1c)] text-white " +
    "enabled:hover:bg-[linear-gradient(180deg,#f87171,#dc2626)] enabled:hover:shadow-danger",
  ghost:
    "border-border bg-[rgba(255,255,255,0.04)] text-accent " +
    "enabled:hover:bg-[rgba(255,255,255,0.08)] enabled:hover:border-[rgba(167,139,250,0.3)] enabled:hover:shadow-soft",
  outline:
    "border-accent bg-transparent text-accent enabled:hover:bg-[rgba(167,139,250,0.1)] enabled:hover:shadow-accent",
  "ghost-red": `${GHOST_TINT} ${GHOST_TINT_FILL} ${GHOST_RED_HOVER} ${GHOST_TINT_FOCUS}`,
  "ghost-purple":
    `${GHOST_TINT} enabled:hover:bg-[rgba(255,255,255,0.08)] enabled:hover:border-primary-600 ` +
    "enabled:hover:outline-2 enabled:hover:outline-offset-2 enabled:hover:outline-primary-600 " +
    "focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-primary-600 disabled:opacity-60",
  "ghost-green": `${GHOST_TINT} ${GHOST_TINT_FILL} ${GHOST_GREEN_HOVER} ${GHOST_TINT_FOCUS}`,
};

const LIFTS: Record<ButtonVariant, boolean> = {
  primary: true,
  secondary: true,
  danger: true,
  ghost: true,
  outline: true,
  "ghost-red": false,
  "ghost-purple": false,
  "ghost-green": false,
};

// sm is .btn.btn-xs, md is .btn, lg is .btn-wide height, confirm is
// #confirm-modal .btns .btn (the .btn height with 8px 10px padding, 12px text).
const SIZE: Record<ButtonSize, string> = {
  sm: "min-h-7 rounded-btn px-2.5 py-1.5 text-xs",
  md: "min-h-[42px] rounded-control px-4 py-2.5 text-sm",
  lg: "min-h-12 rounded-control px-5 py-3 text-base",
  confirm: "min-h-[42px] rounded-btn px-2.5 py-2 text-xs",
};

const FLAT = "ring-0! shadow-none! enabled:hover:shadow-none!";

export function Button({
  variant = "secondary",
  size = "md",
  loading = false,
  wide = false,
  flat = false,
  type = "button",
  disabled,
  className,
  children,
  ref,
  ...rest
}: ButtonProps) {
  return (
    <button
      ref={ref}
      type={type}
      disabled={disabled || loading}
      aria-busy={loading || undefined}
      className={cx(
        BASE,
        LIFTS[variant] && LIFT,
        VARIANT[variant],
        SIZE[size],
        wide && "min-w-[170px]",
        flat && FLAT,
        className,
      )}
      {...rest}
    >
      {loading ? <Spinner size="sm" label="Working" /> : null}
      {children}
    </button>
  );
}
