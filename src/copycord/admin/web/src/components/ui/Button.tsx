// Button variants: primary, danger, outline and the ghost family (plain, red,
// purple, green).

import type { ButtonHTMLAttributes, Ref } from "react";
import { cx } from "./cx";
import { BUSY_HOST, BusyContent } from "./Spinner";

export type ButtonVariant =
  | "primary"
  | "secondary"
  | "danger"
  | "ghost"
  | "outline"
  | "ghost-red"
  | "ghost-purple"
  | "ghost-green"
  | "danger-outline";
export type ButtonSize = "sm" | "md" | "lg" | "confirm";

export interface ButtonProps extends ButtonHTMLAttributes<HTMLButtonElement> {
  variant?: ButtonVariant;
  size?: ButtonSize;
  loading?: boolean;
  wide?: boolean;
  /** No focus ring and no shadow, as on the confirm dialog buttons. */
  flat?: boolean;
  /** Joined to an input, as the runs search submit: hover only tints, no lift, no press scale, no shadow. */
  attached?: boolean;
  ref?: Ref<HTMLButtonElement>;
}

// Ghost red and ghost green: no lift, the accent label and
// border turn red or green on hover, a white outline on keyboard focus.
// pages/system/buttons.tsx and pages/dashboard/parts.tsx build on these.
export const GHOST_TINT_FILL =
  "enabled:hover:bg-line-1 enabled:active:bg-wash-10 disabled:opacity-60";
export const GHOST_TINT_FOCUS =
  "focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-white/28";
export const GHOST_RED_HOVER = "enabled:hover:border-danger enabled:hover:text-danger";
export const GHOST_GREEN_HOVER = "enabled:hover:border-good enabled:hover:text-good";
/** The danger outline for destructive triggers (delete, remove, clear, reset):
 *  danger text and border, no fill, the soft danger fill on hover. Important, so it wins over a base border and fill. */
export const DANGER_OUTLINE =
  "border-danger! bg-transparent! text-danger! enabled:hover:bg-danger-a10! enabled:active:bg-danger/18! " +
  "enabled:hover:translate-y-0! enabled:hover:shadow-none! " +
  // Disabled drops the red: the neutral border and muted icon and text, so it never reads as live.
  "disabled:cursor-not-allowed! disabled:border-border! disabled:text-muted!";
/** The purple tint on the ghost look, as on the Messages Reset filters button; also used by RESET_BTN. */
export const GHOST_PURPLE_TINT =
  "border-primary-a30 bg-primary-a10 text-accent-soft enabled:hover:border-primary-a50 enabled:hover:bg-primary-a20";

// Only buttons with an icon get a gap.
const BASE =
  "inline-flex items-center justify-center has-[svg]:gap-2 border font-semibold leading-none " +
  "cursor-pointer select-none outline-none disabled:cursor-not-allowed";

const LIFT =
  "transition duration-200 focus-visible:ring-2 focus-visible:ring-accent/60 " +
  "enabled:hover:-translate-y-0.5 enabled:hover:shadow-card enabled:active:translate-y-0 enabled:active:scale-[0.97] " +
  "disabled:opacity-55";

const GHOST_TINT =
  "border-border bg-wash-4 text-accent transition-[background-color,border-color,color,outline-color] " +
  "duration-150 motion-reduce:transition-none";

const VARIANT: Record<ButtonVariant, string> = {
  primary:
    "border-border bg-[linear-gradient(180deg,var(--color-primary),var(--color-primary-700))] text-white " +
    "enabled:hover:bg-[linear-gradient(180deg,var(--color-primary-hover),var(--color-primary))] enabled:hover:border-primary-edge",
  secondary: "border-border bg-[linear-gradient(180deg,var(--color-secondary-top),var(--color-secondary-bottom))] text-text",
  danger:
    "border-danger-edge bg-[linear-gradient(180deg,var(--color-danger),var(--color-danger-deep))] text-white " +
    "enabled:hover:bg-[linear-gradient(180deg,var(--color-danger-soft),var(--color-danger-strong))] enabled:hover:shadow-danger",
  ghost: GHOST_PURPLE_TINT,
  outline:
    "border-accent bg-transparent text-accent enabled:hover:bg-accent-a10 enabled:hover:shadow-accent",
  "ghost-red": `${GHOST_TINT} ${GHOST_TINT_FILL} ${GHOST_RED_HOVER} ${GHOST_TINT_FOCUS}`,
  "ghost-purple":
    `${GHOST_TINT} enabled:hover:bg-line-1 enabled:hover:border-primary-600 ` +
    "enabled:hover:outline-2 enabled:hover:outline-offset-2 enabled:hover:outline-primary-600 " +
    "focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-primary-600 disabled:opacity-60",
  "ghost-green": `${GHOST_TINT} ${GHOST_TINT_FILL} ${GHOST_GREEN_HOVER} ${GHOST_TINT_FOCUS}`,
  "danger-outline": `transition-colors duration-150 ${DANGER_OUTLINE} ${GHOST_TINT_FOCUS}`,
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
  "danger-outline": false,
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
// The flat hover without dropping the focus ring, so a button attached to an input stays in its group.
const ATTACHED =
  "enabled:hover:translate-y-0! enabled:hover:shadow-none! enabled:active:translate-y-0! enabled:active:scale-100!";

export function Button({
  variant = "secondary",
  size = "md",
  loading = false,
  wide = false,
  flat = false,
  attached = false,
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
      aria-disabled={disabled || loading || undefined}
      aria-busy={loading || undefined}
      className={cx(
        BASE,
        LIFTS[variant] && LIFT,
        VARIANT[variant],
        SIZE[size],
        wide && "min-w-[170px]",
        flat && FLAT,
        attached && ATTACHED,
        loading && BUSY_HOST,
        className,
      )}
      {...rest}
    >
      <BusyContent busy={loading}>{children}</BusyContent>
    </button>
  );
}
