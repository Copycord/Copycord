// Same look as the old .btn, .btn-primary, .btn-danger, .btn-outline and
// .btn-ghost classes in main.css.

import type { ButtonHTMLAttributes, Ref } from "react";
import { cx } from "./cx";
import { Spinner } from "./Spinner";

export type ButtonVariant = "primary" | "secondary" | "danger" | "ghost" | "outline";
export type ButtonSize = "sm" | "md" | "lg";

export interface ButtonProps extends ButtonHTMLAttributes<HTMLButtonElement> {
  variant?: ButtonVariant;
  size?: ButtonSize;
  loading?: boolean;
  wide?: boolean;
  ref?: Ref<HTMLButtonElement>;
}

// The old .btn had no gap; only buttons with an icon get one.
const BASE =
  "inline-flex items-center justify-center has-[svg]:gap-2 border font-semibold leading-none " +
  "transition duration-200 cursor-pointer select-none outline-none " +
  "focus-visible:ring-2 focus-visible:ring-accent/60 " +
  "enabled:hover:-translate-y-0.5 enabled:hover:shadow-card enabled:active:translate-y-0 enabled:active:scale-[0.97] " +
  "disabled:cursor-not-allowed disabled:opacity-55";

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
};

// sm is .btn.btn-xs, md is .btn, lg is .btn-wide height.
const SIZE: Record<ButtonSize, string> = {
  sm: "min-h-7 rounded-btn px-2.5 py-1.5 text-xs",
  md: "min-h-[42px] rounded-control px-4 py-2.5 text-sm",
  lg: "min-h-12 rounded-control px-5 py-3 text-base",
};

export function Button({
  variant = "secondary",
  size = "md",
  loading = false,
  wide = false,
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
      className={cx(BASE, VARIANT[variant], SIZE[size], wide && "min-w-[170px]", className)}
      {...rest}
    >
      {loading ? <Spinner size="sm" label="Working" /> : null}
      {children}
    </button>
  );
}
