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

const BASE =
  "inline-flex items-center justify-center gap-2 rounded-control border font-semibold leading-none " +
  "transition duration-200 cursor-pointer select-none outline-none " +
  "focus-visible:ring-2 focus-visible:ring-accent/60 " +
  "enabled:hover:-translate-y-0.5 enabled:hover:shadow-card enabled:active:translate-y-0 enabled:active:scale-[0.97] " +
  "disabled:cursor-not-allowed disabled:opacity-55";

const VARIANT: Record<ButtonVariant, string> = {
  primary:
    "border-border bg-linear-to-b from-primary to-primary-700 text-white enabled:hover:from-[#9366f9] enabled:hover:to-primary enabled:hover:border-[#4c2d7a]",
  secondary: "border-border bg-linear-to-b from-[#1f1237] to-[#160b2a] text-text",
  danger:
    "border-[#7f1d1d] bg-linear-to-b from-danger to-[#b91c1c] text-white enabled:hover:from-[#f87171] enabled:hover:to-[#dc2626] enabled:hover:shadow-danger",
  ghost:
    "border-border bg-white/4 text-accent enabled:hover:bg-white/8 enabled:hover:border-accent/30 enabled:hover:shadow-soft",
  outline: "border-accent bg-transparent text-accent enabled:hover:bg-accent/10 enabled:hover:shadow-accent",
};

const SIZE: Record<ButtonSize, string> = {
  sm: "min-h-control px-3 py-1.5 text-[13px]",
  md: "min-h-[42px] px-4 py-2.5 text-sm",
  lg: "min-h-12 px-5 py-3 text-base",
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
