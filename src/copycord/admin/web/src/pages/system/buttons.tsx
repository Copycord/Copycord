// The old page's ghost buttons, which the shared Button does not cover: a
// ghost styled download link (a.btn.btn-ghost), the 13px label the old page
// gave <button> elements, and .btn-ghost-red, a ghost button that turns red
// on hover. Classes follow components/ui/Button.tsx.

import type { AnchorHTMLAttributes, ButtonHTMLAttributes } from "react";
import { cx, Spinner } from "../../components/ui";

const BASE =
  "inline-flex min-h-[42px] items-center justify-center gap-2 rounded-control border border-border bg-white/4 " +
  "px-4 py-2.5 font-semibold leading-none text-accent no-underline select-none outline-none " +
  "transition duration-200 focus-visible:ring-2 focus-visible:ring-accent/60";

const LIFT =
  "hover:-translate-y-0.5 hover:border-accent/30 hover:bg-white/8 hover:shadow-soft active:translate-y-0 active:scale-[0.97]";

export interface LinkButtonProps extends AnchorHTMLAttributes<HTMLAnchorElement> {
  /** Drops the href and greys the link out, like the old .is-disabled. */
  disabled?: boolean;
}

export function LinkButton({ disabled = false, href, className, children, ...rest }: LinkButtonProps) {
  return (
    <a
      href={disabled ? undefined : href}
      aria-disabled={disabled || undefined}
      className={cx(
        BASE,
        "text-sm",
        disabled ? "pointer-events-none cursor-not-allowed opacity-55" : cx("cursor-pointer", LIFT),
        className,
      )}
      {...rest}
    >
      {children}
    </a>
  );
}

export interface GhostButtonProps extends ButtonHTMLAttributes<HTMLButtonElement> {
  /** 13px label, as the old page rendered <button> elements. */
  compact?: boolean;
  /** The old .btn-ghost-red: red text and border on hover. */
  danger?: boolean;
  loading?: boolean;
}

export function GhostButton({
  compact = false,
  danger = false,
  loading = false,
  disabled,
  type = "button",
  className,
  children,
  ...rest
}: GhostButtonProps) {
  return (
    <button
      type={type}
      disabled={disabled || loading}
      aria-busy={loading || undefined}
      className={cx(
        BASE,
        compact ? "text-[13px]" : "text-sm",
        "cursor-pointer disabled:cursor-not-allowed",
        danger
          ? "enabled:hover:border-danger enabled:hover:bg-white/8 enabled:hover:text-danger enabled:active:bg-white/10 disabled:opacity-60"
          : "enabled:hover:-translate-y-0.5 enabled:hover:border-accent/30 enabled:hover:bg-white/8 enabled:hover:shadow-soft " +
              "enabled:active:translate-y-0 enabled:active:scale-[0.97] disabled:opacity-55",
        className,
      )}
      {...rest}
    >
      {loading ? <Spinner size="sm" label="Working" /> : null}
      {children}
    </button>
  );
}
