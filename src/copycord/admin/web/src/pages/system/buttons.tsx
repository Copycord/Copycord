// Two button looks the shared Button does not have: a ghost styled download
// link (the old a.btn.btn-ghost) and the old .btn-ghost-red, a ghost button
// that turns red on hover. Classes follow components/ui/Button.tsx.

import type { AnchorHTMLAttributes, ButtonHTMLAttributes } from "react";
import { cx } from "../../components/ui";

const BASE =
  "inline-flex min-h-[42px] items-center justify-center gap-2 rounded-control border border-border bg-white/4 " +
  "px-4 py-2.5 text-sm font-semibold leading-none text-accent no-underline select-none outline-none " +
  "transition duration-200 focus-visible:ring-2 focus-visible:ring-accent/60";

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
        disabled
          ? "pointer-events-none cursor-not-allowed opacity-55"
          : "cursor-pointer hover:-translate-y-0.5 hover:border-accent/30 hover:bg-white/8 hover:shadow-soft active:translate-y-0 active:scale-[0.97]",
        className,
      )}
      {...rest}
    >
      {children}
    </a>
  );
}

export function GhostRedButton({ className, type = "button", children, ...rest }: ButtonHTMLAttributes<HTMLButtonElement>) {
  return (
    <button
      type={type}
      className={cx(
        BASE,
        "cursor-pointer enabled:hover:border-danger enabled:hover:bg-white/8 enabled:hover:text-danger " +
          "enabled:active:bg-white/10 disabled:cursor-not-allowed disabled:opacity-60",
        className,
      )}
      {...rest}
    >
      {children}
    </button>
  );
}
