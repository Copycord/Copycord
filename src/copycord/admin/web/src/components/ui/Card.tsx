// The old .card: translucent purple gradient, 18px radius, 18px padding.

import type { HTMLAttributes, ReactNode } from "react";
import { cx } from "./cx";

export interface CardProps extends Omit<HTMLAttributes<HTMLElement>, "title"> {
  title?: ReactNode;
  actions?: ReactNode;
}

export function Card({ title, actions, className, children, ...rest }: CardProps) {
  return (
    <section
      className={cx(
        "rounded-card border border-border bg-linear-to-b from-card to-[rgba(20,10,40,0.45)] p-[18px] text-text",
        className,
      )}
      {...rest}
    >
      {title || actions ? (
        <header className="mb-3 flex items-center justify-between gap-3">
          {title ? <h2 className="text-base font-semibold">{title}</h2> : <span />}
          {actions ? <div className="flex items-center gap-2">{actions}</div> : null}
        </header>
      ) : null}
      {children}
    </section>
  );
}
