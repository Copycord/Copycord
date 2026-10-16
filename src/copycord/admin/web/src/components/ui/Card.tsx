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
        "rounded-card border border-border bg-(image:--card-gradient) p-[18px] text-text",
        className,
      )}
      {...rest}
    >
      {title || actions ? (
        <header className="mb-3 flex items-center justify-between gap-3">
          {title ? <h2 className="m-0 text-[15px] font-bold">{title}</h2> : <span />}
          {actions ? <div className="flex items-center gap-2">{actions}</div> : null}
        </header>
      ) : null}
      {children}
    </section>
  );
}

/** No panel (rule f): a page's title, controls and cards sit on the page with no side inset, so the title, toolbar
 *  and first card start on the container edge like Dashboard, Scraper, Logs and Tasks. */
export function PageFrame({ className, children, ...rest }: HTMLAttributes<HTMLElement>) {
  return (
    <section className={cx("mt-[18px] border-y border-transparent py-[18px] text-text", className)} {...rest}>
      {children}
    </section>
  );
}
