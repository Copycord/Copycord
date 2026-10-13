// The old collapsible card (details.srv-proxy-card): title, docs link and chevron in the summary,
// open state remembered in localStorage under the old key.

import { useState } from "react";
import type { ReactNode, SyntheticEvent } from "react";

function readOpen(key: string): boolean {
  try {
    return localStorage.getItem(key) === "1";
  } catch {
    return false;
  }
}

export interface CollapseCardProps {
  id: string;
  title: string;
  docsHref: string;
  /** localStorage key, e.g. cpc.sync-card-open. */
  storageKey: string;
  children: ReactNode;
}

export function CollapseCard({ id, title, docsHref, storageKey, children }: CollapseCardProps) {
  const [open, setOpen] = useState(() => readOpen(storageKey));

  function onToggle(e: SyntheticEvent<HTMLDetailsElement>) {
    const next = e.currentTarget.open;
    setOpen(next);
    try {
      localStorage.setItem(storageKey, next ? "1" : "0");
    } catch {
      // Storage can be blocked; the card still works.
    }
  }

  return (
    <details
      id={id}
      open={open}
      onToggle={onToggle}
      className="group rounded-card border border-border bg-linear-to-b from-card to-[rgba(20,10,40,0.45)] text-text"
    >
      <summary className="flex cursor-pointer list-none items-center gap-2 px-[18px] py-3 [&::-webkit-details-marker]:hidden">
        <h3 className="text-base font-semibold">{title}</h3>
        <a
          href={docsHref}
          target="_blank"
          rel="noopener noreferrer"
          title="View docs"
          aria-label={`${title} docs`}
          className="text-text-2 hover:text-accent"
          onClick={(e) => e.stopPropagation()}
        >
          <svg viewBox="0 0 24 24" width="14" height="14" fill="currentColor" aria-hidden="true">
            <path d="M21 5c-1.11-.35-2.33-.5-3.5-.5-1.95 0-4.05.4-5.5 1.5-1.45-1.1-3.55-1.5-5.5-1.5S2.45 4.9 1 6v14.65c0 .25.25.5.5.5.1 0 .15-.05.25-.05C3.1 20.45 5.05 20 6.5 20c1.95 0 4.05.4 5.5 1.5 1.35-.85 3.8-1.5 5.5-1.5 1.65 0 3.35.3 4.75 1.05.1.05.15.05.25.05.25 0 .5-.25.5-.5V6c-.6-.45-1.25-.75-2-1zm0 13.5c-1.1-.35-2.3-.5-3.5-.5-1.7 0-4.15.65-5.5 1.5V8c1.35-.85 3.8-1.5 5.5-1.5 1.2 0 2.4.15 3.5.5v11.5z" />
          </svg>
        </a>
        <span aria-hidden="true" className="ml-auto text-text-2 transition-transform group-open:rotate-180">
          &#9662;
        </span>
      </summary>
      <div className="flex flex-col gap-3 px-[18px] pb-[18px]">{children}</div>
    </details>
  );
}
