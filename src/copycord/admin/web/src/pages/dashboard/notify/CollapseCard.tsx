// The old collapsible card (details.srv-proxy-card): title, docs link and chevron in the summary,
// open state remembered in localStorage under the old key.

import { useState } from "react";
import type { ReactNode, SyntheticEvent } from "react";
import { CARD, Chevron, DocLink, readCardOpen, SUMMARY, SUMMARY_TITLE } from "../parts";

export interface CollapseCardProps {
  id: string;
  title: string;
  docsHref: string;
  /** localStorage key, e.g. cpc.sync-card-open. */
  storageKey: string;
  children: ReactNode;
}

export function CollapseCard({ id, title, docsHref, storageKey, children }: CollapseCardProps) {
  const [open, setOpen] = useState(() => readCardOpen(storageKey));

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
    <details id={id} open={open} onToggle={onToggle} className={CARD}>
      <summary className={SUMMARY}>
        <h3 className={SUMMARY_TITLE}>{title}</h3>
        <DocLink href={docsHref} label={`${title} docs`} />
        <Chevron />
      </summary>
      <div className="srv-proxy-body mt-3.5">{children}</div>
    </details>
  );
}
