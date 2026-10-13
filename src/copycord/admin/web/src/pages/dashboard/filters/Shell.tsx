// Dialog shell shared by the Filters modal and its two pickers: old ids, the
// .show class, close by X or backdrop. Escape is handled by FiltersModal so
// only the top dialog closes.

import type { ReactNode } from "react";
import { createPortal } from "react-dom";
import { cx } from "../../../components/ui";

interface ShellProps {
  id: string;
  closeId: string;
  titleId: string;
  title: string;
  onClose: () => void;
  wide?: boolean;
  /** Stacking order: the discard confirm (Modal, z-40) sits above the filters modal and below nothing else. */
  layer?: number;
  children: ReactNode;
}

export function Shell({ id, closeId, titleId, title, onClose, wide, layer = 30, children }: ShellProps) {
  return createPortal(
    <div id={id} className="modal show fixed inset-0 flex items-center justify-center p-4" style={{ zIndex: layer }} aria-hidden="false">
      <div className="modal-backdrop absolute inset-0 bg-black/60" onMouseDown={onClose} />
      <div
        role="dialog"
        aria-modal="true"
        aria-labelledby={titleId}
        className={cx(
          "relative max-h-[90vh] w-full overflow-y-auto rounded-2xl border border-border bg-card-solid text-text shadow-card",
          wide ? "max-w-4xl" : "max-w-2xl",
        )}
      >
        <div className="flex items-center justify-between border-b border-border px-5 py-3">
          <h4 id={titleId} className="modal-title text-base font-semibold">
            {title}
          </h4>
          <button
            type="button"
            id={closeId}
            aria-label="Close"
            onClick={onClose}
            className="verify-close rounded-control px-2 py-1 text-muted hover:text-text"
          >
            ✕
          </button>
        </div>
        {children}
      </div>
    </div>,
    document.body,
  );
}
