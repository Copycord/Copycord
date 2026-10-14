// Dialog shell shared by the Filters modal and its two pickers: old ids, the
// .show class, close by X or backdrop. Escape is handled by FiltersModal so
// only the top dialog closes. The look is the old .modal-content with the
// per dialog width, margin and radius passed in `cardClass`.

import type { ReactNode } from "react";
import { createPortal } from "react-dom";
import { MODAL_BACKDROP, MODAL_CARD, MODAL_CLOSE, MODAL_HEADER, MODAL_OVERLAY, MODAL_TITLE, useScrollLock } from "../../../components/Modal";
import { cx } from "../../../components/ui";

interface ShellProps {
  id: string;
  closeId: string;
  titleId: string;
  title: string;
  onClose: () => void;
  /** Width, margin, height and radius of the box. */
  cardClass: string;
  headerClass?: string;
  titleClass?: string;
  /** Stacking order: the discard confirm (Modal, z-40) sits above the filters modal and below nothing else. */
  layer?: number;
  children: ReactNode;
}

export function Shell({ id, closeId, titleId, title, onClose, cardClass, headerClass, titleClass, layer = 30, children }: ShellProps) {
  useScrollLock(true);
  return createPortal(
    <div id={id} className={`modal show ${MODAL_OVERLAY}`} style={{ zIndex: layer }} aria-hidden="false">
      <div className={`modal-backdrop ${MODAL_BACKDROP}`} onMouseDown={onClose} />
      <div role="dialog" aria-modal="true" aria-labelledby={titleId} className={cx("modal-content", MODAL_CARD, cardClass)}>
        <div className={cx("modal-header shrink-0", headerClass ?? MODAL_HEADER)}>
          <h4 id={titleId} className={cx("modal-title", titleClass ?? MODAL_TITLE)}>
            {title}
          </h4>
          <button type="button" id={closeId} aria-label="Close" onClick={onClose} className={`verify-close ${MODAL_CLOSE}`}>
            ✕
          </button>
        </div>
        {children}
      </div>
    </div>,
    document.body,
  );
}
