// Accessible modal: renders into document.body, traps Tab focus, closes on
// Escape and on overlay click, and gives focus back to the opener on close.
// The look is the old .modal / .modal-content / .modal-header / .verify-close:
// the box hangs 6vh below the header instead of being centered, and the
// MODAL_* classes let the dialogs with their own shell match it.

import { useEffect, useId, useRef } from "react";
import type { ReactNode } from "react";
import { createPortal } from "react-dom";

/** The old .modal: fixed below the header, the box at the top center. */
export const MODAL_OVERLAY = "fixed inset-x-0 top-header bottom-0 z-40 flex items-start justify-center";
export const MODAL_BACKDROP = "absolute inset-0 bg-black/[0.68]";
/** The old .modal-content without its width (min(900px, 92vw) unless a dialog sets its own). */
export const MODAL_CARD =
  "relative z-[1] mt-[6vh] flex max-h-[80vh] flex-col rounded-panel border border-border bg-[#0f0820] text-text shadow-[0_20px_56px_rgba(0,0,0,0.5)] outline-none";
export const MODAL_HEADER = "flex items-center justify-between px-4 py-3";
export const MODAL_TITLE = "m-0 text-base font-bold text-accent";
/** The old .icon-btn.verify-close square close button. */
export const MODAL_CLOSE =
  "ml-auto inline-flex size-8 shrink-0 cursor-pointer items-center justify-center rounded-btn border border-border bg-control px-2 py-0.5 text-base leading-none text-text outline-none transition-colors hover:border-danger hover:text-danger focus-visible:border-danger focus-visible:text-danger active:border-[#dc2626] active:text-[#dc2626]";

export interface ModalProps {
  open: boolean;
  onClose: () => void;
  title: string;
  /** Second header line; the title then becomes the old plain .modal-title-main. */
  subtitle?: ReactNode;
  children: ReactNode;
  footer?: ReactNode;
  /** Close when the dark overlay is clicked (default true). */
  closeOnOverlay?: boolean;
  /** Width classes of the box (default the old confirm dialog, min(420px, 92vw)). */
  className?: string;
  bodyClassName?: string;
  id?: string;
  closeId?: string;
}

const FOCUSABLE =
  'a[href], button:not([disabled]), input:not([disabled]), select:not([disabled]), textarea:not([disabled]), [tabindex]:not([tabindex="-1"])';

export function Modal({
  open,
  onClose,
  title,
  subtitle,
  children,
  footer,
  closeOnOverlay = true,
  className = "w-[min(420px,92vw)]",
  bodyClassName = "px-4 pb-4",
  id,
  closeId,
}: ModalProps) {
  const dialogRef = useRef<HTMLDivElement>(null);
  const titleId = useId();
  const onCloseRef = useRef(onClose);
  onCloseRef.current = onClose;

  useEffect(() => {
    if (!open) return;
    const opener = document.activeElement as HTMLElement | null;
    const dialog = dialogRef.current;
    const first =
      dialog?.querySelector<HTMLElement>("[data-autofocus]") ?? dialog?.querySelector<HTMLElement>(FOCUSABLE);
    (first ?? dialog)?.focus();

    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") {
        e.stopPropagation();
        onCloseRef.current();
        return;
      }
      if (e.key !== "Tab" || !dialog) return;
      const items = Array.from(dialog.querySelectorAll<HTMLElement>(FOCUSABLE));
      if (items.length === 0) {
        e.preventDefault();
        dialog.focus();
        return;
      }
      const head = items[0]!;
      const tail = items[items.length - 1]!;
      const active = document.activeElement;
      if (e.shiftKey && (active === head || active === dialog)) {
        e.preventDefault();
        tail.focus();
      } else if (!e.shiftKey && active === tail) {
        e.preventDefault();
        head.focus();
      }
    };
    document.addEventListener("keydown", onKey);
    return () => {
      document.removeEventListener("keydown", onKey);
      opener?.focus?.();
    };
  }, [open]);

  if (!open) return null;

  return createPortal(
    <div
      id={id}
      className={MODAL_OVERLAY}
      data-testid="modal-overlay"
      onMouseDown={(e) => {
        if (closeOnOverlay && e.target === e.currentTarget) onClose();
      }}
    >
      <div className={`${MODAL_BACKDROP} pointer-events-none`} />
      <div
        ref={dialogRef}
        role="dialog"
        aria-modal="true"
        aria-labelledby={titleId}
        tabIndex={-1}
        className={`${MODAL_CARD} ${className}`}
      >
        <header className={MODAL_HEADER}>
          {subtitle ? (
            <div className="flex min-w-0 flex-col">
              <h2 id={titleId} className="m-0 text-base font-normal">
                {title}
              </h2>
              <p className="m-0 text-base">{subtitle}</p>
            </div>
          ) : (
            <h2 id={titleId} className={MODAL_TITLE}>
              {title}
            </h2>
          )}
          <button type="button" id={closeId} className={MODAL_CLOSE} aria-label="Close" onClick={onClose}>
            ✕
          </button>
        </header>
        <div className={`min-h-0 flex-1 overflow-y-auto ${bodyClassName}`}>{children}</div>
        {footer ? <footer className="flex justify-end gap-2.5 px-4 pb-4">{footer}</footer> : null}
      </div>
    </div>,
    document.body,
  );
}
