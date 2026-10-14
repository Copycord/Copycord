// Accessible modal: renders into document.body, traps Tab focus, closes on
// Escape and on overlay click, and gives focus back to the opener on close.
// The look is the old .modal / .modal-content / .modal-header / .verify-close:
// the box hangs 6vh below the header instead of being centered, and the
// MODAL_* classes let the dialogs with their own shell match it.

import { useEffect, useId, useRef, useState } from "react";
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

// The old .body-lock-scroll / body.modal-open: the page behind an open dialog does not scroll.
// Counted so closing the inner of two stacked dialogs keeps the lock; the last one restores
// the overflow the body had before the first opened.
let scrollLocks = 0;
let overflowBeforeLock = "";

export function useScrollLock(active: boolean) {
  useEffect(() => {
    if (!active) return;
    if (scrollLocks === 0) {
      overflowBeforeLock = document.body.style.overflow;
      document.body.style.overflow = "hidden";
    }
    scrollLocks += 1;
    return () => {
      scrollLocks -= 1;
      if (scrollLocks === 0) document.body.style.overflow = overflowBeforeLock;
    };
  }, [active]);
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
  useScrollLock(open);

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

const BUBBLE_ALIGN = {
  /** The old .tip-bubble: under the label's left edge, one line. */
  start:
    "left-0 max-w-[80vw] overflow-x-auto whitespace-nowrap after:left-3.5 " +
    "-translate-y-1 data-[open=true]:translate-y-0",
  /** #mapping-modal .tip-bubble: centered 60px right of the label, wraps at 240px. */
  center:
    "left-1/2 max-w-[min(240px,calc(100vw-32px))] translate-x-[calc(-50%+60px)] -translate-y-1 overflow-x-hidden break-words whitespace-normal " +
    "after:left-[calc(50%-60px)] after:-translate-x-1/2 data-[open=true]:translate-y-0",
  /** #filters-modal .tip-bubble: under the label, 200 to 260px wide, wraps. */
  wrap:
    "left-0 max-w-[260px] min-w-[200px] overflow-x-auto break-words whitespace-normal after:left-3.5 " +
    "-translate-y-1 data-[open=true]:translate-y-0",
} as const;

export interface TipLabelProps {
  htmlFor?: string;
  text: ReactNode;
  tip: ReactNode;
  /** The "i" button's accessible name, e.g. "What is Mapping Name?". */
  tipLabel?: string;
  tipId?: string;
  align?: keyof typeof BUBBLE_ALIGN;
  className?: string;
}

/**
 * The old label.has-tip: 12px muted label text, the round "i" .info-dot and its
 * .tip-bubble. Click the dot to show the tip; another click, a click elsewhere
 * or Escape hides it.
 */
export function TipLabel({ htmlFor, text, tip, tipLabel, tipId, align = "start", className }: TipLabelProps) {
  const auto = useId();
  const id = tipId ?? `tip${auto}`;
  const [open, setOpen] = useState(false);
  const dotRef = useRef<HTMLButtonElement>(null);

  useEffect(() => {
    if (!open) return;
    const onDown = (e: MouseEvent) => {
      const t = e.target as Node;
      if (dotRef.current?.contains(t) || document.getElementById(id)?.contains(t)) return;
      setOpen(false);
    };
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") setOpen(false);
    };
    document.addEventListener("mousedown", onDown);
    document.addEventListener("keydown", onKey);
    return () => {
      document.removeEventListener("mousedown", onDown);
      document.removeEventListener("keydown", onKey);
    };
  }, [open, id]);

  return (
    <label
      htmlFor={htmlFor}
      className={`has-tip relative mt-0 mr-0 mb-1.5 ml-0.5 inline-flex items-center gap-1.5 text-xs text-muted ${className ?? ""}`}
    >
      <span className="label-text leading-none">{text}</span>
      <button
        ref={dotRef}
        type="button"
        className="info-dot inline-flex size-[18px] cursor-pointer touch-manipulation items-center justify-center rounded-full border border-border bg-white/6 p-0 font-sans text-[11px] leading-none font-bold text-accent outline-0 transition-[border-color] duration-150 before:-translate-y-[0.5px] before:content-['i'] hover:border-accent focus-visible:border-accent"
        aria-label={tipLabel}
        aria-describedby={id}
        aria-expanded={open}
        onClick={(e) => {
          e.preventDefault();
          setOpen((o) => !o);
        }}
      />
      <span
        id={id}
        role="tooltip"
        aria-hidden={!open}
        data-open={open}
        className={`tip-bubble pointer-events-none absolute top-[calc(100%+8px)] z-[60] w-max scale-[0.98] rounded-control border border-border bg-[#0f0820] px-3 py-2.5 text-[12.5px] leading-[1.45] text-text opacity-0 shadow-[0_12px_40px_rgba(0,0,0,0.45)] transition-[opacity,translate,scale] duration-150 after:absolute after:-top-1.5 after:border-[6px] after:border-solid after:border-transparent after:border-b-[#0f0820] after:content-[''] data-[open=true]:pointer-events-auto data-[open=true]:scale-100 data-[open=true]:opacity-100 ${BUBBLE_ALIGN[align]}`}
      >
        {tip}
      </span>
    </label>
  );
}
