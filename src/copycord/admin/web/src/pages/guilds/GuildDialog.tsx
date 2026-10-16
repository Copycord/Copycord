// Dialog frame of the old guild modals (.scraper-card): icon and accent title
// in the header, a close X, body and footer. Kept local because the shared
// Modal has no icon, subtitle, width or stacking: the export dialog opens the
// attachment types dialog on top of itself, and only the top one answers
// Escape and Tab. Focus moves in on open and back to the opener on close.

import { useEffect, useId, useRef } from "react";
import type { ReactNode } from "react";
import { createPortal } from "react-dom";
import { MODAL_CLOSE } from "../../components/Modal";
import { cx } from "../../components/ui";

export interface GuildDialogProps {
  onClose: () => void;
  title: string;
  /** Icon left of the title, as in the details dialog. */
  icon?: string;
  onIconError?: () => void;
  subtitle?: ReactNode;
  children: ReactNode;
  footer?: ReactNode;
  /** "top" sits below the header like the details dialog, "center" like the export dialog. */
  placement?: "top" | "center";
  width?: "md" | "sm";
  /** Selector of the control to focus first; defaults to the first focusable one. */
  initialFocus?: string;
  testId?: string;
}

const FOCUSABLE =
  'a[href], button:not([disabled]), input:not([disabled]), select:not([disabled]), textarea:not([disabled]), summary, [tabindex]:not([tabindex="-1"])';

// Open dialogs, innermost last.
const stack: symbol[] = [];

export function GuildDialog({
  onClose,
  title,
  icon,
  onIconError,
  subtitle,
  children,
  footer,
  placement = "top",
  width = "md",
  initialFocus,
  testId,
}: GuildDialogProps) {
  const top = placement === "top";
  const dialogRef = useRef<HTMLDivElement>(null);
  const titleId = useId();
  const onCloseRef = useRef(onClose);
  onCloseRef.current = onClose;
  const focusRef = useRef(initialFocus);

  useEffect(() => {
    const token = Symbol("dialog");
    stack.push(token);
    const opener = document.activeElement as HTMLElement | null;
    const dialog = dialogRef.current;
    const first =
      (focusRef.current ? dialog?.querySelector<HTMLElement>(focusRef.current) : null) ??
      dialog?.querySelector<HTMLElement>(FOCUSABLE);
    (first ?? dialog)?.focus();
    const html = document.documentElement;
    const prevOverflow = html.style.overflow;
    html.style.overflow = "hidden";

    const onKey = (e: KeyboardEvent) => {
      if (stack[stack.length - 1] !== token || !dialog) return;
      if (e.key === "Escape") {
        e.preventDefault();
        e.stopPropagation();
        onCloseRef.current();
        return;
      }
      if (e.key !== "Tab") return;
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
      const at = stack.indexOf(token);
      if (at >= 0) stack.splice(at, 1);
      html.style.overflow = prevOverflow;
      opener?.focus?.();
    };
  }, []);

  return createPortal(
    <div className="fixed inset-0 z-[60]" data-testid={testId}>
      <div
        aria-hidden="true"
        data-testid={testId ? `${testId}-backdrop` : undefined}
        className={cx("absolute inset-0", placement === "top" ? "bg-black/60" : "bg-black/45")}
        onMouseDown={() => onClose()}
      />
      <div
        ref={dialogRef}
        role="dialog"
        aria-modal="true"
        aria-labelledby={titleId}
        tabIndex={-1}
        className={cx(
          "absolute left-1/2 flex -translate-x-1/2 flex-col overflow-hidden border border-border outline-none",
          // The details dialog is the old .scraper-card (16px radius, 18px lower, no shadow); the
          // export dialogs are the old .export-modal card.
          top
            ? "top-[calc(var(--spacing-header)+8vh)] mt-[18px] max-h-[80vh] rounded-panel text-text"
            : "top-1/2 max-h-[min(84vh,860px)] -translate-y-1/2 rounded-[14px] text-dialog-text shadow-[0_20px_56px_var(--color-shadow-50)]",
          width === "md" ? "w-[min(720px,92vw)] bg-surface-deep" : "w-[min(480px,92vw)] bg-dialog-small",
        )}
      >
        <header
          className={cx(
            "flex shrink-0 items-center justify-between",
            top
              ? "px-4 py-3"
              : "border-b border-white/8 bg-linear-to-b from-accent/6 to-accent/0 px-[18px] py-3.5",
          )}
        >
          <div className={cx("scraper-title-wrap min-w-0", top ? "flex items-center gap-2.5" : "grid gap-1")}>
            {icon ? (
              <img src={icon} alt="" className="scraper-icon size-6 shrink-0 rounded-md object-cover" onError={onIconError} />
            ) : null}
            <h2
              id={titleId}
              className={cx(
                "scraper-title m-0 min-w-0 truncate text-text",
                top ? "text-[22px] font-bold" : "font-[system-ui,-apple-system,'Segoe_UI',Roboto,sans-serif] text-lg leading-[1.25] font-semibold",
              )}
            >
              {title}
            </h2>
            {subtitle ? <p className="m-0 text-xs text-muted opacity-80">{subtitle}</p> : null}
          </div>
          <button type="button" aria-label="Close" onClick={onClose} className={`verify-close ${MODAL_CLOSE}`}>
            <span aria-hidden="true">✕</span>
          </button>
        </header>
        <div
          className={cx(
            "scraper-body min-h-0 flex-1 overflow-auto",
            top ? "px-4 py-3.5" : "px-[18px] py-3.5 [scrollbar-gutter:stable]",
          )}
        >
          {children}
        </div>
        {footer ? (
          <footer
            className={cx(
              "scraper-actions flex shrink-0 justify-end gap-2",
              top ? "px-4 py-3" : "border-t border-white/8 bg-linear-to-t from-white/2 to-white/0 px-4 pt-3 pb-4",
            )}
          >
            {footer}
          </footer>
        ) : null}
      </div>
    </div>,
    document.body,
  );
}
