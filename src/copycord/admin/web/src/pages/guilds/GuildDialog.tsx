// Dialog frame of the old guild modals (.scraper-card): icon and accent title
// in the header, a close X, body and footer. Kept local because the shared
// Modal has no icon, subtitle, width or stacking: the export dialog opens the
// attachment types dialog on top of itself, and only the top one answers
// Escape and Tab. Focus moves in on open and back to the opener on close.

import { useEffect, useId, useRef } from "react";
import type { ReactNode } from "react";
import { createPortal } from "react-dom";
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
          "absolute left-1/2 flex -translate-x-1/2 flex-col overflow-hidden rounded-[14px] border border-border",
          "text-text shadow-[0_20px_56px_rgba(0,0,0,0.5),inset_0_0_0_1px_rgba(167,139,250,0.06)] outline-none",
          placement === "top"
            ? "top-[calc(var(--spacing-header)+8vh)] max-h-[80vh]"
            : "top-1/2 max-h-[min(84vh,860px)] -translate-y-1/2",
          width === "md" ? "w-[min(720px,92vw)] bg-[#0f0820]" : "w-[min(480px,92vw)] bg-[#160c30]",
        )}
      >
        <header className="flex items-center justify-between gap-3 border-b border-border bg-linear-to-b from-accent/6 to-transparent px-4 py-3">
          <div className="flex min-w-0 items-center gap-2.5">
            {icon ? (
              <img src={icon} alt="" className="size-6 shrink-0 rounded-md object-cover" onError={onIconError} />
            ) : null}
            <div className="grid min-w-0 gap-1">
              <h2 id={titleId} className="truncate text-base font-semibold text-accent">
                {title}
              </h2>
              {subtitle ? <p className="text-xs text-muted">{subtitle}</p> : null}
            </div>
          </div>
          <button
            type="button"
            aria-label="Close"
            onClick={onClose}
            className="grid size-8 shrink-0 cursor-pointer place-items-center rounded-lg text-text-1 outline-none hover:bg-white/6 focus-visible:ring-2 focus-visible:ring-accent/60"
          >
            <span aria-hidden="true">✕</span>
          </button>
        </header>
        <div className="min-h-0 flex-1 overflow-auto px-4 py-3.5 [scrollbar-color:var(--color-accent)_transparent] [scrollbar-width:thin]">
          {children}
        </div>
        {footer ? (
          <footer className="flex justify-end gap-2 border-t border-border bg-linear-to-t from-accent/6 to-transparent px-4 py-3">
            {footer}
          </footer>
        ) : null}
      </div>
    </div>,
    document.body,
  );
}
