// The "⋯" button of a channel card or category chip and its popup menu
// (the old #ch-menu). The menu is portaled and placed under the button.

import { useEffect, useLayoutEffect, useRef, useState } from "react";
import { createPortal } from "react-dom";
import { cx } from "../../../components/ui";

export interface MenuItem {
  /** The old data-act value. */
  act: string;
  label: string;
  onSelect: () => void;
  danger?: boolean;
}

export interface RowMenuProps {
  kind: "channel" | "category";
  items: MenuItem[];
}

const GAP = 6;
const PAD = 12;

export function RowMenu({ kind, items }: RowMenuProps) {
  const [open, setOpen] = useState(false);
  const [pos, setPos] = useState<{ top: number; left: number } | null>(null);
  const btn = useRef<HTMLButtonElement>(null);
  const menu = useRef<HTMLDivElement>(null);

  useLayoutEffect(() => {
    if (!open || !btn.current || !menu.current) return;
    const r = btn.current.getBoundingClientRect();
    const mw = menu.current.offsetWidth;
    const mh = menu.current.offsetHeight;
    let top = r.bottom + GAP;
    if (window.innerHeight - r.bottom < mh && r.top > window.innerHeight - r.bottom) top = r.top - GAP - mh;
    top = Math.max(PAD, Math.min(top, window.innerHeight - mh - PAD));
    const left = Math.max(PAD, Math.min(r.left, window.innerWidth - mw - PAD));
    setPos({ top: Math.round(top), left: Math.round(left) });
    menu.current.focus({ preventScroll: true });
  }, [open]);

  useEffect(() => {
    if (!open) return;
    const close = () => setOpen(false);
    const onDown = (e: MouseEvent) => {
      const t = e.target as Node;
      if (menu.current?.contains(t) || btn.current?.contains(t)) return;
      close();
    };
    const onKey = (e: KeyboardEvent) => {
      if (e.key !== "Escape") return;
      e.stopPropagation();
      close();
      btn.current?.focus();
    };
    document.addEventListener("mousedown", onDown);
    document.addEventListener("keydown", onKey);
    window.addEventListener("resize", close);
    window.addEventListener("scroll", close, true);
    return () => {
      document.removeEventListener("mousedown", onDown);
      document.removeEventListener("keydown", onKey);
      window.removeEventListener("resize", close);
      window.removeEventListener("scroll", close, true);
    };
  }, [open]);

  const toggle = () => {
    setPos(null);
    setOpen((o) => !o);
  };

  return (
    <>
      <button
        ref={btn}
        type="button"
        aria-haspopup="menu"
        aria-expanded={open}
        aria-label={kind === "channel" ? "Channel menu" : "Category menu"}
        onClick={toggle}
        className={cx(
          kind === "channel" ? "ch-menu-btn" : "cat-menu-trigger ml-1",
          "inline-flex size-6 cursor-pointer items-center justify-center rounded-md border-0 bg-transparent",
          "text-inherit hover:bg-white/10 focus-visible:ring-2 focus-visible:ring-accent/60 outline-none",
        )}
      >
        ⋯
      </button>
      {open
        ? createPortal(
            <div
              ref={menu}
              id="ch-menu"
              role="menu"
              tabIndex={-1}
              style={{ position: "fixed", top: pos?.top ?? -9999, left: pos?.left ?? -9999 }}
              className="z-50 flex min-w-44 flex-col gap-0.5 rounded-panel border border-border bg-card-solid p-1.5 text-sm text-text shadow-card outline-none"
            >
              {items.map((it) => (
                <button
                  key={it.act}
                  type="button"
                  role="menuitem"
                  data-act={it.act}
                  onClick={() => {
                    setOpen(false);
                    it.onSelect();
                  }}
                  className={cx(
                    "cursor-pointer rounded-md border-0 bg-transparent px-3 py-1.5 text-left hover:bg-white/8",
                    it.danger ? "text-danger" : "text-text",
                  )}
                >
                  {it.label}
                </button>
              ))}
            </div>,
            document.body,
          )
        : null}
    </>
  );
}
