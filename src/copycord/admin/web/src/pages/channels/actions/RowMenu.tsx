// The "⋯" button of a channel card or category chip and its popup menu
// (the old #ch-menu). The menu is portaled and placed under the button.

import { useEffect, useLayoutEffect, useRef, useState } from "react";
import { createPortal } from "react-dom";
import { cx } from "../../../components/ui";
import { CATEGORY_MENU_BTN, CHANNEL_MENU_BTN } from "../treeClasses";

export interface MenuItem {
  /** The old data-act value. */
  act: string;
  /** Optional element id. */
  id?: string;
  label: string;
  onSelect: () => void;
  danger?: boolean;
  /** A hairline above the item, setting it apart from the ones before. */
  separator?: boolean;
}

export interface RowMenuProps {
  kind: "channel" | "category";
  items: MenuItem[];
  /** The button's accessible name; "Channel menu" or "Category menu" by default. */
  label?: string;
  /** Inert, as in a loading skeleton. */
  disabled?: boolean;
}

const GAP = 6;
const PAD = 12;

export function RowMenu({ kind, items, label, disabled = false }: RowMenuProps) {
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
        aria-label={label ?? (kind === "channel" ? "Channel menu" : "Category menu")}
        disabled={disabled}
        onClick={toggle}
        className={cx(
          "cursor-pointer outline-none",
          kind === "channel"
            ? // .icon-btn.ch-menu-btn
              cx("ch-menu-btn", CHANNEL_MENU_BTN)
            : // .cat-menu-trigger
              cx("cat-menu-trigger", CATEGORY_MENU_BTN),
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
              className="z-50 max-h-[calc(100vh-24px)] min-w-[200px] overflow-y-auto rounded-control border border-border bg-control p-1.5 text-text shadow-popover outline-none"
            >
              {items.map((it) => [
                it.separator ? <div key={`${it.act}:sep`} role="separator" className="my-1 h-px bg-border" /> : null,
                <button
                  key={it.act}
                  id={it.id}
                  type="button"
                  role="menuitem"
                  data-act={it.act}
                  onClick={() => {
                    setOpen(false);
                    it.onSelect();
                  }}
                  className={cx(
                    // .ctxmenu-item
                    "block w-full cursor-pointer rounded-lg border-0 bg-transparent px-2.5 py-2 text-left outline-none hover:bg-card-solid focus:bg-card-solid",
                    it.danger ? "text-danger" : "text-text",
                  )}
                >
                  {it.label}
                </button>,
              ])}
            </div>,
            document.body,
          )
        : null}
    </>
  );
}
