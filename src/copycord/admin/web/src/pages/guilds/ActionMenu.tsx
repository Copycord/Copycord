// The card menu (.action-menu.popover): opens next to the click, stays in
// the viewport and closes on an outside click, Escape, scroll or resize.
// Arrow keys move between the items.

import { useEffect, useLayoutEffect, useRef, useState } from "react";
import type { KeyboardEvent } from "react";
import { createPortal } from "react-dom";
import { placeMenu } from "./model";
import type { Point } from "./model";

export interface MenuItem {
  key: string;
  label: string;
  onSelect: () => void;
}

export interface ActionMenuProps {
  at: Point;
  anchor: HTMLElement;
  items: MenuItem[];
  onClose: () => void;
  label: string;
}

export function ActionMenu({ at, anchor, items, onClose, label }: ActionMenuProps) {
  const ref = useRef<HTMLDivElement>(null);
  const [pos, setPos] = useState<Point | null>(null);
  const onCloseRef = useRef(onClose);
  onCloseRef.current = onClose;

  useLayoutEffect(() => {
    const el = ref.current;
    if (!el) return;
    const r = el.getBoundingClientRect();
    setPos(placeMenu(at, r, { width: window.innerWidth, height: window.innerHeight }));
  }, [at]);

  useEffect(() => {
    if (pos) ref.current?.querySelector<HTMLElement>('[role="menuitem"]')?.focus({ preventScroll: true });
  }, [pos]);

  useEffect(() => {
    const close = () => onCloseRef.current();
    const onDocClick = (e: MouseEvent) => {
      const t = e.target as Node | null;
      if (t && (ref.current?.contains(t) || anchor.contains(t))) return;
      close();
    };
    const onKey = (e: globalThis.KeyboardEvent) => {
      if (e.key !== "Escape") return;
      e.preventDefault();
      close();
      anchor.focus({ preventScroll: true });
    };
    document.addEventListener("click", onDocClick, true);
    document.addEventListener("keydown", onKey, true);
    window.addEventListener("scroll", close, true);
    window.addEventListener("resize", close);
    return () => {
      document.removeEventListener("click", onDocClick, true);
      document.removeEventListener("keydown", onKey, true);
      window.removeEventListener("scroll", close, true);
      window.removeEventListener("resize", close);
    };
  }, [anchor]);

  const onKeyDown = (e: KeyboardEvent<HTMLDivElement>) => {
    if (e.key === "Tab") {
      e.preventDefault();
      onClose();
      anchor.focus({ preventScroll: true });
      return;
    }
    if (e.key !== "ArrowDown" && e.key !== "ArrowUp" && e.key !== "Home" && e.key !== "End") return;
    e.preventDefault();
    const list = Array.from(ref.current?.querySelectorAll<HTMLElement>('[role="menuitem"]') ?? []);
    const at = list.indexOf(document.activeElement as HTMLElement);
    let next = 0;
    if (e.key === "End") next = list.length - 1;
    else if (e.key === "ArrowDown") next = (at + 1) % list.length;
    else if (e.key === "ArrowUp") next = (at - 1 + list.length) % list.length;
    list[next]?.focus();
  };

  return createPortal(
    <div
      ref={ref}
      role="menu"
      aria-label={label}
      onKeyDown={onKeyDown}
      style={{ left: pos?.x ?? 0, top: pos?.y ?? 0, visibility: pos ? "visible" : "hidden" }}
      className="fixed z-50 w-max max-w-[min(92vw,360px)] min-w-[180px] rounded-xl border border-border bg-control p-1.5"
    >
      {items.map((item) => (
        <button
          key={item.key}
          type="button"
          role="menuitem"
          tabIndex={-1}
          onClick={() => {
            onClose();
            item.onSelect();
          }}
          className="block w-full max-w-full cursor-pointer truncate rounded-lg px-3.5 py-2.5 text-left font-sans text-base text-text outline-none transition-colors duration-150 hover:bg-menu-item-hover focus-visible:bg-menu-item-hover"
        >
          {item.label}
        </button>
      ))}
    </div>,
    document.body,
  );
}
