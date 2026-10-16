// The Columns menu: a checkbox per column, saved by the page. Closes on Escape and outside clicks.

import { useEffect, useId, useRef, useState } from "react";
import { Checkbox } from "../../components/ui/Checkbox";
import { MessagesIcon } from "./icons";
import { COLUMNS_BTN } from "./MessagesSkeleton";
import { COLUMNS } from "./model";
import type { ColumnKey } from "./model";

export function ColumnsMenu({
  columns,
  onToggle,
  onReset,
}: {
  columns: readonly ColumnKey[];
  onToggle: (key: ColumnKey) => void;
  onReset: () => void;
}) {
  const [open, setOpen] = useState(false);
  const boxRef = useRef<HTMLDivElement>(null);
  const btnRef = useRef<HTMLButtonElement>(null);
  const menuId = useId();
  const on = new Set(columns);

  useEffect(() => {
    if (!open) return;
    const onDown = (e: MouseEvent) => {
      if (!boxRef.current?.contains(e.target as Node)) setOpen(false);
    };
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") {
        setOpen(false);
        btnRef.current?.focus();
      }
    };
    document.addEventListener("mousedown", onDown);
    document.addEventListener("keydown", onKey);
    return () => {
      document.removeEventListener("mousedown", onDown);
      document.removeEventListener("keydown", onKey);
    };
  }, [open]);

  return (
    <div ref={boxRef} className="relative max-md:w-full">
      <button
        ref={btnRef}
        type="button"
        id="messages-columns"
        className={`${COLUMNS_BTN} max-md:w-full`}
        aria-expanded={open}
        aria-controls={open ? menuId : undefined}
        onClick={() => setOpen((o) => !o)}
      >
        <MessagesIcon name="columns" size={15} />
        Columns
      </button>
      {open ? (
        <div
          id={menuId}
          role="group"
          aria-label="Visible columns"
          className="absolute top-[calc(100%+6px)] right-0 z-30 w-[240px] rounded-xl border border-border bg-surface-deep p-2 shadow-[0_16px_40px_var(--color-shadow-50)] max-md:left-0 max-md:w-full"
        >
          <ul className="m-0 flex list-none flex-col p-0">
            {COLUMNS.map((c) => {
              const checked = on.has(c.key);
              const last = checked && on.size === 1;
              return (
                <li key={c.key}>
                  <label className="flex cursor-pointer items-center gap-2.5 rounded-md px-2 py-1.5 text-[13px] text-text hover:bg-white/5">
                    <Checkbox
                      checked={checked}
                      disabled={last}
                      title={last ? "At least one column stays visible" : undefined}
                      onChange={() => onToggle(c.key)}
                    />
                    {c.label}
                  </label>
                </li>
              );
            })}
          </ul>
          <div className="mt-1 border-t border-t-border pt-1.5">
            <button
              type="button"
              className="w-full cursor-pointer rounded-md px-2 py-1.5 text-left text-xs text-muted hover:bg-white/5 hover:text-text"
              onClick={onReset}
            >
              Restore default columns
            </button>
          </div>
        </div>
      ) : null}
    </div>
  );
}
