// The old enhanced select of the channels toolbar (.dd from app.js initDropdown):
// the native select stays in the DOM, hidden, and keeps its id and value; a
// toggle button shows the chosen label with a caret and opens a listbox.
// The wrappers are spans so the toolbar header stays the nearest block around the select.

import { useEffect, useRef, useState } from "react";
import type { KeyboardEvent } from "react";
import { cx } from "../../components/ui";

export interface ToolbarOption {
  value: string;
  label: string;
  disabled?: boolean;
}

export interface ToolbarSelectProps {
  id: string;
  label: string;
  value: string;
  options: readonly ToolbarOption[];
  onChange: (value: string) => void;
  disabled?: boolean;
  /** Takes the native select out of the layout, as the old page did until the status gate opened. */
  nativeHidden?: boolean;
}

export function ToolbarSelect({ id, label, value, options, onChange, disabled, nativeHidden }: ToolbarSelectProps) {
  const [open, setOpen] = useState(false);
  const [focus, setFocus] = useState(0);
  const wrap = useRef<HTMLSpanElement>(null);
  const listId = `${id}-listbox`;
  const selected = Math.max(
    0,
    options.findIndex((o) => o.value === value),
  );

  useEffect(() => {
    if (!open) return;
    const onDown = (e: MouseEvent) => {
      if (!wrap.current?.contains(e.target as Node)) setOpen(false);
    };
    document.addEventListener("mousedown", onDown);
    return () => document.removeEventListener("mousedown", onDown);
  }, [open]);

  useEffect(() => {
    if (open) wrap.current?.querySelector(`[data-idx="${focus}"]`)?.scrollIntoView?.({ block: "nearest" });
  }, [open, focus]);

  const show = () => {
    if (disabled) return;
    setFocus(selected);
    setOpen(true);
  };
  const choose = (i: number) => {
    const o = options[i];
    if (!o || o.disabled) return;
    setOpen(false);
    // Like the old dropdown, picking the current option fires change again (Name twice flips the direction).
    onChange(o.value);
  };
  const move = (i: number) => setFocus(Math.min(options.length - 1, Math.max(0, i)));

  const onKey = (e: KeyboardEvent<HTMLButtonElement>) => {
    if (disabled) return;
    switch (e.key) {
      case "ArrowDown":
      case "ArrowUp":
        e.preventDefault();
        if (!open) show();
        else move(focus + (e.key === "ArrowDown" ? 1 : -1));
        break;
      case "Home":
      case "End":
        e.preventDefault();
        if (!open) show();
        move(e.key === "Home" ? 0 : options.length - 1);
        break;
      case "Enter":
      case " ":
        e.preventDefault();
        if (!open) show();
        else choose(focus);
        break;
      case "Escape":
        if (open) {
          e.preventDefault();
          setOpen(false);
        }
        break;
    }
  };

  return (
    <span ref={wrap} data-disabled={disabled || undefined} className="relative inline-block">
      <select
        id={id}
        aria-label={label}
        aria-hidden
        tabIndex={-1}
        value={value}
        disabled={disabled}
        hidden={nativeHidden}
        onChange={(e) => onChange(e.target.value)}
        className="absolute top-0 left-0 size-0 border-0 bg-[#efefef] p-0 text-[13px] text-black opacity-0"
      >
        {options.map((o) => (
          <option key={o.value} value={o.value} disabled={o.disabled}>
            {o.label}
          </option>
        ))}
      </select>
      <button
        type="button"
        aria-haspopup="listbox"
        aria-expanded={open}
        aria-controls={listId}
        disabled={disabled}
        onClick={() => (open ? setOpen(false) : show())}
        onKeyDown={onKey}
        className={cx(
          "relative inline-block min-h-[34px] w-full cursor-pointer rounded-btn border border-border bg-control py-2 pr-9 pl-2.5",
          "text-left font-sans text-[13px] text-text outline-none transition-[border,box-shadow,background] duration-200",
          "focus-visible:border-accent focus-visible:shadow-[0_20px_56px_rgba(0,0,0,0.5)] disabled:cursor-not-allowed disabled:opacity-60",
        )}
      >
        <span>{options[selected]?.label ?? ""}</span>
        <span
          aria-hidden
          className={cx(
            "absolute top-1/2 right-2.5 -translate-y-1/2 text-sm leading-none text-accent",
            open && "rotate-180",
          )}
        >
          ▾
        </span>
      </button>
      {open ? (
        <span
          id={listId}
          role="listbox"
          aria-label={label}
          className={cx(
            "absolute inset-x-0 top-[calc(100%+4px)] z-[55] block max-h-[180px] overflow-x-hidden overflow-y-auto",
            "rounded-control border border-border bg-[#0f0820] p-1 shadow-[0_4px_12px_rgba(0,0,0,0.25),0_14px_50px_rgba(0,0,0,0.45)]",
          )}
        >
          {options.map((o, i) => (
            <span
              key={o.value}
              role="option"
              data-idx={i}
              aria-selected={i === selected}
              aria-disabled={o.disabled || undefined}
              onMouseDown={(e) => e.preventDefault()}
              onClick={() => choose(i)}
              onMouseEnter={() => setFocus(i)}
              className={cx(
                "block cursor-pointer rounded-lg px-2.5 py-2 text-[13px] leading-[1.3] break-words select-none [overflow-wrap:anywhere]",
                "transition-colors duration-150",
                i === selected
                  ? "bg-linear-to-b from-primary to-primary-700 text-white"
                  : i === focus
                    ? "bg-[rgba(167,139,250,0.12)] text-text"
                    : "text-text",
              )}
            >
              {o.label}
            </span>
          ))}
        </span>
      ) : null}
    </span>
  );
}
