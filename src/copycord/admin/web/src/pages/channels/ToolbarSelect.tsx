// The old enhanced select of the channels toolbar (.dd from app.js initDropdown):
// the native select stays in the DOM, hidden, and keeps its id and value; a
// toggle button shows the chosen label with a caret and opens a listbox.
// The wrappers are spans so the toolbar header stays the nearest block around the select.
// The field variant (FieldSelect in components/ui/Field.tsx) is the same dropdown as a
// full width form control: the toggle is the combobox and carries the id, so a Field label
// names it, and there is no native select.

import { useEffect, useRef, useState } from "react";
import type { KeyboardEvent } from "react";
import { cx } from "../../components/ui/cx";
import { Skeleton } from "../../components/ui/Skeleton";

export interface ToolbarOption {
  value: string;
  label: string;
  disabled?: boolean;
}

export interface ToolbarSelectProps {
  id: string;
  /** Names the native select and the listbox; without it the listbox is labelled by the toggle. */
  label?: string;
  value: string;
  options: readonly ToolbarOption[];
  onChange: (value: string) => void;
  disabled?: boolean;
  /** Takes the native select out of the layout, as the old page did until the status gate opened. */
  nativeHidden?: boolean;
  /** The options are still loading: a bar stands where the chosen label goes. */
  pending?: boolean;
  /** Prints before the chosen label on the closed control ("Status" reads "Status: All"); the options stay plain. */
  prefix?: string;
  /** Text that sizes the pending bar, about as wide as the label that replaces it. */
  pendingLabel?: string;
  /** "field": block, full width, the toggle is a role combobox with the id and toggleClassName for its look. */
  variant?: "toolbar" | "field";
  /** Extra classes for the wrapper. */
  className?: string;
  /** Replaces the toolbar look of the toggle (the field variant passes the Field control look). */
  toggleClassName?: string;
  "aria-describedby"?: string;
  "aria-invalid"?: boolean;
}

const TOOLBAR_TOGGLE =
  "inline-block min-h-[34px] rounded-btn border border-border bg-control py-2 pr-9 pl-2.5 " +
  "font-sans text-[13px] text-text outline-none transition-[border,box-shadow,background] duration-200 " +
  "focus-visible:border-accent focus-visible:shadow-[0_20px_56px_var(--color-shadow-50)] disabled:cursor-not-allowed disabled:opacity-60";

export function ToolbarSelect({
  id,
  label,
  value,
  options,
  onChange,
  disabled,
  nativeHidden,
  pending,
  prefix,
  pendingLabel = "Guild mapping",
  variant = "toolbar",
  className,
  toggleClassName,
  ...aria
}: ToolbarSelectProps) {
  const field = variant === "field";
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
    // Like the old dropdown, picking the current option fires change again (Name twice flips the direction);
    // the field variant acts like a native select and stays quiet.
    if (!field || o.value !== value) onChange(o.value);
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
          // Only the list closes, not a Modal around it.
          e.preventDefault();
          e.stopPropagation();
          setOpen(false);
        }
        break;
    }
  };

  return (
    <span ref={wrap} data-disabled={disabled || undefined} className={cx("relative", field ? "block w-full" : "inline-block", className)}>
      {field ? null : (
        <select
          id={id}
          aria-label={label}
          aria-hidden
          tabIndex={-1}
          value={value}
          disabled={disabled}
          hidden={nativeHidden}
          onChange={(e) => onChange(e.target.value)}
          className="absolute top-0 left-0 size-0 border-0 bg-native-select p-0 text-[13px] text-black opacity-0"
        >
          {options.map((o) => (
            <option key={o.value} value={o.value} disabled={o.disabled}>
              {o.label}
            </option>
          ))}
        </select>
      )}
      <button
        type="button"
        id={field ? id : undefined}
        role={field ? "combobox" : undefined}
        aria-haspopup="listbox"
        aria-expanded={open}
        aria-controls={listId}
        aria-activedescendant={field && open ? `${listId}-${focus}` : undefined}
        {...aria}
        disabled={disabled}
        onClick={() => (open ? setOpen(false) : show())}
        onKeyDown={onKey}
        className={cx(
          "relative w-full cursor-pointer text-left",
          toggleClassName ?? TOOLBAR_TOGGLE,
          field && "block pr-[42px]",
        )}
      >
        {pending ? (
          <Skeleton.Text className="align-middle">{pendingLabel}</Skeleton.Text>
        ) : (
          <span className={cx(field && "block truncate")}>
            {prefix && options[selected] ? `${prefix}: ${options[selected].label}` : options[selected]?.label || (field ? "\u00a0" : "")}
          </span>
        )}
        <span
          aria-hidden
          className={cx(
            "absolute top-1/2 -translate-y-1/2 text-sm leading-none text-accent",
            field ? "right-3.5" : "right-2.5",
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
          aria-labelledby={label ? undefined : id}
          className={cx(
            "absolute inset-x-0 top-[calc(100%+4px)] z-[55] block max-h-[180px] overflow-x-hidden overflow-y-auto",
            "rounded-control border border-border bg-surface-deep p-1 shadow-[0_4px_12px_var(--color-shadow-25),0_14px_50px_var(--color-shadow-45)]",
          )}
        >
          {options.map((o, i) => (
            <span
              key={o.value}
              id={`${listId}-${i}`}
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
                    ? "bg-accent-a12 text-text"
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
