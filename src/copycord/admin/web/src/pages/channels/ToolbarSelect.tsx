// The old enhanced select of the channels toolbar (.dd from app.js initDropdown):
// the native select stays in the DOM, hidden, and keeps its id and value; a
// toggle button shows the chosen label with a caret and opens a listbox.
// The wrappers are spans so the toolbar header stays the nearest block around the select.
// The field variant (FieldSelect in components/ui/Field.tsx) is the same dropdown as a
// full width form control: the toggle is the combobox and carries the id, so a Field label
// names it, and there is no native select. Its open list is portalled to the body and fixed
// under the toggle (or above it when there is no room below), so a Modal body never grows a
// scrollbar or clips it; a page scroll closes it. A disabled option with an empty value is the
// placeholder: the toggle shows it muted and the list leaves it out.
// A searchable field draws the toggle as a text input with the same look: closed it shows the
// chosen label, focused it empties and opens the list, and typing keeps the options whose label or
// value holds the text. Escape closes and keeps the old value, Tab and a click outside close.
// While the options load the same input stands there read only, its label shimmering, so the box
// a user clicked or focused stays the one they get once the list lands.

import { useEffect, useLayoutEffect, useRef, useState } from "react";
import type { CSSProperties, KeyboardEvent } from "react";
import { createPortal } from "react-dom";
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
  /** Field variant only: the toggle is a text input that filters the options by label or value. */
  searchable?: boolean;
  "aria-describedby"?: string;
  "aria-invalid"?: boolean;
}

const TOOLBAR_TOGGLE =
  "inline-block min-h-[34px] rounded-btn border border-border-input bg-control py-2 pr-9 pl-2.5 " +
  "font-sans text-[13px] text-text outline-none transition-[border,box-shadow,background] duration-200 " +
  "focus-visible:border-accent disabled:cursor-not-allowed disabled:opacity-60";

/** Tallest the floating field list gets; less when the window has no room for it. */
const FIELD_LIST_MAX = 260;
/** Below this much room under the toggle the field list opens upward if there is more room above. */
const FIELD_LIST_FLIP = 160;
const FIELD_LIST_GAP = 4;
const VIEWPORT_MARGIN = 8;

const isPlaceholder = (o: ToolbarOption) => Boolean(o.disabled) && o.value === "";

/** Fixed box for the field list: the toggle's left and width, under it or flipped above it. */
function placeList(toggle: HTMLElement): CSSProperties {
  const r = toggle.getBoundingClientRect();
  const vh = window.innerHeight;
  const below = vh - r.bottom - FIELD_LIST_GAP - VIEWPORT_MARGIN;
  const above = r.top - FIELD_LIST_GAP - VIEWPORT_MARGIN;
  const up = below < FIELD_LIST_FLIP && above > below;
  const maxHeight = Math.max(80, Math.min(FIELD_LIST_MAX, up ? above : below));
  return {
    position: "fixed",
    left: r.left,
    width: r.width,
    maxHeight,
    ...(up ? { bottom: vh - r.top + FIELD_LIST_GAP } : { top: r.bottom + FIELD_LIST_GAP }),
  };
}

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
  searchable,
  ...aria
}: ToolbarSelectProps) {
  const field = variant === "field";
  const search = field && Boolean(searchable);
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState("");
  const [focus, setFocus] = useState(0);
  const [place, setPlace] = useState<CSSProperties | null>(null);
  const wrap = useRef<HTMLSpanElement>(null);
  const toggle = useRef<HTMLButtonElement & HTMLInputElement>(null);
  const list = useRef<HTMLSpanElement>(null);
  const listId = `${id}-listbox`;
  const selected = Math.max(
    0,
    options.findIndex((o) => o.value === value),
  );
  // Indexes into options of the rows the list shows, and of the ones the keys and clicks can pick.
  const q = search && open ? query.trim().toLocaleLowerCase() : "";
  const matches = (o: ToolbarOption) => !q || o.label.toLocaleLowerCase().includes(q) || o.value.toLocaleLowerCase().includes(q);
  const shown = options.flatMap((o, i) => ((field && isPlaceholder(o)) || !matches(o) ? [] : [i]));
  const pickable = shown.filter((i) => !options[i]!.disabled);
  const current = options[selected];
  const placeholderShown = field && current !== undefined && isPlaceholder(current);

  useEffect(() => {
    if (!open) return;
    const onDown = (e: MouseEvent) => {
      const t = e.target as Node;
      if (!wrap.current?.contains(t) && !list.current?.contains(t)) setOpen(false);
    };
    document.addEventListener("mousedown", onDown);
    return () => document.removeEventListener("mousedown", onDown);
  }, [open]);

  // The floating field list is placed from the toggle before paint; a scroll anywhere but in the
  // list itself, or a resize, would leave it hanging away from the toggle, so it closes.
  useLayoutEffect(() => {
    if (!open || !field || !toggle.current) {
      setPlace(null);
      return;
    }
    setPlace(placeList(toggle.current));
    const onScroll = (e: Event) => {
      if (e.target instanceof Node && list.current?.contains(e.target)) return;
      setOpen(false);
    };
    const onResize = () => setOpen(false);
    window.addEventListener("scroll", onScroll, true);
    window.addEventListener("resize", onResize);
    return () => {
      window.removeEventListener("scroll", onScroll, true);
      window.removeEventListener("resize", onResize);
    };
  }, [open, field]);

  useEffect(() => {
    if (open) list.current?.querySelector(`[data-idx="${focus}"]`)?.scrollIntoView?.({ block: "nearest" });
  }, [open, focus, place]);

  useEffect(() => {
    if (!open) setQuery("");
  }, [open]);

  const show = () => {
    if (disabled) return;
    setFocus(pickable.includes(selected) ? selected : (pickable[0] ?? -1));
    setOpen(true);
  };
  const choose = (i: number) => {
    const o = options[i];
    if (!o || o.disabled) return;
    setOpen(false);
    setQuery("");
    // Like the old dropdown, picking the current option fires change again (Name twice flips the direction);
    // the field variant acts like a native select and stays quiet.
    if (!field || o.value !== value) onChange(o.value);
  };
  // Steps over disabled rows: the focus only lands on what Enter can pick.
  const step = (by: number) => {
    if (pickable.length === 0) return;
    const at = pickable.indexOf(focus);
    const next = at < 0 ? (by > 0 ? 0 : pickable.length - 1) : Math.min(pickable.length - 1, Math.max(0, at + by));
    setFocus(pickable[next]!);
  };

  /** Typing filters the list and moves the highlight to the first row that is left. */
  const onType = (text: string) => {
    setQuery(text);
    if (!open) setOpen(true);
    const t = text.trim().toLocaleLowerCase();
    const first = options.findIndex(
      (o) => !o.disabled && (!t || o.label.toLocaleLowerCase().includes(t) || o.value.toLocaleLowerCase().includes(t)),
    );
    setFocus(first);
  };

  const onKey = (e: KeyboardEvent<HTMLElement>) => {
    if (disabled) return;
    switch (e.key) {
      case "ArrowDown":
      case "ArrowUp":
        e.preventDefault();
        if (!open) show();
        else step(e.key === "ArrowDown" ? 1 : -1);
        break;
      case "Home":
      case "End":
        e.preventDefault();
        if (!open) show();
        if (pickable.length) setFocus(pickable[e.key === "Home" ? 0 : pickable.length - 1]!);
        break;
      case "Tab":
        if (open) setOpen(false);
        break;
      case "Enter":
      case " ":
        // A space is text in the search box.
        if (search && e.key === " ") break;
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

  const listbox = (
    <span
      ref={list}
      id={listId}
      role="listbox"
      aria-label={label}
      aria-labelledby={label ? undefined : id}
      style={field ? (place ?? undefined) : undefined}
      className={cx(
        field
          ? "z-[70] block overflow-x-hidden overflow-y-auto overscroll-contain"
          : "absolute inset-x-0 top-[calc(100%+4px)] z-[55] block max-h-[180px] overflow-x-hidden overflow-y-auto",
        "rounded-control border border-border bg-surface-deep p-1 shadow-popover",
      )}
    >
      {shown.length === 0 ? (
        <span className="block px-2.5 py-2 text-[13px] leading-[1.3] text-muted">
          {q ? "No matches" : options.find(isPlaceholder)?.label || "Nothing to pick"}
        </span>
      ) : null}
      {shown.map((i) => {
        const o = options[i]!;
        return (
          <span
            key={o.value}
            id={`${listId}-${i}`}
            role="option"
            data-idx={i}
            aria-selected={i === selected && !o.disabled}
            aria-disabled={o.disabled || undefined}
            onMouseDown={(e) => e.preventDefault()}
            onClick={() => choose(i)}
            onMouseEnter={() => !o.disabled && setFocus(i)}
            className={cx(
              "block rounded-lg px-2.5 py-2 text-[13px] leading-[1.3] break-words select-none [overflow-wrap:anywhere]",
              "transition-colors duration-150",
              o.disabled
                ? "cursor-default text-muted opacity-70"
                : i === selected
                  ? "cursor-pointer bg-linear-to-b from-primary to-primary-700 text-white"
                  : i === focus
                    ? "cursor-pointer bg-accent-a12 text-text"
                    : "cursor-pointer text-text",
            )}
          >
            {o.label}
          </span>
        );
      })}
    </span>
  );

  return (
    <span
      ref={wrap}
      data-disabled={disabled || undefined}
      aria-busy={(search && pending) || undefined}
      className={cx("relative", field ? "block w-full" : "inline-block", className)}
    >
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
      {search ? (
        <>
          <input
            ref={toggle}
            type="text"
            id={id}
            role="combobox"
            autoComplete="off"
            spellCheck={false}
            aria-autocomplete="list"
            aria-haspopup="listbox"
            aria-expanded={open}
            aria-controls={listId}
            aria-activedescendant={open && focus >= 0 && shown.includes(focus) ? `${listId}-${focus}` : undefined}
            {...aria}
            disabled={disabled}
            readOnly={pending}
            value={open && !pending ? query : pending || placeholderShown ? "" : (current?.label ?? "")}
            placeholder={pending ? undefined : open ? (placeholderShown ? "Type to search" : current?.label) : current?.label}
            onFocus={() => !open && show()}
            onClick={() => !open && show()}
            onChange={(e) => onType(e.target.value)}
            onKeyDown={onKey}
            className={cx("relative block w-full truncate text-left", toggleClassName ?? TOOLBAR_TOGGLE, "pr-[42px]")}
          />
          {pending ? (
            <span className="pointer-events-none absolute inset-y-0 left-3.5 flex items-center text-sm">
              <Skeleton.Text>{pendingLabel}</Skeleton.Text>
            </span>
          ) : null}
          <span
            aria-hidden
            className={cx("pointer-events-none absolute top-1/2 right-3.5 -translate-y-1/2 text-sm leading-none text-accent", open && "rotate-180")}
          >
            ▾
          </span>
        </>
      ) : (
        <button
          ref={toggle}
          type="button"
          id={field ? id : undefined}
          role={field ? "combobox" : undefined}
          aria-haspopup="listbox"
          aria-expanded={open}
          aria-controls={listId}
          aria-activedescendant={field && open && focus >= 0 ? `${listId}-${focus}` : undefined}
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
            <span className={cx(field && "block truncate", placeholderShown && "text-muted")}>
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
      )}
      {open && !field ? listbox : null}
      {open && field && place ? createPortal(listbox, document.body) : null}
    </span>
  );
}
