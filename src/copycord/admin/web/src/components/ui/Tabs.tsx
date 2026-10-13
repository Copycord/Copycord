// WAI-ARIA tabs with automatic activation: Left/Right move and select,
// Home/End jump to the ends, disabled tabs are skipped.

import { useId, useRef, useState } from "react";
import type { KeyboardEvent, ReactNode } from "react";
import { cx } from "./cx";

export interface TabItem {
  id: string;
  label: ReactNode;
  content: ReactNode;
  disabled?: boolean;
}

export interface TabsProps {
  tabs: TabItem[];
  value?: string;
  defaultValue?: string;
  onChange?: (id: string) => void;
  "aria-label"?: string;
  className?: string;
}

export function Tabs({ tabs, value, defaultValue, onChange, className, ...rest }: TabsProps) {
  const base = useId();
  const [inner, setInner] = useState(defaultValue ?? tabs.find((t) => !t.disabled)?.id ?? "");
  const current = value ?? inner;
  const refs = useRef<Record<string, HTMLButtonElement | null>>({});

  const select = (id: string) => {
    if (value === undefined) setInner(id);
    onChange?.(id);
    refs.current[id]?.focus();
  };

  const onKeyDown = (e: KeyboardEvent<HTMLDivElement>) => {
    const enabled = tabs.filter((t) => !t.disabled);
    const ix = enabled.findIndex((t) => t.id === current);
    let next: TabItem | undefined;
    if (e.key === "ArrowRight") next = enabled[(ix + 1) % enabled.length];
    else if (e.key === "ArrowLeft") next = enabled[(ix - 1 + enabled.length) % enabled.length];
    else if (e.key === "Home") next = enabled[0];
    else if (e.key === "End") next = enabled[enabled.length - 1];
    if (!next) return;
    e.preventDefault();
    select(next.id);
  };

  const active = tabs.find((t) => t.id === current);

  return (
    <div className={className}>
      <div
        role="tablist"
        aria-label={rest["aria-label"]}
        onKeyDown={onKeyDown}
        className="flex gap-1 overflow-x-auto border-b border-border"
      >
        {tabs.map((t) => {
          const selected = t.id === current;
          return (
            <button
              key={t.id}
              ref={(el) => {
                refs.current[t.id] = el;
              }}
              id={`${base}-tab-${t.id}`}
              type="button"
              role="tab"
              aria-selected={selected}
              aria-controls={`${base}-panel-${t.id}`}
              tabIndex={selected ? 0 : -1}
              disabled={t.disabled}
              onClick={() => select(t.id)}
              className={cx(
                "-mb-px cursor-pointer whitespace-nowrap border-b-2 px-4 py-2.5 text-sm font-semibold transition-colors outline-none",
                "focus-visible:bg-brand-ghost disabled:cursor-not-allowed disabled:opacity-50",
                selected ? "border-accent text-text" : "border-transparent text-text-2 hover:text-text",
              )}
            >
              {t.label}
            </button>
          );
        })}
      </div>
      {active ? (
        <div
          role="tabpanel"
          id={`${base}-panel-${active.id}`}
          aria-labelledby={`${base}-tab-${active.id}`}
          tabIndex={0}
          className="pt-4 outline-none"
        >
          {active.content}
        </div>
      ) : null}
    </div>
  );
}
