// On/off switch: a button with role="switch", so Space and Enter toggle it.

import { useId } from "react";
import type { ReactNode } from "react";
import { cx } from "./cx";

export interface ToggleProps {
  checked: boolean;
  onChange: (checked: boolean) => void;
  label: ReactNode;
  /** Hide the label visually but keep it as the accessible name. */
  hideLabel?: boolean;
  description?: ReactNode;
  disabled?: boolean;
  id?: string;
  className?: string;
}

export function Toggle({ checked, onChange, label, hideLabel, description, disabled, id, className }: ToggleProps) {
  const auto = useId();
  const switchId = id ?? auto;
  const labelId = `${switchId}-label`;
  const descId = description ? `${switchId}-desc` : undefined;
  return (
    <div className={cx("flex items-center justify-between gap-3", className)}>
      <div className={cx("flex flex-col", hideLabel && "sr-only")}>
        <span id={labelId} className="text-sm font-semibold text-text-1">
          {label}
        </span>
        {description ? (
          <span id={descId} className="text-xs text-text-2">
            {description}
          </span>
        ) : null}
      </div>
      <button
        id={switchId}
        type="button"
        role="switch"
        aria-checked={checked}
        aria-labelledby={labelId}
        aria-describedby={descId}
        disabled={disabled}
        onClick={() => onChange(!checked)}
        className={cx(
          "relative inline-flex h-6 w-11 shrink-0 cursor-pointer items-center rounded-full border transition-colors outline-none",
          "focus-visible:ring-2 focus-visible:ring-accent/60 disabled:cursor-not-allowed disabled:opacity-55",
          checked ? "border-primary-600 bg-primary" : "border-border bg-bg2",
        )}
      >
        <span
          aria-hidden="true"
          className={cx(
            "inline-block size-[18px] rounded-full bg-white shadow-soft transition-transform",
            checked ? "translate-x-[22px]" : "translate-x-[2px]",
          )}
        />
      </button>
    </div>
  );
}
