// Pieces of the #mapping-modal look shared by the mapping form, the user
// tokens section and the message features sub-modal: the uppercase purple
// section row with its round chevron, the bordered toggle row and the
// label.toggle switch (a hidden checkbox and a 38x22 track).

import type { ReactNode } from "react";
import { cx } from "../../../../components/ui";

/** details.mapping-section.mapping-settings-section; the last one drops its rule. */
export const SECTION =
  "mapping-section mapping-settings-section group/sec border-b border-border pt-3.5 pb-1.5 last-of-type:border-b-0";

export function SectionSummary({ children }: { children: ReactNode }) {
  return (
    <summary className="mapping-section-header group/sum flex cursor-pointer list-none items-center gap-2 rounded-md px-1 py-1.5 text-[11px] font-bold tracking-[0.6px] text-text-2 uppercase transition-colors duration-100 select-none hover:bg-white/[0.03] [&::-webkit-details-marker]:hidden">
      <span
        aria-hidden="true"
        className="mapping-section-chev inline-flex size-6 shrink-0 -rotate-90 items-center justify-center rounded-input border border-border bg-white/4 text-xs text-accent transition duration-200 group-open/sec:rotate-0 group-hover/sum:border-accent/30 group-hover/sum:bg-white/8"
      >
        ▾
      </span>
      <span>{children}</span>
    </summary>
  );
}

/** .mapping-toggle-desc */
export const DESC = "mapping-toggle-desc text-[11px] leading-[1.4] text-muted opacity-70";
/** .mapping-toggle-row */
export const ROW =
  "mapping-toggle-row mb-1.5 flex items-center justify-between gap-3.5 rounded-input border border-border bg-white/[0.02] px-2.5 py-2 transition-colors duration-100 hover:border-white/10 hover:bg-white/4";

export interface SwitchProps {
  id?: string;
  checked: boolean;
  disabled?: boolean;
  onChange: (on: boolean) => void;
  /** Accessible name when no visible label points at the checkbox. */
  label?: string;
  labelledBy?: string;
  className?: string;
}

/** label.toggle: a visually hidden checkbox with a track and thumb. */
export function Switch({ id, checked, disabled, onChange, label, labelledBy, className }: SwitchProps) {
  return (
    <label
      className={cx(
        "toggle mapping-toggle-switch group/tg relative m-0 inline-flex shrink-0 cursor-pointer items-center gap-2.5 select-none",
        disabled && "pointer-events-none opacity-45",
        className,
      )}
    >
      <input
        type="checkbox"
        id={id}
        checked={checked}
        disabled={disabled}
        aria-label={label}
        aria-labelledby={labelledBy}
        onChange={(e) => onChange(e.target.checked)}
        className="peer absolute size-0 opacity-0"
      />
      <span className="toggle-track relative h-[22px] w-[38px] rounded-full border border-white/12 bg-white/8 transition duration-200 group-hover/tg:border-white/20 peer-checked:border-transparent peer-checked:bg-[linear-gradient(135deg,var(--color-primary)_0%,var(--color-primary-700)_100%)] peer-focus-visible:border-accent">
        <span className="toggle-thumb absolute top-0.5 left-0.5 size-4 rounded-full bg-muted transition duration-200 [.peer:checked+.toggle-track>&]:translate-x-4 [.peer:checked+.toggle-track>&]:bg-white" />
      </span>
    </label>
  );
}

export interface ToggleRowProps {
  id: string;
  name: ReactNode;
  desc?: ReactNode;
  /** Orange italic note under a setting that cannot be applied. */
  note?: ReactNode;
  checked: boolean;
  disabled?: boolean;
  onChange: (on: boolean) => void;
  settingKey?: string;
  /** Adds .mapping-setting-field, the rows the settings search filters. */
  field?: boolean;
}

export function ToggleRow({ id, name, desc, note, checked, disabled, onChange, settingKey, field = true }: ToggleRowProps) {
  return (
    <div className={cx(ROW, field && "mapping-setting-field", disabled && "is-disabled")} data-setting-key={settingKey}>
      <div className="mapping-toggle-info flex min-w-0 flex-col gap-px">
        <span id={`${id}-name`} className="mapping-toggle-name text-[13px] font-medium text-text">
          {name}
        </span>
        {desc ? <span className={DESC}>{desc}</span> : null}
        {note ? (
          <span className={cx(DESC, "mapping-toggle-disabled-note text-note! italic opacity-90!")}>{note}</span>
        ) : null}
      </div>
      <Switch id={id} labelledBy={`${id}-name`} checked={checked} disabled={disabled} onChange={onChange} />
    </div>
  );
}
