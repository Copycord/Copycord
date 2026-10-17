// Field wraps one control with a label, help text and an error. Input,
// Textarea, Select, FieldSelect and ChipInput read the ids from context so the label and
// aria-describedby / aria-invalid are wired without extra props.

import { createContext, useContext, useId } from "react";
import type {
  InputHTMLAttributes,
  ReactNode,
  Ref,
  SelectHTMLAttributes,
  TextareaHTMLAttributes,
} from "react";
import { ToolbarSelect } from "../../pages/channels/ToolbarSelect";
import { cx } from "./cx";

interface FieldContextValue {
  id: string;
  describedBy: string | undefined;
  invalid: boolean;
}

const FieldContext = createContext<FieldContextValue | null>(null);

export interface FieldProps {
  label: ReactNode;
  help?: ReactNode;
  error?: ReactNode;
  id?: string;
  className?: string;
  children: ReactNode;
}

export function Field({ label, help, error, id, className, children }: FieldProps) {
  const auto = useId();
  const controlId = id ?? auto;
  const helpId = help ? `${controlId}-help` : undefined;
  const errorId = error ? `${controlId}-error` : undefined;
  const describedBy = [errorId, helpId].filter(Boolean).join(" ") || undefined;
  return (
    <FieldContext.Provider value={{ id: controlId, describedBy, invalid: Boolean(error) }}>
      <div className={cx("flex flex-col gap-1.5", className)}>
        <label htmlFor={controlId} className="ml-0.5 text-xs text-muted">
          {label}
        </label>
        {children}
        {help ? (
          <p id={helpId} className="mx-0.5 text-xs text-muted opacity-90">
            {help}
          </p>
        ) : null}
        {error ? (
          <p id={errorId} role="alert" className="text-xs text-danger">
            {error}
          </p>
        ) : null}
      </div>
    </FieldContext.Provider>
  );
}

/** Ids and aria props for a control placed inside a Field. */
export function useFieldControl(props: { id?: string; "aria-describedby"?: string; "aria-invalid"?: unknown }) {
  const ctx = useContext(FieldContext);
  const invalid = props["aria-invalid"] ?? (ctx?.invalid ? true : undefined);
  return {
    id: props.id ?? ctx?.id,
    "aria-describedby": props["aria-describedby"] ?? ctx?.describedBy,
    "aria-invalid": invalid as boolean | undefined,
  };
}

// Input and select look: 12px 14px padding, 12px radius, dark gradient, 14px text;
// the select draws its own caret.
const CONTROL_BASE =
  "w-full rounded-control border border-border-input px-3.5 py-3 text-sm text-text " +
  "placeholder:text-text-2/80 outline-none transition focus:border-accent focus:ring-2 focus:ring-accent/30 " +
  "disabled:cursor-not-allowed disabled:opacity-55 aria-invalid:border-danger";

export const CONTROL_CLASS = `${CONTROL_BASE} bg-(image:--control-gradient)`;
const SELECT_CLASS = `${CONTROL_BASE} cursor-pointer appearance-none pr-[42px] [background:var(--select-bg)]`;

export function Input({ className, ref, ...rest }: InputHTMLAttributes<HTMLInputElement> & { ref?: Ref<HTMLInputElement> }) {
  const aria = useFieldControl(rest);
  return <input ref={ref} {...rest} {...aria} className={cx(CONTROL_CLASS, className)} />;
}

export function Textarea({
  className,
  ref,
  ...rest
}: TextareaHTMLAttributes<HTMLTextAreaElement> & { ref?: Ref<HTMLTextAreaElement> }) {
  const aria = useFieldControl(rest);
  return <textarea ref={ref} rows={4} {...rest} {...aria} className={cx(CONTROL_CLASS, "min-h-24 resize-y", className)} />;
}

export interface SelectOption {
  value: string;
  label: string;
  disabled?: boolean;
}

export function Select({
  className,
  options,
  children,
  ref,
  ...rest
}: SelectHTMLAttributes<HTMLSelectElement> & { options?: SelectOption[]; ref?: Ref<HTMLSelectElement> }) {
  const aria = useFieldControl(rest);
  return (
    <select ref={ref} {...rest} {...aria} className={cx(SELECT_CLASS, className)}>
      {options?.map((o) => (
        <option key={o.value} value={o.value} disabled={o.disabled} className="bg-surface-deep">
          {o.label}
        </option>
      ))}
      {children}
    </select>
  );
}

export interface FieldSelectProps {
  id?: string;
  value: string;
  options: readonly SelectOption[];
  onChange: (value: string) => void;
  disabled?: boolean;
  /** The options are still loading: a shimmer bar stands in the box where the chosen label goes. */
  pending?: boolean;
  /** Text that sizes the pending bar, about as wide as the label that replaces it. */
  pendingLabel?: string;
  "aria-describedby"?: string;
  "aria-invalid"?: boolean;
  /** Replaces the Input look of the toggle, e.g. the 34px toolbar dropdown look; the caret room stays. */
  toggleClassName?: string;
  /** Type in the closed box to filter the options by label or value; for lists that can run long. */
  searchable?: boolean;
}

/** The app's dropdown (ToolbarSelect) as a form control: full width, the Input look, its own listbox. */
export function FieldSelect({
  value,
  options,
  onChange,
  disabled,
  pending,
  pendingLabel,
  toggleClassName = CONTROL_CLASS,
  searchable,
  ...rest
}: FieldSelectProps) {
  const auto = useId();
  const { id, ...aria } = useFieldControl(rest);
  return (
    <ToolbarSelect
      variant="field"
      id={id ?? auto}
      value={value}
      options={options}
      onChange={onChange}
      disabled={disabled}
      pending={pending}
      pendingLabel={pendingLabel}
      toggleClassName={toggleClassName}
      searchable={searchable}
      {...aria}
    />
  );
}
