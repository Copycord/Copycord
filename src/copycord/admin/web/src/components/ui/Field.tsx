// Field wraps one control with a label, help text and an error. Input,
// Textarea, Select and ChipInput read the ids from context so the label and
// aria-describedby / aria-invalid are wired without extra props.

import { createContext, useContext, useId } from "react";
import type {
  InputHTMLAttributes,
  ReactNode,
  Ref,
  SelectHTMLAttributes,
  TextareaHTMLAttributes,
} from "react";
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
        <label htmlFor={controlId} className="text-sm font-semibold text-text-1">
          {label}
        </label>
        {children}
        {help ? (
          <p id={helpId} className="text-xs text-text-2">
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

export const CONTROL_CLASS =
  "w-full rounded-control border border-border bg-linear-to-b from-[#1a1030] to-[#140b26] px-3 py-2.5 text-sm text-text " +
  "placeholder:text-text-2/70 outline-none transition focus:border-accent focus:ring-2 focus:ring-accent/30 " +
  "disabled:cursor-not-allowed disabled:opacity-55 aria-invalid:border-danger";

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
    <select ref={ref} {...rest} {...aria} className={cx(CONTROL_CLASS, "cursor-pointer", className)}>
      {options?.map((o) => (
        <option key={o.value} value={o.value} disabled={o.disabled} className="bg-card-solid">
          {o.label}
        </option>
      ))}
      {children}
    </select>
  );
}
