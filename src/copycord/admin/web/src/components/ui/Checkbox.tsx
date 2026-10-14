// Checkbox and Radio: real inputs drawn with appearance-none in the look of the export dialog's
// boxes (18px, faint fill, accent border on hover, purple when checked), plus a white check mark,
// a dash when indeterminate and a dot on the radio. The marks are the --check-mark, --check-dash
// and --radio-dot images in styles/tokens.css.

import { useEffect, useImperativeHandle, useRef } from "react";
import type { InputHTMLAttributes, Ref } from "react";
import { cx } from "./cx";

type MarkProps = Omit<InputHTMLAttributes<HTMLInputElement>, "type"> & { ref?: Ref<HTMLInputElement> };

const MARK_BASE =
  "m-0 size-[18px] shrink-0 cursor-pointer appearance-none border border-border bg-white/4 bg-center bg-no-repeat " +
  "[background-size:100%_100%] outline-none transition hover:border-accent focus-visible:shadow-[0_0_0_4px_rgba(167,139,250,0.18)] " +
  "checked:border-primary-600 checked:bg-primary disabled:cursor-not-allowed disabled:opacity-60";

const CHECKBOX_CLASS =
  `${MARK_BASE} rounded-md checked:bg-(image:--check-mark) ` +
  "indeterminate:border-primary-600 indeterminate:bg-primary indeterminate:bg-(image:--check-dash)";

const RADIO_CLASS = `${MARK_BASE} rounded-full checked:bg-(image:--radio-dot)`;

export interface CheckboxProps extends MarkProps {
  /** Draws the dash and sets the DOM indeterminate flag (aria-checked="mixed"). */
  indeterminate?: boolean;
}

export function Checkbox({ indeterminate = false, className, ref, ...rest }: CheckboxProps) {
  const inner = useRef<HTMLInputElement>(null);
  useImperativeHandle(ref, () => inner.current!, []);
  useEffect(() => {
    if (inner.current) inner.current.indeterminate = indeterminate;
  }, [indeterminate]);
  return (
    <input
      ref={inner}
      type="checkbox"
      aria-checked={indeterminate ? "mixed" : undefined}
      {...rest}
      className={cx(CHECKBOX_CLASS, className)}
    />
  );
}

export function Radio({ className, ref, ...rest }: MarkProps) {
  return <input ref={ref} type="radio" {...rest} className={cx(RADIO_CLASS, className)} />;
}
