// A password input with the old eye button that shows and hides the value.

import { useState } from "react";
import { Input, cx } from "../../../components/ui";

export interface TokenInputProps {
  id: string;
  value: string;
  onChange: (value: string) => void;
  invalid?: boolean;
  placeholder?: string;
  "aria-describedby"?: string;
}

const EYE = (
  <>
    <path d="M1 12s4-7 11-7 11 7 11 7-4 7-11 7S1 12 1 12Z" />
    <circle cx="12" cy="12" r="3" />
  </>
);
const EYE_OFF = (
  <path d="M17.94 17.94A10.07 10.07 0 0 1 12 19c-7 0-11-7-11-7a18.4 18.4 0 0 1 5.06-5.94M9.9 9.9A3 3 0 0 0 9 12a3 3 0 0 0 3 3 3 3 0 0 0 2.1-.9M1 1l22 22" />
);

export function TokenInput({ id, value, onChange, invalid, placeholder, ...rest }: TokenInputProps) {
  const [shown, setShown] = useState(false);
  return (
    <div className="relative">
      <Input
        id={id}
        name={id}
        type={shown ? "text" : "password"}
        value={value}
        autoComplete="off"
        spellCheck={false}
        placeholder={placeholder}
        aria-invalid={invalid || undefined}
        aria-describedby={rest["aria-describedby"]}
        onChange={(e) => onChange(e.target.value)}
        className={cx("pr-11", invalid && "border-danger")}
      />
      <button
        type="button"
        data-target={id}
        aria-pressed={shown}
        aria-label={shown ? `Hide ${id}` : `Show ${id}`}
        title={shown ? "Hide" : "Show"}
        onClick={() => setShown((s) => !s)}
        className="absolute top-1/2 right-2 grid size-8 -translate-y-1/2 cursor-pointer place-items-center rounded-control text-text-2 outline-none hover:text-accent focus-visible:ring-2 focus-visible:ring-accent/60"
      >
        <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" viewBox="0 0 24 24" aria-hidden="true">
          {shown ? EYE_OFF : EYE}
        </svg>
      </button>
    </div>
  );
}
