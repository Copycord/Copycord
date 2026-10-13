// Chip input with the behaviour of the old ChipsInput / WordChipsInput in
// frontend/src/js/app.js and the forwarding chips:
//  - Enter (and comma in "words" mode) commits the typed text
//  - paste is split and committed at once
//  - Backspace or Delete on an empty entry removes the last chip
//  - clicking a chip, or Enter/Space/Backspace/Delete on a focused chip, removes it
//  - duplicates are dropped, words are capped at 100 characters
//  - "ids" mode keeps only positive integer runs, as the old ID chips did
//  - `locked` freezes everything (the old data-locked="1")

import { useRef, useState } from "react";
import type { ClipboardEvent, KeyboardEvent } from "react";
import { cx } from "./cx";
import { useFieldControl } from "./Field";

export type ChipMode = "words" | "ids";

export const WORD_MAX = 100;

/** Splits raw text the way the old inputs did, before dedupe and validation. */
export function splitChips(text: string, mode: ChipMode = "words"): string[] {
  if (mode === "ids") {
    const out: string[] = [];
    for (const part of String(text).split(/[^\d]+/)) {
      if (!/^\d+$/.test(part)) continue;
      const n = BigInt(part);
      if (n <= 0n) continue;
      out.push(n.toString());
    }
    return out;
  }
  return String(text)
    .split(/[,|\n]/)
    .map((s) => s.trim())
    .filter(Boolean)
    .map((s) => s.slice(0, WORD_MAX));
}

/**
 * Adds candidates to the current list: dedupes against the list and inside
 * the batch, and runs `validate` on each. Returns the new list and the
 * rejected values with their messages.
 */
export function mergeChips(
  current: readonly string[],
  candidates: readonly string[],
  validate?: (value: string) => string | null | undefined,
): { values: string[]; rejected: Array<{ value: string; error: string }> } {
  const values = [...current];
  const rejected: Array<{ value: string; error: string }> = [];
  for (const c of candidates) {
    if (values.includes(c)) continue;
    const error = validate?.(c);
    if (error) {
      rejected.push({ value: c, error });
      continue;
    }
    values.push(c);
  }
  return { values, rejected };
}

export interface ChipInputProps {
  value: string[];
  onChange: (values: string[]) => void;
  mode?: ChipMode;
  /** Return an error message to reject a value, nothing to accept it. */
  validate?: (value: string) => string | null | undefined;
  /** Called with the values rejected by `validate` on each commit. */
  onReject?: (rejected: Array<{ value: string; error: string }>) => void;
  placeholder?: string;
  locked?: boolean;
  id?: string;
  "aria-label"?: string;
  "aria-describedby"?: string;
  className?: string;
}

export function ChipInput({
  value,
  onChange,
  mode = "words",
  validate,
  onReject,
  placeholder,
  locked = false,
  className,
  ...rest
}: ChipInputProps) {
  const [text, setText] = useState("");
  const [error, setError] = useState<string | null>(null);
  const entryRef = useRef<HTMLInputElement>(null);
  const aria = useFieldControl(rest);

  const commit = (raw: string) => {
    const { values, rejected } = mergeChips(value, splitChips(raw, mode), validate);
    setError(rejected.length ? rejected.map((r) => `${r.value}: ${r.error}`).join("; ") : null);
    if (rejected.length) onReject?.(rejected);
    if (values.length !== value.length) onChange(values);
  };

  const remove = (chip: string) => {
    if (locked) return;
    onChange(value.filter((v) => v !== chip));
    entryRef.current?.focus();
  };

  const onKeyDown = (e: KeyboardEvent<HTMLInputElement>) => {
    if (locked) {
      e.preventDefault();
      return;
    }
    if (e.key === "Enter" || (e.key === "," && mode === "words")) {
      e.preventDefault();
      commit(text);
      setText("");
      return;
    }
    if ((e.key === "Backspace" || e.key === "Delete") && text === "" && value.length) {
      e.preventDefault();
      onChange(value.slice(0, -1));
    }
  };

  const onPaste = (e: ClipboardEvent<HTMLInputElement>) => {
    if (locked) {
      e.preventDefault();
      return;
    }
    const clip = e.clipboardData.getData("text");
    if (!clip) return;
    e.preventDefault();
    commit(text + clip);
    setText("");
  };

  const errorId = error ? `${aria.id ?? "chip"}-chip-error` : undefined;
  const describedBy = [aria["aria-describedby"], errorId].filter(Boolean).join(" ") || undefined;

  return (
    <div className={className}>
      <div
        data-locked={locked ? "1" : undefined}
        onClick={(e) => {
          if (!locked && e.target === e.currentTarget) entryRef.current?.focus();
        }}
        className={cx(
          "flex min-h-[46px] cursor-text flex-wrap items-center gap-1.5 rounded-control border border-border px-3 py-2",
          "bg-linear-to-b from-[#1a1030] to-[#140b26] focus-within:border-accent focus-within:ring-2 focus-within:ring-accent/30",
          locked && "cursor-not-allowed opacity-60",
          (aria["aria-invalid"] || error) && "border-danger",
        )}
      >
        <ul className="contents" aria-label="Selected values">
          {value.map((chip) => (
            <li key={chip} className="contents">
              <button
                type="button"
                disabled={locked}
                aria-label={`Remove ${chip}`}
                title={chip}
                onClick={() => remove(chip)}
                onKeyDown={(e) => {
                  if (e.key === "Backspace" || e.key === "Delete") {
                    e.preventDefault();
                    remove(chip);
                  }
                }}
                className="max-w-full cursor-pointer truncate rounded-[6px] border border-white/10 bg-white/5 px-2.5 py-1 text-[13px] leading-snug text-text hover:border-danger/50 hover:bg-danger/10 focus-visible:ring-2 focus-visible:ring-accent/60 outline-none disabled:cursor-not-allowed"
              >
                {chip}
              </button>
            </li>
          ))}
        </ul>
        <input
          ref={entryRef}
          id={aria.id}
          aria-label={rest["aria-label"]}
          aria-describedby={describedBy}
          aria-invalid={aria["aria-invalid"] || Boolean(error) || undefined}
          value={text}
          readOnly={locked}
          placeholder={value.length ? undefined : placeholder}
          inputMode={mode === "ids" ? "numeric" : undefined}
          onChange={(e) => {
            setText(e.target.value);
            if (error) setError(null);
          }}
          onKeyDown={onKeyDown}
          onPaste={onPaste}
          className="min-w-32 flex-1 bg-transparent text-sm text-text outline-none placeholder:text-text-2/70"
        />
      </div>
      {error ? (
        <p id={errorId} role="alert" className="mt-1 text-xs text-danger">
          {error}
        </p>
      ) : null}
    </div>
  );
}
