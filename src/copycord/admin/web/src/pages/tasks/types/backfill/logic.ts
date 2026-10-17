// Pure helpers of the Backfill channels form: the range validation copied from
// pages/channels/backfill/logic.ts (v3 rules) and the POST /api/tasks/runs inputs.

export type RangeMode = "all" | "since" | "between" | "last_n";
export type RunMode = "resume" | "new";

export interface RangeForm {
  mode: RangeMode;
  sinceDate: string;
  sinceTime: string;
  fromDate: string;
  fromTime: string;
  toDate: string;
  toTime: string;
  lastN: string;
}

export const EMPTY_RANGE: RangeForm = {
  mode: "all",
  sinceDate: "",
  sinceTime: "",
  fromDate: "",
  fromTime: "",
  toDate: "",
  toTime: "",
  lastN: "",
};

export type RangeField = "since" | "last" | "from" | "to";
export type RangeErrors = Partial<Record<RangeField, string>>;

const pad = (n: number) => String(n).padStart(2, "0");

/** "2025-03-04" + "13:05" -> "2025-03-04T13:05"; no time means midnight. */
export function startIso(date: string, time: string): string {
  const t = time.trim();
  return `${date}T${t || "00:00"}`;
}

/** End of a "to" bound: the exact time if given, else the start of the next day. */
export function endIso(date: string, time: string): string {
  const t = time.trim();
  if (t) return `${date}T${t}`;
  const d = new Date(`${date}T00:00`);
  d.setDate(d.getDate() + 1);
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}T00:00`;
}

export function validateRange(f: RangeForm): RangeErrors {
  const errors: RangeErrors = {};
  if (f.mode === "since" && !f.sinceDate.trim()) errors.since = "Pick a date.";
  if (f.mode === "last_n") {
    const n = Number.parseInt(f.lastN.trim(), 10);
    if (!Number.isFinite(n) || n <= 0) errors.last = "Enter a valid number.";
  }
  if (f.mode === "between") {
    if (!f.fromDate.trim()) errors.from = "Pick a date.";
    if (!f.toDate.trim()) errors.to = "Pick a date.";
    if (!errors.from && !errors.to && startIso(f.fromDate, f.fromTime) >= endIso(f.toDate, f.toTime)) {
      errors.to = "End must be after the start.";
    }
  }
  return errors;
}

export interface RangeInputs {
  mode: RangeMode;
  since?: string;
  until?: string;
  last_n?: number;
}

export function rangeInputs(f: RangeForm): RangeInputs {
  if (f.mode === "since") return { mode: "since", since: startIso(f.sinceDate.trim(), f.sinceTime) };
  if (f.mode === "last_n") return { mode: "last_n", last_n: Number.parseInt(f.lastN.trim(), 10) };
  if (f.mode === "between") {
    return {
      mode: "between",
      since: startIso(f.fromDate.trim(), f.fromTime),
      until: endIso(f.toDate.trim(), f.toTime),
    };
  }
  return { mode: "all" };
}

/** Where a 422 error key lands on the form. */
export type FormSlot = "mapping" | "channels" | "range" | "general";

export function errorSlot(key: string): FormSlot {
  if (key.startsWith("mapping")) return "mapping";
  if (key.startsWith("range")) return "range";
  if (key.startsWith("channel") || key.startsWith("category") || key === "" || key === "inputs") return "channels";
  return "general";
}

export function slotErrors(errors: Record<string, unknown>): Partial<Record<FormSlot, string>> {
  const out: Partial<Record<FormSlot, string>> = {};
  for (const [k, v] of Object.entries(errors)) {
    const slot = errorSlot(k);
    const msg = String(v);
    out[slot] = out[slot] ? `${out[slot]} ${msg}` : msg;
  }
  return out;
}

/** "Oct 3" from a checkpoint timestamp; the raw text when it does not parse. */
export function shortDate(iso: string): string {
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso;
  return d.toLocaleDateString("en-US", { month: "short", day: "numeric" });
}
