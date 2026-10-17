// Task type titles and descriptions are labels, fixed per build, so the Tasks pages show them as text from
// the first paint rather than as bars. The type list the page fetches is remembered here (localStorage
// tasks:types) and so is each run's type (tasks:run-types), so a revisit, a type page or a run page opened
// from the list names its type at once. A first visit that knows nothing yet says the type id in words
// ("backfill_channels" is "Backfill channels").
//   rememberTypes(types)   typeLabel("backfill_channels").title   rememberRunTypes(runs)   runTypeOf("12")

import type { TaskRun, TaskType } from "../../types/tasks";

const TYPES_KEY = "tasks:types";
const RUN_TYPES_KEY = "tasks:run-types";
/** Run ids remembered at most; the lowest go first. */
const RUN_TYPES_MAX = 500;

export interface TypeLabel {
  title: string;
  /** "" when not known yet. */
  description: string;
}

// The parsed value of each key, reused while the stored text is unchanged.
const parsed = new Map<string, { raw: string | null; value: Record<string, unknown> }>();

function read<T>(key: string): Record<string, T> {
  let raw: string | null = null;
  try {
    raw = localStorage.getItem(key);
  } catch {
    // Storage blocked: nothing remembered.
  }
  const held = parsed.get(key);
  if (held && held.raw === raw) return held.value as Record<string, T>;
  let value: Record<string, unknown> = {};
  try {
    const v: unknown = JSON.parse(raw ?? "null");
    if (v && typeof v === "object" && !Array.isArray(v)) value = v as Record<string, unknown>;
  } catch {
    // A bad value: start empty.
  }
  parsed.set(key, { raw, value });
  return value as Record<string, T>;
}

function write(key: string, value: object): void {
  try {
    localStorage.setItem(key, JSON.stringify(value));
  } catch {
    // Storage blocked or full: the next visit starts from the id in words again.
  }
}

/** "backfill_channels" is "Backfill channels". */
export function humanizeTypeKey(key: string): string {
  const words = key.replace(/[_-]+/g, " ").trim();
  return words ? words.charAt(0).toUpperCase() + words.slice(1) : key;
}

/** Keep the fetched type list; written only when it changed. */
export function rememberTypes(list: TaskType[]): void {
  const next: Record<string, TypeLabel> = {};
  for (const t of list) next[t.key] = { title: t.title, description: t.description ?? "" };
  if (JSON.stringify(next) !== JSON.stringify(read<TypeLabel>(TYPES_KEY))) write(TYPES_KEY, next);
}

/** First visit titles the id in words gets wrong ("Export dms"). */
const FIRST_TITLES: Record<string, string> = { export_dms: "Export DMs" };

/** The known type's title and description, else the id in words and no description. */
export function typeLabel(key: string): TypeLabel {
  const held = read<TypeLabel>(TYPES_KEY)[key];
  if (held && typeof held.title === "string") return { title: held.title, description: held.description ?? "" };
  if (key === "download_avatars") return { title: "Download avatars", description: "Zip the profile pictures of every member the client account can see in one of its servers." };
  return { title: FIRST_TITLES[key] ?? humanizeTypeKey(key), description: "" };
}

/** Keep which type each run is, for the run page's first paint. */
export function rememberRunTypes(list: Pick<TaskRun, "id" | "type">[]): void {
  const held = read<string>(RUN_TYPES_KEY);
  if (list.every((r) => held[String(r.id)] === r.type)) return;
  const next = { ...held };
  for (const r of list) next[String(r.id)] = r.type;
  const ids = Object.keys(next);
  for (const id of ids.slice(0, Math.max(0, ids.length - RUN_TYPES_MAX))) delete next[id];
  write(RUN_TYPES_KEY, next);
}

/**
 * The type of run `id` before it loads: the one remembered for it, or the only type there is when the
 * remembered type list holds one; null when it cannot be known yet.
 */
export function runTypeOf(id: string): string | null {
  const held = read<string>(RUN_TYPES_KEY)[id];
  if (typeof held === "string") return held;
  const keys = Object.keys(read<TypeLabel>(TYPES_KEY));
  return keys.length === 1 ? keys[0]! : null;
}
