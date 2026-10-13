// Pure helpers of the clone history lane: request bodies, form validation,
// websocket event reading and the persisted toast receipts. Storage keys are
// the ones channels.js used, so an old and a new tab agree.

import type {
  BackfillBatchBody,
  BackfillEvent,
  BackfillMode,
  BackfillStartBody,
  CloneState,
  QueueItem,
  ResumeInfo,
} from "../../../types/channels";

export const TOAST_TTL_MS = 24 * 60 * 60 * 1000;

export interface BackfillForm {
  mode: BackfillMode;
  sinceDate: string;
  sinceTime: string;
  fromDate: string;
  fromTime: string;
  toDate: string;
  toTime: string;
  lastN: string;
  ignoreCloned: boolean;
}

export const EMPTY_FORM: BackfillForm = {
  mode: "all",
  sinceDate: "",
  sinceTime: "",
  fromDate: "",
  fromTime: "",
  toDate: "",
  toTime: "",
  lastN: "",
  ignoreCloned: false,
};

export type FormField = "since" | "last" | "from" | "to";
export type FormErrors = Partial<Record<FormField, string>>;

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

export function validateForm(f: BackfillForm): FormErrors {
  const errors: FormErrors = {};
  if (f.mode === "since" && !f.sinceDate.trim()) errors.since = "Pick a date.";
  if (f.mode === "last") {
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

/** The range part shared by both bodies, with the lower bound under `lower`. */
function range(f: BackfillForm, lower: "since" | "after_iso") {
  const out: Record<string, string | number> = {};
  if (f.mode === "since") out[lower] = startIso(f.sinceDate.trim(), f.sinceTime);
  else if (f.mode === "last") out.last_n = Number.parseInt(f.lastN.trim(), 10);
  else if (f.mode === "between") {
    out[lower] = startIso(f.fromDate.trim(), f.fromTime);
    out.before_iso = endIso(f.toDate.trim(), f.toTime);
  }
  return out;
}

export function startBody(channelId: string, mappingId: string, f: BackfillForm): BackfillStartBody {
  return {
    channel_id: channelId,
    mapping_id: mappingId,
    mode: f.mode,
    ...range(f, "since"),
    ...(f.ignoreCloned ? { ignore_cloned: true } : {}),
  };
}

export function batchBody(channelIds: string[], mappingId: string, f: BackfillForm): BackfillBatchBody {
  return {
    channel_ids: channelIds,
    mapping_id: mappingId,
    mode: f.mode,
    ...range(f, "after_iso"),
    ...(f.ignoreCloned ? { ignore_cloned: true } : {}),
    resume: false,
  };
}

export function resumeBody(channelId: string, mappingId: string, info: ResumeInfo): BackfillStartBody {
  return {
    channel_id: channelId,
    mapping_id: mappingId,
    resume: true,
    run_id: info.run_id ?? undefined,
    checkpoint: info.checkpoint ?? null,
  };
}

export const canResume = (info: ResumeInfo | null | undefined): boolean => !!(info?.available ?? info?.resumable);

export function resumeInfoUrl(channelId: string, mappingId: string): string {
  return `/api/backfills/resume-info?channel_id=${encodeURIComponent(channelId)}&mapping_id=${encodeURIComponent(mappingId)}`;
}

export function scopedUrl(path: string, mappingId: string): string {
  return mappingId ? `${path}?mapping_id=${encodeURIComponent(mappingId)}` : path;
}

export function batchSummary(counts: { started?: number; locked?: number; failed?: number } | undefined): {
  text: string;
  ok: boolean;
} {
  const c = counts ?? {};
  return {
    text: `Batch: started ${c.started || 0}, locked ${c.locked || 0}, failed ${c.failed || 0}.`,
    ok: !!c.started,
  };
}

// ---- websocket events --------------------------------------------------

const idOf = (v: unknown): string | null => {
  if (typeof v === "string" && v.trim()) return v.trim();
  if (typeof v === "number" && Number.isSafeInteger(v)) return String(v);
  return null;
};

/** Original channel id of an event, from the task map or the payload. */
export function eventChannelId(p: BackfillEvent, taskMap: ReadonlyMap<string, string>): string | null {
  const tid = p.task_id != null ? String(p.task_id) : "";
  const mapped = tid ? taskMap.get(tid) : undefined;
  return mapped ?? idOf(p.data?.channel_id) ?? idOf(p.data?.original_channel_id) ?? idOf(p.channel_id);
}

const num = (v: unknown): number | null => (typeof v === "number" && Number.isFinite(v) ? v : null);

/** delivered/total of a backfill_progress event; the numbers never go backwards. */
export function progressOf(p: BackfillEvent, prev: { delivered: number | null; total: number | null }) {
  const d0 = p.data ?? {};
  let delivered = num(d0.delivered ?? d0.applied ?? d0.count) ?? prev.delivered;
  const total = num(d0.expected_total ?? d0.total ?? d0.expected) ?? prev.total;
  if (prev.delivered != null && delivered != null && delivered < prev.delivered) delivered = prev.delivered;
  return { delivered, total };
}

export function stageForProgress(delivered: number | null, total: number | null): CloneState["stage"] {
  return (delivered ?? 0) > 0 || (total ?? 0) > 0 ? "cloning" : "pulling";
}

export function progressLabel(s: CloneState): string {
  const f = (n: number) => n.toLocaleString("en-US");
  switch (s.stage) {
    case "launching":
      return "Starting…";
    case "queued":
      return s.position ? `Queued (#${s.position})` : "Queued";
    case "cleaning":
      return "Cleaning up";
    case "pulling":
      return "Cloning";
    default:
      if (s.delivered && s.total) return `Cloning (${f(s.delivered)}/${f(s.total)})`;
      if (s.delivered) return `Cloning (${f(s.delivered)})`;
      return "Cloning";
  }
}

/** Percent for a determinate bar, or null while indeterminate. */
export function progressPct(s: CloneState): number | null {
  if (s.stage !== "cloning" || !s.delivered || !s.total) return null;
  return Math.max(0, Math.min(100, Math.floor((s.delivered / s.total) * 100)));
}

export function cancelText(reason: string): string {
  const r = reason.toLowerCase().trim();
  if (r === "server_shutdown") return "Clone cancelled: server is shutting down.";
  if (r === "user_cancelled" || !r) return "Clone cancelled.";
  return `Clone cancelled: ${r}.`;
}

/** Fold the queue response into the cards: active ones, then queued ones with a position. */
export function queueStates(items: readonly QueueItem[], hasDelivered: (cid: string) => boolean): Map<string, CloneState> {
  const out = new Map<string, CloneState>();
  for (const q of items) {
    const cid = String(q.channel_id);
    if (q.state === "active") {
      out.set(cid, { stage: hasDelivered(cid) ? "cloning" : "queued", delivered: null, total: null });
    }
  }
  for (const q of items) {
    const cid = String(q.channel_id);
    if (q.state === "queued" && !out.has(cid)) out.set(cid, { stage: "queued", delivered: null, total: null, position: q.position });
  }
  return out;
}

// ---- persisted toasts --------------------------------------------------

export const persistKey = (key: string) => `toast:persist:${key}`;
export const receiptKey = (key: string) => `toast:receipt:${key}`;

export function isToastPersisted(key: string, now = Date.now()): boolean {
  try {
    const p = JSON.parse(localStorage.getItem(persistKey(key)) ?? "null") as { expiresAt?: number } | null;
    return !!p && now < Number(p.expiresAt || 0);
  } catch {
    return false;
  }
}

export function markToastDelivered(key: string, ttlMs: number, now = Date.now()): void {
  try {
    localStorage.setItem(receiptKey(key), JSON.stringify({ deliveredAt: now }));
    localStorage.setItem(persistKey(key), JSON.stringify({ expiresAt: now + ttlMs }));
  } catch {
    // Storage full or blocked: the toast just shows again next time.
  }
}

/** True when the toast should show now; records it so a reload or second tab does not repeat it. */
export function claimToast(key: string, ttlMs: number): boolean {
  if (isToastPersisted(key)) return false;
  markToastDelivered(key, ttlMs);
  return true;
}

// ---- running sets in storage (bf:running, bf:queued, ...) --------------

export type StoredSet = "running" | "launching" | "pulling" | "queued" | "cleaning";

export function saveSet(name: StoredSet, ids: Iterable<string>): void {
  try {
    localStorage.setItem(`bf:${name}`, JSON.stringify([...new Set(ids)]));
  } catch {
    // ignore
  }
}

export function loadSet(name: StoredSet): string[] {
  try {
    const v = JSON.parse(localStorage.getItem(`bf:${name}`) ?? "[]") as unknown;
    return Array.isArray(v) ? v.map(String) : [];
  } catch {
    return [];
  }
}

export function loadTaskMap(): Map<string, string> {
  try {
    return new Map(Object.entries(JSON.parse(sessionStorage.getItem("bf:taskmap") ?? "{}") as Record<string, string>));
  } catch {
    return new Map();
  }
}

export function saveTaskMap(m: ReadonlyMap<string, string>): void {
  try {
    sessionStorage.setItem("bf:taskmap", JSON.stringify(Object.fromEntries(m)));
  } catch {
    // ignore
  }
}

export function loadDoneTasks(): Set<string> {
  try {
    const v = JSON.parse(localStorage.getItem("bf:done_tasks") ?? "[]") as unknown;
    return new Set(Array.isArray(v) ? v.map(String) : []);
  } catch {
    return new Set();
  }
}

export function saveDoneTasks(s: ReadonlySet<string>): void {
  try {
    localStorage.setItem("bf:done_tasks", JSON.stringify([...s]));
  } catch {
    // ignore
  }
}
