// Task runs for the Tasks page: one list page per filter set and the open run, kept live by
// task.run.updated frames on /ws/out. A frame patches the row it names when its seq is newer, a run
// the list does not hold goes to the top when it matches the filters (first page only), anything else
// is ignored. On a socket reconnect the page and the open run are fetched again, since frames sent
// while it was down are gone.
//   const { runs, total, loading } = useTaskRuns({ type: "backfill_channels" });
//   const { run } = useTaskRun("12");

import { useCallback, useEffect, useRef, useState } from "react";
import { useWsMessages } from "../../hooks/useWsMessages";
import { api } from "../../lib/api";
import type { BusMessage } from "../../types/api";
import type { RunFilters, TaskContext, TaskRun, TaskRunResponse, TaskRunsResponse, TaskRunUpdated, TaskStep } from "../../types/tasks";

export const PER_PAGE = 25;
export const RUN_UPDATED = "task.run.updated";

export function runsUrl(filters: RunFilters): string {
  const qs = new URLSearchParams();
  if (filters.type) qs.set("type", filters.type);
  if (filters.status) qs.set("status", filters.status);
  if (filters.actor) qs.set("actor", filters.actor);
  if (filters.context) qs.set("context", filters.context);
  if (filters.q) qs.set("q", filters.q);
  qs.set("page", String(filters.page ?? 1));
  qs.set("per_page", String(PER_PAGE));
  return `/api/tasks/runs?${qs.toString()}`;
}

export function runUrl(id: string): string {
  return `/api/tasks/runs/${encodeURIComponent(id)}`;
}

/** The hub puts everything but kind and role under payload; older relays send it flat. */
export function parseRunFrame(msg: BusMessage): TaskRunUpdated | null {
  const p = (msg.payload ?? msg) as Partial<TaskRunUpdated> & { type?: string; kind?: string };
  const kind = msg.kind ?? msg.type ?? p.kind ?? p.type;
  if (kind !== RUN_UPDATED) return null;
  const run = p.run;
  if (!run || typeof run !== "object" || run.id == null) return null;
  return { run: { ...run, id: String(run.id) }, steps: Array.isArray(p.steps) ? p.steps : undefined };
}

/** Does a run belong in the list for these filters? Used before inserting a run the list does not hold. */
export function matchesFilters(run: TaskRun, f: RunFilters): boolean {
  if (f.type && run.type !== f.type) return false;
  if (f.status && run.status !== f.status) return false;
  if (f.actor && run.actor !== f.actor) return false;
  if (f.context && run.context?.mapping_id !== f.context && run.context?.label !== f.context) return false;
  if (f.q) {
    const q = f.q.trim().toLowerCase();
    const hay = [run.title, run.context?.label ?? "", `#${run.number}`].join("\n").toLowerCase();
    if (q && !hay.includes(q)) return false;
  }
  return true;
}

/** Changed steps replace the held step with the same key; new keys are appended in frame order. */
export function mergeSteps(held: TaskStep[] | undefined, changed: TaskStep[] | undefined): TaskStep[] | undefined {
  if (!changed?.length) return held;
  const out = [...(held ?? [])];
  for (const s of changed) {
    const i = out.findIndex((h) => h.key === s.key);
    if (i >= 0) out[i] = { ...out[i], ...s };
    else out.push(s);
  }
  return out;
}

/** One run with a frame applied, or the held run unchanged when the frame is not newer. */
export function applyFrame(held: TaskRun, frame: TaskRunUpdated): TaskRun {
  if (frame.run.seq <= held.seq) return held;
  return { ...held, ...frame.run, steps: mergeSteps(held.steps, frame.steps) };
}

export interface ListState {
  runs: TaskRun[];
  total: number;
}

/** A list page with a frame applied: patch the row, insert at the top, or leave it alone. */
export function mergeIntoList(state: ListState, frame: TaskRunUpdated, filters: RunFilters): ListState {
  const i = state.runs.findIndex((r) => r.id === frame.run.id);
  const held = state.runs[i];
  if (held) {
    const next = applyFrame(held, frame);
    if (next === held) return state;
    const runs = [...state.runs];
    runs[i] = next;
    return { ...state, runs };
  }
  if ((filters.page ?? 1) !== 1 || !matchesFilters(frame.run, filters)) return state;
  // List rows do not carry steps.
  const row: TaskRun = { ...frame.run, steps: undefined };
  return { runs: [row, ...state.runs].slice(0, PER_PAGE), total: state.total + 1 };
}

/** A list page without the run `id` (after a delete); unchanged when the page does not hold it. */
export function removeFromList(state: ListState, id: string): ListState {
  if (!state.runs.some((r) => r.id === id)) return state;
  return { runs: state.runs.filter((r) => r.id !== id), total: Math.max(0, state.total - 1) };
}

/** Calls `refetch` when the socket comes back after a drop, not on the first open. */
function useReconnectRefetch(connected: boolean, refetch: () => void) {
  const was = useRef(connected);
  const seen = useRef(false);
  useEffect(() => {
    if (connected && !was.current && seen.current) refetch();
    if (connected) seen.current = true;
    was.current = connected;
  }, [connected, refetch]);
}

export interface TaskRunsState extends ListState {
  /** No page has landed yet since the list mounted: show the skeleton. Never true again after that. */
  loading: boolean;
  /** A fetch is out while the held rows stay on screen (search, filters, paging, auto-refresh, reconnect, reload). */
  refreshing: boolean;
  /** The held page answers the current filters; until then an empty page is not news (no empty state). */
  settled: boolean;
  error: Error | undefined;
  /** The /ws/out socket is open, so rows update live. */
  connected: boolean;
  reload: () => void;
  /** Actor and mapping choices for the filter dropdowns, from the last page fetched. */
  actors: string[];
  contexts: TaskContext[];
  /** Apply a run the server returned (a cancel response) as if it came in a frame. */
  patchRun: (run: TaskRun) => void;
  /** Drop a deleted run from the page. */
  removeRun: (id: string) => void;
}

/**
 * The list page for `filters`, live. Stale while revalidate: only the first fetch after mount loads (the
 * skeleton); every later one, for new filters too, keeps the held rows with `refreshing` set until its
 * answer replaces them. A fetch made stale by a newer one is aborted and its answer ignored.
 */
export function useTaskRuns(filters: RunFilters): TaskRunsState {
  const url = runsUrl(filters);
  const [state, setState] = useState<ListState>({ runs: [], total: 0 });
  const [choices, setChoices] = useState<{ actors: string[]; contexts: TaskContext[] }>({ actors: [], contexts: [] });
  // The list URL the held page answers, null before the first answer.
  const [loadedUrl, setLoadedUrl] = useState<string | null>(null);
  const [refreshing, setRefreshing] = useState(false);
  const [error, setError] = useState<Error | undefined>(undefined);
  const [tick, setTick] = useState(0);
  const filtersRef = useRef(filters);
  filtersRef.current = filters;

  useEffect(() => {
    const ctl = new AbortController();
    setRefreshing(true);
    api
      .get<TaskRunsResponse>(url, { signal: ctl.signal })
      .then((d) => {
        if (ctl.signal.aborted) return;
        setState({ runs: (d.runs ?? []).map((r) => ({ ...r, id: String(r.id) })), total: d.total ?? 0 });
        setChoices({ actors: d.actors ?? [], contexts: d.contexts ?? [] });
        setError(undefined);
      })
      .catch((e: unknown) => {
        if (!ctl.signal.aborted) setError(e instanceof Error ? e : new Error(String(e)));
      })
      .finally(() => {
        if (ctl.signal.aborted) return;
        setLoadedUrl(url);
        setRefreshing(false);
      });
    return () => ctl.abort();
  }, [url, tick]);

  const reload = useCallback(() => setTick((t) => t + 1), []);
  const { connected } = useWsMessages(
    "/ws/out",
    (msg) => {
      const frame = parseRunFrame(msg);
      if (frame) setState((s) => mergeIntoList(s, frame, filtersRef.current));
    },
    { types: [RUN_UPDATED] },
  );
  useReconnectRefetch(connected, reload);
  const patchRun = useCallback((run: TaskRun) => {
    const frame = { run: { ...run, id: String(run.id) } };
    setState((s) => mergeIntoList(s, frame, filtersRef.current));
  }, []);
  const removeRun = useCallback((id: string) => setState((s) => removeFromList(s, id)), []);

  const loading = loadedUrl === null;
  const settled = loadedUrl === url;
  return { ...state, ...choices, loading, refreshing: (refreshing || !settled) && !loading, settled, error, connected, reload, patchRun, removeRun };
}

export interface TaskRunState {
  run: TaskRun | undefined;
  /** The run has not landed yet for this id: show the skeleton. */
  loading: boolean;
  /** A refetch is out while the held run stays on screen. */
  refreshing: boolean;
  error: Error | undefined;
  connected: boolean;
  reload: () => void;
}

/** One run with its steps, log lines and report, live. Pass null to skip. Refetches keep the held run. */
export function useTaskRun(id: string | null): TaskRunState {
  const [run, setRun] = useState<TaskRun | undefined>(undefined);
  // The id the held run (or error) answers.
  const [loadedId, setLoadedId] = useState<string | null>(null);
  const [refreshing, setRefreshing] = useState(false);
  const [error, setError] = useState<Error | undefined>(undefined);
  const [tick, setTick] = useState(0);

  useEffect(() => {
    if (id === null) return;
    const ctl = new AbortController();
    setRefreshing(true);
    api
      .get<TaskRunResponse>(runUrl(id), { signal: ctl.signal })
      .then((d) => {
        if (ctl.signal.aborted) return;
        setRun({ ...d.run, id: String(d.run.id) });
        setError(undefined);
      })
      .catch((e: unknown) => {
        if (ctl.signal.aborted) return;
        setError(e instanceof Error ? e : new Error(String(e)));
        // A run that failed to load is not shown under another id's data.
        setRun((held) => (held?.id === id ? held : undefined));
      })
      .finally(() => {
        if (ctl.signal.aborted) return;
        setLoadedId(id);
        setRefreshing(false);
      });
    return () => ctl.abort();
  }, [id, tick]);

  const reload = useCallback(() => setTick((t) => t + 1), []);
  const { connected } = useWsMessages(
    "/ws/out",
    (msg) => {
      const frame = parseRunFrame(msg);
      if (!frame || frame.run.id !== id) return;
      setRun((held) => (held ? applyFrame(held, frame) : held));
    },
    { types: [RUN_UPDATED], enabled: id !== null },
  );
  useReconnectRefetch(connected, reload);

  const loading = id !== null && loadedId !== id;
  return { run: loading ? undefined : run, loading, refreshing: refreshing && !loading, error, connected, reload };
}
