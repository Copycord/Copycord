// Task runs for the Tasks page: the run list per filter set, scrolled in a page at a time, and the open
// run, kept live by task.run.updated frames on /ws/out. A frame patches the row it names when its seq is
// newer, a run the list does not hold goes to the top when it matches the filters; a new run or a
// status change also refetches the page quietly so the totals and tab counts follow. On a socket reconnect the page and the open run are fetched again, since frames sent
// while it was down are gone.
//   const { runs, total, loading } = useTaskRuns({ type: "backfill_channels" });
//   const { run } = useTaskRun("12");

import { useCallback, useEffect, useRef, useState } from "react";
import { useWsMessages } from "../../hooks/useWsMessages";
import { api } from "../../lib/api";
import type { BusMessage } from "../../types/api";
import { ACTIVE_STATUSES } from "../../types/tasks";
import type { RunCounts, RunFilters, TaskContext, TaskRun, TaskRunResponse, TaskRunsResponse, TaskRunUpdated, TaskStep } from "../../types/tasks";
import { PER_PAGE } from "./TasksSkeleton";

export { PER_PAGE };
export const RUN_UPDATED = "task.run.updated";
// The ended run's report fetch: tries, and the wait before the first retry (doubling after).
export const FINAL_FETCH_ATTEMPTS = 3;
export const FINAL_FETCH_BACKOFF_MS = 1000;

export function runsUrl(filters: RunFilters, perPage = PER_PAGE): string {
  const qs = new URLSearchParams();
  if (filters.type) qs.set("type", filters.type);
  if (filters.status) qs.set("status", filters.status);
  if (filters.actor) qs.set("actor", filters.actor);
  if (filters.context) qs.set("context", filters.context);
  if (filters.q) qs.set("q", filters.q);
  qs.set("page", String(filters.page ?? 1));
  qs.set("per_page", String(perPage));
  return `/api/tasks/runs?${qs.toString()}`;
}

/** The server's per_page cap (MAX_PER_PAGE in tasks/routes.py): a refetch of every loaded page asks for at most this many. */
export const MAX_PER_PAGE = 200;

/**
 * The mapping choices as objects with a readable label. The server sends the distinct context labels as
 * plain strings; an object without a label falls back to its mapping id, then its guild id.
 */
export function normalizeContexts(raw: unknown): TaskContext[] {
  if (!Array.isArray(raw)) return [];
  const out: TaskContext[] = [];
  const seen = new Set<string>();
  for (const c of raw) {
    let ctx: TaskContext | null = null;
    if (typeof c === "string") ctx = c.trim() ? { label: c.trim() } : null;
    else if (c && typeof c === "object") {
      const o = c as Partial<TaskContext>;
      const mappingId = o.mapping_id != null && String(o.mapping_id).trim() ? String(o.mapping_id) : null;
      const label = (typeof o.label === "string" && o.label.trim()) || mappingId || (o.guild_id != null ? String(o.guild_id).trim() : "");
      ctx = label ? { ...o, label, mapping_id: mappingId } : null;
    }
    if (!ctx) continue;
    const key = ctx.mapping_id ?? ctx.label;
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(ctx);
  }
  return out;
}

/** The held rows then `more`, a run already held kept once (a live insert shifts the pages by a row). */
export function appendRuns(held: TaskRun[], more: TaskRun[]): TaskRun[] {
  const ids = new Set(held.map((r) => r.id));
  return [...held, ...more.filter((r) => !ids.has(r.id))];
}

/**
 * A refetch of the loaded pages over the held rows: the fresh rows in server order, then any held row
 * past them the refetch did not reach (more than MAX_PER_PAGE loaded), so scrolled-in rows stay.
 */
export function refreshRuns(held: TaskRun[], fresh: TaskRun[]): TaskRun[] {
  if (held.length <= fresh.length) return fresh;
  return appendRuns(fresh, held.slice(fresh.length));
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
    const hay = [run.title, run.context?.label ?? "", `#${run.number}`, run.id].join("\n").toLowerCase();
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
  if (!matchesFilters(frame.run, filters)) return state;
  // List rows do not carry steps.
  const row: TaskRun = { ...frame.run, steps: undefined };
  return { runs: [row, ...state.runs], total: state.total + 1 };
}

/** After a frame the list refetches this long after, so a burst of frames costs one fetch. */
export const LIVE_REFETCH_MS = 500;

/**
 * Does a frame change what the page cannot work out alone: a run seen for the first time (a new run, or
 * one off this page) changes the total, the tab counts and maybe the page; a run moving to another
 * status changes the tab counts and the rail's running badge.
 */
export function changesCounts(last: Pick<TaskRun, "status"> | undefined, frame: TaskRunUpdated): boolean {
  return !last || last.status !== frame.run.status;
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
  /** A fetch is out while the held rows stay on screen (search, filters, reconnect, reload); not for a live refetch or a next page. */
  refreshing: boolean;
  /** The held page answers the current filters; until then an empty page is not news (no empty state). */
  settled: boolean;
  error: Error | undefined;
  /** The /ws/out socket is open, so rows update live. */
  connected: boolean;
  reload: () => void;
  /** Runs per status for the quick tabs, from the last page fetched; null before the first answer. */
  counts: RunCounts | null;
  /** Actor and mapping choices for the filter dropdowns, from the last page fetched. */
  actors: string[];
  contexts: TaskContext[];
  /** Apply a run the server returned (a cancel response) as if it came in a frame. */
  patchRun: (run: TaskRun) => void;
  /** Drop a deleted run from the page. */
  removeRun: (id: string) => void;
  /** More runs than the held rows match the filters. */
  hasMore: boolean;
  /** The next page is out. */
  loadingMore: boolean;
  /** Fetch the next page and append it; a no-op while one is out, while new filters load, or at the end. */
  loadMore: () => void;
}

/**
 * The run list for `filters`, live, a page at a time: the first page, then `loadMore` appends the next
 * one (rows already held are kept, never replaced). New filters (a status tab, a dropdown, a search) start
 * again from the first page; `filters.page` is ignored. A reload or live refetch asks for every loaded page
 * at once, so the scrolled-in rows stay. Stale while revalidate: only the first fetch after mount loads (the
 * skeleton); every later one, for new filters too, keeps the held rows with `refreshing` set until its
 * answer replaces them. A fetch made stale by a newer one is aborted and its answer ignored. A frame
 * for a run the page has not seen, or one that changes a run's status, also refetches the page (and its
 * counts) LIVE_REFETCH_MS later, quietly: no refreshing mark; `onCountsChange` is called with it.
 */
export function useTaskRuns(filters: RunFilters, onCountsChange?: () => void): TaskRunsState {
  const listFilters: RunFilters = { ...filters };
  delete listFilters.page;
  const url = runsUrl(listFilters);
  // The first-page URL and how many pages the held rows came from; new filters go back to one page.
  const pagesRef = useRef({ url, pages: 1 });
  const moreCtl = useRef<AbortController | null>(null);
  const [loadingMore, setLoadingMore] = useState(false);
  const stateRef = useRef<ListState>({ runs: [], total: 0 });
  // The next fetch is a live refetch: no refreshing mark, the rows just change when it lands.
  const quiet = useRef(false);
  const liveTimer = useRef<ReturnType<typeof setTimeout> | null>(null);
  const seen = useRef(new Map<string, { seq: number; status: TaskRun["status"] }>());
  const onCountsRef = useRef(onCountsChange);
  onCountsRef.current = onCountsChange;
  const [state, setState] = useState<ListState>({ runs: [], total: 0 });
  const [choices, setChoices] = useState<{ actors: string[]; contexts: TaskContext[]; counts: RunCounts | null }>({
    actors: [],
    contexts: [],
    counts: null,
  });
  // The list URL the held page answers, null before the first answer.
  const [loadedUrl, setLoadedUrl] = useState<string | null>(null);
  const [refreshing, setRefreshing] = useState(false);
  const [error, setError] = useState<Error | undefined>(undefined);
  const [tick, setTick] = useState(0);
  const filtersRef = useRef<RunFilters>(listFilters);
  filtersRef.current = listFilters;
  stateRef.current = state;

  useEffect(() => {
    const ctl = new AbortController();
    if (!quiet.current) setRefreshing(true);
    quiet.current = false;
    // A next page for other filters, or one this refetch covers, is dropped.
    moreCtl.current?.abort();
    moreCtl.current = null;
    setLoadingMore(false);
    const same = pagesRef.current.url === url;
    if (!same) pagesRef.current = { url, pages: 1 };
    const perPage = Math.min(MAX_PER_PAGE, pagesRef.current.pages * PER_PAGE);
    api
      .get<TaskRunsResponse>(perPage === PER_PAGE ? url : runsUrl(filtersRef.current, perPage), { signal: ctl.signal })
      .then((d) => {
        if (ctl.signal.aborted) return;
        const fresh = (d.runs ?? []).map((r) => ({ ...r, id: String(r.id) }));
        setState((s) => ({ runs: same ? refreshRuns(s.runs, fresh) : fresh, total: d.total ?? 0 }));
        setChoices({ actors: d.actors ?? [], contexts: normalizeContexts(d.contexts), counts: d.counts ?? null });
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
  // One refetch for a burst of frames: the first that changes the counts sets the timer, later ones ride on it.
  const refetchLive = useCallback(() => {
    if (liveTimer.current) return;
    liveTimer.current = setTimeout(() => {
      liveTimer.current = null;
      quiet.current = true;
      setTick((t) => t + 1);
      onCountsRef.current?.();
    }, LIVE_REFETCH_MS);
  }, []);
  useEffect(
    () => () => {
      if (liveTimer.current) clearTimeout(liveTimer.current);
    },
    [],
  );
  const { connected } = useWsMessages(
    "/ws/out",
    (msg) => {
      const frame = parseRunFrame(msg);
      if (!frame) return;
      const { id, seq, status } = frame.run;
      const last = stateRef.current.runs.find((r) => r.id === id) ?? seen.current.get(id);
      if (last && seq <= last.seq) return;
      seen.current.set(id, { seq, status });
      setState((s) => mergeIntoList(s, frame, filtersRef.current));
      if (changesCounts(last, frame)) refetchLive();
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
  const hasMore = settled && state.runs.length < state.total;
  const canMore = useRef(false);
  canMore.current = hasMore && !refreshing;
  useEffect(() => () => moreCtl.current?.abort(), []);
  const loadMore = useCallback(() => {
    if (moreCtl.current || !canMore.current) return;
    const ctl = new AbortController();
    moreCtl.current = ctl;
    const at = pagesRef.current;
    const next = at.pages + 1;
    setLoadingMore(true);
    api
      .get<TaskRunsResponse>(runsUrl({ ...filtersRef.current, page: next }), { signal: ctl.signal })
      .then((d) => {
        if (ctl.signal.aborted || pagesRef.current !== at) return;
        const more = (d.runs ?? []).map((r) => ({ ...r, id: String(r.id) }));
        pagesRef.current = { url: at.url, pages: next };
        setState((s) => ({ runs: appendRuns(s.runs, more), total: d.total ?? s.total }));
      })
      .catch((e: unknown) => {
        if (!ctl.signal.aborted) setError(e instanceof Error ? e : new Error(String(e)));
      })
      .finally(() => {
        if (moreCtl.current !== ctl) return;
        moreCtl.current = null;
        setLoadingMore(false);
      });
  }, []);
  return {
    ...state,
    ...choices,
    loading,
    refreshing: (refreshing || !settled) && !loading,
    settled,
    error,
    connected,
    reload,
    patchRun,
    removeRun,
    hasMore,
    loadingMore,
    loadMore,
  };
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

/**
 * One run with its steps, log lines and report, live. Pass null to skip. Refetches keep the held run.
 * Frames carry the run and its changed steps but not the log lines or the report, so the frame that
 * ends the run fetches the detail once more, quietly (no refreshing), and takes it unless a newer
 * frame got there first.
 */
export function useTaskRun(id: string | null): TaskRunState {
  const [run, setRun] = useState<TaskRun | undefined>(undefined);
  const runRef = useRef(run);
  runRef.current = run;
  const finalFetch = useRef<AbortController | null>(null);
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

  useEffect(() => () => finalFetch.current?.abort(), [id]);

  const fetchFinal = useCallback((runId: string) => {
    finalFetch.current?.abort();
    const ctl = new AbortController();
    finalFetch.current = ctl;
    const attempt = (n: number): void => {
      api
        .get<TaskRunResponse>(runUrl(runId), { signal: ctl.signal })
        .then((d) => {
          if (ctl.signal.aborted) return;
          const fresh: TaskRun = { ...d.run, id: String(d.run.id) };
          setRun((held) => (held && held.id === fresh.id && fresh.seq >= held.seq ? fresh : held));
          setError(undefined);
        })
        .catch((e: unknown) => {
          if (ctl.signal.aborted) return;
          if (n + 1 < FINAL_FETCH_ATTEMPTS) {
            const timer = setTimeout(() => attempt(n + 1), FINAL_FETCH_BACKOFF_MS * 2 ** n);
            ctl.signal.addEventListener("abort", () => clearTimeout(timer), { once: true });
            return;
          }
          // The run ended but its report did not load: say so instead of holding the live view.
          setError(e instanceof Error ? e : new Error(String(e)));
        });
    };
    attempt(0);
  }, []);

  const reload = useCallback(() => setTick((t) => t + 1), []);
  const { connected } = useWsMessages(
    "/ws/out",
    (msg) => {
      const frame = parseRunFrame(msg);
      if (!frame || frame.run.id !== id) return;
      const held = runRef.current;
      if (held && frame.run.seq > held.seq && !ACTIVE_STATUSES.includes(frame.run.status)) fetchFinal(frame.run.id);
      setRun((h) => (h ? applyFrame(h, frame) : h));
    },
    { types: [RUN_UPDATED], enabled: id !== null },
  );
  useReconnectRefetch(connected, reload);

  const loading = id !== null && loadedId !== id;
  return { run: loading ? undefined : run, loading, refreshing: refreshing && !loading, error, connected, reload };
}
