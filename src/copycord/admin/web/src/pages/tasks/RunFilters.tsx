// The filters of the Tasks runs list, all kept in the URL so a link reproduces the view:
// ?status= ?context= ?actor= ?q= ?page=. Any change but paging goes back to page 1. ?type= is the
// rail selection, the page scope rather than a filter: it never counts as active and Clear filters keeps it.
//   const { filters, set, clear, active } = useRunFilters();
//   <RunSearch value={filters.q ?? ""} onSearch={(q) => set("q", q)} />

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useSearchParams } from "react-router";
import { Button, Input } from "../../components/ui";
import type { RunFilters, RunStatus, TaskContext, TaskType } from "../../types/tasks";
import { ToolbarSelect } from "../channels/ToolbarSelect";

export const SEARCH_DEBOUNCE_MS = 300;
/** The rail selection: the page scope, not a filter. */
export const SCOPE_PARAM = "type";
/** The filters proper: any of them makes the list active, and Clear filters drops them all. */
export const FILTER_PARAMS = ["status", "context", "actor", "q"] as const;
export type FilterParam = (typeof FILTER_PARAMS)[number] | typeof SCOPE_PARAM;

const STATUSES: { value: RunStatus; label: string }[] = [
  { value: "queued", label: "Queued" },
  { value: "running", label: "Running" },
  { value: "success", label: "Success" },
  { value: "failed", label: "Failed" },
  { value: "cancelled", label: "Cancelled" },
];

export function filtersFromParams(params: URLSearchParams): RunFilters {
  const out: RunFilters = {};
  const type = params.get("type");
  if (type) out.type = type;
  const status = params.get("status");
  if (status && STATUSES.some((s) => s.value === status)) out.status = status as RunStatus;
  const context = params.get("context");
  if (context) out.context = context;
  const actor = params.get("actor");
  if (actor) out.actor = actor;
  const q = params.get("q")?.trim();
  if (q) out.q = q;
  const page = Number(params.get("page"));
  if (Number.isInteger(page) && page > 1) out.page = page;
  return out;
}

export interface RunFiltersApi {
  filters: RunFilters;
  /** Some filter besides paging and the rail type is set. */
  active: boolean;
  set: (key: FilterParam, value: string) => void;
  setPage: (page: number) => void;
  clear: () => void;
}

export function useRunFilters(): RunFiltersApi {
  const [params, setParams] = useSearchParams();
  const key = params.toString();
  const filters = useMemo(() => filtersFromParams(new URLSearchParams(key)), [key]);
  const active = FILTER_PARAMS.some((k) => Boolean(filters[k]));

  const set = useCallback(
    (k: FilterParam, value: string) =>
      setParams(
        (prev) => {
          const next = new URLSearchParams(prev);
          if (value) next.set(k, value);
          else next.delete(k);
          next.delete("page");
          return next;
        },
        { replace: k === "q" },
      ),
    [setParams],
  );
  const setPage = useCallback(
    (page: number) =>
      setParams((prev) => {
        const next = new URLSearchParams(prev);
        if (page > 1) next.set("page", String(page));
        else next.delete("page");
        return next;
      }),
    [setParams],
  );
  // Keeps ?type=: Clear filters on a type page stays on that type.
  const clear = useCallback(
    () =>
      setParams((prev) => {
        const next = new URLSearchParams(prev);
        for (const k of FILTER_PARAMS) next.delete(k);
        next.delete("page");
        return next;
      }),
    [setParams],
  );
  return { filters, active, set, setPage, clear };
}

/** "Search runs": typing settles for 300 ms before the search lands in ?q=. */
export function RunSearch({ value, onSearch }: { value: string; onSearch: (q: string) => void }) {
  const [text, setText] = useState(value);
  const sent = useRef(value);

  // The URL changed from outside (Clear filters, back button): follow it.
  useEffect(() => {
    if (value !== sent.current) {
      sent.current = value;
      setText(value);
    }
  }, [value]);

  useEffect(() => {
    const q = text.trim();
    if (q === sent.current) return;
    const t = window.setTimeout(() => {
      sent.current = q;
      onSearch(q);
    }, SEARCH_DEBOUNCE_MS);
    return () => window.clearTimeout(t);
  }, [text, onSearch]);

  const submit = () => {
    const q = text.trim();
    sent.current = q;
    onSearch(q);
  };

  return (
    <form
      role="search"
      className="mb-4 flex"
      onSubmit={(e) => {
        e.preventDefault();
        submit();
      }}
    >
      <Input
        type="search"
        aria-label="Search runs"
        placeholder="Search runs"
        value={text}
        onChange={(e) => setText(e.target.value)}
        className="min-w-0 flex-1 rounded-r-none"
      />
      <Button type="submit" variant="ghost" attached aria-label="Search" className="rounded-l-none border-l-0">
        <svg aria-hidden viewBox="0 0 16 16" width="14" height="14" fill="none" stroke="currentColor" strokeWidth="1.6">
          <circle cx="7" cy="7" r="4.5" />
          <path d="M10.5 10.5 14 14" strokeLinecap="round" />
        </svg>
      </Button>
    </form>
  );
}

export function contextValue(c: TaskContext): string {
  return c.mapping_id ?? c.label;
}

export interface RunFilterSelectsProps {
  filters: RunFilters;
  types: TaskType[];
  actors: string[];
  contexts: TaskContext[];
  onChange: (key: FilterParam, value: string) => void;
  active: boolean;
  onClear: () => void;
}

/**
 * Task type, Status, Mapping and Actor dropdowns plus Clear filters, for the list frame header.
 * Task type shows on All tasks only; on a type page the rail already names the type.
 */
export function RunFilterSelects({ filters, types, actors, contexts, onChange, active, onClear }: RunFilterSelectsProps) {
  // Keep a value the URL names even when the current page did not list it.
  const actorList: string[] = filters.actor && !actors.includes(filters.actor) ? [...actors, filters.actor] : actors;
  const contextList: TaskContext[] =
    filters.context && !contexts.some((c) => contextValue(c) === filters.context)
      ? [...contexts, { label: filters.context, mapping_id: filters.context }]
      : contexts;

  return (
    <>
      {filters.type ? null : (
        <ToolbarSelect
          id="tasks-filter-type"
          label="Task type"
          value=""
          options={[{ value: "", label: "Task type: All" }, ...types.map((t) => ({ value: t.key, label: t.title }))]}
          onChange={(v) => onChange(SCOPE_PARAM, v)}
        />
      )}
      <ToolbarSelect
        id="tasks-filter-status"
        label="Status"
        value={filters.status ?? ""}
        options={[{ value: "", label: "Status: All" }, ...STATUSES]}
        onChange={(v) => onChange("status", v)}
      />
      <ToolbarSelect
        id="tasks-filter-context"
        label="Mapping"
        value={filters.context ?? ""}
        options={[{ value: "", label: "Mapping: All" }, ...contextList.map((c) => ({ value: contextValue(c), label: c.label }))]}
        onChange={(v) => onChange("context", v)}
      />
      <ToolbarSelect
        id="tasks-filter-actor"
        label="Actor"
        value={filters.actor ?? ""}
        options={[{ value: "", label: "Actor: All" }, ...actorList.map((a) => ({ value: a, label: a }))]}
        onChange={(v) => onChange("actor", v)}
      />
      {active ? (
        <Button variant="ghost" size="sm" onClick={onClear}>
          Clear filters
        </Button>
      ) : null}
    </>
  );
}
