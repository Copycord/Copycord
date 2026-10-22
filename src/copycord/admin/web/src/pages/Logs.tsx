// Event logs page: search, type filter, clear filters, refresh, per row expand and delete,
// clear all, and 50 logs at a time, the next 50 loading as the end of the table scrolls into view.
// Element ids are stable; tests and integrations select by them.

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useToast } from "../components/Toast";
import { useConfirm } from "../components/ui/ConfirmDialog";
import type { ConfirmOptions } from "../components/ui/ConfirmDialog";
import { cx } from "../components/ui/cx";
import { LoadMore } from "../components/ui/LoadMore";
import { useReveal } from "../components/ui/Skeleton";
import { useApi } from "../hooks/useApi";
import { api, errorText } from "../lib/api";
import type {
  EventLog,
  EventLogBulkDeleteResponse,
  EventLogDeleteResponse,
  EventLogsResponse,
  EventLogTypesResponse,
} from "../types/logs";
import { LogsIcon } from "./logs/icons";
import { ToolbarSelect } from "./channels/ToolbarSelect";
import { LogRow } from "./logs/LogRow";
import {
  ACTIONS_TH,
  CLEAR_ALL_BTN,
  COUNT,
  CountBar,
  HEAD_ROW,
  HEADERS,
  LogsHeader,
  LogsLoadingRows,
  REFRESH_BTN,
  SEARCH_INPUT,
  SORT_BTN,
  TABLE,
  TABLE_WRAP,
  TH,
  TOOLBAR,
  TOOLBAR_LEFT,
  TOOLBAR_RIGHT,
  TYPE_BOX,
} from "./logs/LogsSkeleton";
import {
  DEFAULT_SORT,
  appendLogs,
  countText,
  logsUrl,
  nextSort,
  sortLogs,
  typeOptions,
} from "./logs/model";
import type { SortState } from "./logs/model";

export const SEARCH_DEBOUNCE_MS = 300;

/** Calls a context hook, or returns null when its provider is missing (the App shell test renders without them). */
function useOptional<T>(hook: () => T): T | null {
  try {
    return hook();
  } catch {
    return null;
  }
}

const PILL_BTN =
  "inline-flex cursor-pointer items-center gap-1.5 whitespace-nowrap rounded-lg border px-3 py-[5px] text-xs transition-colors " +
  "disabled:cursor-wait disabled:opacity-55 outline-none focus-visible:ring-2 focus-visible:ring-accent/60";

const NO_LOGS: EventLog[] = [];

export function Logs() {
  const toast = useOptional(useToast);
  const sharedConfirm = useOptional(useConfirm);
  const confirm = useCallback(
    (opts: ConfirmOptions) => (sharedConfirm ? sharedConfirm(opts) : Promise.resolve(false)),
    [sharedConfirm],
  );
  const notify = useCallback(
    (message: string, kind: "success" | "error") => toast?.show(message, { kind }),
    [toast],
  );

  const rootRef = useRef<HTMLElement>(null);
  const [type, setType] = useState("");
  const [searchInput, setSearchInput] = useState("");
  const [search, setSearch] = useState("");
  const [sort, setSort] = useState<SortState>(DEFAULT_SORT);
  const [expanded, setExpanded] = useState<ReadonlySet<string>>(() => new Set());
  const [deleting, setDeleting] = useState<ReadonlySet<string>>(() => new Set());
  const [clearBusy, setClearBusy] = useState(false);

  // The first page; scrolling to the end appends the rows after it in `more`, for this url only.
  const url = logsUrl({ page: 1, type, search });
  const list = useApi<EventLogsResponse>(url);
  const [more, setMore] = useState<{ url: string; logs: EventLog[] }>({ url, logs: [] });
  const [loadingMore, setLoadingMore] = useState(false);
  const moreCtl = useRef<AbortController | null>(null);
  const typesApi = useApi<EventLogTypesResponse>("/api/event-log-types");
  const { reload: reloadList } = list;
  const { reload: reloadTypes } = typesApi;

  const reloadAll = useCallback(() => {
    reloadList();
    reloadTypes();
  }, [reloadList, reloadTypes]);

  // Debounce typing; a new search goes back to page 1.
  useEffect(() => {
    const q = searchInput.trim();
    if (q === search) return;
    const t = setTimeout(() => setSearch(q), SEARCH_DEBOUNCE_MS);
    return () => clearTimeout(t);
  }, [searchInput, search]);

  // A new query closes the open rows and starts again from the first page: a next page still out is dropped.
  useEffect(() => {
    setExpanded(new Set());
    moreCtl.current?.abort();
    moreCtl.current = null;
    setLoadingMore(false);
    setMore({ url, logs: [] });
  }, [url]);
  useEffect(() => () => moreCtl.current?.abort(), []);

  // A refresh refetches the first page only: new logs land on top and the scrolled-in rows stay under them.
  const first = useMemo(() => list.data?.logs ?? [], [list.data]);
  const extra = more.url === url ? more.logs : NO_LOGS;
  const logs = useMemo(() => appendLogs(first, extra), [first, extra]);
  const total = list.data?.total ?? 0;
  // Clear all deletes every log, so only an unfiltered empty list means there is nothing to clear.
  const nothingToClear = !type && !search && list.data !== undefined && total === 0;
  const rows = useMemo(() => sortLogs(logs, sort), [logs, sort]);
  const hasMore = Boolean(list.data) && !list.loading && logs.length < total;

  const loadMore = useCallback(() => {
    if (moreCtl.current || !hasMore) return;
    const ctl = new AbortController();
    moreCtl.current = ctl;
    const at = url;
    setLoadingMore(true);
    api
      .get<EventLogsResponse>(logsUrl({ page: 1, type, search, offset: logs.length }), { signal: ctl.signal })
      .then((d) => {
        if (ctl.signal.aborted) return;
        setMore((m) => (m.url === at ? { url: at, logs: appendLogs(m.logs, d.logs ?? []) } : m));
      })
      .catch((e: unknown) => {
        if (!ctl.signal.aborted) notify(errorText(e, "Could not load more logs"), "error");
      })
      .finally(() => {
        if (moreCtl.current !== ctl) return;
        moreCtl.current = null;
        setLoadingMore(false);
      });
  }, [hasMore, url, type, search, logs.length, notify]);

  const allTypes = useMemo(
    () => [...(typesApi.data?.types ?? []), ...(list.data?.types ?? [])],
    [typesApi.data, list.data],
  );
  const typeChoices = useMemo(() => [{ value: "", label: "All Events" }, ...typeOptions(allTypes)], [allTypes]);

  // The selected type disappears once its last log is deleted: fall back to all events.
  useEffect(() => {
    if (type && list.data && !list.loading && !list.data.types.includes(type)) setType("");
  }, [type, list.data, list.loading]);

  const filtersActive = Boolean(type || search);
  // The empty state speaks for the answer on screen: while a new query loads it keeps that answer's wording.
  const answered = useRef({ data: list.data, filtered: filtersActive });
  if (answered.current.data !== list.data) answered.current = { data: list.data, filtered: filtersActive };

  const clearFilters = () => {
    setSearchInput("");
    setSearch("");
    setType("");
  };

  const onToggle = useCallback((id: string) => {
    setExpanded((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  }, []);

  const deleteOne = useCallback(
    async (log: EventLog) => {
      const ok = await confirm({
        title: "Delete Log",
        body: "Delete this event log entry? This cannot be undone.",
        confirmLabel: "Delete",
        confirmVariant: "ghost-red",
      });
      if (!ok) return;
      const id = log.log_id;
      setDeleting((prev) => new Set(prev).add(id));
      try {
        await api.del<EventLogDeleteResponse>(`/api/event-logs/${encodeURIComponent(id)}`);
        setMore((m) => ({ ...m, logs: m.logs.filter((l) => l.log_id !== id) }));
        reloadAll();
      } catch (e) {
        notify(errorText(e, "Could not delete the log"), "error");
      } finally {
        setDeleting((prev) => {
          const next = new Set(prev);
          next.delete(id);
          return next;
        });
      }
    },
    [confirm, notify, reloadAll],
  );

  const clearAll = async () => {
    const ok = await confirm({
      title: "Delete All Logs",
      body: "Are you sure you want to delete all event logs? This cannot be undone.",
      confirmLabel: "Delete All",
      confirmVariant: "ghost-red",
    });
    if (!ok) return;
    setClearBusy(true);
    try {
      await api.del<EventLogBulkDeleteResponse>("/api/event-logs");
      setMore((m) => ({ ...m, logs: [] }));
      reloadAll();
      notify("All logs cleared", "success");
    } catch (e) {
      notify(errorText(e, "Could not clear the logs"), "error");
    } finally {
      setClearBusy(false);
    }
  };

  const firstLoad = list.loading && !list.data;
  const reveal = useReveal(!firstLoad);
  const empty = !firstLoad && !list.error && logs.length === 0;

  return (
    <section
      id="logs-root"
      ref={rootRef}
      aria-label="Event logs"
      className="w-full scroll-mt-4"
    >
      <LogsHeader />

      <div className={TOOLBAR}>
        <div className={TOOLBAR_LEFT}>
          <div className="relative flex items-center">
            <LogsIcon name="search" size={15} className="pointer-events-none absolute left-2.5 text-muted" />
            <input
              type="text"
              id="logs-search"
              className={SEARCH_INPUT}
              placeholder="Search logs"
              aria-label="Search logs"
              autoComplete="off"
              value={searchInput}
              onChange={(e) => setSearchInput(e.target.value)}
            />
          </div>
          <div className={TYPE_BOX}>
            <ToolbarSelect
              id="logs-filter-type"
              label="Filter by type"
              value={type}
              options={typeChoices}
              onChange={setType}
            />
          </div>
        </div>

        <div className={TOOLBAR_RIGHT}>
          <span id="logs-count" className={cx(COUNT, reveal)} aria-live="polite">
            {list.data ? countText(1, logs.length, total) : firstLoad ? <CountBar /> : ""}
          </span>
          {filtersActive ? (
            <button
              type="button"
              id="logs-clear-filters"
              title="Clear Filters"
              className={cx(
                PILL_BTN,
                "border-primary-a30 bg-primary-a10 text-accent-soft",
                "hover:border-primary-a50 hover:bg-primary-a20",
              )}
              onClick={clearFilters}
            >
              <LogsIcon name="funnel" size={14} />
              Clear Filters
            </button>
          ) : null}
          <button
            type="button"
            id="logs-refresh"
            title="Refresh logs"
            aria-label="Refresh logs"
            className={REFRESH_BTN}
            onClick={reloadAll}
          >
            <LogsIcon name="refresh" size={16} className={list.loading ? "animate-spin" : undefined} />
          </button>
          <button
            type="button"
            id="logs-clear-all"
            title={nothingToClear ? "Nothing to clear" : "Clear all logs"}
            aria-label="Clear all logs"
            className={CLEAR_ALL_BTN}
            disabled={clearBusy || nothingToClear}
            aria-disabled={clearBusy || nothingToClear || undefined}
            onClick={() => void clearAll()}
          >
            <LogsIcon name="trash" size={16} />
          </button>
        </div>
      </div>

      {list.error ? (
        <p role="alert" className="mb-3 rounded-lg border border-danger/35 bg-danger/12 px-3.5 py-2.5 text-sm text-danger">
          Could not load event logs: {list.error.message}
        </p>
      ) : null}

      <div className={TABLE_WRAP}>
        <table id="logs-table" className={TABLE} aria-busy={list.loading || undefined}>
          <thead className="sticky top-0 z-[2]">
            <tr className={HEAD_ROW}>
              {HEADERS.map((h) => {
                const active = sort.column === h.key;
                const touched = sort !== DEFAULT_SORT;
                return (
                  <th
                    key={h.key}
                    scope="col"
                    className={cx(TH, "px-3.5 text-left", h.className)}
                    aria-sort={active ? (sort.dir === "asc" ? "ascending" : "descending") : undefined}
                  >
                    <button
                      type="button"
                      data-sort={h.key}
                      className={SORT_BTN}
                      onClick={() => {
                        setSort((s) => nextSort(s, h.key));
                      }}
                    >
                      {h.label}
                      <span
                        aria-hidden="true"
                        className={cx("ml-1 text-[11px] transition-opacity", active && touched ? "text-accent opacity-100" : "opacity-30")}
                      >
                        {active ? (sort.dir === "asc" ? "▲" : "▼") : ""}
                      </span>
                    </button>
                  </th>
                );
              })}
              <th scope="col" className={ACTIONS_TH}>
                <span className="sr-only">Actions</span>
              </th>
            </tr>
          </thead>
          <tbody id="logs-tbody" className={reveal}>
            {firstLoad ? (
              <LogsLoadingRows />
            ) : (
              rows.map((log) => (
                <LogRow
                  key={log.log_id}
                  log={log}
                  expanded={expanded.has(log.log_id)}
                  deleting={deleting.has(log.log_id)}
                  onToggle={onToggle}
                  onDelete={(l) => void deleteOne(l)}
                />
              ))
            )}
          </tbody>
        </table>
        {empty ? (
          <div id="logs-empty" className={cx("flex flex-col items-center justify-center gap-1.5 px-5 py-[50px] text-center text-muted", reveal)}>
            <LogsIcon name="clock" size={40} strokeWidth={1} className="opacity-25" />
            {answered.current.filtered ? (
              <>
                <p className="m-0">No logs match these filters</p>
                <p className="m-0 text-muted/70">Try another search or clear the filters</p>
              </>
            ) : (
              <>
                <p className="m-0">No event logs yet</p>
                <p className="m-0 text-muted/70">Events will appear here as server syncs occur</p>
              </>
            )}
          </div>
        ) : null}
        {firstLoad ? null : <LoadMore hasMore={hasMore} loading={loadingMore} onMore={loadMore} />}
      </div>
    </section>
  );
}
