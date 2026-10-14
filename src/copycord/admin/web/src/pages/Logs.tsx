// Event logs page, the SPA version of templates/logs.html and logs.js:
// search, type filter, clear filters, refresh, per row expand and delete,
// clear all, and 50 per page pagination.
// The element ids match the old page so the e2e parity spec runs unchanged.

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useToast } from "../components/Toast";
import { useConfirm } from "../components/ui/ConfirmDialog";
import type { ConfirmOptions } from "../components/ui/ConfirmDialog";
import { cx } from "../components/ui/cx";
import { SkeletonSection } from "../components/ui/Skeleton";
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
import { COLS, LogRow } from "./logs/LogRow";
import {
  DEFAULT_SORT,
  countText,
  logsUrl,
  nextSort,
  sortLogs,
  totalPages,
  typeOptions,
} from "./logs/model";
import type { SortColumn, SortState } from "./logs/model";
import { Pagination } from "./logs/Pagination";

export const SEARCH_DEBOUNCE_MS = 300;

/** Calls a context hook, or returns null when its provider is missing (the App shell test renders without them). */
function useOptional<T>(hook: () => T): T | null {
  try {
    return hook();
  } catch {
    return null;
  }
}

const CONTROL =
  "rounded-lg border border-border bg-[rgba(20,10,40,0.6)] text-[13px] text-text outline-none transition-colors focus:border-accent";
const ICON_BTN =
  "inline-flex size-8 shrink-0 cursor-pointer items-center justify-center rounded-lg border border-border bg-transparent px-1.5 py-px text-muted " +
  "transition-colors disabled:cursor-wait disabled:opacity-55 focus-visible:ring-2 focus-visible:ring-accent/60 outline-none";
const ICON_BTN_DANGER =
  "enabled:hover:border-[rgba(239,68,68,0.3)] enabled:hover:bg-[rgba(239,68,68,0.1)] enabled:hover:text-[#f87171]";
const PILL_BTN =
  "inline-flex cursor-pointer items-center gap-1.5 whitespace-nowrap rounded-lg border px-3 py-[5px] text-xs transition-colors " +
  "disabled:cursor-wait disabled:opacity-55 outline-none focus-visible:ring-2 focus-visible:ring-accent/60";

const HEADERS: Array<{ key: SortColumn; label: string; className: string }> = [
  { key: "created_at", label: "Timestamp", className: "w-[200px] min-w-[200px] max-md:w-[130px]" },
  { key: "event_type", label: "Type", className: "w-[160px]" },
  { key: "guild_name", label: "Guild", className: "w-[140px] max-md:hidden" },
  { key: "details", label: "Details", className: "" },
];

// Padding and alignment are per header so no two utilities fight over one property.
const TH = "relative border-b border-b-border py-2.5 text-[11px] leading-[normal] font-semibold tracking-[0.5px] whitespace-nowrap text-muted uppercase";

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
  const [page, setPage] = useState(1);
  const [type, setType] = useState("");
  const [searchInput, setSearchInput] = useState("");
  const [search, setSearch] = useState("");
  const [sort, setSort] = useState<SortState>(DEFAULT_SORT);
  const [expanded, setExpanded] = useState<ReadonlySet<string>>(() => new Set());
  const [deleting, setDeleting] = useState<ReadonlySet<string>>(() => new Set());
  const [clearBusy, setClearBusy] = useState(false);

  const url = logsUrl({ page, type, search });
  const list = useApi<EventLogsResponse>(url);
  const typesApi = useApi<EventLogTypesResponse>("/api/event-log-types");
  const { reload: reloadList } = list;
  const { reload: reloadTypes } = typesApi;

  const reloadAll = useCallback(() => {
    reloadList();
    reloadTypes();
  }, [reloadList, reloadTypes]);

  // Debounce typing like the old page; a new search goes back to page 1.
  useEffect(() => {
    const q = searchInput.trim();
    if (q === search) return;
    const t = setTimeout(() => {
      setSearch(q);
      setPage(1);
    }, SEARCH_DEBOUNCE_MS);
    return () => clearTimeout(t);
  }, [searchInput, search]);

  // A new query closes the open rows.
  useEffect(() => {
    setExpanded(new Set());
  }, [url]);

  const logs = useMemo(() => list.data?.logs ?? [], [list.data]);
  const total = list.data?.total ?? 0;
  const pages = totalPages(total);
  const rows = useMemo(() => sortLogs(logs, sort), [logs, sort]);

  // After deletes the current page can run past the end; step back to the last page.
  useEffect(() => {
    if (!list.data || list.loading) return;
    if (list.data.logs.length === 0 && list.data.total > 0 && page > 1) setPage(totalPages(list.data.total));
  }, [list.data, list.loading, page]);

  const allTypes = useMemo(
    () => [...(typesApi.data?.types ?? []), ...(list.data?.types ?? [])],
    [typesApi.data, list.data],
  );
  const typeChoices = useMemo(() => [{ value: "", label: "All Events" }, ...typeOptions(allTypes)], [allTypes]);

  // The selected type disappears once its last log is deleted: fall back to all events.
  useEffect(() => {
    if (type && list.data && !list.loading && !list.data.types.includes(type)) {
      setType("");
      setPage(1);
    }
  }, [type, list.data, list.loading]);

  const filtersActive = Boolean(type || search);

  const goPage = (p: number) => {
    if (p < 1 || p > pages || p === page) return;
    setPage(p);
    rootRef.current?.scrollIntoView?.({ behavior: "smooth", block: "start" });
  };

  const clearFilters = () => {
    setSearchInput("");
    setSearch("");
    setType("");
    setPage(1);
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
      setPage(1);
      reloadAll();
      notify("All logs cleared", "success");
    } catch (e) {
      notify(errorText(e, "Could not clear the logs"), "error");
    } finally {
      setClearBusy(false);
    }
  };

  const firstLoad = list.loading && !list.data;
  const empty = !firstLoad && !list.error && logs.length === 0;

  // The table height leaves room for the header and toolbar so the page fits the screen. On phones the
  // table keeps the old fixed layout without Guild, so Details runs past the edge and the wrap scrolls.
  return (
    <section
      id="logs-root"
      ref={rootRef}
      aria-label="Event logs"
      className="w-full scroll-mt-4"
    >
      <h1 className="sr-only">Logs</h1>
      <div className="my-3 flex items-center gap-3">
        <h2 className="m-0 text-[22px] font-bold text-text">Event Logs</h2>
      </div>

      <div
        className={cx(
          "mb-3 flex flex-wrap items-center justify-between gap-2.5 rounded-xl border border-border px-3.5 py-2.5",
          "bg-linear-135 from-[rgba(27,16,48,0.6)] to-[rgba(20,10,40,0.4)] max-md:flex-col max-md:items-stretch",
        )}
      >
        <div className="flex flex-wrap items-center gap-2.5 max-md:flex-col">
          <div className="relative flex items-center">
            <LogsIcon name="search" size={15} className="pointer-events-none absolute left-2.5 text-muted" />
            <input
              type="text"
              id="logs-search"
              className={cx(CONTROL, "w-[200px] py-[7px] pr-3 pl-[30px] placeholder:text-muted/60 max-md:w-full")}
              placeholder="Search logs"
              aria-label="Search logs"
              autoComplete="off"
              value={searchInput}
              onChange={(e) => setSearchInput(e.target.value)}
            />
          </div>
          <div className="w-[300px] min-w-[300px] [&_select]:w-[300px] [&_select]:max-w-[300px] [&>span]:block">
            <ToolbarSelect
              id="logs-filter-type"
              label="Filter by type"
              value={type}
              options={typeChoices}
              onChange={(v) => {
                setType(v);
                setPage(1);
              }}
            />
          </div>
        </div>

        <div className="flex items-center gap-3">
          <span id="logs-count" className="text-xs whitespace-nowrap text-muted" aria-live="polite">
            {list.data ? countText(page, logs.length, total) : ""}
          </span>
          {filtersActive ? (
            <button
              type="button"
              id="logs-clear-filters"
              title="Clear Filters"
              className={cx(
                PILL_BTN,
                "border-[rgba(139,92,246,0.3)] bg-[rgba(139,92,246,0.1)] text-[#c4b5fd]",
                "hover:border-[rgba(139,92,246,0.5)] hover:bg-[rgba(139,92,246,0.2)]",
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
            className={cx(ICON_BTN, "enabled:hover:border-accent/40 enabled:hover:bg-[rgba(139,92,246,0.1)] enabled:hover:text-accent")}
            onClick={reloadAll}
          >
            <LogsIcon name="refresh" size={16} className={list.loading ? "animate-spin" : undefined} />
          </button>
          <button
            type="button"
            id="logs-clear-all"
            title="Clear all logs"
            aria-label="Clear all logs"
            className={cx(ICON_BTN, ICON_BTN_DANGER)}
            disabled={clearBusy}
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

      <div
        className={cx(
          "h-[calc(100vh-280px)] min-h-[400px] overflow-auto rounded-xl border border-border",
          "bg-linear-to-b from-[rgba(27,16,48,0.5)] to-[rgba(15,8,30,0.6)] max-md:h-[calc(100vh-320px)] max-md:min-h-[300px]",
        )}
      >
        <table id="logs-table" className="mt-2 w-full table-fixed border-collapse border-[gray]" aria-busy={list.loading || undefined}>
          <thead className="sticky top-0 z-[2]">
            <tr className="border-[gray] bg-[rgba(20,10,40,0.95)] backdrop-blur-sm">
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
                      className="cursor-pointer uppercase outline-none select-none hover:text-text focus-visible:text-text"
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
              <th scope="col" className={cx(TH, "w-11 px-3.5 text-center")}>
                <span className="sr-only">Actions</span>
              </th>
            </tr>
          </thead>
          <tbody id="logs-tbody">
            {firstLoad ? (
              <tr>
                <td colSpan={COLS} className="px-3.5 py-2">
                  <SkeletonSection rows={8} rowHeight={41} label="Loading event logs" />
                </td>
              </tr>
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
          <div id="logs-empty" className="flex flex-col items-center justify-center gap-1.5 px-5 py-[50px] text-center text-muted">
            <LogsIcon name="clock" size={40} strokeWidth={1} className="opacity-25" />
            {filtersActive ? (
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
      </div>

      <Pagination page={page} pages={pages} onPage={goPage} />
    </section>
  );
}
