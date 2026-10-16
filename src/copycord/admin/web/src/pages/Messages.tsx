// Messages page: every cloned message in one table. Backend search (debounced, superseded requests
// aborted), filters and sort; rows load 50 at a time as the sentinel under the table scrolls into
// view (keyset cursor). A refetch keeps the current rows on screen until the new first page lands.

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { cx } from "../components/ui/cx";
import { useReveal } from "../components/ui/Skeleton";
import { Skeleton } from "../components/ui/Skeleton";
import { api, errorText } from "../lib/api";
import type { MessageFacetsResponse, MessageRow as Row, MessagesResponse } from "../types/messages";
import { ColumnsMenu } from "./messages/ColumnsMenu";
import { MessagesIcon } from "./messages/icons";
import { MessageRow, PREVIEW_BTN } from "./messages/MessageRow";
import {
  MessagesHead,
  MessagesHeader,
  MessagesLoadingRows,
  MessagesToolbar,
  RESET_BTN,
  TABLE,
  TABLE_WRAP,
} from "./messages/MessagesSkeleton";
import { COUNT } from "./logs/LogsSkeleton";
import {
  DEFAULT_COLUMNS,
  NO_FILTERS,
  SEARCH_DEBOUNCE_MS,
  filtersActive,
  loadColumns,
  messagesUrl,
  rowKey,
  saveColumns,
  toggleColumn,
} from "./messages/model";
import type { ColumnKey, Filters, SortKey } from "./messages/model";
import { PreviewModal } from "./messages/PreviewModal";

export { SEARCH_DEBOUNCE_MS };

function isAbort(e: unknown): boolean {
  return e instanceof DOMException && e.name === "AbortError";
}

export function Messages() {
  const [columns, setColumns] = useState<ColumnKey[]>(() => loadColumns());
  const [searchInput, setSearchInput] = useState("");
  const [filters, setFilters] = useState<Filters>(NO_FILTERS);
  const [sort, setSort] = useState<SortKey>("newest");
  const [reloadTick, setReloadTick] = useState(0);

  const [rows, setRows] = useState<Row[]>([]);
  const [cursor, setCursor] = useState<string | null>(null);
  /** The first page for the current query has answered (rows or an error). */
  const [settled, setSettled] = useState(false);
  /** Some answer has landed since mount: skeleton rows only show before it. */
  const [everLoaded, setEverLoaded] = useState(false);
  const [busy, setBusy] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [loadingMore, setLoadingMore] = useState(false);
  const [moreError, setMoreError] = useState<string | null>(null);
  const [preview, setPreview] = useState<Row | null>(null);
  const [facets, setFacets] = useState<MessageFacetsResponse | null>(null);
  const [now, setNow] = useState(() => Date.now());

  const wrapRef = useRef<HTMLDivElement>(null);
  const sentinelRef = useRef<HTMLDivElement>(null);
  const moreCtl = useRef<AbortController | null>(null);

  const listUrl = messagesUrl(filters, sort);

  // Debounce typing; the committed query starts a new first page.
  useEffect(() => {
    const q = searchInput.trim();
    if (q === filters.q) return;
    const t = setTimeout(() => setFilters((f) => ({ ...f, q })), SEARCH_DEBOUNCE_MS);
    return () => clearTimeout(t);
  }, [searchInput, filters.q]);

  // First page for the current query. Old rows stay until it lands; a newer query aborts this one.
  useEffect(() => {
    const ctl = new AbortController();
    moreCtl.current?.abort();
    moreCtl.current = null;
    setLoadingMore(false);
    setMoreError(null);
    setBusy(true);
    setSettled(false);
    api.get<MessagesResponse>(listUrl, { signal: ctl.signal }).then(
      (data) => {
        if (ctl.signal.aborted) return;
        setRows(data.items);
        setCursor(data.next_cursor);
        setError(null);
        setBusy(false);
        setSettled(true);
        setEverLoaded(true);
        setNow(Date.now());
        if (wrapRef.current) wrapRef.current.scrollTop = 0;
      },
      (e: unknown) => {
        if (ctl.signal.aborted || isAbort(e)) return;
        setError(errorText(e, "Could not load messages"));
        setBusy(false);
        setSettled(true);
        setEverLoaded(true);
      },
    );
    return () => ctl.abort();
  }, [listUrl, reloadTick]);

  // Facets for the mapping and channel filters, once.
  useEffect(() => {
    const ctl = new AbortController();
    api.get<MessageFacetsResponse>("/api/messages/facets", { signal: ctl.signal }).then(
      (data) => !ctl.signal.aborted && setFacets(data),
      () => !ctl.signal.aborted && setFacets({ ok: false, mappings: [], channels: [] }),
    );
    return () => ctl.abort();
  }, []);

  const state = useRef({ cursor, busy, loadingMore, moreError, listUrl });
  state.current = { cursor, busy, loadingMore, moreError, listUrl };

  const loadMore = useCallback(() => {
    const s = state.current;
    if (!s.cursor || s.busy || s.loadingMore) return;
    const ctl = new AbortController();
    moreCtl.current = ctl;
    setLoadingMore(true);
    setMoreError(null);
    api.get<MessagesResponse>(`${s.listUrl}&cursor=${encodeURIComponent(s.cursor)}`, { signal: ctl.signal }).then(
      (data) => {
        if (ctl.signal.aborted) return;
        setRows((prev) => {
          const seen = new Set(prev.map(rowKey));
          return [...prev, ...data.items.filter((r) => !seen.has(rowKey(r)))];
        });
        setCursor(data.next_cursor);
        setLoadingMore(false);
      },
      (e: unknown) => {
        if (ctl.signal.aborted || isAbort(e)) return;
        setMoreError(errorText(e, "Could not load more messages"));
        setLoadingMore(false);
      },
    );
  }, []);

  // The sentinel under the last row asks for the next page as it nears the bottom of the wrap.
  useEffect(() => {
    const el = sentinelRef.current;
    if (!el || typeof IntersectionObserver === "undefined") return;
    const io = new IntersectionObserver(
      (entries) => {
        if (entries.some((e) => e.isIntersecting) && !state.current.moreError) loadMore();
      },
      { root: wrapRef.current, rootMargin: "0px 0px 300px 0px" },
    );
    io.observe(el);
    return () => io.disconnect();
  }, [loadMore, everLoaded]);

  // A page that landed while the sentinel was already in view (short pages, tall screens) asks again.
  useEffect(() => {
    if (!cursor || busy || loadingMore || moreError) return;
    const wrap = wrapRef.current;
    const el = sentinelRef.current;
    if (!wrap || !el || typeof IntersectionObserver === "undefined") return;
    if (wrap.scrollHeight <= wrap.clientHeight + 300) loadMore();
  }, [cursor, busy, loadingMore, moreError, rows.length, loadMore]);

  useEffect(() => {
    saveColumns(columns);
  }, [columns]);

  const onFilter = useCallback((patch: Partial<Filters>) => setFilters((f) => ({ ...f, ...patch })), []);
  const resetFilters = useCallback(() => {
    setSearchInput("");
    setFilters(NO_FILTERS);
  }, []);
  const active = filtersActive(filters) || searchInput.trim() !== "";

  const mappingOptions = useMemo(
    () => [
      { value: "", label: "All mappings" },
      ...(facets?.mappings ?? []).map((m) => ({ value: m.mapping_id, label: m.mapping_name ?? m.mapping_id })),
    ],
    [facets],
  );
  const channelOptions = useMemo(() => {
    const mapping = facets?.mappings.find((m) => m.mapping_id === filters.mapping);
    const byId = new Map<string, { name: string | null; count: number }>();
    for (const c of facets?.channels ?? []) {
      if (mapping && c.cloned_guild_id !== mapping.cloned_guild_id) continue;
      const prev = byId.get(c.original_channel_id);
      byId.set(c.original_channel_id, { name: prev?.name ?? c.name, count: (prev?.count ?? 0) + c.count });
    }
    const opts = [...byId.entries()]
      .map(([id, c]) => ({ value: id, label: `${c.name ? `#${c.name}` : id} (${c.count.toLocaleString()})` }))
      .sort((a, b) => a.label.localeCompare(b.label));
    return [{ value: "", label: "All channels" }, ...opts];
  }, [facets, filters.mapping]);

  const firstLoad = !everLoaded;
  const reveal = useReveal(!firstLoad);
  const empty = settled && !error && rows.length === 0;
  const visible = useMemo(() => columns, [columns]);
  const colSpan = visible.length + 1;

  const countText = firstLoad ? (
    <Skeleton.Text className={COUNT} width={90} />
  ) : rows.length ? (
    `${rows.length.toLocaleString()}${cursor ? "+" : ""} message${rows.length === 1 && !cursor ? "" : "s"}`
  ) : (
    ""
  );

  return (
    <section id="messages-root" aria-label="Cloned messages" className="w-full">
      <MessagesHeader count={countText} />

      <MessagesToolbar
        searchInput={searchInput}
        onSearch={setSearchInput}
        filters={filters}
        onFilter={onFilter}
        sort={sort}
        onSort={setSort}
        mappingOptions={mappingOptions}
        channelOptions={channelOptions}
        facetsPending={!facets}
        columnsMenu={
          <ColumnsMenu
            columns={columns}
            onToggle={(k) => setColumns((c) => toggleColumn(c, k))}
            onReset={() => setColumns([...DEFAULT_COLUMNS])}
          />
        }
        reset={
          active ? (
            <button type="button" id="messages-reset" className={RESET_BTN} onClick={resetFilters}>
              <MessagesIcon name="funnel" size={14} />
              Reset filters
            </button>
          ) : null
        }
      />

      {error && rows.length > 0 ? (
        <p role="alert" className="mb-3 flex items-center justify-between gap-3 rounded-lg border border-danger/35 bg-danger/12 px-3.5 py-2 text-sm text-danger">
          <span>Could not refresh messages: {error}</span>
          <button type="button" className={PREVIEW_BTN} onClick={() => setReloadTick((t) => t + 1)}>
            Retry
          </button>
        </p>
      ) : null}

      <div ref={wrapRef} className={cx(TABLE_WRAP, busy && everLoaded && "opacity-70")} aria-busy={busy || undefined}>
        <table id="messages-table" className={TABLE}>
          <MessagesHead columns={visible} />
          <tbody className={reveal}>
            {firstLoad ? (
              <MessagesLoadingRows columns={visible} />
            ) : (
              rows.map((r) => <MessageRow key={rowKey(r)} row={r} columns={visible} now={now} onPreview={setPreview} />)
            )}
            {!firstLoad && rows.length > 0 ? (
              <tr>
                <td colSpan={colSpan} className="px-3 py-3 text-center text-xs text-muted">
                  {loadingMore ? (
                    <span role="status" className="inline-flex items-center gap-2">
                      <span className="size-3 animate-spin rounded-full border-2 border-muted/40 border-t-muted" aria-hidden="true" />
                      Loading more messages
                    </span>
                  ) : moreError ? (
                    <span role="alert" className="inline-flex items-center gap-3 text-danger">
                      {moreError}
                      <button type="button" className={PREVIEW_BTN} onClick={loadMore}>
                        Retry
                      </button>
                    </span>
                  ) : cursor ? (
                    <span aria-hidden="true">&nbsp;</span>
                  ) : (
                    <span id="messages-end">End of list, {rows.length.toLocaleString()} messages</span>
                  )}
                </td>
              </tr>
            ) : null}
          </tbody>
        </table>
        <div ref={sentinelRef} data-testid="messages-sentinel" className="h-px" />

        {empty ? (
          <div id="messages-empty" className={cx("flex flex-col items-center justify-center gap-1.5 px-5 py-[50px] text-center text-muted", reveal)}>
            <MessagesIcon name="inbox" size={40} strokeWidth={1} className="opacity-25" />
            {active ? (
              <>
                <p className="m-0">No messages match</p>
                <p className="m-0 text-muted/70">Try another search or reset the filters</p>
                <button type="button" className={cx(RESET_BTN, "mt-2")} onClick={resetFilters}>
                  Reset filters
                </button>
              </>
            ) : (
              <>
                <p className="m-0">No cloned messages yet</p>
                <p className="m-0 text-muted/70">Messages appear here as Copycord clones them</p>
              </>
            )}
          </div>
        ) : null}

        {error && rows.length === 0 ? (
          <div role="alert" className="flex flex-col items-center gap-2 px-5 py-[50px] text-center">
            <p className="m-0 text-sm text-danger">Could not load messages: {error}</p>
            <button type="button" className={PREVIEW_BTN} onClick={() => setReloadTick((t) => t + 1)}>
              Retry
            </button>
          </div>
        ) : null}
      </div>

      <PreviewModal row={preview} onClose={() => setPreview(null)} />
    </section>
  );
}
