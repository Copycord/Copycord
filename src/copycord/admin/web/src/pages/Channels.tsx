// Channels page, ported from templates/channels.html and frontend/src/js/channels.js:
// the mapping select, the tree of categories and channels (GET /api/channels)
// with search, filter and sort. Row actions (lane 23) and the orphan scan plug
// into the ChannelTree slots. Cloning messages lives on the Tasks page.
// Like the old page it waits until both Server and Client are connected.

import { useDeferredValue, useEffect, useMemo, useState } from "react";
import { Card } from "../components/ui";
import { ChannelRowsSkeleton } from "./channels/ChannelsSkeleton";

export { ChannelsSkeleton } from "./channels/ChannelsSkeleton";
import { StatusGate } from "../components/ui/StatusGate";
import { useApi } from "../hooks/useApi";
import { useStatusGate } from "../hooks/useStatusGate";
import type { ChannelListResponse, ChannelRow, GuildMappingListResponse } from "../types/channels";
import { useRowActions } from "./channels/actions/useRowActions";
import { ChannelTree } from "./channels/ChannelTree";
import { clearLegacyCloneStorageOnce } from "./channels/legacyStorage";
import { ToolbarSelect } from "./channels/ToolbarSelect";
import { orphanStore, useOrphans } from "./channels/verify/orphanStore";
import { useVerify } from "./channels/verify/useVerify";
import {
  buildCategoryPins,
  buildTree,
  channelsUrl,
  FILTER_OPTIONS,
  mappingLabel,
  NO_ORPHANS,
  nextSort,
  SORT_OPTIONS,
} from "./channels/model";
import type { FilterMode, SortDir, SortMode } from "./channels/model";

const NO_ROWS: ChannelRow[] = [];

export function Channels() {
  // The tree loads once the gate first opens, as afterGateReady did.
  const [booted, setBooted] = useState(false);
  const gate = useStatusGate({ require: "both", onUp: () => setBooted(true) });
  const mappings = useApi<GuildMappingListResponse>("/api/guild-mappings");
  const list = mappings.data?.mappings ?? [];

  // The template preselected the first mapping; "" lists every channel.
  const [picked, setPicked] = useState<string | null>(null);
  const mappingId = picked ?? list[0]?.mapping_id ?? "";
  const mappingsSettled = mappings.data !== undefined || mappings.error !== undefined;
  const channels = useApi<ChannelListResponse>(booted && mappingsSettled ? channelsUrl(mappingId) : null);

  const [query, setQuery] = useState("");
  const [filter, setFilter] = useState<FilterMode>("all");
  const [sort, setSort] = useState<{ sort: SortMode; dir: SortDir }>({ sort: "name", dir: "asc" });
  // Filled by the verify scan (verify/orphanStore); a mapping switch clears it.
  const orphans = useOrphans();
  const setOrphans = orphanStore.set;
  // Asks the client for the orphan list on load, as the old page did.
  useVerify(mappingId);

  // Typing stays responsive on large guilds: the tree rebuilds from the deferred query.
  const deferredQuery = useDeferredValue(query);
  const rows = channels.data?.items;
  const pins = useMemo(() => buildCategoryPins(rows ?? []), [rows]);
  const tree = useMemo(
    () => buildTree(rows ?? [], orphans, pins, { query: deferredQuery, filter, sort: sort.sort, dir: sort.dir }),
    [rows, orphans, pins, deferredQuery, filter, sort],
  );

  useEffect(() => () => orphanStore.set(NO_ORPHANS), []);
  // Drops the bf:* keys the old "Clone messages" UI left in storage.
  useEffect(() => clearLegacyCloneStorageOnce(), []);

  const actions = useRowActions({
    mappingId,
    rows: rows ?? NO_ROWS,
    pins,
    orphans,
    setOrphans: (next) => setOrphans(next),
    reload: channels.reload,
  });

  const onMapping = (id: string) => {
    if (id === mappingId) return;
    setOrphans(NO_ORPHANS);
    setPicked(id);
  };

  const loaded = channels.data !== undefined;

  return (
    <StatusGate gate={gate}>
      {/* The old .container.wide sat 20px in from the body padding at every width; the shell gives 12px below sm. */}
      <Card className="mt-[18px] max-sm:mx-2">
        {/* .card-titlebar: the h2, then one wrapping row of controls (.titlebar-actions). */}
        <header className="mb-1.5 block gap-3">
          {/* The old visible h2, still the page's top heading. */}
          <h2 aria-level={1} className="my-[0.83em] text-[24px] font-bold">
            Channels
          </h2>
          <span className="flex flex-wrap items-center gap-2.5">
            <ToolbarSelect
              id="mapping-select"
              nativeHidden={!booted}
              label="Guild mapping"
              value={mappingId}
              disabled={!list.length}
              pending={!mappingsSettled}
              onChange={onMapping}
              options={
                list.length
                  ? list.map((m) => ({ value: m.mapping_id, label: mappingLabel(m) }))
                  : [{ value: "", label: "(no mappings configured)", disabled: true }]
              }
            />
            <input
              id="ch-search"
              type="text"
              placeholder="Search channels…"
              aria-label="Search channels"
              value={query}
              hidden={!booted}
              onChange={(e) => setQuery(e.target.value)}
              className={
                "w-auto max-w-[260px] min-w-[220px] flex-none rounded-input border border-[#3b365a] bg-[#1c1830] px-2.5 py-1.5 text-[14px] text-[#e6e1ff] " +
                "caret-[#7c5cff] outline-none transition-[border-color,box-shadow,background-color] duration-150 " +
                "placeholder:text-[color-mix(in_srgb,#e6e1ff_55%,transparent)] placeholder:opacity-75 " +
                "hover:border-[#7c5cff] hover:[box-shadow:0_0_0_1px_#7c5cff] focus:border-[#7c5cff] focus:[box-shadow:0_0_0_1px_#7c5cff]"
              }
            />
            <ToolbarSelect
              id="ch-filter"
              nativeHidden={!booted}
              label="Filter"
              value={filter}
              onChange={(v) => setFilter(v as FilterMode)}
              options={FILTER_OPTIONS}
            />
            <ToolbarSelect
              id="ch-sort"
              nativeHidden={!booted}
              label="Sort"
              value={sort.sort}
              onChange={(v) => setSort((cur) => nextSort(cur, v))}
              options={SORT_OPTIONS}
            />
            {/* .btn.btn-ghost-red */}
            <button
              id="orph-delall"
              type="button"
              disabled={actions.deleteAll.disabled}
              onClick={actions.deleteAll.onClick}
              className={
                "flex h-[34px] cursor-pointer items-center justify-center rounded-input border border-border bg-[rgba(255,255,255,0.04)] px-2.5 " +
                "text-[13px] leading-8 font-semibold text-accent transition-colors duration-150 " +
                "hover:border-[color-mix(in_oklab,#2c1d4a_20%,#ef4444_80%)] hover:bg-[rgba(255,255,255,0.08)] hover:text-danger active:bg-[rgba(255,255,255,0.1)] " +
                "focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-white/28 " +
                "disabled:cursor-not-allowed disabled:opacity-60 disabled:hover:border-border disabled:hover:bg-[rgba(255,255,255,0.04)] disabled:hover:text-accent"
              }
            >
              Delete all orphans
            </button>
          </span>
        </header>

        <ChannelTree
          tree={tree}
          hidden={!booted}
          channelActions={actions.channelActions}
          categoryActions={actions.categoryActions}
        />
        {actions.dialogs}

        {channels.loading && !loaded ? <ChannelRowsSkeleton /> : null}

        {channels.error && !loaded ? (
          <div role="alert" className="flex flex-wrap items-center gap-3 px-2.5 py-2 text-sm text-muted">
            <span>The channel list could not be loaded.</span>
            <button
              type="button"
              onClick={channels.reload}
              className="cursor-pointer rounded-lg border border-border px-2.5 py-1 text-accent hover:bg-white/6"
            >
              Try again
            </button>
          </div>
        ) : null}

        <div id="channels-empty" hidden={!(loaded && tree.empty)} className="text-[0.9em] text-muted opacity-80">
          No channels found.
        </div>
      </Card>
    </StatusGate>
  );
}
