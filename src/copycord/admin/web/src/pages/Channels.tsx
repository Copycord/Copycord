// Channels page, ported from templates/channels.html and frontend/src/js/channels.js:
// the mapping select, the tree of categories and channels (GET /api/channels)
// with search, filter and sort. Row actions (lane 23) and the orphan scan plug
// into the ChannelTree slots. Cloning messages lives on the Tasks page.
// Like the old page it waits until both Server and Client are connected.

import { useDeferredValue, useEffect, useMemo, useState } from "react";
import { Card, useReveal } from "../components/ui";
import { useLastShape } from "../components/ui/lastCount";
import {
  CHANNELS_CARD,
  ChannelRowsSkeleton,
  ChannelsTitle,
  CONTROLS,
  DELETE_ALL_BTN,
  SEARCH_INPUT,
  TREE_SHAPE_KEY,
} from "./channels/ChannelsSkeleton";

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
  const reveal = useReveal(loaded);
  // The tree as a fresh visit shows it (no search, all channels, by name) sizes the next loading tree.
  const pristine = !deferredQuery && filter === "all" && sort.sort === "name" && sort.dir === "asc";
  useLastShape(TREE_SHAPE_KEY, loaded && pristine ? tree.sections.map((s) => s.channels.length) : undefined);

  return (
    <StatusGate gate={gate}>
      <Card className={CHANNELS_CARD}>
        <header className="mb-1.5 block gap-3">
          <ChannelsTitle />
          <span className={CONTROLS}>
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
              onChange={(e) => setQuery(e.target.value)}
              className={SEARCH_INPUT}
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
            <button
              id="orph-delall"
              type="button"
              disabled={actions.deleteAll.disabled}
              onClick={actions.deleteAll.onClick}
              className={DELETE_ALL_BTN}
            >
              Delete all orphans
            </button>
          </span>
        </header>

        <ChannelTree
          tree={tree}
          hidden={!booted}
          busy={channels.loading && loaded}
          className={reveal}
          channelActions={actions.channelActions}
          categoryActions={actions.categoryActions}
        />
        {actions.dialogs}

        {!loaded && !channels.error ? <ChannelRowsSkeleton /> : null}

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
