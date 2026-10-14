// Channels page, ported from templates/channels.html and frontend/src/js/channels.js:
// the mapping select, the tree of categories and channels (GET /api/channels)
// with search, filter and sort. Row actions (lane 23), the orphan scan and the
// backfill modal plug into the ChannelTree slots.
// Like the old page it waits until both Server and Client are connected.

import { useCallback, useDeferredValue, useEffect, useMemo, useState } from "react";
import { Button, Card, Input, Select, Spinner } from "../components/ui";
import { StatusGate } from "../components/ui/StatusGate";
import { useApi } from "../hooks/useApi";
import { useStatusGate } from "../hooks/useStatusGate";
import type { ChannelListResponse, ChannelRow, GuildMappingListResponse } from "../types/channels";
import { useRowActions } from "./channels/actions/useRowActions";
import { useBackfill } from "./channels/backfill/useBackfill";
import { ChannelTree } from "./channels/ChannelTree";
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

  const nameOf = useCallback(
    (cid: string) => {
      const r = rows?.find((x) => x.original_channel_id === cid);
      return r ? r.clone_channel_name || r.original_channel_name : "";
    },
    [rows],
  );
  // The clone action and live clone progress sit on the channel menu and the card status slot.
  const backfill = useBackfill({ mappingId, nameOf });

  const actions = useRowActions({
    mappingId,
    rows: rows ?? NO_ROWS,
    pins,
    orphans,
    setOrphans: (next) => setOrphans(next),
    reload: channels.reload,
    extraItems: backfill.items,
  });

  const onMapping = (id: string) => {
    setOrphans(NO_ORPHANS);
    setPicked(id);
  };

  const loaded = channels.data !== undefined;

  return (
    <StatusGate gate={gate}>
      <Card className="shadow-card">
        <header className="mb-1.5 flex flex-col items-start gap-3">
          <h1 className="text-xl font-bold tracking-wide">Channels</h1>
          <div className="flex flex-wrap items-center gap-2.5">
            <Select
              id="mapping-select"
              aria-label="Guild mapping"
              value={mappingId}
              disabled={!list.length}
              onChange={(e) => onMapping(e.target.value)}
              className="w-auto! max-w-64"
            >
              {list.length ? (
                list.map((m) => (
                  <option key={m.mapping_id} value={m.mapping_id} className="bg-card-solid">
                    {mappingLabel(m)}
                  </option>
                ))
              ) : (
                <option value="" disabled className="bg-card-solid">
                  (no mappings configured)
                </option>
              )}
            </Select>
            <Input
              id="ch-search"
              type="text"
              placeholder="Search channels…"
              aria-label="Search channels"
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              className="max-w-60"
            />
            <Select
              id="ch-filter"
              aria-label="Filter"
              value={filter}
              onChange={(e) => setFilter(e.target.value as FilterMode)}
              options={[...FILTER_OPTIONS]}
              className="w-auto!"
            />
            <Select
              id="ch-sort"
              aria-label="Sort"
              value={sort.sort}
              onChange={(e) => setSort((cur) => nextSort(cur, e.target.value))}
              options={[...SORT_OPTIONS]}
              className="w-auto!"
            />
            <Button
              id="orph-delall"
              type="button"
              variant="danger"
              disabled={actions.deleteAll.disabled}
              onClick={actions.deleteAll.onClick}
            >
              Delete all orphans
            </Button>
          </div>
        </header>

        <ChannelTree
          tree={tree}
          channelActions={actions.channelActions}
          categoryActions={actions.categoryActions}
          channelStatus={backfill.channelStatus}
        />
        {actions.dialogs}
        {backfill.dialogs}

        {channels.loading && !loaded ? (
          <div className="flex justify-center py-6">
            <Spinner label="Loading channels" />
          </div>
        ) : null}

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

        {loaded && tree.empty ? (
          <div id="channels-empty" className="rounded-lg px-2.5 py-2 text-[0.9em] text-muted opacity-80">
            No channels found.
          </div>
        ) : null}
      </Card>
    </StatusGate>
  );
}
