// Guilds page, ported from templates/guilds.html and frontend/src/js/guilds.js:
// the grid of guilds the bot has seen (GET /api/guilds) with search and sort
// direction, a menu per card with "View details" and "Export messages".
// Like the old page it waits until both Server and Client are connected.

import { useCallback, useMemo, useState } from "react";
import { PageFrame, cx, useReveal } from "../components/ui";
import { StatusGate } from "../components/ui/StatusGate";
import { useLastCount } from "../components/ui/lastCount";
import { useApi } from "../hooks/useApi";
import { useStatusGate } from "../hooks/useStatusGate";
import type { GuildListResponse, GuildSummary } from "../types/guilds";
import { ActionMenu } from "./guilds/ActionMenu";
import { ExportDialog } from "./guilds/ExportDialog";
import { GuildCard } from "./guilds/GuildCard";
import { GuildDetailsDialog } from "./guilds/GuildDetailsDialog";
import { sortLabel, visibleGuilds } from "./guilds/model";
import type { Point, SortDir } from "./guilds/model";
import {
  CONTROLS,
  GUILD_GRID,
  GUILDS_COUNT_KEY,
  GuildTileSkeletons,
  GuildsTitle,
  SEARCH_INPUT,
  SORT_BTN,
} from "./guilds/GuildsSkeleton";

export { GuildsSkeleton } from "./guilds/GuildsSkeleton";

interface MenuState {
  guild: GuildSummary;
  at: Point;
  anchor: HTMLElement;
}

export function Guilds() {
  // The list loads once, the first time the gate opens, as afterGateReady did.
  const [booted, setBooted] = useState(false);
  const gate = useStatusGate({ require: "both", onUp: () => setBooted(true) });
  const { data, error, loading, reload } = useApi<GuildListResponse>(booted ? "/api/guilds" : null);

  const [query, setQuery] = useState("");
  const [dir, setDir] = useState<SortDir>("asc");
  const [menu, setMenu] = useState<MenuState | null>(null);
  const [details, setDetails] = useState<GuildSummary | null>(null);
  const [exporting, setExporting] = useState<GuildSummary | null>(null);

  const guilds = useMemo(() => visibleGuilds(data?.items ?? [], query, dir), [data, query, dir]);

  const openMenu = useCallback((guild: GuildSummary, at: Point, anchor: HTMLElement) => {
    setMenu((m) => (m?.guild.id === guild.id ? null : { guild, at, anchor }));
  }, []);

  const pick = (open: (g: GuildSummary) => void) => () => {
    if (!menu) return;
    // Focus the card first so the dialog hands focus back to it on close.
    menu.anchor.focus({ preventScroll: true });
    open(menu.guild);
  };

  const loaded = data !== undefined;
  const reveal = useReveal(loaded);
  useLastCount(GUILDS_COUNT_KEY, data ? (data.items ?? []).length : undefined);

  return (
    <>
      <StatusGate gate={gate}>
        <PageFrame>
          {/* .card-titlebar: the h2, then the search and the sort direction button (.titlebar-actions). */}
          <header className="mb-1.5 block gap-3">
            <GuildsTitle />
            <span className={CONTROLS}>
              <input
                id="g-search"
                type="text"
                placeholder="Search guilds…"
                aria-label="Search guilds"
                value={query}
                onChange={(e) => setQuery(e.target.value)}
                className={SEARCH_INPUT}
              />
              <button
                id="g-sortdir"
                type="button"
                aria-label="Toggle sort direction"
                aria-pressed={dir === "desc"}
                onClick={() => setDir((d) => (d === "asc" ? "desc" : "asc"))}
                className={SORT_BTN}
              >
                {sortLabel(dir)}
              </button>
            </span>
          </header>

          <div
            id="guilds-root"
            aria-live="polite"
            hidden={!booted}
            aria-busy={(loading && loaded) || undefined}
            className={cx(GUILD_GRID, reveal)}
          >
            {guilds.map((g) => (
              <GuildCard key={g.id} guild={g} menuOpen={menu?.guild.id === g.id} onOpenMenu={openMenu} />
            ))}
          </div>

          {!loaded && !error ? <GuildTileSkeletons /> : null}

          {error && !loaded ? (
            <div role="alert" className="flex flex-wrap items-center gap-3 px-2.5 py-2 text-sm text-muted">
              <span>The guild list could not be loaded.</span>
              <button
                type="button"
                onClick={reload}
                className="cursor-pointer rounded-lg border border-border px-2.5 py-1 text-accent hover:bg-white/6"
              >
                Try again
              </button>
            </div>
          ) : null}

          <div id="guilds-empty" hidden={!(loaded && guilds.length === 0)} className="text-[0.9em] text-muted opacity-80">
            No guilds found.
          </div>
        </PageFrame>
      </StatusGate>

      {menu ? (
        <ActionMenu
          at={menu.at}
          anchor={menu.anchor}
          label={`Actions for ${menu.guild.name}`}
          onClose={() => setMenu(null)}
          items={[
            { key: "view", label: "View details", onSelect: pick(setDetails) },
            { key: "export", label: "Export messages", onSelect: pick(setExporting) },
          ]}
        />
      ) : null}

      {details ? <GuildDetailsDialog guild={details} onClose={() => setDetails(null)} /> : null}
      {exporting ? <ExportDialog key={exporting.id} guild={exporting} onClose={() => setExporting(null)} /> : null}
    </>
  );
}
