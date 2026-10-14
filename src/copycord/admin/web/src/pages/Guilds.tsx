// Guilds page, ported from templates/guilds.html and frontend/src/js/guilds.js:
// the grid of guilds the bot has seen (GET /api/guilds) with search and sort
// direction, a menu per card with "View details" and "Export messages".
// Like the old page it waits until both Server and Client are connected.

import { useCallback, useMemo, useState } from "react";
import { Card, Skeleton } from "../components/ui";
import { StatusGate } from "../components/ui/StatusGate";
import { useApi } from "../hooks/useApi";
import { useStatusGate } from "../hooks/useStatusGate";
import type { GuildListResponse, GuildSummary } from "../types/guilds";
import { ActionMenu } from "./guilds/ActionMenu";
import { ExportDialog } from "./guilds/ExportDialog";
import { GuildCard } from "./guilds/GuildCard";
import { GuildDetailsDialog } from "./guilds/GuildDetailsDialog";
import { sortLabel, visibleGuilds } from "./guilds/model";
import type { Point, SortDir } from "./guilds/model";

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

  return (
    <>
      <StatusGate gate={gate}>
        <Card className="mt-[18px]">
          {/* .card-titlebar: the h2, then the search and the sort direction button (.titlebar-actions). */}
          <header className="mb-1.5 block gap-3">
            {/* The old visible h2, still the page's top heading. */}
            <h2 aria-level={1} className="my-[0.83em] text-[24px] font-bold">
              Guilds
            </h2>
            <span className="flex flex-wrap items-center gap-2.5">
              <input
                id="g-search"
                type="text"
                placeholder="Search guilds…"
                aria-label="Search guilds"
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
              {/* .btn.btn-select */}
              <button
                id="g-sortdir"
                type="button"
                aria-label="Toggle sort direction"
                aria-pressed={dir === "desc"}
                hidden={!booted}
                onClick={() => setDir((d) => (d === "asc" ? "desc" : "asc"))}
                className={
                  "flex min-h-[34px] cursor-pointer items-center justify-center rounded-btn border border-border bg-control px-2.5 py-2 " +
                  "text-[13px] leading-none font-semibold text-text outline-none hover:border-[#7c5cff] hover:shadow-[0_0_0_0.25px_#7c5cff] " +
                  "focus-visible:border-accent focus-visible:shadow-[0_0_0_4px_rgba(167,139,250,0.18)]"
                }
              >
                {sortLabel(dir)}
              </button>
            </span>
          </header>

          <div
            id="guilds-root"
            aria-live="polite"
            hidden={!booted}
            className="grid grid-cols-[repeat(auto-fill,minmax(180px,1fr))] gap-4"
          >
            {guilds.map((g) => (
              <GuildCard key={g.id} guild={g} menuOpen={menu?.guild.id === g.id} onOpenMenu={openMenu} />
            ))}
          </div>

          {loading && !loaded ? (
            <div
              role="status"
              aria-label="Loading guilds"
              aria-busy="true"
              className="grid grid-cols-[repeat(auto-fill,minmax(180px,1fr))] gap-4"
            >
              {Array.from({ length: 8 }, (_, i) => (
                // The .guild-card box: 96 px icon tile, then the name line.
                <div key={i} aria-hidden="true" className="flex flex-col items-center rounded-[12px] border border-border bg-card p-3">
                  <Skeleton.Block width={96} height={96} rounded="rounded-[20px]" className="mt-1.5 mb-2.5" />
                  <Skeleton.Line width="60%" height={14} className="mt-[3px] mb-[12px]" />
                </div>
              ))}
            </div>
          ) : null}

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
        </Card>
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
