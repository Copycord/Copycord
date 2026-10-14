// The Cloning Configuration card from index.html (#guild-config-card): the mapping
// cards from GET /api/guild-mappings with search, a status toggle, Clone and Delete
// per card, and the Add tile. Settings, Filters, Clone and Add call the openers in
// mappings/openers.ts, which the modal lanes fill in.

import { useCallback, useEffect, useState } from "react";
import { Skeleton, cx } from "../../components/ui";
import type { DashboardMapping } from "../../types/dashboard-mappings";
import { useNotify, useSafeConfirm } from "../system/providers";
import { MappingCard } from "./mappings/MappingCard";
import { NewMappingIcon } from "./mappings/icons";
import { CARD, Chevron, DocLink, SUMMARY, SUMMARY_TITLE } from "./parts";
import {
  deleteBody,
  deleteMapping,
  fetchMappings,
  filterMappings,
  statusToast,
  toggleMappingStatus,
} from "./mappings/model";
import { MappingModalHost } from "./mappings/modal/MappingModalHost";
import { openFilters, openMappingModal } from "./mappings/openers";

export interface MappingsSectionProps {
  /** A bot is running; the cards are locked like the old setGuildCardsLocked. */
  locked?: boolean;
}

const DOCS_URL = "https://copycord.github.io/Copycord/docs/configuration/cloning-options";

const errText = (e: unknown, fallback: string) => (e instanceof Error && e.message ? e.message : fallback);

export function MappingsSection({ locked = false }: MappingsSectionProps = {}) {
  const notify = useNotify();
  const confirm = useSafeConfirm();
  const [mappings, setMappings] = useState<DashboardMapping[]>([]);
  const [query, setQuery] = useState("");
  const [busyId, setBusyId] = useState<string | null>(null);
  const [loaded, setLoaded] = useState(false);

  const refresh = useCallback(async () => {
    try {
      setMappings(await fetchMappings());
    } catch {
      // The list keeps its last state.
    } finally {
      setLoaded(true);
    }
  }, []);

  useEffect(() => {
    void refresh();
  }, [refresh]);

  const toggle = async (m: DashboardMapping) => {
    setBusyId(m.mapping_id);
    try {
      const res = await toggleMappingStatus(m.mapping_id);
      const status = (res.status || "").toLowerCase();
      setMappings((all) => all.map((x) => (x.mapping_id === m.mapping_id ? { ...x, status } : x)));
      notify(statusToast(status), "success");
    } catch (e) {
      notify(errText(e, "Failed to update status"), "error");
    } finally {
      setBusyId(null);
    }
  };

  const remove = async (m: DashboardMapping) => {
    const ok = await confirm({
      title: "Delete guild mapping?",
      body: deleteBody(m),
      confirmLabel: "Delete mapping",
      danger: true,
    });
    if (!ok) return;
    try {
      await deleteMapping(m.mapping_id);
      notify("Mapping deleted.", "success");
      await refresh();
    } catch (e) {
      notify(errText(e, "Delete failed"), "error");
    }
  };

  const shown = filterMappings(mappings, query);

  return (
    <details id="guild-config-card" open className={CARD}>
      <summary className={SUMMARY}>
        <h3 className={SUMMARY_TITLE}>Cloning Configuration</h3>
        <DocLink href={DOCS_URL} />
        <Chevron />
      </summary>

      <div className="mt-3.5">
        <div className="flex flex-wrap items-center justify-between gap-3">
          <p className="m-0 max-w-[480px] min-w-[200px] flex-[1_1_auto] text-xs leading-[1.4] text-muted opacity-90">
            Map a source server to a target server to enable cloning for that pair
          </p>
          <div className="ml-auto">
            <input
              id="mappingSearchInput"
              type="text"
              placeholder="Search mappings…"
              aria-label="Search mappings"
              autoComplete="off"
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              onKeyDown={(e) => {
                if (e.key === "Escape") {
                  e.preventDefault();
                  setQuery("");
                  e.currentTarget.blur();
                }
              }}
              className="ml-0 inline-block min-w-[220px] rounded-lg border border-[#3b365a] bg-[#1c1830] px-2.5 py-1.5 text-[13.3333px] text-[#e6e1ff] caret-[#7c5cff] outline-none transition placeholder:text-[#e6e1ff]/55 focus-visible:border-[#7c5cff]"
            />
          </div>
        </div>

        <div
          id="guild-mapping-list"
          className="guild-mapping-list mt-4 grid max-h-80 min-h-[180px] grid-cols-[repeat(auto-fill,minmax(220px,1fr))] gap-4 overflow-y-auto py-2 pr-1 max-[600px]:grid-cols-[repeat(auto-fill,minmax(180px,1fr))] max-[600px]:gap-3"
        >
          <button
            id="new-mapping-card"
            type="button"
            aria-label="Add new mapping"
            title="Add new mapping"
            onClick={() => openMappingModal()}
            className={cx(
              "guild-card guild-card--new group/new relative flex cursor-pointer items-center justify-center rounded-xl border border-dashed border-border bg-[rgba(255,255,255,0.02)] p-4 text-left text-muted transition max-[600px]:min-h-[120px] max-[600px]:p-3.5",
              "hover:-translate-y-0.5 hover:border-accent hover:bg-accent/8 hover:text-text focus:border-accent focus:bg-accent/8 focus:outline-none",
              locked && "locked pointer-events-none opacity-50 grayscale-[0.4]",
            )}
          >
            <span className="new-card-inner flex flex-col items-center text-center leading-none select-none">
              <span className="new-card-plus leading-[0] text-accent select-none group-hover/new:text-text group-focus/new:text-text">
                <NewMappingIcon />
              </span>
            </span>
          </button>
          {!loaded
            ? [0, 1].map((i) => <Skeleton.Card key={i} height="auto" testId="mapping-skeleton" className="min-h-[150px]" />)
            : null}
          {shown.map((m) => (
            <MappingCard
              key={m.mapping_id}
              mapping={m}
              locked={locked}
              busy={busyId === m.mapping_id}
              onEdit={openMappingModal}
              onFilters={openFilters}
              onToggle={(x) => void toggle(x)}
              onClone={(x) => openMappingModal(x, { mode: "clone" })}
              onDelete={(x) => void remove(x)}
            />
          ))}
        </div>
      </div>
      <MappingModalHost onSaved={refresh} />
    </details>
  );
}
