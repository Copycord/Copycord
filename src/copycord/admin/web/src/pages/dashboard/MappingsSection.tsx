// The Cloning Configuration card from index.html (#guild-config-card): the mapping
// cards from GET /api/guild-mappings with search, a status toggle, Clone and Delete
// per card, and the Add tile. Settings, Filters, Clone and Add call the openers in
// mappings/openers.ts, which the modal lanes fill in.

import { useCallback, useEffect, useState } from "react";
import { cx } from "../../components/ui";
import type { DashboardMapping } from "../../types/dashboard-mappings";
import { useNotify, useSafeConfirm } from "../system/providers";
import { MappingCard } from "./mappings/MappingCard";
import { NewMappingIcon } from "./mappings/icons";
import {
  deleteBody,
  deleteMapping,
  fetchMappings,
  filterMappings,
  statusToast,
  toggleMappingStatus,
} from "./mappings/model";
import { openFilters, openMappingModal } from "./mappings/openers";

export interface MappingsSectionProps {
  /** A bot is running; the cards are locked like the old setGuildCardsLocked. */
  locked?: boolean;
}

const errText = (e: unknown, fallback: string) => (e instanceof Error && e.message ? e.message : fallback);

export function MappingsSection({ locked = false }: MappingsSectionProps = {}) {
  const notify = useNotify();
  const confirm = useSafeConfirm();
  const [mappings, setMappings] = useState<DashboardMapping[]>([]);
  const [query, setQuery] = useState("");
  const [busyId, setBusyId] = useState<string | null>(null);

  const refresh = useCallback(async () => {
    try {
      setMappings(await fetchMappings());
    } catch {
      // The list keeps its last state.
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
    <details
      id="guild-config-card"
      open
      className="rounded-card border border-border bg-linear-to-b from-card to-[rgba(20,10,40,0.45)] p-[18px] text-text"
    >
      <summary className="mb-3 cursor-pointer list-none">
        <h3 className="inline text-base font-semibold">Cloning Configuration</h3>
      </summary>

      <div className="mb-4 flex flex-wrap items-start justify-between gap-x-4 gap-y-3">
        <p className="m-0 max-w-[480px] min-w-[200px] flex-1 text-xs leading-[1.4] text-muted opacity-90">
          Map a source server to a target server to enable cloning for that pair
        </p>
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
          className="min-h-9 w-full max-w-[260px] rounded-control border border-border bg-white/4 px-3 py-1.5 text-sm text-text outline-none placeholder:text-muted focus:border-accent"
        />
      </div>

      <div
        id="guild-mapping-list"
        className="guild-mapping-list grid max-h-80 min-h-[180px] grid-cols-[repeat(auto-fill,minmax(180px,1fr))] gap-3 overflow-y-auto py-2 pr-1 sm:grid-cols-[repeat(auto-fill,minmax(220px,1fr))] sm:gap-4"
      >
        <button
          id="new-mapping-card"
          type="button"
          aria-label="Add new mapping"
          title="Add new mapping"
          onClick={() => openMappingModal()}
          className={cx(
            "guild-card guild-card--new flex min-h-[120px] cursor-pointer items-center justify-center rounded-xl border border-dashed border-border bg-white/2 p-4 text-muted transition",
            "hover:-translate-y-0.5 hover:border-accent hover:bg-accent/8 hover:text-text focus:border-accent focus:bg-accent/8 focus:outline-none",
            locked && "locked pointer-events-none opacity-50 grayscale-[0.4]",
          )}
        >
          <span className="new-card-plus text-accent">
            <NewMappingIcon />
          </span>
        </button>
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
    </details>
  );
}
