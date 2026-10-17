// The Cloning Configuration card (#guild-config-card): the mapping cards from
// GET /api/guild-mappings with search, a status toggle, Clone and Delete per
// card, and the Add tile. Settings, Filters, Clone and Add call the openers in
// mappings/openers.ts.

import { useCallback, useEffect, useState } from "react";
import { cx, useReveal } from "../../components/ui";
import { writeLastCount } from "../../components/ui/lastCount";
import {
  MAPPING_GRID,
  MAPPINGS_COUNT_KEY,
  mappingSkeletons,
  MAPPINGS_HINT,
  MAPPINGS_HINT_TEXT,
  MAPPINGS_SEARCH,
  MappingTileSkeleton,
  NewMappingPlus,
  NEW_MAPPING_TILE,
} from "./DashboardSkeleton";
import { errorText } from "../../lib/api";
import type { DashboardMapping } from "../../types/dashboard-mappings";
import { useNotify, useSafeConfirm } from "../system/providers";
import { MappingCard } from "./mappings/MappingCard";
import { CARD, CARD_DOCS, Chevron, DocLink, SUMMARY, SUMMARY_TITLE } from "./parts";
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
  /** A bot is running; the cards are locked. */
  locked?: boolean;
}

const errText = errorText;

export function MappingsSection({ locked = false }: MappingsSectionProps = {}) {
  const notify = useNotify();
  const confirm = useSafeConfirm();
  const [mappings, setMappings] = useState<DashboardMapping[]>([]);
  const [query, setQuery] = useState("");
  const [busyId, setBusyId] = useState<string | null>(null);
  const [loaded, setLoaded] = useState(false);
  const reveal = useReveal(loaded);

  const refresh = useCallback(async () => {
    try {
      const list = await fetchMappings();
      setMappings(list);
      writeLastCount(MAPPINGS_COUNT_KEY, list.length);
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
      confirmVariant: "ghost-red",
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
        <DocLink href={CARD_DOCS.mappings} />
        <Chevron />
      </summary>

      <div className="mt-3.5">
        <div className="flex flex-wrap items-center justify-between gap-3">
          <p className={MAPPINGS_HINT}>{MAPPINGS_HINT_TEXT}</p>
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
              className={MAPPINGS_SEARCH}
            />
          </div>
        </div>

        <div id="guild-mapping-list" className={cx("guild-mapping-list", MAPPING_GRID)}>
          <button
            id="new-mapping-card"
            type="button"
            aria-label="Add new mapping"
            title="Add new mapping"
            onClick={() => openMappingModal()}
            className={cx(
              "guild-card guild-card--new",
              NEW_MAPPING_TILE,
              "hover:-translate-y-0.5 hover:border-accent hover:bg-accent/8 hover:text-text focus:border-accent focus:bg-accent/8 focus:outline-none",
              locked && "locked pointer-events-none opacity-50 grayscale-[0.4]",
            )}
          >
            <NewMappingPlus />
          </button>
          {!loaded
            ? Array.from({ length: mappingSkeletons() }, (_, i) => <MappingTileSkeleton key={i} />)
            : null}
          {shown.map((m) => (
            <MappingCard
              key={m.mapping_id}
              mapping={m}
              className={reveal}
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
