// Entry points for the modals the mapping cards open. The mapping modal and the
// filters modal are later lanes: each replaces the body of its opener, the cards
// already call them with the same arguments the old page used.

import type { DashboardMapping, MappingModalOptions } from "../../../types/dashboard-mappings";

/** Settings button (mapping given), Clone button (mode "clone") and the Add tile (no mapping). */
export function openMappingModal(_mapping?: DashboardMapping | null, _opts?: MappingModalOptions): void {
  // Filled in by the mapping modal lane.
}

/** Filters button on a card. */
export function openFilters(_mapping: DashboardMapping | null): void {
  // Filled in by the filters modal lane.
}
