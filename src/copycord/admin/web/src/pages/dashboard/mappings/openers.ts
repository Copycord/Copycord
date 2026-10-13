// Entry points for the modals the mapping cards open. openMappingModal records the
// request in a tiny store that MappingModalHost (mounted by MappingsSection) renders;
// openFilters is the filters modal lane's to fill in.

import type { DashboardMapping, MappingModalOptions } from "../../../types/dashboard-mappings";
import { modeOf } from "./modal/model";
import type { MappingMode } from "./modal/model";
import { mountFilters } from "../filters/open";

export interface MappingModalRequest {
  mapping: DashboardMapping | null;
  mode: MappingMode;
  /** Bumped per open so a reopen remounts the form with fresh state. */
  seq: number;
  opener: HTMLElement | null;
}

let current: MappingModalRequest | null = null;
let seq = 0;
const listeners = new Set<() => void>();

export const subscribeMappingModal = (fn: () => void) => {
  listeners.add(fn);
  return () => {
    listeners.delete(fn);
  };
};
export const getMappingModal = () => current;

function emit(next: MappingModalRequest | null) {
  current = next;
  listeners.forEach((fn) => fn());
}

/** Settings button (mapping given), Clone button (mode "clone") and the Add tile (no mapping). */
export function openMappingModal(mapping?: DashboardMapping | null, opts?: MappingModalOptions): void {
  const opener = typeof document !== "undefined" ? (document.activeElement as HTMLElement | null) : null;
  emit({ mapping: mapping ?? null, mode: modeOf(mapping, opts?.mode), seq: ++seq, opener });
}

export function closeMappingModal(): void {
  const opener = current?.opener;
  emit(null);
  if (opener?.isConnected) opener.focus?.();
}

/** Filters button on a card. */
export function openFilters(mapping: DashboardMapping | null): void {
  mountFilters(mapping);
}
