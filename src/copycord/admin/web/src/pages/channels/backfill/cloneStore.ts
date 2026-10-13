// Per-channel clone state shared by the cards. Each card subscribes to its own
// channel with useCloneState(cid), so a progress event re-renders one card, not the page.

import { useSyncExternalStore } from "react";
import type { CloneState } from "../../../types/channels";

export interface CloneStore {
  get: (cid: string) => CloneState | undefined;
  set: (cid: string, s: CloneState) => void;
  clear: (cid: string) => void;
  /** Replaces every entry; used by the inflight and queue sync. */
  replace: (next: ReadonlyMap<string, CloneState>) => void;
  entries: () => ReadonlyMap<string, CloneState>;
  subscribe: (fn: () => void) => () => void;
  /** Channels that just finished, for the 2 second "Synced" pill. */
  synced: (cid: string) => boolean;
  markSynced: (cid: string) => void;
}

export function createCloneStore(): CloneStore {
  let map = new Map<string, CloneState>();
  let synced = new Set<string>();
  const listeners = new Set<() => void>();
  const emit = () => {
    for (const fn of listeners) fn();
  };
  return {
    get: (cid) => map.get(cid),
    set: (cid, s) => {
      map = new Map(map).set(cid, s);
      emit();
    },
    clear: (cid) => {
      if (!map.has(cid)) return;
      map = new Map(map);
      map.delete(cid);
      emit();
    },
    replace: (next) => {
      map = new Map(next);
      emit();
    },
    entries: () => map,
    subscribe: (fn) => {
      listeners.add(fn);
      return () => listeners.delete(fn);
    },
    synced: (cid) => synced.has(cid),
    markSynced: (cid) => {
      synced = new Set(synced).add(cid);
      emit();
      setTimeout(() => {
        synced = new Set(synced);
        synced.delete(cid);
        emit();
      }, 2000);
    },
  };
}

export function useCloneState(store: CloneStore, cid: string): CloneState | undefined {
  return useSyncExternalStore(store.subscribe, () => store.get(cid), () => undefined);
}

export function useSynced(store: CloneStore, cid: string): boolean {
  return useSyncExternalStore(store.subscribe, () => store.synced(cid), () => false);
}
