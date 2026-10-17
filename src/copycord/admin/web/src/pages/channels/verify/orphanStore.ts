// The orphan list shared by the verify scan and the row actions.
// useVerify() writes it from the "orphans" verify event; Channels.tsx reads it
// with useOrphans() and hands it to useRowActions, which only prunes it on the
// "deleted" event (that event is handled in pages/channels/actions, not here).

import { useSyncExternalStore } from "react";
import type { Orphans } from "../../../types/channels";
import { NO_ORPHANS } from "../model";

type Listener = () => void;

export interface OrphanStore {
  get: () => Orphans;
  set: (next: Orphans | ((cur: Orphans) => Orphans)) => void;
  subscribe: (fn: Listener) => () => void;
}

export function createOrphanStore(): OrphanStore {
  let cur: Orphans = NO_ORPHANS;
  const listeners = new Set<Listener>();
  return {
    get: () => cur,
    set: (next) => {
      const value = typeof next === "function" ? next(cur) : next;
      if (value === cur) return;
      cur = value;
      for (const fn of listeners) fn();
    },
    subscribe: (fn) => {
      listeners.add(fn);
      return () => listeners.delete(fn);
    },
  };
}

/** The page-wide store; one Channels page is mounted at a time. */
export const orphanStore = createOrphanStore();

export function useOrphans(store: OrphanStore = orphanStore): Orphans {
  return useSyncExternalStore(store.subscribe, store.get, store.get);
}
