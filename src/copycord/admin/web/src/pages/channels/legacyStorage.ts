// One-time cleanup of the keys the old Channels "Clone messages" UI left in
// browser storage: the bf:* running sets, task map and cancel marks, and the
// persisted toast receipts keyed by bf:*. Cloning now lives on the Tasks page.

const PREFIXES = ["bf:", "toast:persist:bf:", "toast:receipt:bf:"];

let cleared = false;

/** Removes every old clone UI key from the given storages; returns how many went. */
export function clearLegacyCloneStorage(stores?: Storage[]): number {
  let removed = 0;
  for (const store of stores ?? defaultStores()) {
    try {
      const keys: string[] = [];
      for (let i = 0; i < store.length; i++) {
        const k = store.key(i);
        if (k && PREFIXES.some((p) => k.startsWith(p))) keys.push(k);
      }
      for (const k of keys) store.removeItem(k);
      removed += keys.length;
    } catch {
      // Storage blocked: nothing to clean.
    }
  }
  return removed;
}

/** Runs the cleanup once per page load. */
export function clearLegacyCloneStorageOnce(): void {
  if (cleared) return;
  cleared = true;
  clearLegacyCloneStorage();
}

function defaultStores(): Storage[] {
  const out: Storage[] = [];
  try {
    out.push(localStorage, sessionStorage);
  } catch {
    // Storage blocked.
  }
  return out;
}
