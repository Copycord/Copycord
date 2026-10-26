// The sending accounts on the System page: /api/system/senders every 5 s while the tab is visible,
// at once when it comes back. Stale while revalidate: a refetch keeps the last rows on screen and a
// failed one keeps them too. A newer poll aborts the one still running and its late answer is ignored.
//   const { items, enabled, mappings, error, busy } = useSenders();

import { useEffect, useState } from "react";
import { api } from "../../lib/api";
import type { Sender, SenderMapping, SendersResponse } from "../../types/system";

export const SENDERS_URL = "/api/system/senders";
export const SENDERS_POLL_MS = 5000;

export interface SendersState {
  /** Undefined until the first answer. */
  items: Sender[] | undefined;
  /** Token message cloning is on for a mapping; undefined until the first answer. */
  enabled: boolean | undefined;
  /** The mappings the table filters by; undefined until the first answer. */
  mappings: SenderMapping[] | undefined;
  /** The last poll failed. */
  error: boolean;
  /** A poll is in flight. */
  busy: boolean;
}

export function useSenders(): SendersState {
  const [items, setItems] = useState<Sender[] | undefined>(undefined);
  const [enabled, setEnabled] = useState<boolean | undefined>(undefined);
  const [mappings, setMappings] = useState<SenderMapping[] | undefined>(undefined);
  const [error, setError] = useState(false);
  const [busy, setBusy] = useState(false);

  useEffect(() => {
    let alive = true;
    let timer: ReturnType<typeof setTimeout> | undefined;
    let current: AbortController | undefined;

    const schedule = () => {
      clearTimeout(timer);
      if (alive && !document.hidden) timer = setTimeout(() => void tick(), SENDERS_POLL_MS);
    };
    const tick = async () => {
      clearTimeout(timer);
      current?.abort();
      const mine = new AbortController();
      current = mine;
      setBusy(true);
      try {
        const res = await api.get<SendersResponse>(SENDERS_URL, { signal: mine.signal });
        if (!alive || current !== mine) return;
        if (!res?.ok || !Array.isArray(res.items)) throw new Error("bad reply");
        setItems(res.items);
        if (typeof res.enabled === "boolean") setEnabled(res.enabled);
        setMappings(Array.isArray(res.mappings) ? res.mappings : []);
        setError(false);
      } catch {
        if (!alive || current !== mine) return;
        setError(true);
      } finally {
        if (alive && current === mine) {
          setBusy(false);
          schedule();
        }
      }
    };
    const onVisibility = () => {
      if (document.hidden) clearTimeout(timer);
      else void tick();
    };

    document.addEventListener("visibilitychange", onVisibility);
    void tick();
    return () => {
      alive = false;
      clearTimeout(timer);
      current?.abort();
      document.removeEventListener("visibilitychange", onVisibility);
    };
  }, []);

  return { items, enabled, mappings, error, busy };
}
