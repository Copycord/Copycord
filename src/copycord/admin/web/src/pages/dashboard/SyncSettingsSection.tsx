// The old #sync-settings-card card: startup delay, inter-guild delay and randomize order.
// Every change is saved 600 ms after the last one.

import { useEffect, useState } from "react";
import { api } from "../../lib/api";
import type { SyncForm, SyncSettingsReply } from "../../types/dashboard-notify";
import { CollapseCard } from "./notify/CollapseCard";
import { DEFAULT_SYNC, syncFormFrom, syncPayload } from "./notify/model";
import { SyncFields } from "./notify/SyncFields";
import { useDebouncedSave } from "./notify/useDebouncedSave";
import { CARD_DOCS, CARD_OPEN_KEYS } from "./parts";

export function SyncSettingsSection() {
  const [form, setForm] = useState<SyncForm>(DEFAULT_SYNC);
  // The boxes stay empty and the switch a bar, like the page skeleton's, until the settings answer.
  const [loaded, setLoaded] = useState(false);
  const scheduleSave = useDebouncedSave((f: SyncForm) => api.put("/api/sync/settings", syncPayload(f)));

  useEffect(() => {
    let live = true;
    api
      .get<SyncSettingsReply>("/api/sync/settings")
      .then((j) => {
        if (live && j.ok && j.settings) setForm(syncFormFrom(j.settings));
      })
      .catch((e: unknown) => console.error("Failed to load sync settings:", e))
      .finally(() => live && setLoaded(true));
    return () => {
      live = false;
    };
  }, []);

  function change(patch: Partial<SyncForm>) {
    const next = { ...form, ...patch };
    setForm(next);
    scheduleSave(next);
  }

  return (
    <CollapseCard id="sync-settings-card" title="Sync Settings" docsHref={CARD_DOCS.sync} storageKey={CARD_OPEN_KEYS.sync}>
      <SyncFields form={loaded ? form : null} onChange={change} />
    </CollapseCard>
  );
}
