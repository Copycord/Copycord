// The old #sync-settings-card card: startup delay, inter-guild delay and randomize order.
// Every change is saved 600 ms after the last one.

import { useEffect, useState } from "react";
import { api } from "../../lib/api";
import type { SyncForm, SyncSettingsReply } from "../../types/dashboard-notify";
import { CollapseCard } from "./notify/CollapseCard";
import { DEFAULT_SYNC, syncFormFrom, syncPayload } from "./notify/model";
import { useDebouncedSave } from "./notify/useDebouncedSave";
import { PS_FIELD, PS_FIELD_INPUT, PS_UNIT, SECTION_HINT, SETTING_LABEL, SETTING_SUB, Switch } from "./parts";

interface DelayProps {
  id: string;
  label: string;
  hint: string;
  min: number;
  max: number;
  value: string;
  onChange: (v: string) => void;
}

/** label.proxy-settings-field around the number box, as the old card had it. */
function Delay({ id, label, hint, min, max, value, onChange }: DelayProps) {
  return (
    <label className={PS_FIELD}>
      <div className={SETTING_LABEL}>
        <span className="font-medium">{label}</span>
        <span className={SETTING_SUB}>{hint}</span>
      </div>
      <div className="flex shrink-0 items-center gap-1.5">
        <input
          type="number"
          id={id}
          min={min}
          max={max}
          value={value}
          onChange={(e) => onChange(e.target.value)}
          className={PS_FIELD_INPUT}
        />
        <span className={PS_UNIT}>sec</span>
      </div>
    </label>
  );
}

export function SyncSettingsSection() {
  const [form, setForm] = useState<SyncForm>(DEFAULT_SYNC);
  const scheduleSave = useDebouncedSave((f: SyncForm) => api.put("/api/sync/settings", syncPayload(f)));

  useEffect(() => {
    let live = true;
    api
      .get<SyncSettingsReply>("/api/sync/settings")
      .then((j) => {
        if (live && j.ok && j.settings) setForm(syncFormFrom(j.settings));
      })
      .catch((e: unknown) => console.error("Failed to load sync settings:", e));
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
    <CollapseCard
      id="sync-settings-card"
      title="Sync Settings"
      docsHref="https://copycord.github.io/Copycord/docs/configuration/advanced#sync-timing"
      storageKey="cpc.sync-card-open"
    >
      <p className={SECTION_HINT}>
        When the client starts, it scans each server you're cloning and sends the full structure (channels, roles,
        emojis) to the bot so it knows what to sync. These settings control how fast that happens. Slower is always
        safer if you have many servers. Changes apply on next client restart.
      </p>
      <div className="proxy-settings-fields mt-2.5 flex flex-col gap-3">
        <Delay
          id="sync-startup-delay"
          label="Startup Delay"
          hint="Wait time after login before sending sitemaps"
          min={1}
          max={120}
          value={form.startupDelay}
          onChange={(v) => change({ startupDelay: v })}
        />
        <Delay
          id="sync-guild-delay"
          label="Inter-Guild Delay"
          hint="Pause between each server sitemap to help avoid rate limits."
          min={1}
          max={30}
          value={form.guildDelay}
          onChange={(v) => change({ guildDelay: v })}
        />
        <div className={PS_FIELD}>
          <div className={SETTING_LABEL}>
            <span className="font-medium">Randomize Order</span>
            <span className={SETTING_SUB}>Process servers in random order each startup to avoid predictable patterns</span>
          </div>
          <Switch
            id="sync-randomize"
            label="Randomize Order"
            checked={form.randomize}
            onChange={(on) => change({ randomize: on })}
            className="m-0 shrink-0"
          />
        </div>
      </div>
    </CollapseCard>
  );
}
