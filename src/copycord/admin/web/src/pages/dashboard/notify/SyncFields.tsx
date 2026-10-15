// The Sync Settings card body, shared by the card and the dashboard skeleton: the hint, the two
// delay boxes and the Randomize Order switch. Without a form (still loading) the boxes are empty
// and the switch is a bar.

import type { SyncForm } from "../../../types/dashboard-notify";
import { PS_FIELD, PS_FIELD_INPUT, PS_UNIT, SECTION_HINT, SETTING_LABEL, SETTING_SUB, Switch, SwitchPending } from "../parts";
import { SYNC_DELAYS, SYNC_HINT, SYNC_RANDOMIZE } from "./model";

export function SyncFields({ form, onChange }: { form: SyncForm | null; onChange?: (patch: Partial<SyncForm>) => void }) {
  return (
    <>
      <p className={SECTION_HINT}>{SYNC_HINT}</p>
      <div className="proxy-settings-fields mt-2.5 flex flex-col gap-3">
        {SYNC_DELAYS.map((d) => (
          // label.proxy-settings-field around the number box, as the old card had it.
          <label key={d.key} className={PS_FIELD}>
            <div className={SETTING_LABEL}>
              <span className="font-medium">{d.label}</span>
              <span className={SETTING_SUB}>{d.hint}</span>
            </div>
            <div className="flex shrink-0 items-center gap-1.5">
              <input
                type="number"
                id={d.id}
                min={d.min}
                max={d.max}
                value={form ? form[d.key] : ""}
                readOnly={!form}
                tabIndex={form ? undefined : -1}
                onChange={(e) => onChange?.({ [d.key]: e.target.value })}
                className={PS_FIELD_INPUT}
              />
              <span className={PS_UNIT}>sec</span>
            </div>
          </label>
        ))}
        <div className={PS_FIELD}>
          <div className={SETTING_LABEL}>
            <span className="font-medium">{SYNC_RANDOMIZE.label}</span>
            <span className={SETTING_SUB}>{SYNC_RANDOMIZE.hint}</span>
          </div>
          {form ? (
            <Switch
              id="sync-randomize"
              label={SYNC_RANDOMIZE.label}
              checked={form.randomize}
              onChange={(on) => onChange?.({ randomize: on })}
              className="m-0 shrink-0"
            />
          ) : (
            <SwitchPending className="m-0" />
          )}
        </div>
      </div>
    </>
  );
}
