// The Notifications card body, shared by the card and the dashboard skeleton: the hint, the webhook
// box with Test, and one switch per alert. Without events (still loading) the switches are bars.

import type { NotifEvent, NotifEvents } from "../../../types/dashboard-notify";
import { EVENT_ROW, GHOST_XS_GREEN, SECTION_HINT, SETTING_LABEL, SETTING_SUB, Switch, SwitchPending, WEBHOOK_INPUT } from "../parts";
import { NOTIF_HINT, NOTIF_ROWS } from "./model";

export interface NotifFieldsProps {
  url: string;
  events: NotifEvents | null;
  testing?: boolean;
  onUrl?: (url: string) => void;
  onEvent?: (event: NotifEvent, on: boolean) => void;
  onTest?: () => void;
}

export function NotifFields({ url, events, testing = false, onUrl, onEvent, onTest }: NotifFieldsProps) {
  return (
    <>
      <p className={SECTION_HINT}>{NOTIF_HINT}</p>
      <div className="notif-webhook-row mb-3.5 flex gap-2">
        <input
          type="text"
          id="notif-webhook-url"
          aria-label="Webhook URL"
          placeholder="https://discord.com/api/webhooks/..."
          spellCheck={false}
          value={url}
          onChange={(e) => onUrl?.(e.target.value)}
          className={WEBHOOK_INPUT}
        />
        <button id="notif-test-btn" type="button" disabled={testing} onClick={onTest} className={GHOST_XS_GREEN}>
          {testing ? "Sending…" : "Test"}
        </button>
      </div>
      <div className="notif-events-list flex flex-col gap-1.5">
        {NOTIF_ROWS.map((r) => (
          <div key={r.event} className={EVENT_ROW}>
            <div className={SETTING_LABEL}>
              <span className="text-[13px] font-medium">{r.label}</span>
              <span className={SETTING_SUB}>{r.hint}</span>
            </div>
            {events ? (
              <Switch
                id={`notif-toggle-${r.event}`}
                label={r.label}
                checked={events[r.event]}
                onChange={(on) => onEvent?.(r.event, on)}
                data={{ "data-event": r.event }}
                className="notif-toggle shrink-0"
              />
            ) : (
              <SwitchPending />
            )}
          </div>
        ))}
      </div>
    </>
  );
}
