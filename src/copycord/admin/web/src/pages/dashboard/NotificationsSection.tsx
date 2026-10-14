// The old #notifications-card card: Discord webhook URL, a Test button and one switch per alert.
// Every change is saved 600 ms after the last one.

import { useEffect, useState } from "react";
import { useToast } from "../../components/Toast";
import { api } from "../../lib/api";
import type { NotifEvent, NotifEvents, NotifSettingsReply, NotifTestReply } from "../../types/dashboard-notify";
import { CollapseCard } from "./notify/CollapseCard";
import { DEFAULT_EVENTS, NOTIF_HINT, NOTIF_ROWS, mergeEvents, notifPayload } from "./notify/model";
import { useDebouncedSave } from "./notify/useDebouncedSave";
import { EVENT_ROW, GHOST_XS_GREEN, SECTION_HINT, SETTING_LABEL, SETTING_SUB, Switch, WEBHOOK_INPUT } from "./parts";

/** The App shell test renders without a ToastProvider. */
function useOptionalToast(): ReturnType<typeof useToast> | null {
  try {
    return useToast();
  } catch {
    return null;
  }
}

export function NotificationsSection() {
  const toast = useOptionalToast();
  const [url, setUrl] = useState("");
  const [events, setEvents] = useState<NotifEvents>(DEFAULT_EVENTS);
  const [testing, setTesting] = useState(false);
  const scheduleSave = useDebouncedSave((v: { url: string; events: NotifEvents }) =>
    api.put("/api/notifications/settings", notifPayload(v.url, v.events)),
  );

  useEffect(() => {
    let live = true;
    api
      .get<NotifSettingsReply>("/api/notifications/settings")
      .then((j) => {
        if (!live || !j.ok) return;
        setUrl(j.webhook_url || "");
        setEvents(mergeEvents(j.events));
      })
      .catch((e: unknown) => console.error("Failed to load notification settings:", e));
    return () => {
      live = false;
    };
  }, []);

  function changeUrl(next: string) {
    setUrl(next);
    scheduleSave({ url: next, events });
  }

  function changeEvent(event: NotifEvent, on: boolean) {
    const next = { ...events, [event]: on };
    setEvents(next);
    scheduleSave({ url, events: next });
  }

  async function sendTest() {
    setTesting(true);
    try {
      const j = await api.post<NotifTestReply>("/api/notifications/test");
      if (j.ok) toast?.show("Test notification sent!", { kind: "success" });
      else toast?.show(j.error || "Failed to send test notification", { kind: "error" });
    } catch {
      toast?.show("Network error sending test", { kind: "error" });
    } finally {
      setTesting(false);
    }
  }

  return (
    <CollapseCard
      id="notifications-card"
      title="Notifications"
      docsHref="https://copycord.github.io/Copycord/docs/configuration/advanced#notifications"
      storageKey="cpc.notifications-card-open"
    >
      <p className={SECTION_HINT}>{NOTIF_HINT}</p>
      <div className="notif-webhook-row mb-3.5 flex gap-2">
        <input
          type="text"
          id="notif-webhook-url"
          aria-label="Webhook URL"
          placeholder="https://discord.com/api/webhooks/..."
          spellCheck={false}
          value={url}
          onChange={(e) => changeUrl(e.target.value)}
          className={WEBHOOK_INPUT}
        />
        <button id="notif-test-btn" type="button" disabled={testing} onClick={sendTest} className={GHOST_XS_GREEN}>
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
            <Switch
              id={`notif-toggle-${r.event}`}
              label={r.label}
              checked={events[r.event]}
              onChange={(on) => changeEvent(r.event, on)}
              data={{ "data-event": r.event }}
              className="notif-toggle shrink-0"
            />
          </div>
        ))}
      </div>
    </CollapseCard>
  );
}
