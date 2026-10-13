// The old #notifications-card card: Discord webhook URL, a Test button and one switch per alert.
// Every change is saved 600 ms after the last one.

import { useEffect, useState } from "react";
import { useToast } from "../../components/Toast";
import { Button, Input, Toggle } from "../../components/ui";
import { api } from "../../lib/api";
import type { NotifEvent, NotifEvents, NotifSettingsReply, NotifTestReply } from "../../types/dashboard-notify";
import { CollapseCard } from "./notify/CollapseCard";
import { DEFAULT_EVENTS, NOTIF_ROWS, mergeEvents, notifPayload } from "./notify/model";
import { useDebouncedSave } from "./notify/useDebouncedSave";

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
      <p className="text-sm text-text-2">
        Get notified via Discord webhook when something goes wrong — client goes offline, proxies die, tokens get
        revoked.
      </p>
      <div className="flex items-center gap-2">
        <Input
          type="text"
          id="notif-webhook-url"
          aria-label="Webhook URL"
          placeholder="https://discord.com/api/webhooks/..."
          spellCheck={false}
          value={url}
          onChange={(e) => changeUrl(e.target.value)}
        />
        <Button id="notif-test-btn" variant="ghost" size="sm" disabled={testing} onClick={sendTest}>
          {testing ? "Sending…" : "Test"}
        </Button>
      </div>
      <div className="flex flex-col gap-3">
        {NOTIF_ROWS.map((r) => (
          <Toggle
            key={r.event}
            id={`notif-toggle-${r.event}`}
            label={r.label}
            description={r.hint}
            checked={events[r.event]}
            onChange={(on) => changeEvent(r.event, on)}
          />
        ))}
      </div>
    </CollapseCard>
  );
}
