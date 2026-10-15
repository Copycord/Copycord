// The old #notifications-card card: Discord webhook URL, a Test button and one switch per alert.
// Every change is saved 600 ms after the last one.

import { useEffect, useState } from "react";
import { useToast } from "../../components/Toast";
import { api } from "../../lib/api";
import type { NotifEvent, NotifEvents, NotifSettingsReply, NotifTestReply } from "../../types/dashboard-notify";
import { CollapseCard } from "./notify/CollapseCard";
import { DEFAULT_EVENTS, mergeEvents, notifPayload } from "./notify/model";
import { NotifFields } from "./notify/NotifFields";
import { useDebouncedSave } from "./notify/useDebouncedSave";
import { CARD_DOCS, CARD_OPEN_KEYS } from "./parts";

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
  // The switches are bars, like the page skeleton's, until the settings answer.
  const [loaded, setLoaded] = useState(false);
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
      .catch((e: unknown) => console.error("Failed to load notification settings:", e))
      .finally(() => live && setLoaded(true));
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
      docsHref={CARD_DOCS.notifications}
      storageKey={CARD_OPEN_KEYS.notifications}
    >
      <NotifFields
        url={url}
        events={loaded ? events : null}
        testing={testing}
        onUrl={changeUrl}
        onEvent={changeEvent}
        onTest={() => void sendTest()}
      />
    </CollapseCard>
  );
}
