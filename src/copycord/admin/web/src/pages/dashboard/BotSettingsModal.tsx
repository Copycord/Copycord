// The Bot settings popup from the gear in the Bots card: a Status dropdown for the
// server bot and one for the client bot. Loads and saves only SERVER_STATUS and
// CLIENT_STATUS; a running bot takes the new status right away.

import { useEffect, useRef, useState } from "react";
import { Modal } from "../../components/Modal";
import { Button } from "../../components/ui";
import { CONTROL_CLASS } from "../../components/ui/Field";
import { errorText } from "../../lib/api";
import { ToolbarSelect } from "../channels/ToolbarSelect";
import { useNotify } from "../system/providers";
import type { BotPresenceKey, BotPresences } from "./botSettings";
import { DEFAULT_PRESENCES, loadPresences, PRESENCE_OPTIONS, presencesDirty, savedMessage, savePresences, toPresence } from "./botSettings";

const SECTIONS: Array<{ key: BotPresenceKey; title: string; id: string }> = [
  { key: "SERVER_STATUS", title: "Server bot", id: "bot-status-server" },
  { key: "CLIENT_STATUS", title: "Client bot", id: "bot-status-client" },
];

export function BotIcon({ className = "size-[18px]" }: { className?: string }) {
  return (
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" className={className} aria-hidden>
      <rect x="4" y="8" width="16" height="11" rx="3" />
      <path d="M12 4v4M9 13h.01M15 13h.01" />
    </svg>
  );
}

export interface BotSettingsModalProps {
  open: boolean;
  onClose: () => void;
}

export function BotSettingsModal({ open, onClose }: BotSettingsModalProps) {
  const notify = useNotify();
  const [saved, setSaved] = useState<BotPresences>(DEFAULT_PRESENCES);
  const [draft, setDraft] = useState<BotPresences>(DEFAULT_PRESENCES);
  const [loading, setLoading] = useState(false);
  const [saving, setSaving] = useState(false);
  // useNotify hands out a new function each render; the load runs once per open.
  const notifyRef = useRef(notify);
  notifyRef.current = notify;

  useEffect(() => {
    if (!open) return;
    let live = true;
    setLoading(true);
    loadPresences()
      .then((p) => {
        if (!live) return;
        setSaved(p);
        setDraft(p);
      })
      .catch((e) => live && notifyRef.current(errorText(e, "Failed to load bot settings"), "error"))
      .finally(() => live && setLoading(false));
    return () => {
      live = false;
    };
  }, [open]);

  const dirty = presencesDirty(draft, saved);

  const save = async () => {
    setSaving(true);
    try {
      const res = await savePresences(draft);
      setSaved(draft);
      notify(savedMessage(res), "success");
      onClose();
    } catch (e) {
      notify(errorText(e, "Failed to save bot settings"), "error");
    } finally {
      setSaving(false);
    }
  };

  return (
    <Modal
      open={open}
      onClose={onClose}
      id="bot-settings-modal"
      title="Bot settings"
      footer={
        <>
          <Button id="bot-settings-cancel" variant="ghost" onClick={onClose}>
            Cancel
          </Button>
          <Button id="bot-settings-save" variant="primary" disabled={!dirty || loading} loading={saving} onClick={() => void save()}>
            Save
          </Button>
        </>
      }
      className="w-[min(460px,calc(100vw-32px))]"
    >
      <div className="flex flex-col gap-3">
        {SECTIONS.map(({ key, title, id }) => (
          <section key={key} aria-label={title} className="rounded-control border border-border bg-wash-4 p-3.5">
            <div className="mb-3 flex items-center gap-2.5">
              <span className="inline-flex size-8 items-center justify-center rounded-lg bg-accent-a12 text-accent">
                <BotIcon />
              </span>
              <b className="text-sm font-semibold">{title}</b>
            </div>
            <div className="flex items-center justify-between gap-3.5 max-[599px]:flex-col max-[599px]:items-stretch max-[599px]:gap-2.5">
              <label htmlFor={id} className="text-[13.5px] font-medium">
                Status
              </label>
              <div className="w-44 max-[599px]:w-full">
                <ToolbarSelect
                  id={id}
                  variant="field"
                  value={draft[key]}
                  options={PRESENCE_OPTIONS}
                  pending={loading}
                  pendingLabel="Online"
                  toggleClassName={CONTROL_CLASS}
                  checkSelected
                  onChange={(v) => setDraft((d) => ({ ...d, [key]: toPresence(v) }))}
                />
              </div>
            </div>
          </section>
        ))}
      </div>
    </Modal>
  );
}
