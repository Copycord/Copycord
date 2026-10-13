// #msg-features-modal: the six per mapping message options with a live preview. The toggles
// write straight into the mapping form state, so Save sends them with the rest.

import { useEffect, useRef } from "react";
import type { ReactNode } from "react";
import { createPortal } from "react-dom";
import { Toggle } from "../../../../components/ui";
import type { MappingSettings, MsgFeatureKey } from "../../../../types/dashboard-mappings";
import { MSG_FEATURES, toBool } from "./model";

interface Props {
  settings: MappingSettings;
  onToggle: (key: MsgFeatureKey, on: boolean) => void;
  onClose: () => void;
}

const MENTION = "rounded bg-accent/25 px-0.5 text-accent";

export function MsgFeaturesModal({ settings, onToggle, onClose }: Props) {
  const closeRef = useRef(onClose);
  closeRef.current = onClose;
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") closeRef.current();
    };
    document.addEventListener("keydown", onKey);
    return () => document.removeEventListener("keydown", onKey);
  }, []);

  const on = (k: MsgFeatureKey) => toBool(settings[k]);
  const anon = on("ANONYMIZE_USERS");
  const name = anon ? "SwiftFox123" : "Copycord";
  const footer: ReactNode[] = [];
  if (on("APPEND_TIMESTAMP")) footer.push("Oct 15, 2025 3:42 PM");
  if (on("APPEND_AUTHOR")) footer.push(<strong>{name}</strong>);

  return createPortal(
    <div id="msg-features-modal" className="modal show fixed inset-0 z-50 flex items-center justify-center p-4" aria-hidden="false">
      <div className="modal-backdrop absolute inset-0 bg-black/60" onClick={onClose} />
      <div
        role="dialog"
        aria-modal="true"
        aria-label="Optional Message Features"
        className="msg-features-content relative max-h-[88vh] w-full max-w-lg overflow-y-auto rounded-2xl border border-border bg-card-solid text-text shadow-card"
      >
        <div className="flex items-center justify-between border-b border-border px-4 py-3">
          <h4 className="modal-title text-base font-semibold">Optional Message Features</h4>
          <button
            type="button"
            id="msg-features-close"
            aria-label="Close"
            onClick={onClose}
            className="verify-close rounded-control px-2 py-1 text-muted hover:text-text"
          >
            ✕
          </button>
        </div>
        <div className="flex flex-col gap-4 p-4">
          <p className="text-xs leading-relaxed text-muted">
            Customize how cloned messages appear in the clone server. Toggle features on or off per mapping.
          </p>
          <div className="flex flex-col gap-3">
            {MSG_FEATURES.map((f) => (
              <div key={f.key} className="msg-feature-item" data-key={f.key}>
                <Toggle
                  id={`msgf_${f.key}`}
                  label={f.name}
                  description={f.desc}
                  checked={on(f.key)}
                  onChange={(v) => onToggle(f.key, v)}
                />
              </div>
            ))}
          </div>
          <div className="msg-features-preview">
            <div className="mb-1 text-xs font-semibold uppercase tracking-wide text-muted">Preview</div>
            <div className="msg-features-preview-box rounded-control border border-border bg-black/20 p-3">
              <div className="flex gap-3">
                <img src="/static/logo.png" alt="" draggable={false} className="msg-preview-avatar size-10 rounded-full" />
                <div className="min-w-0 text-sm">
                  <div className="flex items-center gap-2">
                    <span className="msg-preview-username font-semibold">{name}</span>
                    <span className="rounded bg-accent px-1 text-[10px] font-bold text-white">APP</span>
                    <span className="text-xs text-muted">Today at 3:42 PM</span>
                  </div>
                  <div id="msg-features-preview-reply" className="msg-preview-reply text-xs text-muted">
                    {on("TAG_REPLY_MSG") ? (
                      <>
                        ↩ In reply to: <span className="text-accent underline">Copycord is the best!</span>
                      </>
                    ) : null}
                  </div>
                  <div id="msg-features-preview-content" className="msg-preview-content">
                    Hey{" "}
                    <span className={on("DISABLE_EVERYONE_MENTIONS") ? "" : MENTION}>@everyone</span> check this out{" "}
                    <span className={on("DISABLE_ROLE_MENTIONS") ? "" : MENTION}>@Moderator</span>
                  </div>
                  <div id="msg-features-preview-footer" className="msg-preview-footer text-xs text-muted">
                    {footer.map((p, i) => (
                      <span key={i}>
                        {i > 0 ? " · " : ""}
                        {p}
                      </span>
                    ))}
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>,
    document.body,
  );
}
