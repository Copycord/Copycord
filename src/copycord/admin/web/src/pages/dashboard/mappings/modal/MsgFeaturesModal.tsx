// #msg-features-modal: the seven per mapping message options with a live preview. The toggles
// write straight into the mapping form state, so Save sends them with the rest.

import { useEffect, useRef } from "react";
import type { ReactNode } from "react";
import { createPortal } from "react-dom";
import { MODAL_BACKDROP, MODAL_CARD, MODAL_CLOSE, MODAL_HEADER, MODAL_OVERLAY, MODAL_TITLE, useScrollLock } from "../../../../components/Modal";
import type { MappingSettings, MsgFeatureKey } from "../../../../types/dashboard-mappings";
import { MSG_FEATURES, toBool } from "./model";
import { Switch } from "./parts";

interface Props {
  settings: MappingSettings;
  onToggle: (key: MsgFeatureKey, on: boolean) => void;
  onClose: () => void;
}

// .msg-mention and .msg-mention-plain
const MENTION = "cursor-default rounded-[3px] bg-discord-a30 px-[3px] font-medium text-discord-mention";
const PLAIN = "text-discord-text";

export function MsgFeaturesModal({ settings, onToggle, onClose }: Props) {
  useScrollLock(true);
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
    <div id="msg-features-modal" className={`modal show ${MODAL_OVERLAY} z-50!`} aria-hidden="false">
      <div className={`modal-backdrop ${MODAL_BACKDROP}`} onClick={onClose} />
      <div
        role="dialog"
        aria-modal="true"
        aria-label="Optional Message Features"
        className={`modal-content msg-features-content ${MODAL_CARD} w-[min(620px,92vw)]`}
      >
        <div className={`modal-header ${MODAL_HEADER}`}>
          <h4 className={`modal-title ${MODAL_TITLE}`}>Optional Message Features</h4>
          <button type="button" id="msg-features-close" aria-label="Close" onClick={onClose} className={`verify-close ${MODAL_CLOSE}`}>
            ✕
          </button>
        </div>
        <div className="msg-features-body max-h-[70vh] overflow-y-auto px-5 pb-5">
          <p className="msg-features-desc mt-0 mb-4 text-xs leading-normal text-muted">
            Customize how cloned messages appear in the clone server. Toggle features on or off per mapping.
          </p>
          <div className="msg-features-list mb-[18px] grid grid-cols-2 gap-2.5">
            {MSG_FEATURES.map((f) => (
              <div
                key={f.key}
                className="msg-feature-item flex items-center justify-between gap-2.5 rounded-input border border-border bg-white/[0.02] px-2.5 py-2 transition-colors duration-100 hover:border-white/10 hover:bg-white/4"
                data-key={f.key}
              >
                <div className="msg-feature-info flex min-w-0 flex-col gap-0.5">
                  <span id={`msgf_${f.key}-name`} className="msg-feature-name text-[13px] font-semibold text-text">
                    {f.name}
                  </span>
                  <span className="msg-feature-desc text-[11px] leading-[1.4] text-muted">{f.desc}</span>
                </div>
                <Switch
                  id={`msgf_${f.key}`}
                  labelledBy={`msgf_${f.key}-name`}
                  className="msg-feature-toggle"
                  checked={on(f.key)}
                  onChange={(v) => onToggle(f.key, v)}
                />
              </div>
            ))}
          </div>
          <div className="msg-features-preview">
            <div className="msg-features-preview-label mb-1.5 text-[11px] font-semibold tracking-[0.5px] text-muted uppercase">
              Preview
            </div>
            <div className="msg-features-preview-box rounded-btn border border-white/6 bg-discord-bg px-3.5 py-3 leading-normal">
              <div className="msg-preview-msg flex items-start gap-3">
                <img
                  src="/static/logo.png"
                  alt=""
                  draggable={false}
                  className="msg-preview-avatar size-10 min-w-10 shrink-0 rounded-full object-cover"
                />
                <div className="msg-preview-body min-w-0 flex-1">
                  <div className="msg-preview-header mb-0.5 flex items-baseline gap-2">
                    <span className="msg-preview-username text-sm font-semibold text-text">{name}</span>
                    <span className="msg-preview-bot-tag rounded-[3px] bg-discord px-[5px] py-px align-middle text-[9px] leading-[1.4] font-semibold tracking-[0.3px] text-white uppercase">
                      APP
                    </span>
                    <span className="msg-preview-time text-[11px] text-muted opacity-60">Today at 3:42 PM</span>
                  </div>
                  {on("TAG_REPLY_MSG") ? (
                    <div
                      id="msg-features-preview-reply"
                      className="msg-preview-reply mb-1 rounded-r border-l-[3px] border-white/12 bg-white/[0.03] px-2 py-1 text-[11px] leading-[1.4] text-muted"
                    >
                      <span className="msg-reply-icon mr-0.5 text-accent">↩</span> In reply to:{" "}
                      <a className="msg-reply-link text-accent no-underline opacity-80">Copycord is the best!</a>
                    </div>
                  ) : (
                    <div id="msg-features-preview-reply" className="msg-preview-reply hidden" />
                  )}
                  <div id="msg-features-preview-content" className="msg-preview-content text-sm leading-[1.4] text-discord-text">
                    {on("STRIP_MENTIONS") ? (
                      "Hey check this out"
                    ) : (
                      <>
                        Hey <span className={on("DISABLE_EVERYONE_MENTIONS") ? PLAIN : MENTION}>@everyone</span> check this
                        out <span className={on("DISABLE_ROLE_MENTIONS") ? PLAIN : MENTION}>@Moderator</span>
                      </>
                    )}
                  </div>
                  <div
                    id="msg-features-preview-footer"
                    className="msg-preview-footer mt-[3px] text-[11px] leading-[1.3] text-muted opacity-70 empty:hidden"
                  >
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
