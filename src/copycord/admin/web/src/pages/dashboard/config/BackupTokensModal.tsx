// The Backup Tokens modal: lists the saved fallback CLIENT_TOKENs (masked by
// the server), adds one with an optional note and removes one after a confirm.

import { useCallback, useEffect, useRef, useState } from "react";
import { Spinner } from "../../../components/ui";
import { Modal } from "../../../components/Modal";
import { ApiError } from "../../../lib/api";
import type { BackupToken } from "../../../types/dashboard-config";
import { GhostButton } from "../../system/buttons";
import { useNotify, useSafeConfirm } from "../../system/providers";
import { addBackupToken, deleteBackupToken, fmtUnix, listBackupTokens } from "./model";

export interface BackupTokensModalProps {
  open: boolean;
  onClose: () => void;
}

const errText = (e: unknown, fallback: string) => (e instanceof ApiError && e.message.trim() && !/^request to /.test(e.message) ? e.message.trim() : fallback);

// The old .field label.has-tip and .field-token .token-input / .field > input, which keep the browser font size.
const LABEL = "relative mb-1.5 ml-0.5 inline-flex items-center gap-1.5 text-xs text-muted";
const INPUT =
  "w-full rounded-control border border-border bg-control py-3 pl-3 text-text outline-0 transition-[border-color] focus:border-accent";

const EYE = (
  <>
    <path d="M1 12s4-7 11-7 11 7 11 7-4 7-11 7S1 12 1 12Z" />
    <circle cx="12" cy="12" r="3" />
  </>
);
const EYE_OFF = (
  <path d="M17.94 17.94A10.07 10.07 0 0 1 12 19c-7 0-11-7-11-7a18.4 18.4 0 0 1 5.06-5.94M9.9 9.9A3 3 0 0 0 9 12a3 3 0 0 0 3 3 3 3 0 0 0 2.1-.9M1 1l22 22" />
);

export function BackupTokensModal({ open, onClose }: BackupTokensModalProps) {
  const notify = useNotify();
  const confirm = useSafeConfirm();
  const [tokens, setTokens] = useState<BackupToken[] | null>(null);
  const [loadError, setLoadError] = useState(false);
  const [value, setValue] = useState("");
  const [note, setNote] = useState("");
  const [busy, setBusy] = useState(false);
  const [shown, setShown] = useState(false);
  // Escape in the confirm dialog must not also close this modal.
  const confirming = useRef(false);

  const refresh = useCallback(async () => {
    try {
      setTokens(await listBackupTokens());
      setLoadError(false);
    } catch {
      setLoadError(true);
    }
  }, []);

  useEffect(() => {
    if (!open) return;
    setTokens(null);
    void refresh();
  }, [open, refresh]);

  const add = async () => {
    const token = value.trim();
    if (!token) {
      notify("Paste a token first.", "error");
      return;
    }
    setBusy(true);
    try {
      const res = await addBackupToken(token, note.trim());
      if (!res?.ok) throw new Error("Failed to add backup token.");
      setValue("");
      setNote("");
      notify("Backup token added.", "success");
      await refresh();
    } catch (e) {
      notify(errText(e, "Failed to add backup token."), "error");
    } finally {
      setBusy(false);
    }
  };

  const remove = async (id: string) => {
    confirming.current = true;
    let ok = false;
    try {
      ok = await confirm({
        title: "Remove backup token?",
        body: "This will delete the backup token from the database.",
        confirmLabel: "Remove",
        danger: true,
      });
    } finally {
      confirming.current = false;
    }
    if (!ok) return;
    try {
      const res = await deleteBackupToken(id);
      if (!res?.ok) throw new Error("delete failed");
      notify("Backup token removed.", "success");
      await refresh();
    } catch {
      notify("Failed to remove backup token.", "error");
    }
  };

  return (
    <Modal
      open={open}
      id="backup-token-modal"
      closeId="backup-token-close"
      title="Backup Tokens"
      subtitle="Used as fallbacks if the primary token fails."
      className="w-[min(900px,92vw)]"
      bodyClassName="flex flex-col pt-3.5"
      onClose={() => {
        if (!confirming.current) onClose();
      }}
    >
      <div className="flex min-h-0 flex-1 flex-col px-3.5">
        <form
          className="relative mb-3.5"
          onSubmit={(e) => {
            e.preventDefault();
            void add();
          }}
        >
          <label htmlFor="backup_token_value" className={LABEL}>
            <span className="leading-none">Add backup token</span>
          </label>
          <div className="relative w-full">
            <input
              id="backup_token_value"
              name="backup_token_value"
              type={shown ? "text" : "password"}
              data-autofocus
              autoComplete="off"
              spellCheck={false}
              value={value}
              onChange={(e) => setValue(e.target.value)}
              placeholder="Paste a CLIENT_TOKEN"
              className={`${INPUT} pr-9`}
            />
            <button
              type="button"
              data-target="backup_token_value"
              aria-pressed={shown}
              aria-label={shown ? "Hide backup_token_value" : "Show backup_token_value"}
              title={shown ? "Hide" : "Show"}
              onClick={() => setShown((s) => !s)}
              className="absolute top-1/2 right-3 flex -translate-y-1/2 cursor-pointer items-center justify-center border-0 bg-transparent p-0 leading-none text-text opacity-60 outline-none hover:opacity-100 focus:opacity-100"
            >
              <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" viewBox="0 0 24 24" aria-hidden="true" className="pointer-events-none block">
                {shown ? EYE_OFF : EYE}
              </svg>
            </button>
          </div>
          <div className="relative mt-2.5">
            <input
              id="backup_token_note"
              name="backup_token_note"
              type="text"
              maxLength={120}
              value={note}
              onChange={(e) => setNote(e.target.value)}
              placeholder="Optional note (e.g. alt account, backup account)"
              aria-label="Backup token note"
              className={`${INPUT} pr-3`}
            />
          </div>
          <div className="btns mt-2.5 flex flex-wrap justify-start gap-2.5">
            <GhostButton id="backup-token-add" type="submit" loading={busy} className="ml-1.5 gap-[normal]!">
              Add
            </GhostButton>
          </div>
        </form>

        <div
          id="backupTokensList"
          className="mt-2.5 min-h-[140px] flex-1 overflow-auto overscroll-contain p-2.5 [scrollbar-color:var(--color-accent)_transparent] [scrollbar-gutter:stable_both-edges] [scrollbar-width:thin]"
        >
          {tokens === null && !loadError ? (
            <Spinner />
          ) : loadError ? (
            <p role="alert" className="py-2.5 text-danger">
              Could not load backup tokens.
            </p>
          ) : tokens!.length === 0 ? (
            <div className="py-2.5">No backup tokens saved yet.</div>
          ) : (
            tokens!.map((t) => (
              <div
                key={t.token_id}
                data-token-id={t.token_id}
                className="mt-[18px] mb-2.5 rounded-card border border-border bg-(image:--card-gradient) px-3 py-2.5"
              >
                <div className="flex items-start justify-between gap-3">
                  <div className="min-w-0">
                    <div className="font-bold tracking-[0.2px]">{t.masked || "(hidden)"}</div>
                    {t.note ? <div className="mt-1 break-words">{t.note}</div> : null}
                    <div className="mt-1.5 text-[0.85rem]">
                      <span>Added: {fmtUnix(t.added_at) || "—"}</span>
                      <span className="mx-2 opacity-60">•</span>
                      <span>Last used: {fmtUnix(t.last_used) || "Never"}</span>
                    </div>
                  </div>
                  <button
                    type="button"
                    aria-label="Remove backup token"
                    title="Remove"
                    onClick={() => void remove(t.token_id)}
                    className="delete-token-btn inline-flex min-h-8 w-8 flex-none cursor-pointer items-center justify-center rounded-btn border border-border bg-transparent p-0 text-muted outline-none hover:border-danger hover:bg-[rgba(255,76,76,0.1)] hover:text-danger focus:border-danger focus:bg-[rgba(255,76,76,0.1)] focus:text-danger"
                  >
                    <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
                      <path d="M3 6h18M8 6V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2m3 0v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6" />
                    </svg>
                  </button>
                </div>
              </div>
            ))
          )}
        </div>
      </div>
    </Modal>
  );
}
