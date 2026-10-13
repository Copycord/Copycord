// The Backup Tokens modal: lists the saved fallback CLIENT_TOKENs (masked by
// the server), adds one with an optional note and removes one after a confirm.

import { useCallback, useEffect, useRef, useState } from "react";
import { Input, Spinner } from "../../../components/ui";
import { Modal } from "../../../components/Modal";
import { ApiError } from "../../../lib/api";
import type { BackupToken } from "../../../types/dashboard-config";
import { GhostButton } from "../../system/buttons";
import { useNotify, useSafeConfirm } from "../../system/providers";
import { TokenInput } from "./TokenInput";
import { addBackupToken, deleteBackupToken, fmtUnix, listBackupTokens } from "./model";

export interface BackupTokensModalProps {
  open: boolean;
  onClose: () => void;
}

const errText = (e: unknown, fallback: string) => (e instanceof ApiError && e.message.trim() && !/^request to /.test(e.message) ? e.message.trim() : fallback);

export function BackupTokensModal({ open, onClose }: BackupTokensModalProps) {
  const notify = useNotify();
  const confirm = useSafeConfirm();
  const [tokens, setTokens] = useState<BackupToken[] | null>(null);
  const [loadError, setLoadError] = useState(false);
  const [value, setValue] = useState("");
  const [note, setNote] = useState("");
  const [busy, setBusy] = useState(false);
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
      title="Backup Tokens"
      onClose={() => {
        if (!confirming.current) onClose();
      }}
    >
      <div id="backup-token-modal" className="flex w-full flex-col gap-4">
        <p className="text-sm text-text-2">Used as fallbacks if the primary token fails.</p>
        <form
          className="flex flex-col gap-2.5"
          onSubmit={(e) => {
            e.preventDefault();
            void add();
          }}
        >
          <label htmlFor="backup_token_value" className="text-sm font-semibold text-text-1">
            Add backup token
          </label>
          <TokenInput id="backup_token_value" value={value} onChange={setValue} placeholder="Paste a CLIENT_TOKEN" />
          <Input
            id="backup_token_note"
            name="backup_token_note"
            maxLength={120}
            value={note}
            onChange={(e) => setNote(e.target.value)}
            placeholder="Optional note (e.g. alt account, backup account)"
            aria-label="Backup token note"
          />
          <div>
            <GhostButton id="backup-token-add" type="submit" loading={busy}>
              Add
            </GhostButton>
          </div>
        </form>

        <div id="backupTokensList" className="max-h-72 overflow-y-auto">
          {tokens === null && !loadError ? (
            <Spinner />
          ) : loadError ? (
            <p role="alert" className="py-2.5 text-sm text-danger">
              Could not load backup tokens.
            </p>
          ) : tokens!.length === 0 ? (
            <p className="py-2.5 text-sm text-text-2">No backup tokens saved yet.</p>
          ) : (
            <ul className="flex flex-col gap-2.5">
              {tokens!.map((t) => (
                <li
                  key={t.token_id}
                  data-token-id={t.token_id}
                  className="flex items-start justify-between gap-3 rounded-control border border-border px-3 py-2.5"
                >
                  <div className="min-w-0">
                    <div className="font-bold tracking-wide">{t.masked || "(hidden)"}</div>
                    {t.note ? <div className="mt-1 text-sm break-words text-text-2">{t.note}</div> : null}
                    <div className="mt-1.5 text-[0.85rem] text-text-2">
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
                    className="delete-token-btn grid size-8 flex-none cursor-pointer place-items-center rounded-control text-text-2 outline-none hover:text-danger focus-visible:ring-2 focus-visible:ring-accent/60"
                  >
                    <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
                      <path d="M3 6h18M8 6V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2m3 0v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6" />
                    </svg>
                  </button>
                </li>
              ))}
            </ul>
          )}
        </div>
      </div>
    </Modal>
  );
}
