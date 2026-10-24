// System page: the Database card, and the sending accounts table under it.
// Collapsed by default; open it to back up now, download the latest or any
// archive, restore from an upload or an existing archive, and delete archives.
// Restore and delete ask first. Element ids are stable; tests and integrations select by them.

import { useLayoutEffect, useRef, useState } from "react";
import type { ChangeEvent, MouseEvent } from "react";
import { Card, useReveal } from "../components/ui";
import { useLastCount } from "../components/ui/lastCount";
import { useApi } from "../hooks/useApi";
import { errorText } from "../lib/api";
import type { BackupInfo } from "../types/system";
import {
  BACKUP_INFO_URL,
  backupNow,
  deleteBackup,
  downloadUrl,
  restoreExisting,
  restoreUpload,
} from "./system/backupApi";
import { CHEV, CHEV_BUTTON, GhostButton, LinkButton } from "./system/buttons";
import { archiveLabel, fmtBackupWhen, fmtBytes, NO_VALUE, SHRUG } from "./system/format";
import {
  ACTIONS_CELL,
  ARCHIVES_COUNT_KEY,
  ARCHIVES_TABLE,
  ARCHIVES_WRAP,
  ArchiveRowsLoading,
  ArchivesHead,
  CELL,
  DB_CARD,
  SystemTitle,
  DB_HEADER,
  DB_TITLE,
  LastBackupPending,
  NAME_CODE,
  NOWRAP_CELL,
  PANEL_ACTIONS,
  ROW_ACTIONS,
  ROW_BUTTON,
} from "./system/SystemSkeleton";
import { useNotify, useSafeConfirm } from "./system/providers";
import { SendersTable } from "./system/SendersTable";

/** The archive list scrolls once it has more rows than this. */
export const VISIBLE_ROWS = 5;

const INTERACTIVE = 'a,button,input,select,textarea,label,[role="button"],[contenteditable]';

function isInteractive(target: EventTarget): boolean {
  return target instanceof Element && target.closest(INTERACTIVE) !== null;
}

interface RowBusy {
  name: string;
  action: "restore" | "delete";
}

export function System() {
  const { data, error, reload } = useApi<BackupInfo>(BACKUP_INFO_URL);
  const notify = useNotify();
  const confirm = useSafeConfirm();

  const [open, setOpen] = useState(false);
  const [backingUp, setBackingUp] = useState(false);
  const [uploading, setUploading] = useState(false);
  const [rowBusy, setRowBusy] = useState<RowBusy | null>(null);
  const [maxHeight, setMaxHeight] = useState<number | undefined>(undefined);

  const fileRef = useRef<HTMLInputElement>(null);
  const tableRef = useRef<HTMLTableElement>(null);
  const openBeforePointer = useRef(false);

  const archives = data?.ok ? data.archives : undefined;
  const reveal = useReveal(data !== undefined || error !== undefined);
  const latest = archives?.[0];
  const lastBackup = archives ? (latest ? fmtBackupWhen(latest.mtime) : SHRUG) : NO_VALUE;
  useLastCount(ARCHIVES_COUNT_KEY, archives?.length);

  // Show at most VISIBLE_ROWS rows and scroll the rest.
  useLayoutEffect(() => {
    const table = tableRef.current;
    const rows = archives?.length ?? 0;
    if (!open || !table || rows <= VISIBLE_ROWS) {
      setMaxHeight(undefined);
      return;
    }
    const measure = () => {
      const head = Math.ceil(table.tHead?.getBoundingClientRect().height ?? 0) || 36;
      let body = 0;
      for (let i = 0; i < VISIBLE_ROWS; i++) {
        body += Math.ceil(table.tBodies[0]?.rows[i]?.getBoundingClientRect().height ?? 0) || 44;
      }
      // The table's own top margin sits inside the scroll box too.
      setMaxHeight(head + body + 8);
    };
    measure();
    window.addEventListener("resize", measure);
    return () => window.removeEventListener("resize", measure);
  }, [open, archives]);

  // A failed info call is said in the table rather than leaving it blank.
  const loadFailed = error !== undefined || (data !== undefined && !data.ok);

  const onCardMouseDown = (e: MouseEvent) => {
    // Remember the state at the first press of a click sequence so a double
    // click on a collapsed card opens it and leaves it open.
    if (e.detail <= 1) openBeforePointer.current = open;
  };
  const onCardClick = (e: MouseEvent) => {
    if (!open && !isInteractive(e.target)) setOpen(true);
  };
  const onCardDoubleClick = (e: MouseEvent) => {
    if (open && openBeforePointer.current && !isInteractive(e.target)) setOpen(false);
  };

  async function onBackupNow() {
    setBackingUp(true);
    try {
      await backupNow();
      reload();
      notify("Backup created", "success");
    } catch (e) {
      notify(errorText(e, "Backup failed"), "error");
    } finally {
      setBackingUp(false);
    }
  }

  async function onRestoreFile(e: ChangeEvent<HTMLInputElement>) {
    const input = e.currentTarget;
    const file = input.files?.[0];
    if (!file) return;
    try {
      const ok = await confirm({
        title: "Restore database?",
        body: `Restore database from uploaded file “${file.name}”? This will stop bots and replace the database.`,
        confirmLabel: "Restore",
        confirmVariant: "ghost-red",
      });
      if (!ok) return;
      setUploading(true);
      await restoreUpload(file);
      reload();
      notify("Database restored from upload.", "success");
    } catch (err) {
      notify(errorText(err, "Restore failed"), "error");
    } finally {
      input.value = "";
      setUploading(false);
    }
  }

  async function onRestoreExisting(name: string) {
    const ok = await confirm({
      title: "Restore database?",
      body: `Restore database from “${name}”? This will stop bots and replace the database.`,
      confirmLabel: "Restore",
      confirmVariant: "ghost-red",
    });
    if (!ok) return;
    setRowBusy({ name, action: "restore" });
    try {
      await restoreExisting(name);
      reload();
      notify("Database restored.", "success");
    } catch (e) {
      notify(errorText(e, "Restore failed"), "error");
    } finally {
      setRowBusy(null);
    }
  }

  async function onDelete(name: string) {
    const ok = await confirm({
      title: "Delete backup?",
      body: `Permanently delete “${name}”? This cannot be undone.`,
      confirmLabel: "Delete",
      confirmVariant: "ghost-red",
    });
    if (!ok) return;
    setRowBusy({ name, action: "delete" });
    try {
      await deleteBackup(name);
      reload();
      notify("Backup deleted", "success");
    } catch (e) {
      notify(errorText(e, "Delete failed"), "error");
    } finally {
      setRowBusy(null);
    }
  }

  return (
    <div id="adv-root">
      <SystemTitle />
      <Card
        id="adv-data-card"
        aria-label="Database"
        data-collapsed={open ? "false" : "true"}
        className={DB_CARD}
        onMouseDownCapture={onCardMouseDown}
        onClick={onCardClick}
        onDoubleClick={onCardDoubleClick}
      >
        <div id="adv-collapse-header" className={DB_HEADER}>
          <div className="flex flex-col gap-1 leading-[normal]">
            <h2 className={DB_TITLE}>Database</h2>
            <div id="adv-last-wrap" className="opacity-85">
              Last backup: <span id="adv-last-backup" className={reveal}>{data === undefined && !error ? <LastBackupPending /> : lastBackup}</span>
            </div>
          </div>
          <button
            id="adv-collapse-btn"
            type="button"
            aria-controls="adv-collapse-panel"
            aria-expanded={open}
            aria-label={open ? "Collapse panel" : "Expand panel"}
            className={CHEV_BUTTON}
            onClick={(e) => {
              e.stopPropagation();
              setOpen((v) => !v);
            }}
          >
            <span aria-hidden="true" className={CHEV}>
              {open ? "▾" : "▸"}
            </span>
          </button>
        </div>

        <div id="adv-collapse-panel" className="mt-1.5">
          <div className={PANEL_ACTIONS} hidden={!open}>
            <GhostButton id="adv-backup-now" loading={backingUp} onClick={() => void onBackupNow()}>
              Backup now
            </GhostButton>
            <LinkButton
              id="adv-download-latest"
              href={latest ? downloadUrl(latest.name) : undefined}
              disabled={!latest}
              keepOpacity
              download
            >
              Download latest
            </LinkButton>
            <GhostButton id="adv-restore-upload" loading={uploading} onClick={() => fileRef.current?.click()}>
              Restore
            </GhostButton>
            <input
              ref={fileRef}
              id="adv-restore-file"
              type="file"
              accept=".gz"
              hidden
              aria-label="Restore from file"
              onChange={(e) => void onRestoreFile(e)}
            />
          </div>

          <div
            className={ARCHIVES_WRAP}
            style={maxHeight ? { maxHeight } : undefined}
            data-testid="adv-archives-wrap"
          >
            <table
              ref={tableRef}
              id="adv-archives"
              className={ARCHIVES_TABLE}
            >
              <ArchivesHead hidden={!open} />
              <tbody hidden={!open} className={reveal}>
                {data === undefined && !error ? <ArchiveRowsLoading /> : null}
                {archives?.map((a) => {
                  const busy = rowBusy?.name === a.name ? rowBusy.action : null;
                  return (
                    <tr key={a.name} data-archive={a.name}>
                      <td className={CELL}>
                        <code title={a.name} className={NAME_CODE}>
                          {archiveLabel(a.name)}
                        </code>
                      </td>
                      <td className={NOWRAP_CELL}>{fmtBytes(a.size)}</td>
                      <td className={NOWRAP_CELL}>{fmtBackupWhen(a.mtime)}</td>
                      <td className={ACTIONS_CELL}>
                        <span className={ROW_ACTIONS}>
                          <LinkButton href={downloadUrl(a.name)} download className={ROW_BUTTON}>
                            Download
                          </LinkButton>
                          <GhostButton
                            compact
                            className={ROW_BUTTON}
                            data-restore={a.name}
                            loading={busy === "restore"}
                            disabled={busy !== null}
                            onClick={() => void onRestoreExisting(a.name)}
                          >
                            Restore
                          </GhostButton>
                          <GhostButton
                            compact
                            danger
                            className={ROW_BUTTON}
                            data-delete={a.name}
                            disabled={busy !== null}
                            onClick={() => void onDelete(a.name)}
                          >
                            {busy === "delete" ? "Deleting…" : "Delete"}
                          </GhostButton>
                        </span>
                      </td>
                    </tr>
                  );
                })}
                {archives && archives.length === 0 ? (
                  <tr>
                    <td colSpan={4} className="px-2 py-2.5 text-text-2">
                      No backups yet.
                    </td>
                  </tr>
                ) : null}
                {loadFailed ? (
                  <tr>
                    <td colSpan={4} role="alert" className="px-2 py-2.5 text-danger">
                      Could not load backups.
                    </td>
                  </tr>
                ) : null}
              </tbody>
            </table>
          </div>
        </div>
      </Card>
      <SendersTable />
    </div>
  );
}
