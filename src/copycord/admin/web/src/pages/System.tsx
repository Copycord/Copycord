// System page: the Database card from the old system.html and system.js.
// Collapsed by default; open it to back up now, download the latest or any
// archive, restore from an upload or an existing archive, and delete archives.
// Restore and delete ask first. Element ids match the old page for the e2e specs.

import { useLayoutEffect, useRef, useState } from "react";
import type { ChangeEvent, MouseEvent } from "react";
import { Card, cx } from "../components/ui";
import { useApi } from "../hooks/useApi";
import type { BackupInfo } from "../types/system";
import {
  BACKUP_INFO_URL,
  backupNow,
  deleteBackup,
  downloadUrl,
  restoreExisting,
  restoreUpload,
} from "./system/backupApi";
import { GhostButton, LinkButton } from "./system/buttons";
import { archiveLabel, fmtBackupWhen, fmtBytes, NO_VALUE, SHRUG } from "./system/format";
import { useNotify, useSafeConfirm } from "./system/providers";

/** The archive list scrolls once it has more rows than this. */
export const VISIBLE_ROWS = 5;

// Narrow screens let the columns size to their content and stack the row buttons.
const HEAD = "px-1.5 py-2.5 font-semibold text-accent md:px-2";
const CELL = "border-b border-border px-1.5 py-2.5 md:px-2";
const ROW_BUTTON = "max-md:min-h-[34px] max-md:px-3";

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
  const latest = archives?.[0];
  const lastBackup = archives ? (latest ? fmtBackupWhen(latest.mtime) : SHRUG) : NO_VALUE;

  // Show at most VISIBLE_ROWS rows and scroll the rest, measured like the old page.
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

  // The old page stayed blank when the info call failed; say so in the table.
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
    } catch {
      notify("Backup failed", "error");
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
        danger: true,
      });
      if (!ok) return;
      setUploading(true);
      await restoreUpload(file);
      reload();
      notify("Database restored from upload.", "success");
    } catch {
      notify("Restore failed", "error");
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
      danger: true,
    });
    if (!ok) return;
    setRowBusy({ name, action: "restore" });
    try {
      await restoreExisting(name);
      reload();
      notify("Database restored.", "success");
    } catch {
      notify("Restore failed", "error");
    } finally {
      setRowBusy(null);
    }
  }

  async function onDelete(name: string) {
    const ok = await confirm({
      title: "Delete backup?",
      body: `Permanently delete “${name}”? This cannot be undone.`,
      confirmLabel: "Delete",
      danger: true,
    });
    if (!ok) return;
    setRowBusy({ name, action: "delete" });
    try {
      await deleteBackup(name);
      reload();
      notify("Backup deleted", "success");
    } catch {
      notify("Delete failed", "error");
    } finally {
      setRowBusy(null);
    }
  }

  return (
    <div id="adv-root" className="px-3 pt-3.5 md:px-0 md:pt-0">
      <h1 className="sr-only">System</h1>
      <Card
        id="adv-data-card"
        aria-label="Database"
        data-collapsed={open ? "false" : "true"}
        className="md:mt-[18px]"
        onMouseDownCapture={onCardMouseDown}
        onClick={onCardClick}
        onDoubleClick={onCardDoubleClick}
      >
        <div id="adv-collapse-header" className="mb-1.5 flex items-center justify-between gap-3 px-4 py-3">
          <div className="flex flex-col gap-1 leading-[normal]">
            <h2 className="my-[1em] text-[1.17em] font-bold">Database</h2>
            <div id="adv-last-wrap" className="text-text/85">
              Last backup: <span id="adv-last-backup">{lastBackup}</span>
            </div>
          </div>
          <button
            id="adv-collapse-btn"
            type="button"
            aria-controls="adv-collapse-panel"
            aria-expanded={open}
            aria-label={open ? "Collapse panel" : "Expand panel"}
            className={cx(
              "inline-flex size-8 shrink-0 cursor-pointer items-center justify-center rounded-control border border-border",
              "bg-white/4 text-accent outline-none transition duration-200 focus-visible:ring-2 focus-visible:ring-accent/60",
              "hover:-translate-y-0.5 hover:border-accent/30 hover:bg-white/8 hover:shadow-soft",
            )}
            onClick={(e) => {
              e.stopPropagation();
              setOpen((v) => !v);
            }}
          >
            <span aria-hidden="true" className="text-sm leading-none">
              {open ? "▾" : "▸"}
            </span>
          </button>
        </div>

        <div id="adv-collapse-panel" className="mt-1.5">
          <div className="flex flex-wrap gap-2" hidden={!open}>
            <GhostButton id="adv-backup-now" compact loading={backingUp} onClick={() => void onBackupNow()}>
              Backup now
            </GhostButton>
            <LinkButton
              id="adv-download-latest"
              href={latest ? downloadUrl(latest.name) : undefined}
              disabled={!latest}
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
            className="overflow-auto [scrollbar-width:none] [&::-webkit-scrollbar]:hidden"
            style={maxHeight ? { maxHeight } : undefined}
            data-testid="adv-archives-wrap"
          >
            <table
              ref={tableRef}
              id="adv-archives"
              className="mt-2 w-full border-separate border-spacing-0 text-left md:table-fixed"
            >
              <thead hidden={!open}>
                <tr>
                  <th scope="col" className={HEAD}>Name</th>
                  <th scope="col" className={HEAD}>Size</th>
                  <th scope="col" className={HEAD}>Time</th>
                  <th scope="col" className={cx(HEAD, "pr-4 text-right md:w-[330px]")}>
                    <span className="sr-only">Actions</span>
                  </th>
                </tr>
              </thead>
              <tbody hidden={!open}>
                {archives?.map((a) => {
                  const busy = rowBusy?.name === a.name ? rowBusy.action : null;
                  return (
                    <tr key={a.name} data-archive={a.name}>
                      <td className={CELL}>
                        <code title={a.name} className="font-mono text-[13px] break-all">
                          {archiveLabel(a.name)}
                        </code>
                      </td>
                      <td className={cx(CELL, "whitespace-nowrap")}>{fmtBytes(a.size)}</td>
                      <td className={cx(CELL, "whitespace-nowrap")}>{fmtBackupWhen(a.mtime)}</td>
                      <td className={cx(CELL, "pr-4 text-right")}>
                        <span className="inline-flex flex-col items-end gap-2 md:flex-row md:whitespace-nowrap">
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
    </div>
  );
}
