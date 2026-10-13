// Backup calls for the System page. Mutations post multipart forms, which
// lib/api.ts sends as is with the CSRF header; a reply without ok: true is
// treated as a failure, as on the old page.

import { api } from "../../lib/api";
import type {
  BackupDeleteResponse,
  BackupNowResponse,
  BackupRestoreResponse,
} from "../../types/system";

export const BACKUP_INFO_URL = "/api/backup/info";
export const BACKUP_NOW_URL = "/admin/backup-now";
export const BACKUP_DELETE_URL = "/api/backup/delete";
export const BACKUP_RESTORE_URL = "/api/backup/restore";

export function downloadUrl(name: string): string {
  return `/api/backup/download/${encodeURIComponent(name)}`;
}

/** Builds the multipart body the FastAPI Form(...) parameters expect. */
export function formBody(fields: Record<string, string | Blob>): FormData {
  const fd = new FormData();
  for (const [key, value] of Object.entries(fields)) fd.append(key, value);
  return fd;
}

function requireOk<T extends { ok: boolean }>(what: string) {
  return (reply: T | undefined): T => {
    if (!reply?.ok) throw new Error(`${what} failed`);
    return reply;
  };
}

export function backupNow(): Promise<BackupNowResponse> {
  return api.post<BackupNowResponse>(BACKUP_NOW_URL).then(requireOk("backup"));
}

export function deleteBackup(name: string): Promise<BackupDeleteResponse> {
  return api
    .post<BackupDeleteResponse>(BACKUP_DELETE_URL, formBody({ name }))
    .then(requireOk("delete"));
}

export function restoreUpload(file: File): Promise<BackupRestoreResponse> {
  return api
    .post<BackupRestoreResponse>(BACKUP_RESTORE_URL, formBody({ source: "upload", file }))
    .then(requireOk("restore"));
}

export function restoreExisting(name: string): Promise<BackupRestoreResponse> {
  return api
    .post<BackupRestoreResponse>(BACKUP_RESTORE_URL, formBody({ source: "existing", name }))
    .then(requireOk("restore"));
}
