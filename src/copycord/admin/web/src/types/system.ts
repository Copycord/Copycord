// Replies of the System page endpoints (app.py backup routes).

/** One archive in the backups folder, newest first. */
export interface BackupArchive {
  name: string;
  /** Bytes. */
  size: number;
  /** Unix seconds. */
  mtime: number;
}

/** GET /api/backup/info */
export interface BackupInfo {
  ok: boolean;
  last_backup_at: string;
  last_backup_file: string;
  last_backup_size: number;
  dir: string;
  archives: BackupArchive[];
}

/** POST /admin/backup-now */
export interface BackupNowResponse {
  ok: boolean;
  file?: string;
}

/** POST /api/backup/delete */
export interface BackupDeleteResponse {
  ok: boolean;
  deleted?: string;
}

/** POST /api/backup/restore */
export interface BackupRestoreResponse {
  ok: boolean;
  restored_from?: string;
}
