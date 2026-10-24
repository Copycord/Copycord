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

/** Where a sending account stands: online in the warm pool, connecting, not held, switched off or rejected by Discord. */
export type SenderState = "online" | "connecting" | "offline" | "disabled" | "revoked";

/** One sending account, every token row of one account merged. */
export interface Sender {
  id: string;
  username: string | null;
  display_name: string | null;
  avatar_url: string | null;
  state: SenderState;
  /** The Discord status of an online session. */
  presence: "online" | "idle" | "dnd" | "invisible" | null;
  /** Unix seconds the session came online; null when not online. */
  since: number | null;
  /** Unix seconds of the last send. */
  last_used: number | null;
  use_count: number;
}

/** GET /api/system/senders */
export interface SendersResponse {
  ok: boolean;
  items: Sender[];
}
