// Calls and pure helpers for the Global Configuration card and the backup
// tokens modal, ported from app.js and the index.html form.

import { splitChips } from "../../../components/ui";
import { api } from "../../../lib/api";
import type {
  BackupToken,
  BackupTokenResult,
  BackupTokensResponse,
  GlobalConfig,
  TokenKey,
  TokenValidation,
} from "../../../types/dashboard-config";

export const SAVE_URL = "/save";
export const VALIDATE_URL = "/api/validate-tokens";
export const BACKUP_URL = "/api/backup-tokens";

export const TOKEN_KEYS: TokenKey[] = ["SERVER_TOKEN", "CLIENT_TOKEN"];

export const EMPTY_CONFIG: GlobalConfig = {
  SERVER_TOKEN: "",
  CLIENT_TOKEN: "",
  COMMAND_USERS: [],
  COPYCORD_AUTOSTART: false,
  LOG_LEVEL: "INFO",
  LOG_MAX_SIZE_MB: "10",
};

export const FIELD_TIPS = {
  SERVER_TOKEN:
    "Your bot token from the Discord Developer Portal. Only needs to be invited to your clone servers.",
  CLIENT_TOKEN:
    "Your Discord account token from the console. Must be in any of the servers you want to clone.",
  COMMAND_USERS: "Comma-separated Discord user IDs allowed to run slash commands.",
  COPYCORD_AUTOSTART:
    "Automatically start the server and client when Copycord launches. Valid tokens are required.",
  LOG_LEVEL: "INFO for normal use, DEBUG for troubleshooting.",
  LOG_MAX_SIZE_MB:
    "Maximum size of each log file in megabytes before older entries are pruned. Set to 0 to disable pruning.",
} as const;

export const DOCS_URL = "https://copycord.github.io/Copycord/docs/getting-started/first-run";

/** The links the old page got from links.json at startup. */
export const PROMO_LINKS = {
  github: "https://github.com/Copycord/Copycord",
  discord: "https://discord.gg/ArFdqrJHBj",
  kofi: "https://ko-fi.com/xmacj",
} as const;

export const parseIdList = (csv: string): string[] => splitChips(csv, "ids");
export const idsToCsv = (ids: readonly string[]): string => ids.join(",");

/**
 * The server renders the saved values into the old page and has no JSON
 * endpoint for them, so read them from that markup.
 */
export function parseConfigHtml(html: string): GlobalConfig {
  const doc = new DOMParser().parseFromString(html, "text/html");
  const val = (id: string) => doc.getElementById(id)?.getAttribute("value") ?? "";
  const level = (
    doc.querySelector<HTMLOptionElement>("#LOG_LEVEL option[selected]")?.value ?? "INFO"
  ).toUpperCase();
  const auto = (
    doc.querySelector<HTMLOptionElement>("#COPYCORD_AUTOSTART option[selected]")?.value ?? "false"
  ).toLowerCase();
  return {
    SERVER_TOKEN: val("SERVER_TOKEN"),
    CLIENT_TOKEN: val("CLIENT_TOKEN"),
    COMMAND_USERS: parseIdList(val("COMMAND_USERS")),
    COPYCORD_AUTOSTART: auto === "true",
    LOG_LEVEL: level === "DEBUG" ? "DEBUG" : "INFO",
    LOG_MAX_SIZE_MB: doc.getElementById("LOG_MAX_SIZE_MB") ? val("LOG_MAX_SIZE_MB") : EMPTY_CONFIG.LOG_MAX_SIZE_MB,
  };
}

export async function loadConfig(): Promise<GlobalConfig> {
  return parseConfigHtml(await api.get<string>("/"));
}

/** The form body the old #cfg-form posted. */
export function buildSaveForm(c: GlobalConfig): FormData {
  const fd = new FormData();
  fd.append("SERVER_TOKEN", c.SERVER_TOKEN);
  fd.append("CLIENT_TOKEN", c.CLIENT_TOKEN);
  fd.append("COMMAND_USERS", idsToCsv(c.COMMAND_USERS));
  fd.append("COPYCORD_AUTOSTART", c.COPYCORD_AUTOSTART ? "true" : "false");
  fd.append("LOG_LEVEL", c.LOG_LEVEL);
  fd.append("LOG_MAX_SIZE_MB", c.LOG_MAX_SIZE_MB);
  return fd;
}

/** POST /save answers with a 303 to "/"; fetch follows it, so any 2xx is success. */
export const saveConfig = (c: GlobalConfig) => api.post<unknown>(SAVE_URL, buildSaveForm(c));

export const validateTokens = () => api.get<TokenValidation>(VALIDATE_URL);

export function isDirty(a: GlobalConfig, b: GlobalConfig): boolean {
  return (
    a.SERVER_TOKEN !== b.SERVER_TOKEN ||
    a.CLIENT_TOKEN !== b.CLIENT_TOKEN ||
    idsToCsv(a.COMMAND_USERS) !== idsToCsv(b.COMMAND_USERS) ||
    a.COPYCORD_AUTOSTART !== b.COPYCORD_AUTOSTART ||
    a.LOG_LEVEL !== b.LOG_LEVEL ||
    a.LOG_MAX_SIZE_MB !== b.LOG_MAX_SIZE_MB
  );
}

export function missingTokens(c: GlobalConfig): TokenKey[] {
  return TOKEN_KEYS.filter((k) => !c[k].trim());
}

/** Which token inputs the validation errors point at. */
export function tokensFromErrors(errors: readonly string[] | undefined): Set<TokenKey> {
  const bad = new Set<TokenKey>();
  for (const msg of errors ?? []) {
    for (const k of TOKEN_KEYS) if (String(msg ?? "").includes(k)) bad.add(k);
  }
  return bad;
}

export const BAD_TOKENS_MESSAGE = "Saved tokens are no longer valid. Please update SERVER_TOKEN and CLIENT_TOKEN.";

export function inviteUrl(clientId: string | null | undefined): string | null {
  return clientId ? `https://discord.com/oauth2/authorize?client_id=${clientId}&permissions=8&integration_type=0&scope=bot` : null;
}

export function fmtUnix(ts: number | null | undefined): string {
  const n = Number(ts);
  if (!n || n <= 0) return "";
  try {
    return new Date(n * 1000).toLocaleString();
  } catch {
    return String(n);
  }
}

export async function listBackupTokens(): Promise<BackupToken[]> {
  const res = await api.get<BackupTokensResponse>(BACKUP_URL);
  if (!res?.ok) throw new Error("Backup tokens request failed");
  return res.tokens ?? [];
}

export function buildAddForm(tokenValue: string, note: string): FormData {
  const fd = new FormData();
  fd.append("token_value", tokenValue);
  fd.append("note", note);
  return fd;
}

export const addBackupToken = (tokenValue: string, note: string) =>
  api.post<BackupTokenResult>(`${BACKUP_URL}/add`, buildAddForm(tokenValue, note));

export function deleteBackupToken(tokenId: string) {
  const fd = new FormData();
  fd.append("token_id", tokenId);
  return api.post<BackupTokenResult>(`${BACKUP_URL}/delete`, fd);
}
