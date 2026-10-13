// Defaults, section layout and pure form logic of the mapping modal, ported from
// openMappingModal, collectMappingForm and validateMappingFields in app.js.

import { api } from "../../../../lib/api";
import type {
  DashboardMapping,
  MappingFormState,
  MappingSavePayload,
  MappingSaveResponse,
  MappingSettings,
  MappingSettingValue,
  MsgFeatureKey,
} from "../../../../types/dashboard-mappings";

export const DEFAULT_MAPPING_SETTINGS: MappingSettings = {
  DELETE_CHANNELS: true,
  CLONE_MESSAGES: true,
  DELETE_THREADS: true,
  DELETE_MESSAGES: true,
  EDIT_MESSAGES: true,
  RESEND_EDITED_MESSAGES: true,
  REPOSITION_CHANNELS: true,
  DELETE_ROLES: true,
  UPDATE_ROLES: true,
  CLONE_EMOJI: true,
  CLONE_STICKER: true,
  CLONE_ROLES: true,
  MIRROR_ROLE_PERMISSIONS: false,
  MIRROR_CHANNEL_PERMISSIONS: false,
  ENABLE_CLONING: true,
  RENAME_CHANNELS: true,
  SYNC_CHANNEL_NSFW: false,
  SYNC_CHANNEL_TOPIC: false,
  SYNC_CHANNEL_SLOWMODE: false,
  REARRANGE_ROLES: false,
  CLONE_ROLE_ICONS: false,
  CLONE_ROLE_STYLES: false,
  CLONE_VOICE: true,
  CLONE_VOICE_PROPERTIES: false,
  CLONE_STAGE: true,
  CLONE_STAGE_PROPERTIES: false,
  CLONE_GUILD_ICON: false,
  CLONE_GUILD_BANNER: false,
  CLONE_GUILD_SPLASH: false,
  CLONE_GUILD_DISCOVERY_SPLASH: false,
  CLONE_SERVER_TAG: false,
  SYNC_GUILD_DESCRIPTION: false,
  SYNC_FORUM_PROPERTIES: false,
  ANONYMIZE_USERS: false,
  DISABLE_EVERYONE_MENTIONS: false,
  DISABLE_ROLE_MENTIONS: false,
  TAG_REPLY_MSG: false,
  APPEND_TIMESTAMP: false,
  APPEND_AUTHOR: false,
  DB_CLEANUP_MSG: true,
  ON_DEMAND_WEBHOOKS: true,
  USE_USER_TOKENS: false,
  USER_TOKEN_STRATEGY: "round_robin",
  USER_TOKEN_FALLBACK_WEBHOOK: true,
  USER_TOKEN_TYPING: false,
  USER_TOKEN_MIN_DELAY: 0,
  USER_TOKEN_MAX_DELAY: 0,
  USER_TOKEN_LINKS_ONLY: false,
  USER_TOKEN_USE_PROXIES: false,
  USER_TOKEN_STICKY_NICKNAME: false,
  USER_TOKEN_STICKY_AVATAR: false,
  USER_TOKEN_STICKY_ROLES: false,
};

export const FRIENDLY_NAMES: Record<string, string> = {
  ENABLE_CLONING: "Enable Cloning",
  CLONE_MESSAGES: "Clone Messages",
  ON_DEMAND_WEBHOOKS: "On-Demand Webhooks",
  DB_CLEANUP_MSG: "Auto-Clean Message DB",
  DELETE_CHANNELS: "Delete Removed Channels",
  DELETE_THREADS: "Delete Removed Threads",
  REPOSITION_CHANNELS: "Reposition Channels",
  RENAME_CHANNELS: "Rename Channels",
  SYNC_CHANNEL_NSFW: "Sync NSFW Flag",
  SYNC_CHANNEL_TOPIC: "Sync Channel Topic",
  SYNC_CHANNEL_SLOWMODE: "Sync Slowmode",
  SYNC_FORUM_PROPERTIES: "Sync Forum Properties",
  MIRROR_CHANNEL_PERMISSIONS: "Mirror Channel Permissions",
  DELETE_MESSAGES: "Delete Messages",
  EDIT_MESSAGES: "Edit Messages",
  RESEND_EDITED_MESSAGES: "Resend Edited Messages",
  CLONE_ROLES: "Clone Roles",
  DELETE_ROLES: "Delete Removed Roles",
  UPDATE_ROLES: "Update Role Properties",
  REARRANGE_ROLES: "Rearrange Roles",
  CLONE_ROLE_ICONS: "Clone Role Icons",
  CLONE_ROLE_STYLES: "Clone Role Styles",
  MIRROR_ROLE_PERMISSIONS: "Mirror Role Permissions",
  CLONE_EMOJI: "Clone Emoji",
  CLONE_STICKER: "Clone Stickers",
  CLONE_VOICE: "Clone Voice Channels",
  CLONE_VOICE_PROPERTIES: "Sync Voice Properties",
  CLONE_STAGE: "Clone Stage Channels",
  CLONE_STAGE_PROPERTIES: "Sync Stage Properties",
  CLONE_GUILD_ICON: "Clone Server Icon",
  CLONE_GUILD_BANNER: "Clone Server Banner",
  CLONE_GUILD_SPLASH: "Clone Invite Splash",
  CLONE_GUILD_DISCOVERY_SPLASH: "Clone Discovery Splash",
  CLONE_SERVER_TAG: "Clone Server Tag",
  SYNC_GUILD_DESCRIPTION: "Sync Server Description",
};

export const SECTIONS: ReadonlyArray<{ name: string; keys: readonly string[] }> = [
  { name: "General", keys: ["ENABLE_CLONING", "CLONE_MESSAGES", "ON_DEMAND_WEBHOOKS", "DB_CLEANUP_MSG"] },
  {
    name: "Channels",
    keys: [
      "DELETE_CHANNELS", "DELETE_THREADS", "REPOSITION_CHANNELS", "RENAME_CHANNELS", "SYNC_CHANNEL_NSFW",
      "SYNC_CHANNEL_TOPIC", "SYNC_CHANNEL_SLOWMODE", "SYNC_FORUM_PROPERTIES", "MIRROR_CHANNEL_PERMISSIONS",
    ],
  },
  { name: "Messages", keys: ["DELETE_MESSAGES", "EDIT_MESSAGES", "RESEND_EDITED_MESSAGES"] },
  {
    name: "Roles",
    keys: [
      "CLONE_ROLES", "DELETE_ROLES", "UPDATE_ROLES", "REARRANGE_ROLES", "CLONE_ROLE_ICONS", "CLONE_ROLE_STYLES",
      "MIRROR_ROLE_PERMISSIONS",
    ],
  },
  {
    name: "Assets",
    keys: ["CLONE_EMOJI", "CLONE_STICKER", "CLONE_VOICE", "CLONE_VOICE_PROPERTIES", "CLONE_STAGE", "CLONE_STAGE_PROPERTIES"],
  },
  {
    name: "Server Identity",
    keys: [
      "CLONE_GUILD_ICON", "CLONE_GUILD_BANNER", "CLONE_GUILD_SPLASH", "CLONE_GUILD_DISCOVERY_SPLASH",
      "CLONE_SERVER_TAG", "SYNC_GUILD_DESCRIPTION",
    ],
  },
];

export const DISABLED_SETTINGS: Record<string, string> = {
  CLONE_SERVER_TAG: "Discord doesn't allow bots to set server tags yet, so this can't be applied.",
};

export const SETTING_TIPS: Record<string, string> = {
  HOST_GUILD_ID: "ID of the host server to mirror. Your account bot must be a member of this server.",
  CLONE_GUILD_ID: "ID of the clone server. Your bot must be a member of this server.",
  DELETE_ROLES: "When enabled, remove any roles in the clone that do not exist in the host.",
  UPDATE_ROLES:
    "Keep role properties (name, color, permissions) in sync after creation. When off, you can freely edit roles in the clone.",
  MIRROR_ROLE_PERMISSIONS: "When enabled, mirror role permissions from the host to the clone.",
  EDIT_MESSAGES: "Messages that are edited in the host will be edited in the clone.",
  RESEND_EDITED_MESSAGES:
    "When enabled, if a message edited in the host does not exist in the clone, it will be resent as a new message. Requires Edit Messages to be enabled.",
  DELETE_MESSAGES: "Messages that are deleted in the host will be deleted in the clone.",
  DELETE_THREADS: "When enabled, delete in the clone any threads that were removed in the host.",
  DELETE_CHANNELS: "When enabled, delete in the clone any channels that were removed in the host.",
  CLONE_EMOJI: "When enabled, synchronize emojis from the host to the clone.",
  CLONE_STICKER: "When enabled, synchronize stickers from the host to the clone.",
  CLONE_ROLES: "When enabled, synchronize roles from the host to the clone.",
  MIRROR_CHANNEL_PERMISSIONS: "When enabled, channel permissions will sync with the host. Clone Roles must be enabled.",
  ENABLE_CLONING: "Master switch for cloning. Turn this off to stop all sync activity while keeping other features running.",
  CLONE_MESSAGES:
    "When enabled, messages sent in the host will be copied to the clone in real-time. When disabled, webhook creation is also skipped during sync. Webhooks will be created automatically when re-enabled.",
  REPOSITION_CHANNELS: "When enabled, channels moved in the host server will be repositioned in the clone.",
  RENAME_CHANNELS: "When enabled, channels/categories renamed in the host server will be renamed in the clone.",
  SYNC_CHANNEL_NSFW: "When enabled, NSFW flags for channels will be synchronized from the host to the clone.",
  SYNC_CHANNEL_TOPIC: "When enabled, channel topics will be synchronized from the host to the clone.",
  SYNC_CHANNEL_SLOWMODE: "When enabled, slowmode settings for channels will be synchronized from the host to the clone.",
  CLONE_VOICE: "When enabled, voice channels will be cloned from the host to the clone.",
  CLONE_VOICE_PROPERTIES:
    "When enabled, voice channel properties (bitrate, user limit) will be synchronized from the host to the clone.",
  CLONE_STAGE: "When enabled, stage channels will be cloned from the host to the clone.",
  CLONE_STAGE_PROPERTIES:
    "When enabled, stage channel properties (bitrate, user limit) will be synchronized from the host to the clone.",
  CLONE_GUILD_ICON: "Clone the server icon from the host to the clone.",
  CLONE_GUILD_BANNER: "Clone the server banner from the host to the clone.",
  CLONE_GUILD_SPLASH: "Clone the invite splash image from the host to the clone.",
  CLONE_GUILD_DISCOVERY_SPLASH: "Clone the discovery splash image from the host to the clone.",
  CLONE_SERVER_TAG:
    "Clone the server tag from the host to the clone. Requires the clone server to have access to Server Tags.",
  SYNC_GUILD_DESCRIPTION: "Sync the server description from the host to the clone.",
  SYNC_FORUM_PROPERTIES:
    "When enabled, forum properties (topic, tags, etc.) will be synchronized from the host to the clone.",
  REARRANGE_ROLES: "When enabled, roles will be rearranged in the clone to match the host server's role hierarchy.",
  CLONE_ROLE_ICONS:
    "When enabled, role icons (custom images and unicode emoji) will be cloned from the host to the clone. Requires the clone server to have Boost Level 2+.",
  CLONE_ROLE_STYLES:
    "When enabled, role color styles will be cloned from the host to the clone. Requires the clone server to have Boost Level 2+.",
  DB_CLEANUP_MSG: "Automatically clean up message records older than 7 days from the database.",
  ON_DEMAND_WEBHOOKS:
    "When enabled, webhooks are only created when a channel receives its first message, instead of during sync. This makes server cloning much faster by skipping upfront webhook creation.",
};

export interface MsgFeature {
  key: MsgFeatureKey;
  name: string;
  desc: string;
}

export const MSG_FEATURES: readonly MsgFeature[] = [
  { key: "TAG_REPLY_MSG", name: "Tag Replies", desc: "When a message is a reply, prepend a link to the original message being replied to." },
  { key: "ANONYMIZE_USERS", name: "Anonymize Users", desc: "Replace usernames and avatars with random names and images." },
  { key: "DISABLE_EVERYONE_MENTIONS", name: "Disable @everyone", desc: "Strip @everyone and @here mentions so they don't ping in the clone." },
  { key: "DISABLE_ROLE_MENTIONS", name: "Disable Role Mentions", desc: "Strip role mentions so they don't ping in the clone." },
  { key: "APPEND_TIMESTAMP", name: "Append Timestamp", desc: "Shows when the original message was sent using Discord's native timestamp format." },
  { key: "APPEND_AUTHOR", name: "Append Author", desc: "Shows the original author's username below the cloned message." },
];

export type MappingMode = "create" | "edit" | "clone";

/** "true"/"false" strings from older rows count as booleans, like the old normalizer. */
export function toBool(raw: unknown): boolean {
  if (typeof raw === "boolean") return raw;
  if (typeof raw === "string") {
    const lower = raw.toLowerCase();
    if (lower === "true") return true;
    if (lower === "false") return false;
  }
  return !!raw;
}

/** "random" was removed; any legacy or unknown strategy is treated as rotate. */
export function normalizeStrategy(v: unknown): string {
  return v === "sticky_author" ? "sticky_author" : "round_robin";
}

export function modeOf(mapping: DashboardMapping | null | undefined, mode?: "clone"): MappingMode {
  if (mode === "clone" && mapping) return "clone";
  return mapping ? "edit" : "create";
}

/** Initial form state: edit shows the mapping, clone only copies its settings, create uses the defaults. */
export function initialForm(mapping: DashboardMapping | null | undefined, mode: MappingMode): MappingFormState {
  const source = mode === "create" ? null : (mapping?.settings ?? null);
  const settings: MappingSettings = {};
  for (const [key, def] of Object.entries(DEFAULT_MAPPING_SETTINGS)) {
    const raw = source && key in source ? source[key] : def;
    if (key === "USER_TOKEN_STRATEGY") settings[key] = normalizeStrategy(raw);
    else if (typeof def === "boolean") settings[key] = toBool(raw);
    else settings[key] = (raw as MappingSettingValue | null | undefined) ?? def;
  }
  const edit = mode === "edit" && mapping;
  return {
    mapping_id: edit ? String(mapping.mapping_id) : "",
    mapping_name: edit ? (mapping.mapping_name ?? "") : "",
    original_guild_id: edit && mapping.original_guild_id ? String(mapping.original_guild_id) : "",
    cloned_guild_id: edit && mapping.cloned_guild_id ? String(mapping.cloned_guild_id) : "",
    settings,
  };
}

export type MappingField = "mapping_name" | "original_guild_id" | "cloned_guild_id";

/** Fields left empty, in form order; the first one gets focus. */
export function invalidFields(form: MappingFormState): MappingField[] {
  const out: MappingField[] = [];
  if (!form.mapping_name.trim()) out.push("mapping_name");
  if (!form.original_guild_id.trim()) out.push("original_guild_id");
  if (!form.cloned_guild_id.trim()) out.push("cloned_guild_id");
  return out;
}

const num = (v: MappingSettingValue | undefined): number => {
  const n = parseFloat(String(v ?? ""));
  return Number.isFinite(n) ? n : 0;
};

export function buildPayload(form: MappingFormState): MappingSavePayload {
  const settings: MappingSettings = { ...form.settings };
  settings.USER_TOKEN_STRATEGY = String(settings.USER_TOKEN_STRATEGY ?? "round_robin");
  settings.USER_TOKEN_MIN_DELAY = num(settings.USER_TOKEN_MIN_DELAY);
  settings.USER_TOKEN_MAX_DELAY = num(settings.USER_TOKEN_MAX_DELAY);
  return {
    mapping_name: form.mapping_name.trim(),
    original_guild_id: form.original_guild_id.trim(),
    cloned_guild_id: form.cloned_guild_id.trim(),
    original_guild_name: "",
    cloned_guild_name: "",
    settings,
  };
}

export const snapshot = (form: MappingFormState): string => JSON.stringify(form);

export function saveMapping(form: MappingFormState): Promise<MappingSaveResponse> {
  const payload = buildPayload(form);
  return form.mapping_id
    ? api.patch<MappingSaveResponse>(`/api/guild-mappings/${encodeURIComponent(form.mapping_id)}`, payload)
    : api.post<MappingSaveResponse>("/api/guild-mappings", payload);
}

/** Rows (by key, name and tip) that match the settings search; an empty query matches all. */
export function settingMatches(key: string, query: string): boolean {
  const q = query.trim().toLowerCase();
  if (!q) return true;
  const hay = [key, FRIENDLY_NAMES[key] ?? key, SETTING_TIPS[key] ?? "", DISABLED_SETTINGS[key] ?? ""];
  return hay.some((h) => h.toLowerCase().includes(q));
}
