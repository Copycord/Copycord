// Form state of the rule modal: prefill for edit and duplicate, the payload
// built from the form, and the client side provider checks. Secrets are
// prefilled as they come from GET /api/forwarding, in plain text inputs; HTTP
// header values arrive masked and the mask is posted back unless edited.

import { api, ApiError, errorText } from "../../lib/api";
import type {
  ForwardingRule,
  ForwardingRulePayload,
  ForwardingSaveResponse,
  ForwardingValidateResponse,
} from "../../types/forwarding";
import type { RuleEditorRequest } from "./editor";
import { RULES_URL, toList } from "./forwardingApi";

export const VALIDATE_URL = "/api/forwarding/validate";
export const CLIENT_GUILDS_URL = "/api/client-guilds";

// Warning shown when there is no working client token.
export const CLIENT_TOKEN_MESSAGE =
  "We couldn’t load any guilds yet. It looks like your Discord client token isn’t configured. Open the Configuration page, add your tokens, then come back here.";
export const GUILDS_RATE_LIMITED_MESSAGE = "Discord is rate limiting guild lookups. Try again in a few seconds.";
export const GUILDS_FAILED_MESSAGE = "Could not load the guild list; the rule can still apply to all guilds.";

/** What the rule modal says when GET /api/client-guilds fails. */
export function guildsErrorMessage(error: unknown): string {
  if (!error) return "";
  if (error instanceof ApiError) {
    const detail = String((error.body as { detail?: unknown } | null)?.detail ?? "");
    if (error.status === 429 || detail === "discord-rate-limited") return GUILDS_RATE_LIMITED_MESSAGE;
    if (error.status === 400 || detail.startsWith("client-token")) return CLIENT_TOKEN_MESSAGE;
  }
  return GUILDS_FAILED_MESSAGE;
}

export const PROVIDERS = [
  { value: "pushover", label: "Pushover" },
  { value: "telegram", label: "Telegram" },
  { value: "discord", label: "Discord" },
  { value: "http", label: "HTTP / API" },
] as const;

const WEBHOOK_RE = /^https?:\/\/(canary\.|ptb\.)?discord(app)?\.com\/api\/webhooks\/\d+\/.+/i;

/** What GET /api/forwarding sends in place of each HTTP header value; sent back, the server keeps the stored one. */
export const HEADER_MASK = "********";
export const MAX_HTTP_HEADERS = 20;
// An RFC 9110 token, the same check the server makes.
const HEADER_NAME_RE = /^[!#$%&'*+.^_`|~0-9A-Za-z-]+$/;

export interface HeaderRow {
  name: string;
  value: string;
}

export interface RuleForm {
  rule_id: string;
  label: string;
  guild_id: string;
  provider: string;
  enabled: boolean;
  pushover_app_token: string;
  pushover_user_key: string;
  discord_urls: string[];
  discord_username: string;
  discord_avatar_url: string;
  telegram_bot_token: string;
  telegram_chat_id: string;
  http_url: string;
  http_headers: HeaderRow[];
  keywords_any: string[];
  keywords_all: string[];
  channel_ids: string[];
  user_ids: string[];
  case_sensitive: boolean;
  include_embeds: boolean;
  include_bots: boolean;
  include_system: boolean;
  has_attachments: boolean;
}

export const EMPTY_FORM: RuleForm = {
  rule_id: "",
  label: "",
  guild_id: "",
  provider: "",
  enabled: true,
  pushover_app_token: "",
  pushover_user_key: "",
  discord_urls: [],
  discord_username: "",
  discord_avatar_url: "",
  telegram_bot_token: "",
  telegram_chat_id: "",
  http_url: "",
  http_headers: [],
  keywords_any: [],
  keywords_all: [],
  channel_ids: [],
  user_ids: [],
  case_sensitive: false,
  include_embeds: false,
  include_bots: false,
  include_system: false,
  has_attachments: false,
};

function str(v: unknown): string {
  return typeof v === "string" || typeof v === "number" ? String(v) : "";
}

function clean(list: string[]): string[] {
  return list.map((s) => s.trim()).filter(Boolean);
}

function headerRows(v: unknown): HeaderRow[] {
  if (!v || typeof v !== "object" || Array.isArray(v)) return [];
  return Object.entries(v as Record<string, unknown>).map(([name, value]) => ({ name, value: str(value) }));
}

/** Header rows with something typed in, trimmed; a row left blank on both sides is dropped. */
function filledHeaders(rows: HeaderRow[]): HeaderRow[] {
  return rows.map((r) => ({ name: r.name.trim(), value: r.value.trim() })).filter((r) => r.name || r.value);
}

export function formFromRule(rule: ForwardingRule): RuleForm {
  const cfg = rule.config ?? {};
  const f = rule.filters ?? {};
  const urls = Array.isArray(cfg.urls) && cfg.urls.length ? cfg.urls.map(String) : cfg.url ? [str(cfg.url)] : [];
  return {
    rule_id: rule.rule_id ?? "",
    label: rule.label ?? "",
    guild_id: str(rule.guild_id),
    provider: rule.provider ?? "",
    enabled: Boolean(rule.enabled),
    pushover_app_token: str(cfg.app_token),
    pushover_user_key: str(cfg.user_key),
    discord_urls: clean(urls),
    discord_username: str(cfg.username || cfg.bot_username || cfg.webhook_username),
    discord_avatar_url: str(cfg.avatar_url || cfg.bot_avatar_url || cfg.bot_avatar),
    telegram_bot_token: str(cfg.bot_token),
    telegram_chat_id: str(cfg.chat_id),
    http_url: rule.provider === "http" ? str(cfg.url) : "",
    http_headers: rule.provider === "http" ? headerRows(cfg.headers) : [],
    keywords_any: clean(toList(f.keywords_any)),
    keywords_all: clean(toList(f.keywords_all)),
    channel_ids: clean(toList(f.channel_ids)),
    user_ids: clean(toList(f.user_ids)),
    case_sensitive: Boolean(f.case_sensitive),
    include_embeds: Boolean(f.include_embeds),
    include_bots: Boolean(f.include_bots),
    include_system: Boolean(f.include_system),
    has_attachments: Boolean(f.has_attachments),
  };
}

export function formFromRequest(req: RuleEditorRequest): RuleForm {
  if (req.mode === "create") return { ...EMPTY_FORM };
  const form = formFromRule(req.rule);
  if (req.mode === "edit") return form;
  // A copy is a new rule with no stored headers to fall back on, so masked values must be typed again.
  const http_headers = form.http_headers.map((h) => (h.value === HEADER_MASK ? { ...h, value: "" } : h));
  return { ...form, rule_id: "", label: `${req.rule.label || ""} (Copy)`.trim(), http_headers };
}

export function modalTitle(req: RuleEditorRequest | null): string {
  if (req?.mode === "edit") return "Edit Forwarding Rule";
  if (req?.mode === "duplicate") return "Duplicate Forwarding Rule";
  return "New Forwarding Rule";
}

/** More filters starts open when any of its switches is on. */
export function hasAdvancedFilters(form: RuleForm): boolean {
  return form.case_sensitive || form.include_embeds || form.include_bots || form.include_system || form.has_attachments;
}

export function buildPayload(form: RuleForm): ForwardingRulePayload {
  const provider = form.provider.toLowerCase().trim();
  const config: Record<string, unknown> = {};
  if (provider === "pushover") {
    config.app_token = form.pushover_app_token.trim();
    config.user_key = form.pushover_user_key.trim();
  } else if (provider === "discord") {
    config.urls = clean(form.discord_urls);
    const username = form.discord_username.trim();
    const avatar = form.discord_avatar_url.trim();
    if (username) config.username = username;
    if (avatar) config.avatar_url = avatar;
  } else if (provider === "telegram") {
    config.bot_token = form.telegram_bot_token.trim();
    config.chat_id = form.telegram_chat_id.trim();
  } else if (provider === "http") {
    config.url = form.http_url.trim();
    config.headers = Object.fromEntries(filledHeaders(form.http_headers).map((h) => [h.name, h.value]));
  }
  return {
    rule_id: form.rule_id.trim() || null,
    guild_id: form.guild_id.trim() || null,
    label: form.label.trim(),
    provider,
    enabled: form.enabled,
    config,
    filters: {
      keywords_any: clean(form.keywords_any),
      keywords_all: clean(form.keywords_all),
      channel_ids: clean(form.channel_ids),
      user_ids: clean(form.user_ids),
      case_sensitive: form.case_sensitive,
      include_embeds: form.include_embeds,
      include_bots: form.include_bots,
      include_system: form.include_system,
      has_attachments: form.has_attachments,
    },
  };
}

function isHttpUrl(raw: string): boolean {
  try {
    const u = new URL(raw);
    return (u.protocol === "http:" || u.protocol === "https:") && Boolean(u.host);
  } catch {
    return false;
  }
}

/** The HTTP header rows as typed; the payload object alone would hide blank and repeated names. */
export function headersProblem(rows: HeaderRow[]): string | null {
  const filled = filledHeaders(rows);
  if (filled.length > MAX_HTTP_HEADERS) return `At most ${MAX_HTTP_HEADERS} headers are allowed.`;
  const seen = new Set<string>();
  for (const { name, value } of filled) {
    if (!name) return "Every header needs a name.";
    if (!HEADER_NAME_RE.test(name)) return `Invalid header name: ${name}`;
    const key = name.toLowerCase();
    if (key === "content-type") return "Content-Type is always application/json and cannot be set.";
    if (seen.has(key)) return `Header ${name} is listed more than once.`;
    seen.add(key);
    if (!value) return `Header ${name} needs a value.`;
  }
  return null;
}

/** The checks run before asking the server; null when they pass. */
export function localProblem(payload: ForwardingRulePayload, headers: HeaderRow[] = []): string | null {
  const cfg = payload.config;
  if (!payload.provider) return "Please choose a provider.";
  if (!payload.label) return "Please enter a name for this rule.";
  if (payload.provider === "telegram" && (!str(cfg.bot_token) || !str(cfg.chat_id))) {
    return "Telegram requires both a Bot Token and a Chat/Channel ID.";
  }
  if (payload.provider === "pushover" && (!str(cfg.app_token) || !str(cfg.user_key))) {
    return "Pushover requires both an Application Token and a User/Group Key.";
  }
  if (payload.provider === "discord") {
    const urls = Array.isArray(cfg.urls) ? (cfg.urls as string[]) : [];
    if (!urls.length) return "Discord requires at least one webhook URL.";
    const bad = urls.filter((u) => !WEBHOOK_RE.test(u));
    if (bad.length) return `Invalid Discord webhook URL(s):\n${bad.join("\n")}`;
  }
  if (payload.provider === "http") {
    const url = str(cfg.url);
    if (!url) return "HTTP / API requires an endpoint URL.";
    if (!isHttpUrl(url)) return "The endpoint URL must start with http:// or https://.";
    return headersProblem(headers);
  }
  return null;
}

/** Only Telegram and Pushover are checked by the server before saving. */
export function needsServerValidation(provider: string): boolean {
  return provider === "telegram" || provider === "pushover";
}

export function validateProvider(payload: ForwardingRulePayload): Promise<ForwardingValidateResponse> {
  return api.post<ForwardingValidateResponse>(VALIDATE_URL, { provider: payload.provider, config: payload.config });
}

export function saveRule(payload: ForwardingRulePayload): Promise<ForwardingSaveResponse> {
  return api.post<ForwardingSaveResponse>(RULES_URL, payload);
}

/** The toast text for a failed validate call. */
export function validateFailure(e: unknown): string {
  if (e instanceof ApiError) {
    const body = e.body as { errors?: unknown; detail?: unknown } | string | undefined;
    if (body && typeof body === "object") {
      if (Array.isArray(body.errors) && body.errors.length) return body.errors.map(String).join("\n");
      if (typeof body.detail === "string" && body.detail) return body.detail;
    }
    return errorText(e, "Provider details are invalid");
  }
  return errorText(e, "Validation failed");
}

export function saveFailure(e: unknown): string {
  return errorText(e, "Failed to save forwarding rule");
}
