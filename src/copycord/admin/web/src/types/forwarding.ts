// Replies of the Forwarding page endpoints (app.py forwarding routes).

export type ForwardingProvider = "discord" | "pushover" | "telegram";

/** Filters as _normalize_forwarding_rule_filters returns them. */
export interface ForwardingFilters {
  keywords_any?: string[] | string;
  keywords_all?: string[] | string;
  channel_ids?: string[] | string;
  user_ids?: string[] | string;
  [key: string]: unknown;
}

/** One rule; provider is free text on the server, so unknown values stay possible. */
export interface ForwardingRule {
  rule_id: string;
  label: string;
  guild_id?: string | null;
  provider: ForwardingProvider | (string & {});
  enabled: boolean | number;
  config?: Record<string, unknown>;
  filters?: ForwardingFilters;
  [key: string]: unknown;
}

/** GET /api/forwarding */
export interface ForwardingListResponse {
  ok: boolean;
  items: ForwardingRule[];
}

/** GET /api/forwarding/count */
export interface ForwardingCountResponse {
  ok: boolean;
  count: number | string;
  by_provider?: Record<string, number>;
}

/** GET /api/forwarding/count/by-rule */
export interface ForwardingCountByRuleResponse {
  ok: boolean;
  by_rule: Record<string, number | string>;
}

/** DELETE /api/forwarding/{rule_id} */
export interface ForwardingDeleteResponse {
  ok: boolean;
  deleted: string;
}

/** POST /api/forwarding; the server answers with the id only, so callers reload the list. */
export interface ForwardingSaveResponse {
  ok: boolean;
  rule_id: string;
  guild_id: string;
  item?: ForwardingRule;
}
