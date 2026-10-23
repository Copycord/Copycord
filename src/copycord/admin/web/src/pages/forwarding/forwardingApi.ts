// Forwarding calls and the small helpers that read the replies: counts arrive as numbers or strings, filters as lists or CSV.

import { api } from "../../lib/api";
import type {
  ForwardingDeleteResponse,
  ForwardingRule,
  ForwardingSaveResponse,
} from "../../types/forwarding";

export const RULES_URL = "/api/forwarding";
export const COUNT_URL = "/api/forwarding/count";
export const COUNT_BY_RULE_URL = "/api/forwarding/count/by-rule";

export function ruleUrl(id: string): string {
  return `${RULES_URL}/${encodeURIComponent(id)}`;
}

export function deleteRule(id: string): Promise<ForwardingDeleteResponse> {
  return api.del<ForwardingDeleteResponse>(ruleUrl(id));
}

/** The card's Enable/Disable button: the whole rule posted back with enabled flipped. */
export function setRuleEnabled(rule: ForwardingRule, enabled: boolean): Promise<ForwardingSaveResponse> {
  return api.post<ForwardingSaveResponse>(RULES_URL, { ...rule, enabled });
}

export function toCount(v: unknown): number {
  const n = typeof v === "number" ? v : Number(v ?? 0);
  return Number.isFinite(n) ? n : 0;
}

export function toList(v: unknown): string[] {
  if (Array.isArray(v)) return v.map(String);
  if (typeof v === "string" && v.trim() !== "") return v.split(",").map((s) => s.trim());
  return [];
}

export function describeProvider(provider: string): string {
  switch (provider) {
    case "pushover":
      return "Pushover";
    case "discord":
      return "Discord";
    case "telegram":
      return "Telegram";
    case "http":
      return "Custom Webhook";
    default:
      return provider || "Custom";
  }
}

/** The card's one line about where a rule sends: the endpoint host for HTTP, never its path, query or headers. */
export function ruleSummary(rule: ForwardingRule): string | null {
  if ((rule.provider || "").toLowerCase() !== "http") return null;
  const raw = rule.config?.url;
  if (typeof raw !== "string" || !raw.trim()) return null;
  try {
    return new URL(raw.trim()).host || null;
  } catch {
    return null;
  }
}

/** Search over label, provider and both keyword lists, case insensitive. */
export function filterRules(rules: ForwardingRule[], query: string): ForwardingRule[] {
  const q = query.trim().toLowerCase();
  if (!q) return rules;
  return rules.filter((r) => {
    const f = r.filters ?? {};
    const hay = [r.label ?? "", r.provider ?? "", toList(f.keywords_any).join(" "), toList(f.keywords_all).join(" ")]
      .join(" ")
      .toLowerCase();
    return hay.includes(q);
  });
}
