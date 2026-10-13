// Pure helpers for the Token Message Cloning block: strategy text, token search, the
// bulk and verify summaries and the request paths. No token value is ever read here.

import type { BulkResult, UserToken, VerifyResult } from "../../../../types/user-tokens";

export const STRATEGY_DESCS: Record<string, string> = {
  round_robin: "Cycles through your accounts in turn so each sends an equal share.",
  sticky_author: "Each source user always sends from the same account, keeping identities consistent.",
};

export const BULK_LABELS: Record<string, string> = {
  added: "Added",
  duplicate: "Already added",
  invalid: "Invalid / not in server",
  error: "Error",
};

export const tokensPath = (mappingId: string, tail = ""): string =>
  `/api/guild-mappings/${encodeURIComponent(mappingId)}/user-tokens${tail}`;

export const tokenName = (t: UserToken): string => t.username || t.user_id || "User token";

/** Masked value plus the username, the second line of a row (and what the search matches). */
export const tokenSub = (t: UserToken): string => t.token_masked + (t.username ? ` · ${t.username}` : "");

export function filterTokens(tokens: UserToken[], query: string): UserToken[] {
  const q = query.trim().toLowerCase();
  if (!q) return tokens;
  return tokens.filter((t) => `${tokenName(t)}${tokenSub(t)}`.toLowerCase().includes(q));
}

export const countLabel = (n: number): string => `${n} token${n === 1 ? "" : "s"}`;

export function bulkSummary(added: number, total: number): string {
  const skipped = total - added;
  return `Added ${added} of ${total}${skipped ? `, ${skipped} skipped` : ""}.`;
}

export const bulkResultLabel = (r: BulkResult): string => r.masked + (r.username ? ` · ${r.username}` : "");

/** Non-working tokens first so failures are visible straight away. */
export function orderVerify(results: VerifyResult[]): VerifyResult[] {
  return results.slice().sort((a, b) => Number(a.ok) - Number(b.ok));
}

export const brokenIds = (results: VerifyResult[]): string[] => results.filter((r) => !r.ok).map((r) => r.id);

/** Delay inputs keep the typed text in the form; the save payload parses it. */
export const delayText = (v: unknown): string => (v === undefined || v === null ? "" : String(v));
