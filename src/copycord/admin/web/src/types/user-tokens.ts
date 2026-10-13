// Per mapping user tokens (/api/guild-mappings/{id}/user-tokens). Only the masked value ever
// comes back from the server.

export interface UserToken {
  id: string;
  label: string;
  username: string;
  user_id: string;
  enabled: boolean;
  token_masked: string;
  use_count: number;
  last_used: string | number | null;
}

export type BulkStatus = "added" | "duplicate" | "invalid" | "error";

export interface BulkResult {
  masked: string;
  status: BulkStatus | string;
  username?: string;
}

export interface BulkResponse {
  ok: boolean;
  error?: string;
  added: number;
  total: number;
  results: BulkResult[];
}

export interface VerifyResult {
  id: string;
  masked: string;
  username: string;
  ok: boolean;
}

export interface VerifyResponse {
  ok: boolean;
  error?: string;
  total: number;
  working: number;
  broken: number;
  results: VerifyResult[];
}
