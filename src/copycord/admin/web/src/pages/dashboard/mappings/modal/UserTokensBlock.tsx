// "Token Message Cloning" section of the mapping modal (#user-tokens-section): the
// USE_USER_TOKENS switch, strategy, sticky options, delays and the per mapping token manager.
// The option values live in the mapping form state, so Save sends them with the rest. The
// token list is server state: add, bulk add, enable, remove, verify and delete all go straight
// to /api/guild-mappings/{id}/user-tokens. Only masked values are shown and nothing is logged.

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import type { ReactNode } from "react";
import { createPortal } from "react-dom";
import { api } from "../../../../lib/api";
import { Toggle, cx } from "../../../../components/ui";
import type {
  BulkResponse,
  UserToken,
  VerifyResponse,
} from "../../../../types/user-tokens";
import type { MappingSettingValue, MappingSettings } from "../../../../types/dashboard-mappings";
import { GhostButton } from "../../../system/buttons";
import { useNotify, useSafeConfirm } from "../../../system/providers";
import { normalizeStrategy, toBool } from "./model";
import {
  BULK_LABELS,
  STRATEGY_DESCS,
  brokenIds,
  bulkResultLabel,
  bulkSummary,
  countLabel,
  delayText,
  filterTokens,
  orderVerify,
  tokenName,
  tokenSub,
  tokensPath,
} from "./userTokens";

export interface UserTokensBlockProps {
  /** Null while creating or cloning: tokens can only be managed once the mapping exists. */
  mappingId: string | null;
  settings: MappingSettings;
  setSetting: (key: string, value: MappingSettingValue) => void;
  /** Current #mappingSettingsSearch text. The old page never filtered this section by it. */
  search: string;
}

const INPUT =
  "rounded-control border border-border bg-bg-2 px-3 py-2 text-sm text-text outline-none focus:border-accent";
const DESC = "mapping-toggle-desc text-xs leading-relaxed text-muted";
const ROW = "mapping-toggle-row";

interface Status {
  text: string;
  kind: "" | "ok" | "error";
}

function StatusText({ id, status }: { id: string; status: Status }) {
  return (
    <span
      id={id}
      aria-live="polite"
      className={cx(
        "user-token-status text-xs",
        status.kind === "error" ? "is-error text-danger" : status.kind === "ok" ? "is-ok text-success" : "text-muted",
      )}
    >
      {status.text}
    </span>
  );
}

function ResultRow({ label, status, cls }: { label: string; status: string; cls: string }) {
  return (
    <div className="user-token-bulk-row flex items-center justify-between gap-3 py-1 text-xs">
      <span className="btok truncate font-mono">{label}</span>
      <span className={cx("bstatus shrink-0", cls, cls === "added" ? "text-success" : "text-danger")}>{status}</span>
    </div>
  );
}

function BulkModal({ mappingId, onClose, onChanged }: { mappingId: string; onClose: () => void; onChanged: () => void }) {
  const [text, setText] = useState("");
  const [busy, setBusy] = useState(false);
  const [status, setStatus] = useState<Status>({ text: "", kind: "" });
  const [results, setResults] = useState<BulkResponse["results"]>([]);
  const closeRef = useRef(onClose);
  closeRef.current = onClose;

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key !== "Escape") return;
      e.stopImmediatePropagation();
      closeRef.current();
    };
    document.addEventListener("keydown", onKey, true);
    return () => document.removeEventListener("keydown", onKey, true);
  }, []);

  const submit = async () => {
    if (!text.trim()) {
      setStatus({ text: "Paste at least one token.", kind: "error" });
      return;
    }
    setBusy(true);
    setStatus({ text: "Validating… this can take a moment.", kind: "" });
    try {
      const j = await api.post<BulkResponse>(tokensPath(mappingId, "/bulk"), { tokens: text.trim() });
      if (!j.ok) {
        setStatus({ text: j.error || "Failed to add tokens.", kind: "error" });
        return;
      }
      setResults(j.results ?? []);
      setStatus({ text: bulkSummary(j.added, j.total), kind: j.added ? "ok" : "error" });
      setText("");
      onChanged();
    } catch (err) {
      setStatus({ text: err instanceof Error && err.message ? err.message : "Network error.", kind: "error" });
    } finally {
      setBusy(false);
    }
  };

  return createPortal(
    <div id="user-token-bulk-modal" className="modal show fixed inset-0 z-[60] flex items-center justify-center p-4" aria-hidden="false">
      <div className="modal-backdrop absolute inset-0 bg-black/60" onClick={onClose} />
      <div
        role="dialog"
        aria-modal="true"
        aria-labelledby="user-token-bulk-title"
        className="user-token-bulk-content relative max-h-[88vh] w-full max-w-lg overflow-y-auto rounded-2xl border border-border bg-card-solid text-text shadow-card"
      >
        <div className="flex items-center justify-between border-b border-border px-4 py-3">
          <div id="user-token-bulk-title" className="modal-title-main text-base font-semibold">
            Bulk add user tokens
          </div>
          <button
            type="button"
            id="user-token-bulk-close"
            aria-label="Close"
            onClick={onClose}
            className="verify-close rounded-control px-2 py-1 text-muted hover:text-text"
          >
            ✕
          </button>
        </div>
        <div className="user-token-bulk-body flex flex-col gap-3 p-4">
          <textarea
            id="user_token_bulk_input"
            className={cx(INPUT, "user-token-bulk-textarea min-h-32 w-full font-mono")}
            rows={6}
            spellCheck={false}
            autoComplete="off"
            wrap="off"
            placeholder="Paste tokens here, one per line…"
            value={text}
            onChange={(e) => setText(e.target.value)}
          />
          <div className="user-token-bulk-actions flex items-center gap-3">
            <GhostButton type="button" id="user-token-bulk-submit" loading={busy} onClick={() => void submit()}>
              Verify &amp; add
            </GhostButton>
            <StatusText id="user-token-bulk-status" status={status} />
          </div>
          <div id="user-token-bulk-results" className="user-token-bulk-results flex flex-col">
            {results.map((r, i) => (
              <ResultRow key={i} label={bulkResultLabel(r)} status={BULK_LABELS[r.status] ?? r.status} cls={r.status} />
            ))}
          </div>
        </div>
      </div>
    </div>,
    document.body,
  );
}

export function UserTokensBlock({ mappingId, settings, setSetting }: UserTokensBlockProps) {
  const notify = useNotify();
  const confirm = useSafeConfirm();
  const on = (k: string) => toBool(settings[k]);
  const strategy = normalizeStrategy(settings.USER_TOKEN_STRATEGY);
  const sticky = strategy === "sticky_author";

  const [tokens, setTokens] = useState<UserToken[] | null>(null);
  const [loadError, setLoadError] = useState(false);
  const [query, setQuery] = useState("");
  const [value, setValue] = useState("");
  const [reveal, setReveal] = useState(false);
  const [adding, setAdding] = useState(false);
  const [status, setStatus] = useState<Status>({ text: "", kind: "" });
  const [menuOpen, setMenuOpen] = useState(false);
  const [bulkOpen, setBulkOpen] = useState(false);
  const [verifying, setVerifying] = useState(false);
  const menuRef = useRef<HTMLDivElement>(null);
  const idRef = useRef(mappingId);
  idRef.current = mappingId;

  const load = useCallback(async () => {
    const id = idRef.current;
    if (!id) return;
    try {
      const j = await api.get<{ ok: boolean; tokens?: UserToken[] }>(tokensPath(id));
      if (idRef.current !== id) return;
      if (!j.ok) throw new Error("load");
      setTokens(j.tokens ?? []);
      setLoadError(false);
    } catch {
      if (idRef.current !== id) return;
      setLoadError(true);
    }
  }, []);

  useEffect(() => {
    setTokens(null);
    setLoadError(false);
    setQuery("");
    setValue("");
    setReveal(false);
    setStatus({ text: "", kind: "" });
    setMenuOpen(false);
    setBulkOpen(false);
    void load();
  }, [mappingId, load]);

  useEffect(() => {
    if (!menuOpen) return;
    const onDown = (e: MouseEvent) => {
      if (menuRef.current && !menuRef.current.contains(e.target as Node)) setMenuOpen(false);
    };
    const onKey = (e: KeyboardEvent) => {
      if (e.key !== "Escape") return;
      e.stopImmediatePropagation();
      setMenuOpen(false);
    };
    document.addEventListener("mousedown", onDown);
    document.addEventListener("keydown", onKey, true);
    return () => {
      document.removeEventListener("mousedown", onDown);
      document.removeEventListener("keydown", onKey, true);
    };
  }, [menuOpen]);

  const shown = useMemo(() => filterTokens(tokens ?? [], query), [tokens, query]);

  const addToken = async () => {
    if (!mappingId) return;
    const token = value.trim();
    if (!token) {
      setStatus({ text: "Enter a token.", kind: "error" });
      return;
    }
    setAdding(true);
    setStatus({ text: "Validating…", kind: "" });
    try {
      const j = await api.post<{ ok: boolean; error?: string }>(tokensPath(mappingId), { token });
      if (!j.ok) {
        setStatus({ text: j.error || "Failed to add token.", kind: "error" });
        return;
      }
      setValue("");
      setReveal(false);
      setStatus({ text: "Token added.", kind: "ok" });
      await load();
    } catch (err) {
      setStatus({ text: err instanceof Error && err.message ? err.message : "Network error.", kind: "error" });
    } finally {
      setAdding(false);
    }
  };

  const toggleToken = async (t: UserToken, enabled: boolean) => {
    if (!mappingId) return;
    try {
      await api.patch(tokensPath(mappingId, `/${encodeURIComponent(t.id)}`), { enabled });
    } catch {
      notify("Failed to update token.", "error");
    }
    await load();
  };

  const removeToken = async (t: UserToken) => {
    if (!mappingId) return;
    const name = tokenName(t);
    if (!(await confirm({ title: `Remove ${name}?`, confirmLabel: "Remove", danger: true }))) return;
    try {
      await api.del(tokensPath(mappingId, `/${encodeURIComponent(t.id)}`));
    } catch {
      setStatus({ text: "Failed to remove token.", kind: "error" });
    }
    await load();
  };

  /** Returns the deleted count, or null when the request failed. */
  const deleteTokens = async (body: { scope: "all" } | { ids: string[] }): Promise<number | null> => {
    if (!mappingId) return null;
    try {
      const j = await api.post<{ ok: boolean; deleted?: number; error?: string }>(tokensPath(mappingId, "/delete"), body);
      await load();
      if (!j.ok) {
        notify(j.error || "Failed to delete tokens.", "error");
        return null;
      }
      return j.deleted ?? 0;
    } catch {
      await load();
      notify("Network error while deleting tokens.", "error");
      return null;
    }
  };

  const verifyAll = async () => {
    setMenuOpen(false);
    if (!mappingId) return;
    setVerifying(true);
    let j: VerifyResponse;
    try {
      j = await api.post<VerifyResponse>(tokensPath(mappingId, "/verify"));
    } catch (err) {
      setVerifying(false);
      notify(err instanceof Error && err.message ? err.message : "Network error while verifying tokens.", "error");
      return;
    }
    setVerifying(false);
    if (!j.ok || !j.total) {
      notify(j.ok ? "There are no tokens to verify." : j.error || "Failed to verify tokens.", j.ok ? "info" : "error");
      return;
    }
    const body: ReactNode = (
      <div id="user-token-verify-results">
        <div className="user-token-verify-summary mb-2 text-xs">
          <span className="uv-ok text-success">{j.working} working</span>
          <span className="uv-sep mx-1">·</span>
          <span className="uv-bad text-danger">{j.broken} not working</span>
        </div>
        <div className="user-token-bulk-results user-token-verify-list flex max-h-64 flex-col overflow-y-auto">
          {orderVerify(j.results ?? []).map((r) => (
            <ResultRow
              key={r.id}
              label={r.username ? `${r.masked} · ${r.username}` : r.masked}
              status={r.ok ? "Working" : "Not working"}
              cls={r.ok ? "added" : "invalid"}
            />
          ))}
        </div>
      </div>
    );
    if (!j.broken) {
      await confirm({ title: "Token verification", body, confirmLabel: "Done" });
      return;
    }
    const ok = await confirm({
      title: "Token verification",
      body,
      confirmLabel: `Delete ${j.broken} non-working`,
      danger: true,
    });
    if (!ok) return;
    const deleted = await deleteTokens({ ids: brokenIds(j.results ?? []) });
    if (deleted != null) notify(`Removed ${deleted} non-working token${deleted === 1 ? "" : "s"}.`, "success");
  };

  const deleteAll = async () => {
    setMenuOpen(false);
    const ok = await confirm({
      title: "Delete all tokens?",
      body: (
        <>
          This removes <strong>all</strong> user tokens from this mapping. This cannot be undone.
        </>
      ),
      confirmLabel: "Delete all",
      danger: true,
    });
    if (!ok) return;
    const deleted = await deleteTokens({ scope: "all" });
    if (deleted != null) notify(`Deleted ${deleted} token${deleted === 1 ? "" : "s"}.`, "success");
  };

  const switchRow = (key: string, name: string, desc: string, settingKey: string) => (
    <div className={ROW} data-setting-key={settingKey}>
      <Toggle
        id={`map_${key}`}
        label={<span className="mapping-toggle-name">{name}</span>}
        description={<span className={DESC}>{desc}</span>}
        checked={on(key)}
        onChange={(v) => setSetting(key, v)}
      />
    </div>
  );

  const hasTokens = (tokens?.length ?? 0) > 0;

  return (
    <details id="user-tokens-section" className="mapping-section mapping-settings-section rounded-control border border-border">
      <summary className="mapping-section-header cursor-pointer select-none px-3 py-2 text-sm font-semibold">
        Token Message Cloning
      </summary>
      <div className="mapping-section-body flex flex-col gap-3 border-t border-border p-3">
        <div className="user-token-disclaimer rounded-control border border-danger/40 bg-danger/10 p-3 text-xs leading-relaxed">
          <strong>Warning:</strong> This feature uses user tokens. Discord may rate limit your accounts or permanently ban
          them at any time. Only use accounts you are willing to lose.
        </div>

        <div className={cx(ROW, "mapping-setting-field")} data-setting-key="use_user_tokens">
          <Toggle
            id="map_USE_USER_TOKENS"
            label={<span className="mapping-toggle-name">Send messages as users</span>}
            description={
              <span className={DESC}>
                Send cloned messages from real user accounts instead of webhooks. Accounts must be in the clone server.
              </span>
            }
            checked={on("USE_USER_TOKENS")}
            onChange={(v) => setSetting("USE_USER_TOKENS", v)}
          />
        </div>

        <div id="user-token-options" className="user-token-options flex flex-col gap-3">
          <div className={cx(ROW, "flex items-center justify-between gap-3")}>
            <div className="mapping-toggle-info flex flex-col">
              <label htmlFor="ut_strategy" className="mapping-toggle-name text-sm font-semibold">
                Account selection
              </label>
              <span id="ut_strategy_desc" className={DESC}>
                {STRATEGY_DESCS[strategy]}
              </span>
            </div>
            <select
              id="ut_strategy"
              className={cx(INPUT, "user-token-select cursor-pointer")}
              value={strategy}
              onChange={(e) => setSetting("USER_TOKEN_STRATEGY", e.target.value)}
            >
              <option value="round_robin" className="bg-card-solid">
                Rotate evenly
              </option>
              <option value="sticky_author" className="bg-card-solid">
                Sticky per author
              </option>
            </select>
          </div>

          {sticky ? (
            <div id="user-token-sticky-options" className="user-token-sticky-options flex flex-col gap-3 pl-3">
              {switchRow(
                "USER_TOKEN_STICKY_NICKNAME",
                "Mirror author nickname",
                "Rename the assigned account in the clone server to the host author's display name.",
                "user_token_sticky_nickname",
              )}
              {switchRow(
                "USER_TOKEN_STICKY_AVATAR",
                "Mirror author avatar",
                "Attempt to replace the avatar of the token via API to match the sender.",
                "user_token_sticky_avatar",
              )}
              {switchRow(
                "USER_TOKEN_STICKY_ROLES",
                "Mirror author roles",
                "Give the assigned account the cloned roles that match the host author's roles.",
                "user_token_sticky_roles",
              )}
            </div>
          ) : null}

          {switchRow(
            "USER_TOKEN_FALLBACK_WEBHOOK",
            "Fall back to webhook",
            "If no token can send the message, use the normal webhook instead of dropping it.",
            "user_token_fallback_webhook",
          )}
          {switchRow(
            "USER_TOKEN_TYPING",
            "Show typing indicator",
            "Briefly show “typing…” before each message so it looks more human.",
            "user_token_typing",
          )}
          {switchRow(
            "USER_TOKEN_LINKS_ONLY",
            "Attachments as links",
            "Post source attachment links instead of re-uploading the files.",
            "user_token_links_only",
          )}
          {switchRow(
            "USER_TOKEN_USE_PROXIES",
            "Send through proxies",
            "Give each account its own proxy from your proxy list. Uses the same proxy list on the dashboard.",
            "user_token_use_proxies",
          )}

          <div className={cx(ROW, "flex items-center justify-between gap-3")}>
            <div className="mapping-toggle-info flex flex-col">
              <span className="mapping-toggle-name text-sm font-semibold">Send delay</span>
              <span className={DESC}>Random pause before each message, in seconds.</span>
            </div>
            <div className="user-token-delay flex items-center gap-2">
              <input
                type="number"
                id="ut_min_delay"
                aria-label="Minimum send delay"
                className={cx(INPUT, "user-token-num w-20")}
                min={0}
                max={30}
                step={0.5}
                placeholder="min"
                value={delayText(settings.USER_TOKEN_MIN_DELAY)}
                onChange={(e) => setSetting("USER_TOKEN_MIN_DELAY", e.target.value)}
              />
              <span className="user-token-delay-sep text-xs text-muted">to</span>
              <input
                type="number"
                id="ut_max_delay"
                aria-label="Maximum send delay"
                className={cx(INPUT, "user-token-num w-20")}
                min={0}
                max={30}
                step={0.5}
                placeholder="max"
                value={delayText(settings.USER_TOKEN_MAX_DELAY)}
                onChange={(e) => setSetting("USER_TOKEN_MAX_DELAY", e.target.value)}
              />
            </div>
          </div>
        </div>

        <div id="user-tokens-manager" className="user-tokens-manager flex flex-col gap-3">
          {!mappingId ? (
            <div id="user-tokens-save-first" className="user-tokens-hint text-xs text-muted">
              Save this mapping first, then reopen it to add user tokens.
            </div>
          ) : (
            <div id="user-tokens-editor" className="flex flex-col gap-3">
              <div className="user-token-add flex flex-col gap-2">
                <div className="user-token-add-label text-sm font-semibold">Add user token</div>
                <div className="token-wrap relative">
                  <input
                    type={reveal ? "text" : "password"}
                    id="user_token_value"
                    autoComplete="off"
                    spellCheck={false}
                    className={cx(INPUT, "token-input w-full pr-10")}
                    placeholder="Paste a user (self-bot) token"
                    value={value}
                    onChange={(e) => setValue(e.target.value)}
                    onKeyDown={(e) => {
                      if (e.key === "Enter") {
                        e.preventDefault();
                        void addToken();
                      }
                    }}
                  />
                  <button
                    type="button"
                    className="reveal-btn token-user-btn absolute right-2 top-1/2 -translate-y-1/2 text-xs text-muted hover:text-text"
                    data-target="user_token_value"
                    aria-pressed={reveal}
                    title={reveal ? "Hide" : "Show"}
                    aria-label={reveal ? "Hide token" : "Show token"}
                    onClick={() => setReveal((r) => !r)}
                  >
                    {reveal ? "Hide" : "Show"}
                  </button>
                </div>
                <div className="user-token-add-row flex items-center gap-2">
                  <GhostButton type="button" id="user-token-add" loading={adding} onClick={() => void addToken()}>
                    Add token
                  </GhostButton>
                  <GhostButton type="button" id="user-token-bulk-open" onClick={() => setBulkOpen(true)}>
                    Bulk add
                  </GhostButton>
                  <StatusText id="user-token-status" status={status} />
                </div>
              </div>

              {hasTokens ? (
                <>
                  <input
                    type="text"
                    id="user_tokens_search"
                    className={cx(INPUT, "mapping-search-input user-tokens-search")}
                    placeholder="Search tokens…"
                    autoComplete="off"
                    value={query}
                    onChange={(e) => setQuery(e.target.value)}
                  />
                  <div id="user-tokens-list-header" className="user-tokens-list-header flex items-center justify-between">
                    <div id="user-tokens-count" className="user-tokens-count text-xs text-muted">
                      {countLabel(tokens?.length ?? 0)}
                    </div>
                    <div ref={menuRef} className="user-token-settings relative">
                      <button
                        type="button"
                        id="user-token-settings-open"
                        className="user-token-settings-btn rounded-control border border-border px-2 py-1 text-sm text-accent hover:bg-white/8"
                        title="Token options"
                        aria-haspopup="true"
                        aria-expanded={menuOpen}
                        aria-label="Token options"
                        onClick={() => setMenuOpen((o) => !o)}
                      >
                        ⚙
                      </button>
                      {menuOpen ? (
                        <div
                          id="user-token-settings-menu"
                          role="menu"
                          className="user-token-settings-menu absolute right-0 z-10 mt-1 flex min-w-44 flex-col rounded-control border border-border bg-card-solid p-1 shadow-card"
                        >
                          <button
                            type="button"
                            id="user-token-verify"
                            role="menuitem"
                            className="user-token-menu-item rounded-control px-3 py-2 text-left text-sm hover:bg-white/8"
                            onClick={() => void verifyAll()}
                          >
                            <span>Verify tokens</span>
                          </button>
                          <button
                            type="button"
                            id="user-token-delete-all"
                            role="menuitem"
                            className="user-token-menu-item is-danger rounded-control px-3 py-2 text-left text-sm text-danger hover:bg-white/8"
                            onClick={() => void deleteAll()}
                          >
                            <span>Delete all tokens</span>
                          </button>
                        </div>
                      ) : null}
                    </div>
                  </div>
                </>
              ) : null}

              <div id="userTokensList" className="user-tokens-list flex flex-col gap-2">
                {verifying ? <div className={cx(DESC, "user-token-verify-loading")}>Verifying tokens…</div> : null}
                {tokens === null && !loadError ? <div className={DESC}>Loading…</div> : null}
                {loadError ? <div className={DESC}>Failed to load tokens.</div> : null}
                {tokens !== null && !hasTokens ? <div className={DESC}>No user tokens yet. Add one above.</div> : null}
                {shown.map((t) => (
                  <div
                    key={t.id}
                    className="user-token-row flex items-center justify-between gap-3 rounded-control border border-border px-3 py-2"
                    data-token-id={t.id}
                  >
                    <div className="user-token-meta flex min-w-0 flex-col">
                      <span className="user-token-name truncate text-sm font-semibold">{tokenName(t)}</span>
                      <span className={cx(DESC, "user-token-sub truncate")}>{tokenSub(t)}</span>
                    </div>
                    <div className="user-token-actions flex items-center gap-3">
                      <Toggle
                        id={`user-token-enabled-${t.id}`}
                        label={`Enable ${tokenName(t)}`}
                        hideLabel
                        checked={t.enabled}
                        onChange={(v) => void toggleToken(t, v)}
                      />
                      <GhostButton type="button" danger compact className="user-token-remove" onClick={() => void removeToken(t)}>
                        Remove
                      </GhostButton>
                    </div>
                  </div>
                ))}
                {hasTokens && query.trim() && shown.length === 0 ? (
                  <div id="user-tokens-no-match" className={DESC}>
                    No tokens match your search.
                  </div>
                ) : null}
              </div>
            </div>
          )}
        </div>
      </div>
      {bulkOpen && mappingId ? (
        <BulkModal mappingId={mappingId} onClose={() => setBulkOpen(false)} onChanged={() => void load()} />
      ) : null}
    </details>
  );
}
