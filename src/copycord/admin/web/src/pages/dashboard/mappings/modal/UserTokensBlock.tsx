// "Token Message Cloning" section of the mapping modal (#user-tokens-section): the
// USE_USER_TOKENS switch, strategy, sticky options, delays, the per mapping token manager and the
// warm pool options (Keep online, Token status). Live account status is on the System page.
// The option values live in the mapping form state, so Save sends them with the rest. The
// token list is server state: add, bulk add, enable, remove, verify and delete all go straight
// to /api/guild-mappings/{id}/user-tokens. Only masked values are shown and nothing is logged.

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import type { ReactNode } from "react";
import { createPortal } from "react-dom";
import { api, errorText } from "../../../../lib/api";
import { Button, cx } from "../../../../components/ui";
import { MODAL_BACKDROP, MODAL_CARD, MODAL_CLOSE, MODAL_HEADER, MODAL_OVERLAY, useScrollLock } from "../../../../components/Modal";
import type {
  BulkResponse,
  UserToken,
  VerifyResponse,
} from "../../../../types/user-tokens";
import type { MappingSettingValue, MappingSettings } from "../../../../types/dashboard-mappings";
import { GhostButton } from "../../../system/buttons";
import { useNotify, useSafeConfirm } from "../../../system/providers";
import { ToolbarSelect } from "../../../channels/ToolbarSelect";
import { normalizeKeepOnline, normalizePresence, normalizeStrategy, toBool } from "./model";
import { DESC, ROW, SECTION, SectionSummary, Switch, ToggleRow } from "./parts";
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
  /** Current #mappingSettingsSearch text. */
  search: string;
}

// The strategy dropdown toggle: 34px, 13px, purple caret; the field dropdown adds the caret room.
const STRATEGY_TOGGLE =
  "user-token-select dd-toggle min-h-[34px] rounded-btn border border-border-input bg-control py-2 pl-2.5 font-sans text-[13px] text-text outline-0 transition-[border,box-shadow,background] duration-200 focus:border-accent";
const STRATEGIES = [
  { value: "round_robin", label: "Rotate evenly" },
  { value: "sticky_author", label: "Sticky per author" },
];

const PRESENCES = [
  { value: "random", label: "Random per session" },
  { value: "online", label: "Online" },
  { value: "busy", label: "Busy" },
  { value: "invisible", label: "Invisible" },
];
const PRESENCE_DESCS: Record<string, string> = {
  random: "Each session picks Online, Busy or Invisible once.",
  online: "Every kept online account shows as Online.",
  busy: "Every kept online account shows as Busy (Do Not Disturb).",
  invisible: "Every kept online account stays connected but appears offline.",
};

// .user-token-num, .user-token-menu-item and .user-token-bulk-results.
const NUM =
  "user-token-num w-[72px] rounded-btn border border-border-input bg-control px-2.5 py-2 text-[13px] text-text outline-0 focus:border-accent";
const MENU_ITEM =
  "user-token-menu-item flex w-full cursor-pointer items-center gap-[9px] rounded-input border-0 bg-transparent px-2.5 py-2 text-left text-[13px] font-medium text-text hover:bg-white/6";
const RESULTS =
  "user-token-bulk-results mt-2.5 flex max-h-[180px] flex-col gap-1 overflow-y-auto";
const SEARCH_ICON =
  "url(\"data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='14' height='14' viewBox='0 0 24 24' fill='none' stroke='%239e95b7' stroke-width='2' stroke-linecap='round' stroke-linejoin='round'%3E%3Ccircle cx='11' cy='11' r='8'/%3E%3Cpath d='m21 21-4.3-4.3'/%3E%3C/svg%3E\")";
const GEAR =
  "M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 1 1-2.83 2.83l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 0 1-4 0v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 1 1-2.83-2.83l.06-.06a1.65 1.65 0 0 0 .33-1.82 1.65 1.65 0 0 0-1.51-1H3a2 2 0 0 1 0-4h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 1 1 2.83-2.83l.06.06a1.65 1.65 0 0 0 1.82.33H9a1.65 1.65 0 0 0 1-1.51V3a2 2 0 0 1 4 0v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 1 1 2.83 2.83l-.06.06a1.65 1.65 0 0 0-.33 1.82V9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 0 1 0 4h-.09a1.65 1.65 0 0 0-1.51 1z";

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
        "user-token-status min-w-0 flex-auto text-[11px] leading-[1.4]",
        status.kind === "error"
          ? "is-error text-danger-pale"
          : status.kind === "ok"
            ? "is-ok text-good-pale"
            : "text-muted opacity-85",
      )}
    >
      {status.text}
    </span>
  );
}

const BSTATUS: Record<string, string> = { added: "text-good-pale", duplicate: "text-muted" };

function ResultRow({ label, status, cls }: { label: string; status: string; cls: string }) {
  return (
    <div className="user-token-bulk-row flex items-center gap-2 px-0.5 py-1 text-xs">
      <span className="btok truncate font-mono text-muted">{label}</span>
      <span className={cx("bstatus ml-auto shrink-0 font-semibold", cls, BSTATUS[cls] ?? "text-danger-pale")}>{status}</span>
    </div>
  );
}

function BulkModal({ mappingId, onClose, onChanged }: { mappingId: string; onClose: () => void; onChanged: () => void }) {
  useScrollLock(true);
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
    <div id="user-token-bulk-modal" className={`modal show ${MODAL_OVERLAY} z-[60]!`} aria-hidden="false">
      <div className={`modal-backdrop ${MODAL_BACKDROP}`} onClick={onClose} />
      <div
        role="dialog"
        aria-modal="true"
        aria-labelledby="user-token-bulk-title"
        className={`modal-content user-token-bulk-content ${MODAL_CARD} w-[94vw] max-w-[620px]`}
      >
        <div className={`modal-header ${MODAL_HEADER}`}>
          <div className="modal-title-wrap flex flex-col leading-[1.2]">
            <div id="user-token-bulk-title" className="modal-title-main">
              Bulk add user tokens
            </div>
          </div>
          <button type="button" id="user-token-bulk-close" aria-label="Close" onClick={onClose} className={`verify-close ${MODAL_CLOSE}`}>
            ✕
          </button>
        </div>
        <div className="user-token-bulk-body min-h-0 overflow-y-auto px-4 pt-3 pb-4">
          <textarea
            id="user_token_bulk_input"
            className="user-token-bulk-textarea h-[132px] w-full resize-none overflow-auto rounded-btn border border-border-input bg-control px-3 py-2.5 font-mono text-xs leading-[1.6] whitespace-pre text-text outline-0 focus:border-accent"
            rows={6}
            spellCheck={false}
            autoComplete="off"
            wrap="off"
            placeholder="Paste tokens here, one per line…"
            value={text}
            onChange={(e) => setText(e.target.value)}
          />
          <div className="user-token-bulk-actions mt-2.5 flex items-center gap-2.5">
            <Button variant="primary" id="user-token-bulk-submit" loading={busy} onClick={() => void submit()}>
              Verify &amp; add
            </Button>
            <StatusText id="user-token-bulk-status" status={status} />
          </div>
          <div id="user-token-bulk-results" className={RESULTS}>
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

  const enabledIds = useMemo(() => (tokens ?? []).filter((t) => t.enabled).map((t) => t.id), [tokens]);
  const keepOnline = normalizeKeepOnline(settings.USER_TOKEN_KEEP_ONLINE);
  const presence = normalizePresence(settings.USER_TOKEN_PRESENCE);
  // Without a token list (create, clone, still loading) there is no ceiling yet.
  const keepMax = tokens === null ? undefined : enabledIds.length;

  const setKeepOnline = (raw: string) => {
    const n = raw.trim() === "" ? 0 : normalizeKeepOnline(raw);
    setSetting("USER_TOKEN_KEEP_ONLINE", keepMax === undefined ? n : Math.min(n, keepMax));
  };

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
    } catch (e) {
      notify(errorText(e, "Failed to update token"), "error");
    }
    await load();
  };

  const removeToken = async (t: UserToken) => {
    if (!mappingId) return;
    const name = tokenName(t);
    if (!(await confirm({ title: `Remove ${name}?`, confirmLabel: "Remove", confirmVariant: "ghost-red" }))) return;
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
    } catch (e) {
      await load();
      notify(errorText(e, "Failed to delete tokens"), "error");
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
      notify(errorText(err, "Failed to verify tokens"), "error");
      return;
    }
    setVerifying(false);
    if (!j.ok || !j.total) {
      notify(j.ok ? "There are no tokens to verify." : j.error || "Failed to verify tokens.", j.ok ? "info" : "error");
      return;
    }
    const body: ReactNode = (
      <div id="user-token-verify-results">
        <div className="user-token-verify-summary mb-2.5 flex items-center gap-2 text-xs font-semibold">
          <span className="uv-ok text-good-pale">{j.working} working</span>
          <span className="uv-sep font-normal text-muted">·</span>
          <span className="uv-bad text-danger-pale">{j.broken} not working</span>
        </div>
        <div className={cx(RESULTS, "user-token-verify-list mt-0! max-h-[260px]! pr-1")}>
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
      await confirm({ title: "Token verification", body, confirmLabel: "Done", confirmVariant: "primary" });
      return;
    }
    const ok = await confirm({
      title: "Token verification",
      body,
      confirmLabel: `Delete ${j.broken} non-working`,
      confirmVariant: "ghost-red",
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
      confirmVariant: "ghost-red",
    });
    if (!ok) return;
    const deleted = await deleteTokens({ scope: "all" });
    if (deleted != null) notify(`Deleted ${deleted} token${deleted === 1 ? "" : "s"}.`, "success");
  };

  const switchRow = (key: string, name: string, desc: string, settingKey: string) => (
    <ToggleRow
      id={`map_${key}`}
      settingKey={settingKey}
      field={false}
      name={name}
      desc={desc}
      checked={on(key)}
      onChange={(v) => setSetting(key, v)}
    />
  );

  const hasTokens = (tokens?.length ?? 0) > 0;

  return (
    <details id="user-tokens-section" className={SECTION}>
      <SectionSummary>Token Message Cloning</SectionSummary>
      <div className="mapping-section-body pt-1">
        <div className="user-token-disclaimer mb-3 rounded-input border border-danger-a35 bg-danger-a8 px-3 py-2.5 text-xs leading-normal text-text">
          <strong className="text-danger-pale">Warning:</strong> This feature uses user tokens. Discord may rate limit your
          accounts or permanently ban them at any time. Only use accounts you are willing to lose.
        </div>

        <ToggleRow
          id="map_USE_USER_TOKENS"
          settingKey="use_user_tokens"
          name="Send messages as users"
          desc="Send cloned messages from real user accounts instead of webhooks. Accounts must be in the clone server."
          checked={on("USE_USER_TOKENS")}
          onChange={(v) => setSetting("USE_USER_TOKENS", v)}
        />

        <div id="user-token-options" className="user-token-options mt-1.5">
          <div className={ROW}>
            <div className="mapping-toggle-info flex min-w-0 flex-col gap-px">
              <label htmlFor="ut_strategy" className="mapping-toggle-name text-[13px] font-medium text-text">
                Account selection
              </label>
              <span id="ut_strategy_desc" className={DESC}>
                {STRATEGY_DESCS[strategy]}
              </span>
            </div>
            <div className="w-[200px] max-w-[55%] flex-none">
              <ToolbarSelect
                variant="field"
                id="ut_strategy"
                value={strategy}
                options={STRATEGIES}
                onChange={(v) => setSetting("USER_TOKEN_STRATEGY", v)}
                toggleClassName={STRATEGY_TOGGLE}
              />
            </div>
          </div>

          {sticky ? (
            <div
              id="user-token-sticky-options"
              className="user-token-sticky-options mt-0.5 mr-0 mb-0.5 ml-2.5 border-l-2 border-border pl-3"
            >
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

          <div className={ROW}>
            <div className="mapping-toggle-info flex min-w-0 flex-col gap-px">
              <span className="mapping-toggle-name text-[13px] font-medium text-text">Send delay</span>
              <span className={DESC}>Random pause before each message, in seconds.</span>
            </div>
            <div className="user-token-delay flex items-center gap-2">
              <input
                type="number"
                id="ut_min_delay"
                aria-label="Minimum send delay"
                className={NUM}
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
                className={NUM}
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

        <div
          id="user-tokens-manager"
          className="user-tokens-manager mt-2 rounded-btn border border-border bg-white/[0.02] p-3"
        >
          {!mappingId ? (
            <div id="user-tokens-save-first" className="user-tokens-hint px-0.5 py-1 text-xs text-muted opacity-85">
              Save this mapping first, then reopen it to add user tokens.
            </div>
          ) : (
            <div id="user-tokens-editor">
              <div className="user-token-add">
                <div className="user-token-add-label mb-1.5 text-xs font-semibold text-text">Add user token</div>
                <div className="field field-token user-token-input-wrap relative m-0">
                  <div className="token-wrap relative w-full max-w-full">
                    <input
                      type={reveal ? "text" : "password"}
                      id="user_token_value"
                      autoComplete="off"
                      spellCheck={false}
                      className="token-input w-full rounded-control border border-border-input bg-control py-3 pr-9 pl-3 text-[13px] text-text outline-0 transition-[border,box-shadow] duration-200 focus:border-accent"
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
                      className="reveal-btn token-user-btn absolute top-1/2 right-2 -translate-y-1/2 cursor-pointer border-0 bg-transparent p-0 leading-[0] text-text opacity-60 outline-none hover:opacity-100 focus:opacity-100"
                      data-target="user_token_value"
                      aria-pressed={reveal}
                      title={reveal ? "Hide" : "Show"}
                      aria-label={reveal ? "Hide token" : "Show token"}
                      onClick={() => setReveal((r) => !r)}
                    >
                      <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" viewBox="0 0 24 24" aria-hidden="true" className="pointer-events-none block size-4 text-text">
                        {reveal ? (
                          <path d="M17.94 17.94A10.07 10.07 0 0 1 12 19c-7 0-11-7-11-7a18.4 18.4 0 0 1 5.06-5.94M9.9 9.9A3 3 0 0 0 9 12a3 3 0 0 0 3 3 3 3 0 0 0 2.1-.9M1 1l22 22" />
                        ) : (
                          <>
                            <path d="M1 12s4-7 11-7 11 7 11 7-4 7-11 7S1 12 1 12Z" />
                            <circle cx="12" cy="12" r="3" />
                          </>
                        )}
                      </svg>
                    </button>
                  </div>
                </div>
                <div className="user-token-add-row mt-2.5 flex items-center gap-2.5">
                  <Button
                    variant="primary"
                    id="user-token-add"
                    loading={adding}
                    className="user-token-add-btn min-w-[108px] flex-none whitespace-nowrap"
                    onClick={() => void addToken()}
                  >
                    Add token
                  </Button>
                  <GhostButton
                    type="button"
                    id="user-token-bulk-open"
                    className="user-token-bulk-btn flex-none whitespace-nowrap"
                    onClick={() => setBulkOpen(true)}
                  >
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
                    style={{ backgroundImage: SEARCH_ICON }}
                    className="mapping-search-input user-tokens-search mt-3 w-full rounded-input border border-border-input bg-control bg-[position:10px_center] bg-no-repeat py-[7px] pr-3 pl-8 text-[13px] text-text outline-0 transition-[border-color] duration-150 placeholder:text-muted placeholder:opacity-50 focus:border-accent"
                    placeholder="Search tokens…"
                    autoComplete="off"
                    value={query}
                    onChange={(e) => setQuery(e.target.value)}
                  />
                  <div
                    id="user-tokens-list-header"
                    className="user-tokens-list-header mx-0.5 mt-3 mb-1.5 flex items-center justify-between gap-2.5"
                  >
                    <div
                      id="user-tokens-count"
                      className="user-tokens-count m-0 text-[11px] font-semibold tracking-[0.04em] text-muted uppercase"
                    >
                      {countLabel(tokens?.length ?? 0)}
                    </div>
                    <div ref={menuRef} className="user-token-settings relative flex-none">
                      <button
                        type="button"
                        id="user-token-settings-open"
                        className="user-token-settings-btn inline-flex size-[26px] cursor-pointer items-center justify-center rounded-[7px] border border-border bg-transparent p-0 text-muted hover:border-accent hover:text-text aria-expanded:border-accent aria-expanded:text-accent"
                        title="Token options"
                        aria-haspopup="true"
                        aria-expanded={menuOpen}
                        aria-label="Token options"
                        onClick={() => setMenuOpen((o) => !o)}
                      >
                        <svg xmlns="http://www.w3.org/2000/svg" width="15" height="15" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" viewBox="0 0 24 24" aria-hidden="true">
                          <circle cx="12" cy="12" r="3" />
                          <path d={GEAR} />
                        </svg>
                      </button>
                      {menuOpen ? (
                        <div
                          id="user-token-settings-menu"
                          role="menu"
                          className="user-token-settings-menu absolute top-[calc(100%+6px)] right-0 z-40 flex min-w-[184px] flex-col gap-0.5 rounded-btn border border-border bg-card-solid p-1.5 shadow-popover"
                        >
                          <button
                            type="button"
                            id="user-token-verify"
                            role="menuitem"
                            className={MENU_ITEM}
                            onClick={() => void verifyAll()}
                          >
                            <svg xmlns="http://www.w3.org/2000/svg" width="15" height="15" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" viewBox="0 0 24 24" aria-hidden="true" className="flex-none opacity-85">
                              <path d="M22 11.08V12a10 10 0 1 1-5.93-9.14" />
                              <polyline points="22 4 12 14.01 9 11.01" />
                            </svg>
                            <span>Verify tokens</span>
                          </button>
                          <button
                            type="button"
                            id="user-token-delete-all"
                            role="menuitem"
                            className={cx(MENU_ITEM, "is-danger text-danger-pale hover:bg-danger-pale-a12")}
                            onClick={() => void deleteAll()}
                          >
                            <svg xmlns="http://www.w3.org/2000/svg" width="15" height="15" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" viewBox="0 0 24 24" aria-hidden="true" className="flex-none opacity-85">
                              <polyline points="3 6 5 6 21 6" />
                              <path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6m3 0V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2" />
                              <line x1="10" y1="11" x2="10" y2="17" />
                              <line x1="14" y1="11" x2="14" y2="17" />
                            </svg>
                            <span>Delete all tokens</span>
                          </button>
                        </div>
                      ) : null}
                    </div>
                  </div>
                </>
              ) : null}

              <div
                id="userTokensList"
                className="user-tokens-list mt-1.5 flex max-h-[216px] flex-col gap-1.5 overflow-y-auto overscroll-contain pr-1"
              >
                {verifying ? (
                  <div className="user-token-verify-loading flex items-center justify-center gap-2.5 px-2 py-[26px] text-[13px] text-muted">
                    <span aria-hidden="true" className="uv-spinner size-4 animate-spin rounded-full border-2 border-accent border-r-transparent" />
                    <span>Verifying tokens…</span>
                  </div>
                ) : null}
                {tokens === null && !loadError ? <div className={DESC}>Loading…</div> : null}
                {loadError ? <div className={DESC}>Failed to load tokens.</div> : null}
                {tokens !== null && !hasTokens ? <div className={DESC}>No user tokens yet. Add one above.</div> : null}
                {shown.map((t) => (
                  <div
                    key={t.id}
                    className="user-token-row flex items-center justify-between gap-2.5 rounded-input border border-border bg-white/[0.03] px-2.5 py-2"
                    data-token-id={t.id}
                  >
                    <div className="user-token-meta flex min-w-0 flex-col gap-0.5">
                      <span className="user-token-name truncate text-[13px] font-medium text-text">{tokenName(t)}</span>
                      <span className={cx(DESC, "user-token-sub font-mono")}>{tokenSub(t)}</span>
                    </div>
                    <div className="user-token-actions flex shrink-0 items-center gap-2.5">
                      <Switch
                        id={`user-token-enabled-${t.id}`}
                        label={`Enable ${tokenName(t)}`}
                        checked={t.enabled}
                        onChange={(v) => void toggleToken(t, v)}
                      />
                      <GhostButton
                        type="button"
                        danger
                        className="user-token-remove min-h-[30px]! px-3! py-1! text-xs!"
                        onClick={() => void removeToken(t)}
                      >
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

        <div id="user-token-pool" className="user-token-pool mt-1.5">
          <div className={ROW}>
            <div className="mapping-toggle-info flex min-w-0 flex-col gap-px">
              <label htmlFor="ut_keep_online" className="mapping-toggle-name text-[13px] font-medium text-text">
                Keep online
              </label>
              <span className={DESC}>Accounts held connected so sends start at once. 0 turns it off.</span>
            </div>
            <div className="user-token-keep flex flex-none items-center">
              <input
                type="number"
                id="ut_keep_online"
                className={NUM}
                min={0}
                max={keepMax}
                step={1}
                value={keepOnline}
                onChange={(e) => setKeepOnline(e.target.value)}
              />
            </div>
          </div>
          <div className={ROW}>
            <div className="mapping-toggle-info flex min-w-0 flex-col gap-px">
              <label htmlFor="ut_presence" className="mapping-toggle-name text-[13px] font-medium text-text">
                Token status
              </label>
              <span id="ut_presence_desc" className={DESC}>
                {PRESENCE_DESCS[presence]}
              </span>
            </div>
            <div className="w-[200px] max-w-[55%] flex-none">
              <ToolbarSelect
                variant="field"
                id="ut_presence"
                value={presence}
                options={PRESENCES}
                onChange={(v) => setSetting("USER_TOKEN_PRESENCE", v)}
                toggleClassName={STRATEGY_TOGGLE}
              />
            </div>
          </div>
        </div>
      </div>
      {bulkOpen && mappingId ? (
        <BulkModal mappingId={mappingId} onClose={() => setBulkOpen(false)} onChanged={() => void load()} />
      ) : null}
    </details>
  );
}
