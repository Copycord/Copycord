// The old #global-config-card: tokens with show/hide, command users, auto
// start, log level and size, Save/Cancel, the bot invite link and the backup
// tokens modal. Save and Cancel show only while the form differs from what
// was loaded, and stay off while a bot is running. Laid out as the old
// two-column grid of bordered field boxes (.grid, .field-token, .gc-setting-row).

import { useCallback, useEffect, useRef, useState } from "react";
import { ChipInput, SkeletonSection, Spinner, cx } from "../../components/ui";
import { api } from "../../lib/api";
import type { StatusResponse } from "../../types/dashboard";
import type { GlobalConfig, TokenKey } from "../../types/dashboard-config";
import { useNotify } from "../system/providers";
import { BackupTokensModal } from "./config/BackupTokensModal";
import { TokenInput } from "./config/TokenInput";
import {
  BAD_TOKENS_MESSAGE,
  DOCS_URL,
  EMPTY_CONFIG,
  FIELD_TIPS,
  inviteUrl,
  isDirty,
  loadConfig,
  missingTokens,
  saveConfig,
  tokensFromErrors,
  validateTokens,
} from "./config/model";
import { STATUS_URL, statusesFrom, anyRunning } from "./model";
import { CARD, Chevron, DocLink, GHOST, GHOST_DISABLED, PS_INPUT, PS_UNIT, SettingRow, SETTING_LABEL, SETTING_ROW, SETTING_SUB, SUMMARY, SUMMARY_TITLE, Switch } from "./parts";

const LOCK_TITLE = "Stop the bot to edit global configuration";
const START_NEEDS_TOKENS = "Provide SERVER_TOKEN and CLIENT_TOKEN to start.";
const START_BAD_TOKENS = "Saved tokens are no longer valid. Please update SERVER_TOKEN and CLIENT_TOKEN to start.";

/** .field.field-token */
const FIELD = "relative block rounded-lg border border-border bg-[rgba(255,255,255,0.02)] px-2.5 py-2 transition-colors duration-100 hover:bg-[rgba(255,255,255,0.04)]";
/** .field label */
const LABEL = "mb-1.5 ml-0.5 block text-xs text-muted";
/** .gc-field-hint */
const HINT = "mb-1 block text-[11px] leading-[1.4] text-muted opacity-70";

const SELECT_ARROW =
  "bg-[url(\"data:image/svg+xml,%3Csvg_xmlns='http://www.w3.org/2000/svg'_width='10'_height='6'_fill='none'%3E%3Cpath_d='M1_1l4_4_4-4'_stroke='%239e95b7'_stroke-width='1.5'_stroke-linecap='round'_stroke-linejoin='round'/%3E%3C/svg%3E\")]";

export interface GlobalConfigSectionProps {
  /** Why the Bots card's Start is off (missing or rejected tokens), or null; the old updateStartButtonOnly. */
  onStartBlockedChange?: (reason: string | null) => void;
}

export function GlobalConfigSection({ onStartBlockedChange }: GlobalConfigSectionProps = {}) {
  const notify = useNotify();
  const [baseline, setBaseline] = useState<GlobalConfig | null>(null);
  const [values, setValues] = useState<GlobalConfig>(EMPTY_CONFIG);
  const [loadError, setLoadError] = useState(false);
  const [saving, setSaving] = useState(false);
  const [running, setRunning] = useState(false);
  const [invalid, setInvalid] = useState<Set<TokenKey>>(new Set());
  const [clientId, setClientId] = useState<string | null>(null);
  const [badSaved, setBadSaved] = useState(false);
  const [backupOpen, setBackupOpen] = useState(false);
  const mounted = useRef(true);

  useEffect(() => {
    mounted.current = true;
    return () => {
      mounted.current = false;
    };
  }, []);

  const checkTokens = useCallback(async (announce: boolean) => {
    try {
      const res = await validateTokens();
      if (!mounted.current) return;
      const bad = res.has_tokens && !res.ok;
      setInvalid(bad ? tokensFromErrors(res.errors) : new Set());
      setBadSaved(bad);
      if (bad && announce) notify(res.errors?.join(" ") || BAD_TOKENS_MESSAGE, "error");
      setClientId(res.bot_client_id || null);
    } catch {
      // The old page only logged this.
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  useEffect(() => {
    let live = true;
    loadConfig().then(
      (c) => {
        if (!live) return;
        setBaseline(c);
        setValues(c);
      },
      () => live && setLoadError(true),
    );
    void checkTokens(true);
    return () => {
      live = false;
    };
  }, [checkTokens]);

  const dirty = baseline !== null && isDirty(values, baseline);

  // Bot state is read when the form first changes, so opening the page costs no extra poll.
  useEffect(() => {
    if (!dirty) return;
    let live = true;
    api.get<StatusResponse>(STATUS_URL).then(
      (s) => live && setRunning(anyRunning(statusesFrom(s))),
      () => undefined,
    );
    return () => {
      live = false;
    };
  }, [dirty]);

  const set = <K extends keyof GlobalConfig>(key: K, value: GlobalConfig[K]) => setValues((v) => ({ ...v, [key]: value }));

  const cancel = () => {
    if (baseline) setValues(baseline);
    setInvalid(new Set());
  };

  const save = async () => {
    const missing = missingTokens(values);
    setInvalid(new Set(missing));
    setSaving(true);
    try {
      await saveConfig(values);
      notify("Configuration saved.", "success");
      setBaseline(values);
      setInvalid(new Set());
      void checkTokens(false);
    } catch (e) {
      notify(e instanceof Error ? e.message : "Network error", "error");
      void checkTokens(false);
    } finally {
      setSaving(false);
    }
  };

  const invite = inviteUrl(clientId);
  const locked = running;


  const needsTokens = baseline !== null && missingTokens(values).length > 0;
  const startBlocked = badSaved ? START_BAD_TOKENS : needsTokens ? START_NEEDS_TOKENS : null;
  useEffect(() => {
    onStartBlockedChange?.(startBlocked);
  }, [startBlocked, onStartBlockedChange]);

  return (
    <details id="global-config-card" open className={CARD}>
      <summary className={SUMMARY}>
        <h3 className={SUMMARY_TITLE}>Global Configuration</h3>
        <DocLink href={DOCS_URL} />
        <Chevron />
      </summary>

      {baseline === null ? (
        <div className="mt-3.5">{loadError ? <p role="alert" className="text-sm text-danger">Could not load the configuration.</p> : <SkeletonSection rows={6} rowHeight={34} label="Loading configuration" />}</div>
      ) : (
        <form
          id="cfg-form"
          method="post"
          action="/save"
          noValidate
          className="mt-3.5"
          onSubmit={(e) => {
            e.preventDefault();
            if (!locked && dirty) void save();
          }}
        >
          <div className="grid grid-cols-2 gap-x-[22px] gap-y-3 max-[900px]:grid-cols-1">
            <div className={FIELD}>
              <label htmlFor="SERVER_TOKEN" className={LABEL}>
                Server Token
              </label>
              <span id="SERVER_TOKEN-help" className={HINT}>
                {FIELD_TIPS.SERVER_TOKEN}
              </span>
              <TokenInput field id="SERVER_TOKEN" value={values.SERVER_TOKEN} onChange={(v) => set("SERVER_TOKEN", v)} invalid={invalid.has("SERVER_TOKEN")} aria-describedby="SERVER_TOKEN-help" />
            </div>
            <div className={FIELD}>
              {/* The old .gc-label-row: the label and the 28px backup tokens button on one 28px row. */}
              <label htmlFor="CLIENT_TOKEN" className={cx(LABEL, "mt-1 mb-2.5 w-fit")}>
                Client Token
              </label>
              <button
                type="button"
                id="backupTokensBtn"
                aria-label="Manage backup CLIENT_TOKENs"
                title="Manage backup tokens"
                onClick={() => setBackupOpen(true)}
                className="absolute top-2 right-2.5 flex size-7 cursor-pointer items-center justify-center rounded-[10px] border border-border bg-[rgba(255,255,255,0.04)] px-1.5 py-px text-accent opacity-90 outline-none hover:border-accent hover:opacity-100 focus-visible:border-accent focus-visible:opacity-100"
              >
                <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" viewBox="0 0 24 24" aria-hidden="true" className="shrink-0">
                  <path d="M20 21v-2a4 4 0 0 0-4-4H8a4 4 0 0 0-4 4v2" />
                  <circle cx="12" cy="7" r="4" />
                </svg>
              </button>
              <span id="CLIENT_TOKEN-help" className={HINT}>
                {FIELD_TIPS.CLIENT_TOKEN}
              </span>
              <TokenInput field id="CLIENT_TOKEN" value={values.CLIENT_TOKEN} onChange={(v) => set("CLIENT_TOKEN", v)} invalid={invalid.has("CLIENT_TOKEN")} aria-describedby="CLIENT_TOKEN-help" />
            </div>

            <div className="col-span-full -mt-1 grid grid-cols-2 gap-x-[22px] gap-y-2 max-[900px]:grid-cols-1">
              <div className={cx(SETTING_ROW, "flex-col items-stretch")}>
                <div>
                  <div className={SETTING_LABEL}>
                    <span className="text-[13px] font-medium">Command Users</span>
                    <span className={SETTING_SUB}>{FIELD_TIPS.COMMAND_USERS}</span>
                  </div>
                  <ChipInput
                    id="cmd_users_input"
                    mode="ids"
                    aria-label="Command Users"
                    value={values.COMMAND_USERS}
                    onChange={(ids) => set("COMMAND_USERS", ids)}
                    className={cx(
                      "mt-1.5 [&>div]:min-h-[42px] [&>div]:gap-2 [&>div]:bg-[#140b26] [&>div]:bg-none [&>div]:px-3 [&>div]:py-2",
                      "[&_input]:min-w-0 [&_input]:font-[inherit] [&_input]:text-[14px] [&_input]:leading-[1.4]",
                    )}
                  />
                  <input type="hidden" id="COMMAND_USERS" name="COMMAND_USERS" value={values.COMMAND_USERS.join(",")} readOnly />
                </div>
              </div>

              <SettingRow label="Auto Start" hint={FIELD_TIPS.COPYCORD_AUTOSTART}>
                <div className="ml-auto flex shrink-0 items-center">
                  <Switch id="gc-autostart-toggle" label="Auto Start" checked={values.COPYCORD_AUTOSTART} onChange={(on) => set("COPYCORD_AUTOSTART", on)} />
                </div>
              </SettingRow>

              <SettingRow label="Log Level" hint={FIELD_TIPS.LOG_LEVEL}>
                <select
                  id="LOG_LEVEL"
                  aria-label="Log Level"
                  name="LOG_LEVEL"
                  value={values.LOG_LEVEL}
                  onChange={(e) => set("LOG_LEVEL", e.target.value === "DEBUG" ? "DEBUG" : "INFO")}
                  className={cx(
                    "ml-auto flex h-[34px] w-[100px] shrink-0 cursor-pointer appearance-none items-center rounded-lg border border-border bg-[#140b26] py-0 pr-7 pl-3 text-[13px] text-text outline-none transition-colors focus:border-accent",
                    SELECT_ARROW,
                    "bg-[position:right_10px_center] bg-no-repeat",
                  )}
                >
                  <option value="INFO">INFO</option>
                  <option value="DEBUG">DEBUG</option>
                </select>
              </SettingRow>

              <SettingRow label="Max Log Size" hint={FIELD_TIPS.LOG_MAX_SIZE_MB}>
                <div className="ml-auto flex shrink-0 items-center gap-1.5">
                  <input
                    id="LOG_MAX_SIZE_MB"
                    aria-label="Max Log Size"
                    name="LOG_MAX_SIZE_MB"
                    type="number"
                    min={0}
                    step={1}
                    value={values.LOG_MAX_SIZE_MB}
                    onChange={(e) => set("LOG_MAX_SIZE_MB", e.target.value)}
                    className={PS_INPUT}
                  />
                  <span className={PS_UNIT}>MB</span>
                </div>
              </SettingRow>
            </div>

            <div className="col-span-full mt-3 flex flex-wrap gap-2.5 max-sm:flex-col">
              <button
                id="cfg-save-btn"
                type="submit"
                disabled={!dirty || locked || saving}
                aria-busy={saving || undefined}
                title={locked ? LOCK_TITLE : undefined}
                className={cx(GHOST, "flex", dirty ? GHOST_DISABLED : "invisible")}
              >
                {saving ? <Spinner size="sm" label="Working" /> : null}
                Save
              </button>
              <button
                id="cfg-cancel-btn"
                type="button"
                disabled={!dirty || locked || saving}
                title={locked ? LOCK_TITLE : undefined}
                className={cx(GHOST, "flex", dirty ? GHOST_DISABLED : "invisible")}
                onClick={cancel}
              >
                Cancel
              </button>
              <span className="flex-1 max-sm:hidden" />
              <a
                id="bot-invite-btn"
                href={invite ?? undefined}
                aria-disabled={!invite || undefined}
                target="_blank"
                rel="noopener noreferrer"
                title="Invite your bot to a clone server"
                className={cx(
                  "bot-invite-btn inline-flex items-center gap-[5px] text-xs font-medium text-accent no-underline transition-opacity max-sm:self-start",
                  invite ? "opacity-80 hover:opacity-100" : "bot-invite-disabled pointer-events-none cursor-default opacity-30",
                )}
              >
                <svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true" className="shrink-0">
                  <path d="M12 8V4H8" />
                  <rect width="16" height="12" x="4" y="8" rx="2" />
                  <path d="M2 14h2" />
                  <path d="M20 14h2" />
                  <path d="M15 13v2" />
                  <path d="M9 13v2" />
                </svg>
                Add Bot to Server
              </a>
            </div>
          </div>
        </form>
      )}

      <BackupTokensModal open={backupOpen} onClose={() => setBackupOpen(false)} />
    </details>
  );
}
