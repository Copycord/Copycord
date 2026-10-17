// The old #global-config-card: tokens with show/hide, command users, auto
// start, log level and size, Save/Cancel, the bot invite link and the backup
// tokens modal. Save and Cancel show only while the form differs from what
// was loaded, and stay off while a bot is running. Laid out as the old
// two-column grid of bordered field boxes (.grid, .field-token, .gc-setting-row).

import { useCallback, useEffect, useRef, useState } from "react";
import { BUSY_HOST, BusyContent, ChipInput, cx } from "../../components/ui";
import { BACKUP_BTN, BackupIcon, GlobalConfigLoading, INVITE_LINK, INVITE_OFF, InviteIcon, LOG_LEVEL_BOX } from "./DashboardSkeleton";
import { ToolbarSelect } from "../channels/ToolbarSelect";
import { api, errorText } from "../../lib/api";
import type { StatusResponse } from "../../types/dashboard";
import type { GlobalConfig, TokenKey } from "../../types/dashboard-config";
import { useNotify } from "../system/providers";
import { BackupTokensModal } from "./config/BackupTokensModal";
import { TokenInput } from "./config/TokenInput";
import {
  BAD_TOKENS_MESSAGE,
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
import { CARD, CARD_DOCS, Chevron, DocLink, GC_FIELD as FIELD, GC_HINT as HINT, GC_LABEL as LABEL, GHOST, GHOST_DISABLED, PS_INPUT, PS_UNIT, SettingRow, SETTING_LABEL, SETTING_ROW, SETTING_SUB, SUMMARY, SUMMARY_TITLE, Switch } from "./parts";

const LOCK_TITLE = "Stop the bot to edit global configuration";
const START_NEEDS_TOKENS = "Provide SERVER_TOKEN and CLIENT_TOKEN to start.";
const START_BAD_TOKENS = "Saved tokens are no longer valid. Please update SERVER_TOKEN and CLIENT_TOKEN to start.";

const LOG_LEVELS = [
  { value: "INFO", label: "INFO" },
  { value: "DEBUG", label: "DEBUG" },
];

export interface GlobalConfigSectionProps {
  /** Why the Bots card's Start is off (missing or rejected tokens), or null; the old updateStartButtonOnly. */
  onStartBlockedChange?: (reason: string | null) => void;
  /** The Bots card's live running state. When given, the lock follows it; without it the section reads
   *  /api/status once when the form first changes. */
  botsRunning?: boolean;
}

export function GlobalConfigSection({ onStartBlockedChange, botsRunning }: GlobalConfigSectionProps = {}) {
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
    if (!dirty || botsRunning !== undefined) return;
    let live = true;
    api.get<StatusResponse>(STATUS_URL).then(
      (s) => live && setRunning(anyRunning(statusesFrom(s))),
      () => undefined,
    );
    return () => {
      live = false;
    };
  }, [dirty, botsRunning]);

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
      notify(errorText(e, "Failed to save configuration"), "error");
      void checkTokens(false);
    } finally {
      setSaving(false);
    }
  };

  const invite = inviteUrl(clientId);
  const locked = botsRunning ?? running;


  const needsTokens = baseline !== null && missingTokens(values).length > 0;
  const startBlocked = badSaved ? START_BAD_TOKENS : needsTokens ? START_NEEDS_TOKENS : null;
  useEffect(() => {
    onStartBlockedChange?.(startBlocked);
  }, [startBlocked, onStartBlockedChange]);

  return (
    <details id="global-config-card" open className={CARD}>
      <summary className={SUMMARY}>
        <h3 className={SUMMARY_TITLE}>Global Configuration</h3>
        <DocLink href={CARD_DOCS.global} />
        <Chevron />
      </summary>

      {baseline === null ? (
        loadError ? (
          <div className="mt-3.5">
            <p role="alert" className="text-sm text-danger">Could not load the configuration.</p>
          </div>
        ) : (
          <GlobalConfigLoading />
        )
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
                className={BACKUP_BTN}
              >
                <BackupIcon />
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
                      "mt-1.5 [&>div]:min-h-[42px] [&>div]:gap-2 [&>div]:bg-control [&>div]:bg-none [&>div]:px-3 [&>div]:py-2",
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
                <ToolbarSelect
                  id="LOG_LEVEL"
                  label="Log Level"
                  value={values.LOG_LEVEL}
                  options={LOG_LEVELS}
                  onChange={(v) => set("LOG_LEVEL", v === "DEBUG" ? "DEBUG" : "INFO")}
                  className={LOG_LEVEL_BOX}
                />
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
                className={cx(GHOST, "flex", dirty ? GHOST_DISABLED : "invisible", saving && BUSY_HOST)}
              >
                <BusyContent busy={saving}>Save</BusyContent>
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
                  "bot-invite-btn",
                  INVITE_LINK,
                  !invite && cx("bot-invite-disabled", INVITE_OFF),
                )}
              >
                <InviteIcon />
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
