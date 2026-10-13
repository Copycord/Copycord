// The old #global-config-card: tokens with show/hide, command users, auto
// start, log level and size, Save/Cancel, the bot invite link and the backup
// tokens modal. Save and Cancel show only while the form differs from what
// was loaded, and stay off while a bot is running.

import { useCallback, useEffect, useRef, useState } from "react";
import type { ReactNode } from "react";
import { ChipInput, Field, Input, Select, Spinner, Toggle, cx } from "../../components/ui";
import { api } from "../../lib/api";
import type { StatusResponse } from "../../types/dashboard";
import type { GlobalConfig, TokenKey } from "../../types/dashboard-config";
import { GhostButton, LinkButton } from "../system/buttons";
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

const LOCK_TITLE = "Stop the bot to edit global configuration";

function Row({ label, hint, children }: { label: string; hint: string; children: ReactNode }) {
  return (
    <div className="flex items-center justify-between gap-4 border-t border-border py-3 max-sm:flex-col max-sm:items-stretch">
      <div className="flex min-w-0 flex-col">
        <span className="text-sm font-semibold text-text-1">{label}</span>
        <span className="text-xs text-text-2">{hint}</span>
      </div>
      <div className="flex-none">{children}</div>
    </div>
  );
}

export function GlobalConfigSection() {
  const notify = useNotify();
  const [baseline, setBaseline] = useState<GlobalConfig | null>(null);
  const [values, setValues] = useState<GlobalConfig>(EMPTY_CONFIG);
  const [loadError, setLoadError] = useState(false);
  const [saving, setSaving] = useState(false);
  const [running, setRunning] = useState(false);
  const [invalid, setInvalid] = useState<Set<TokenKey>>(new Set());
  const [clientId, setClientId] = useState<string | null>(null);
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

  return (
    <details
      id="global-config-card"
      open
      className="group rounded-card border border-border bg-linear-to-b from-card to-[rgba(20,10,40,0.45)] p-[18px] text-text"
    >
      <summary className="flex cursor-pointer list-none items-center gap-3 [&::-webkit-details-marker]:hidden">
        <h2 className="text-base font-semibold">Global Configuration</h2>
        <a
          href={DOCS_URL}
          target="_blank"
          rel="noopener noreferrer"
          title="View docs"
          aria-label="View docs"
          onClick={(e) => e.stopPropagation()}
          className="grid size-6 place-items-center text-text-2 hover:text-accent"
        >
          <svg viewBox="0 0 24 24" width="14" height="14" fill="currentColor" aria-hidden="true">
            <path d="M21 5c-1.11-.35-2.33-.5-3.5-.5-1.95 0-4.05.4-5.5 1.5-1.45-1.1-3.55-1.5-5.5-1.5S2.45 4.9 1 6v14.65c0 .25.25.5.5.5.1 0 .15-.05.25-.05C3.1 20.45 5.05 20 6.5 20c1.95 0 4.05.4 5.5 1.5 1.35-.85 3.8-1.5 5.5-1.5 1.65 0 3.35.3 4.75 1.05.1.05.15.05.25.05.25 0 .5-.25.5-.5V6c-.6-.45-1.25-.75-2-1zm0 13.5c-1.1-.35-2.3-.5-3.5-.5-1.7 0-4.15.65-5.5 1.5V8c1.35-.85 3.8-1.5 5.5-1.5 1.2 0 2.4.15 3.5.5v11.5z" />
          </svg>
        </a>
        <span aria-hidden="true" className="ml-auto text-text-2 -rotate-90 transition-transform group-open:rotate-0">
          &#9662;
        </span>
      </summary>

      {baseline === null ? (
        <div className="mt-4">{loadError ? <p role="alert" className="text-sm text-danger">Could not load the configuration.</p> : <Spinner />}</div>
      ) : (
        <form
          id="cfg-form"
          method="post"
          action="/save"
          noValidate
          className="mt-4 flex flex-col gap-4"
          onSubmit={(e) => {
            e.preventDefault();
            if (!locked && dirty) void save();
          }}
        >
          <div className="grid gap-4 md:grid-cols-2">
            <Field label="Server Token" help={FIELD_TIPS.SERVER_TOKEN} id="SERVER_TOKEN">
              <TokenInput id="SERVER_TOKEN" value={values.SERVER_TOKEN} onChange={(v) => set("SERVER_TOKEN", v)} invalid={invalid.has("SERVER_TOKEN")} aria-describedby="SERVER_TOKEN-help" />
            </Field>
            <div className="flex flex-col gap-1.5">
              <div className="flex items-center justify-between gap-2">
                <label htmlFor="CLIENT_TOKEN" className="text-sm font-semibold text-text-1">
                  Client Token
                </label>
                <button
                  type="button"
                  id="backupTokensBtn"
                  aria-label="Manage backup CLIENT_TOKENs"
                  title="Manage backup tokens"
                  onClick={() => setBackupOpen(true)}
                  className="grid size-7 cursor-pointer place-items-center rounded-control text-text-2 outline-none hover:text-accent focus-visible:ring-2 focus-visible:ring-accent/60"
                >
                  <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" viewBox="0 0 24 24" aria-hidden="true">
                    <path d="M20 21v-2a4 4 0 0 0-4-4H8a4 4 0 0 0-4 4v2" />
                    <circle cx="12" cy="7" r="4" />
                  </svg>
                </button>
              </div>
              <TokenInput id="CLIENT_TOKEN" value={values.CLIENT_TOKEN} onChange={(v) => set("CLIENT_TOKEN", v)} invalid={invalid.has("CLIENT_TOKEN")} aria-describedby="CLIENT_TOKEN-help" />
              <p id="CLIENT_TOKEN-help" className="text-xs text-text-2">
                {FIELD_TIPS.CLIENT_TOKEN}
              </p>
            </div>
          </div>

          <div>
            <div className="flex flex-col border-t border-border pt-3 pb-1">
              <span className="text-sm font-semibold text-text-1">Command Users</span>
              <span className="mb-1.5 text-xs text-text-2">{FIELD_TIPS.COMMAND_USERS}</span>
            </div>
            <ChipInput
              id="cmd_users_input"
              mode="ids"
              aria-label="Command Users"
              value={values.COMMAND_USERS}
              onChange={(ids) => set("COMMAND_USERS", ids)}
              className="mb-1"
            />
            <input type="hidden" id="COMMAND_USERS" name="COMMAND_USERS" value={values.COMMAND_USERS.join(",")} readOnly />

            <Row label="Auto Start" hint={FIELD_TIPS.COPYCORD_AUTOSTART}>
              <Toggle id="gc-autostart-toggle" label="Auto Start" hideLabel checked={values.COPYCORD_AUTOSTART} onChange={(on) => set("COPYCORD_AUTOSTART", on)} />
            </Row>

            <Row label="Log Level" hint={FIELD_TIPS.LOG_LEVEL}>
              <Select
                id="LOG_LEVEL"
                name="LOG_LEVEL"
                value={values.LOG_LEVEL}
                onChange={(e) => set("LOG_LEVEL", e.target.value === "DEBUG" ? "DEBUG" : "INFO")}
                options={[
                  { value: "INFO", label: "INFO" },
                  { value: "DEBUG", label: "DEBUG" },
                ]}
                className="min-w-32"
              />
            </Row>

            <Row label="Max Log Size" hint={FIELD_TIPS.LOG_MAX_SIZE_MB}>
              <div className="flex items-center gap-2">
                <Input
                  id="LOG_MAX_SIZE_MB"
                  name="LOG_MAX_SIZE_MB"
                  type="number"
                  min={0}
                  step={1}
                  value={values.LOG_MAX_SIZE_MB}
                  onChange={(e) => set("LOG_MAX_SIZE_MB", e.target.value)}
                  className="w-28"
                />
                <span className="text-sm text-text-2">MB</span>
              </div>
            </Row>
          </div>

          <div className="flex flex-wrap items-center gap-2">
            <GhostButton
              id="cfg-save-btn"
              type="submit"
              loading={saving}
              disabled={!dirty || locked}
              title={locked ? LOCK_TITLE : undefined}
              className={cx(!dirty && "invisible")}
            >
              Save
            </GhostButton>
            <GhostButton
              id="cfg-cancel-btn"
              disabled={!dirty || locked || saving}
              title={locked ? LOCK_TITLE : undefined}
              className={cx(!dirty && "invisible")}
              onClick={cancel}
            >
              Cancel
            </GhostButton>
            <span className="flex-1" />
            <LinkButton
              id="bot-invite-btn"
              href={invite ?? "#"}
              disabled={!invite}
              target="_blank"
              rel="noopener noreferrer"
              title="Invite your bot to a clone server"
            >
              <svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
                <path d="M12 8V4H8" />
                <rect width="16" height="12" x="4" y="8" rx="2" />
                <path d="M2 14h2" />
                <path d="M20 14h2" />
                <path d="M15 13v2" />
                <path d="M9 13v2" />
              </svg>
              Add Bot to Server
            </LinkButton>
          </div>
        </form>
      )}

      <BackupTokensModal open={backupOpen} onClose={() => setBackupOpen(false)} />
    </details>
  );
}
