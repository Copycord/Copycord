// #mapping-modal / #mapping-form: name, host and clone guild IDs, the per mapping
// settings with search, the Message Features sub-modal and create, edit or clone save.
// The user tokens section is the UserTokensBlock slot (next lane).

import { useEffect, useMemo, useRef, useState } from "react";
import type { FormEvent } from "react";
import { createPortal } from "react-dom";
import { Toggle, cx } from "../../../../components/ui";
import type {
  DashboardMapping,
  MappingFormState,
  MappingSettingValue,
  MsgFeatureKey,
} from "../../../../types/dashboard-mappings";
import { GhostButton } from "../../../system/buttons";
import { useNotify, useSafeConfirm } from "../../../system/providers";
import { MsgFeaturesModal } from "./MsgFeaturesModal";
import { UserTokensBlock } from "./UserTokensBlock";
import {
  DISABLED_SETTINGS,
  FRIENDLY_NAMES,
  SECTIONS,
  SETTING_TIPS,
  initialForm,
  invalidFields,
  saveMapping,
  settingMatches,
  snapshot,
} from "./model";
import type { MappingField, MappingMode } from "./model";

export interface MappingModalProps {
  mapping: DashboardMapping | null;
  mode: MappingMode;
  onClose: () => void;
  /** Called after a successful save, before the modal closes. */
  onSaved: () => void | Promise<void>;
}

const INPUT =
  "w-full rounded-control border border-border bg-bg-2 px-3 py-2 text-sm text-text outline-none focus:border-accent aria-invalid:border-danger read-only:opacity-60";
const HINT = "text-xs leading-relaxed text-muted";

interface GuildFieldProps {
  id: string;
  name: MappingField;
  label: string;
  tip: string;
  value: string;
  placeholder?: string;
  maxLength?: number;
  readOnly?: boolean;
  title?: string;
  invalid: boolean;
  onChange: (v: string) => void;
  inputRef: (el: HTMLInputElement | null) => void;
}

function TextField({ id, name, label, tip, value, placeholder, maxLength, readOnly, title, invalid, onChange, inputRef }: GuildFieldProps) {
  return (
    <div className="field flex flex-col gap-1.5">
      <label htmlFor={id} className="text-sm font-semibold" title={tip}>
        {label}
      </label>
      <input
        ref={inputRef}
        id={id}
        name={name}
        type="text"
        inputMode={name === "mapping_name" ? undefined : "numeric"}
        maxLength={maxLength}
        placeholder={placeholder}
        readOnly={readOnly}
        aria-readonly={readOnly || undefined}
        aria-invalid={invalid || undefined}
        title={title}
        value={value}
        onChange={(e) => onChange(e.target.value)}
        className={cx(INPUT, readOnly && "field-readonly")}
      />
      <span className={HINT}>{tip}</span>
    </div>
  );
}

export function MappingModal({ mapping, mode, onClose, onSaved }: MappingModalProps) {
  const notify = useNotify();
  const confirm = useSafeConfirm();
  const isEdit = mode === "edit";
  const [baseline] = useState(() => initialForm(mapping, mode));
  const [form, setForm] = useState<MappingFormState>(baseline);
  const [search, setSearch] = useState("");
  const [openSections, setOpenSections] = useState<Set<string>>(new Set());
  const [featuresOpen, setFeaturesOpen] = useState(false);
  const [validated, setValidated] = useState(false);
  const [saving, setSaving] = useState(false);
  const refs = useRef<Partial<Record<MappingField, HTMLInputElement | null>>>({});

  const dirty = useMemo(() => snapshot(form) !== snapshot(baseline), [form, baseline]);
  const bad = validated ? invalidFields(form) : [];

  const setSetting = (key: string, value: MappingSettingValue) =>
    setForm((f) => ({ ...f, settings: { ...f.settings, [key]: value } }));
  const setField = (key: MappingField, value: string) => setForm((f) => ({ ...f, [key]: value }));

  // Close, Escape and a click outside ask before dropping unsaved changes.
  const live = useRef({ dirty, featuresOpen, saving });
  live.current = { dirty, featuresOpen, saving };
  const asking = useRef(false);
  const tryClose = async () => {
    if (!live.current.dirty) return onClose();
    if (asking.current) return;
    asking.current = true;
    const ok = await confirm({
      title: "Discard unsaved mapping changes?",
      body: "You have unsaved changes to this mapping. If you close now, your changes will be lost.",
      confirmLabel: "Discard changes",
      danger: true,
    });
    asking.current = false;
    if (ok) onClose();
  };
  const tryCloseRef = useRef(tryClose);
  tryCloseRef.current = tryClose;

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key !== "Escape" || live.current.featuresOpen || asking.current) return;
      void tryCloseRef.current();
    };
    document.addEventListener("keydown", onKey);
    return () => document.removeEventListener("keydown", onKey);
  }, []);

  useEffect(() => {
    refs.current.mapping_name?.focus();
  }, []);

  const submit = async (e: FormEvent) => {
    e.preventDefault();
    if (saving) return;
    setValidated(true);
    const missing = invalidFields(form);
    if (missing.length) {
      refs.current[missing[0]!]?.focus();
      return;
    }
    setSaving(true);
    try {
      await saveMapping(form);
      await onSaved();
      notify(isEdit ? "Mapping updated." : "Mapping created.", "success");
      onClose();
    } catch (err) {
      const msg = err instanceof Error && err.message ? err.message : "Network error while saving mapping.";
      notify(msg, "error");
      setSaving(false);
    }
  };

  const q = search.trim();
  const sections = SECTIONS.map((s) => ({ ...s, shown: s.keys.filter((k) => settingMatches(k, q)) }));
  const noResults = q !== "" && sections.every((s) => s.shown.length === 0);

  return createPortal(
    <div id="mapping-modal" className="modal show fixed inset-0 z-40 flex items-center justify-center p-4" aria-hidden="false">
      <div className="modal-backdrop absolute inset-0 bg-black/60" onMouseDown={() => void tryClose()} />
      <div
        role="dialog"
        aria-modal="true"
        aria-labelledby="mapping-title"
        className="modal-content relative flex max-h-[90vh] w-full max-w-2xl flex-col overflow-hidden rounded-2xl border border-border bg-card-solid text-text shadow-card"
      >
        <div className="modal-header flex items-center justify-between border-b border-border px-5 py-3">
          <div className="modal-title-wrap">
            <div id="mapping-title" className="modal-title-main text-lg font-semibold">
              Clone Mapping
            </div>
            {isEdit && mapping ? (
              <div id="mapping-id-subtle" className="modal-title-subtle text-xs text-muted">
                {mapping.mapping_id}
              </div>
            ) : null}
          </div>
          <button
            type="button"
            id="mapping-close"
            aria-label="Close"
            onClick={() => void tryClose()}
            className="verify-close rounded-control px-2 py-1 text-muted hover:text-text"
          >
            ✕
          </button>
        </div>

        <form id="mapping-form" autoComplete="off" noValidate onSubmit={(e) => void submit(e)} className="mapping-form flex min-h-0 flex-1 flex-col">
          <input type="hidden" id="map_mapping_id" name="mapping_id" value={form.mapping_id} readOnly />
          <div className="flex-1 space-y-4 overflow-y-auto p-5">
            <div className="mapping-section space-y-3">
              <TextField
                id="map_mapping_name"
                name="mapping_name"
                label="Mapping Name"
                tip="A friendly label for this clone pair. Shown on the dashboard cards."
                maxLength={64}
                value={form.mapping_name}
                invalid={bad.includes("mapping_name")}
                onChange={(v) => setField("mapping_name", v)}
                inputRef={(el) => (refs.current.mapping_name = el)}
              />
              <div className="mapping-guild-row grid gap-3 sm:grid-cols-2">
                <TextField
                  id="map_original_guild_id"
                  name="original_guild_id"
                  label="Host Guild"
                  tip={SETTING_TIPS.HOST_GUILD_ID!}
                  placeholder="Server ID to mirror from"
                  value={form.original_guild_id}
                  readOnly={isEdit}
                  title={isEdit ? "HOST_GUILD_ID cannot be changed after the mapping is created." : undefined}
                  invalid={bad.includes("original_guild_id")}
                  onChange={(v) => setField("original_guild_id", v)}
                  inputRef={(el) => (refs.current.original_guild_id = el)}
                />
                <TextField
                  id="map_cloned_guild_id"
                  name="cloned_guild_id"
                  label="Clone Guild"
                  tip={SETTING_TIPS.CLONE_GUILD_ID!}
                  placeholder="Server ID to mirror to"
                  value={form.cloned_guild_id}
                  readOnly={isEdit}
                  title={isEdit ? "CLONE_GUILD_ID cannot be changed after the mapping is created." : undefined}
                  invalid={bad.includes("cloned_guild_id")}
                  onChange={(v) => setField("cloned_guild_id", v)}
                  inputRef={(el) => (refs.current.cloned_guild_id = el)}
                />
              </div>
            </div>

            <div className="mapping-toolbar flex items-center gap-3">
              <input
                id="mappingSettingsSearch"
                type="text"
                aria-label="Search settings"
                placeholder="Search settings…"
                autoComplete="off"
                value={search}
                onChange={(e) => setSearch(e.target.value)}
                className={cx(INPUT, "mapping-search-input flex-1")}
              />
              <button
                type="button"
                id="msg-features-open"
                onClick={() => setFeaturesOpen(true)}
                className="mapping-msg-features-btn shrink-0 rounded-control border border-border bg-white/4 px-3 py-2 text-sm font-semibold text-accent hover:bg-white/8"
              >
                Message Features
              </button>
            </div>
            {noResults ? (
              <div id="mapping-no-results" className="mapping-no-results text-sm text-muted">
                No settings match your search
              </div>
            ) : null}

            {sections.map((s) => {
              if (s.shown.length === 0) return null;
              const open = q !== "" || openSections.has(s.name);
              return (
                <details
                  key={s.name}
                  open={open}
                  onToggle={(e) => {
                    const isOpen = e.currentTarget.open;
                    if (q !== "" || isOpen === openSections.has(s.name)) return;
                    setOpenSections((cur) => {
                      const next = new Set(cur);
                      if (isOpen) next.add(s.name);
                      else next.delete(s.name);
                      return next;
                    });
                  }}
                  className="mapping-section mapping-settings-section rounded-control border border-border"
                >
                  <summary className="mapping-section-header cursor-pointer select-none px-3 py-2 text-sm font-semibold">
                    {s.name}
                  </summary>
                  <div className="mapping-section-body flex flex-col gap-3 border-t border-border p-3">
                    {s.shown.map((k) => {
                      const reason = DISABLED_SETTINGS[k];
                      return (
                        <div
                          key={k}
                          className={cx("mapping-toggle-row mapping-setting-field", reason && "is-disabled")}
                          data-setting-key={k.toLowerCase()}
                        >
                          <Toggle
                            id={`map_${k}`}
                            label={<span className="mapping-toggle-name">{FRIENDLY_NAMES[k] ?? k}</span>}
                            description={
                              <>
                                {SETTING_TIPS[k] ? <span className="mapping-toggle-desc">{SETTING_TIPS[k]}</span> : null}
                                {reason ? <span className="mapping-toggle-desc mapping-toggle-disabled-note block">{reason}</span> : null}
                              </>
                            }
                            disabled={Boolean(reason)}
                            checked={form.settings[k] === true}
                            onChange={(v) => setSetting(k, v)}
                          />
                        </div>
                      );
                    })}
                  </div>
                </details>
              );
            })}

            {/* Slot for the user tokens lane (inventory item 21); renders nothing until then. */}
            <UserTokensBlock
              mappingId={isEdit ? form.mapping_id : null}
              settings={form.settings}
              setSetting={setSetting}
              search={search}
            />
          </div>

          <div className="mapping-form-footer flex justify-end gap-2 border-t border-border px-5 py-3">
            {dirty ? (
              <GhostButton type="button" id="mapping-cancel-btn" onClick={() => setForm(baseline)}>
                Cancel
              </GhostButton>
            ) : null}
            <GhostButton type="submit" id="mapping-save-btn" disabled={saving}>
              {saving ? "Saving…" : "Save Mapping"}
            </GhostButton>
          </div>
        </form>
      </div>

      {featuresOpen ? (
        <MsgFeaturesModal
          settings={form.settings}
          onToggle={(key: MsgFeatureKey, on) => setSetting(key, on)}
          onClose={() => setFeaturesOpen(false)}
        />
      ) : null}
    </div>,
    document.body,
  );
}
