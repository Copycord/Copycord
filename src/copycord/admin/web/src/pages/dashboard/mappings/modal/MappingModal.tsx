// #mapping-modal / #mapping-form: name, host and clone guild IDs, the per mapping
// settings with search, the Message Features sub-modal and create, edit or clone save.
// The look is the old 720px #mapping-modal: labels with "i" tips, a sticky search
// toolbar, uppercase section rows with a round chevron and a sticky footer.

import { useEffect, useMemo, useRef, useState } from "react";
import type { FormEvent } from "react";
import { createPortal } from "react-dom";
import { cx } from "../../../../components/ui";
import { MODAL_BACKDROP, MODAL_CARD, MODAL_CLOSE, MODAL_OVERLAY, TipLabel, useScrollLock } from "../../../../components/Modal";
import type {
  DashboardMapping,
  MappingFormState,
  MappingSettingValue,
  MsgFeatureKey,
} from "../../../../types/dashboard-mappings";
import { GhostButton } from "../../../system/buttons";
import { useNotify, useSafeConfirm } from "../../../system/providers";
import { MsgFeaturesModal } from "./MsgFeaturesModal";
import { SECTION, SectionSummary, ToggleRow } from "./parts";
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

// .field > input, .field-readonly and the red flash of a missing field.
const INPUT =
  "w-full rounded-control border border-border bg-control p-3 text-text outline-0 transition-[border] duration-200 focus:border-accent aria-invalid:border-danger";
const READONLY =
  "field-readonly cursor-not-allowed border-white/6! bg-[#1a102d]! text-muted opacity-60 transition-none focus:border-white/6!";
const SEARCH_ICON =
  "url(\"data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='14' height='14' viewBox='0 0 24 24' fill='none' stroke='%239e95b7' stroke-width='2' stroke-linecap='round' stroke-linejoin='round'%3E%3Ccircle cx='11' cy='11' r='8'/%3E%3Cpath d='m21 21-4.3-4.3'/%3E%3C/svg%3E\")";

interface GuildFieldProps {
  id: string;
  name: MappingField;
  label: string;
  tip: string;
  tipLabel: string;
  tipId: string;
  value: string;
  placeholder?: string;
  maxLength?: number;
  readOnly?: boolean;
  title?: string;
  invalid: boolean;
  className?: string;
  onChange: (v: string) => void;
  inputRef: (el: HTMLInputElement | null) => void;
}

function TextField(p: GuildFieldProps) {
  return (
    <div className="field relative mb-4">
      <TipLabel htmlFor={p.id} text={p.label} tip={p.tip} tipLabel={p.tipLabel} tipId={p.tipId} align="center" />
      <input
        ref={p.inputRef}
        id={p.id}
        name={p.name}
        type="text"
        inputMode={p.name === "mapping_name" ? undefined : "numeric"}
        maxLength={p.maxLength}
        placeholder={p.placeholder}
        readOnly={p.readOnly}
        aria-readonly={p.readOnly || undefined}
        aria-invalid={p.invalid || undefined}
        title={p.title}
        value={p.value}
        onChange={(e) => p.onChange(e.target.value)}
        className={cx(INPUT, p.readOnly && READONLY, p.className)}
      />
    </div>
  );
}

export function MappingModal({ mapping, mode, onClose, onSaved }: MappingModalProps) {
  useScrollLock(true);
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
      confirmVariant: "ghost-red",
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
    <div id="mapping-modal" className={`modal show ${MODAL_OVERLAY}`} aria-hidden="false">
      <div className={`modal-backdrop ${MODAL_BACKDROP}`} onMouseDown={() => void tryClose()} />
      <div
        role="dialog"
        aria-modal="true"
        aria-labelledby="mapping-title"
        className={`modal-content ${MODAL_CARD} w-[min(720px,92vw)] overflow-hidden`}
      >
        <div className="modal-header flex shrink-0 items-center justify-between px-6 pt-5 pb-3.5">
          <div className="modal-title-wrap flex flex-col leading-[1.2]">
            <div id="mapping-title" className="modal-title-main flex items-center text-sm font-semibold tracking-[-0.03em] text-text">
              Clone Mapping
            </div>
            {isEdit && mapping ? (
              <div
                id="mapping-id-subtle"
                className="modal-title-subtle mt-[3px] max-w-[220px] overflow-hidden text-[11px] font-normal text-ellipsis whitespace-nowrap text-text opacity-40 select-text"
              >
                {mapping.mapping_id}
              </div>
            ) : null}
          </div>
          <button type="button" id="mapping-close" aria-label="Close" onClick={() => void tryClose()} className={`verify-close ${MODAL_CLOSE}`}>
            ✕
          </button>
        </div>

        <form
          id="mapping-form"
          autoComplete="off"
          noValidate
          onSubmit={(e) => void submit(e)}
          className="mapping-form min-h-0 flex-auto overflow-x-hidden overflow-y-auto px-6 pt-5 [scrollbar-color:var(--color-accent)_transparent] [scrollbar-width:thin]"
        >
          <input type="hidden" id="map_mapping_id" name="mapping_id" value={form.mapping_id} readOnly />
          <div className="mapping-section border-b border-border py-3.5">
            <TextField
              id="map_mapping_name"
              name="mapping_name"
              label="Mapping Name"
              tip="A friendly label for this clone pair. Shown on the dashboard cards."
              tipLabel="What is Mapping Name?"
              tipId="tip-mapping_name"
              maxLength={64}
              value={form.mapping_name}
              invalid={bad.includes("mapping_name")}
              className="block max-w-[320px]"
              onChange={(v) => setField("mapping_name", v)}
              inputRef={(el) => (refs.current.mapping_name = el)}
            />
            <div className="mapping-guild-row mt-2.5 grid grid-cols-2 gap-3">
              <TextField
                id="map_original_guild_id"
                name="original_guild_id"
                label="Host Guild"
                tip={SETTING_TIPS.HOST_GUILD_ID!}
                tipLabel="What is HOST_GUILD_ID?"
                tipId="tip-HOST_GUILD_ID"
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
                tipLabel="What is CLONE_GUILD_ID?"
                tipId="tip-CLONE_GUILD_ID"
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

          <div className="mapping-toolbar sticky top-0 z-[2] -mx-5 flex items-center gap-2 bg-[#0f0820] px-5 py-2.5">
            <div className="mapping-search-row min-w-0 flex-1">
              <input
                id="mappingSettingsSearch"
                type="text"
                aria-label="Search settings"
                placeholder="Search settings…"
                autoComplete="off"
                value={search}
                onChange={(e) => setSearch(e.target.value)}
                style={{ backgroundImage: SEARCH_ICON }}
                className="mapping-search-input w-full rounded-input border border-border bg-control bg-[position:10px_center] bg-no-repeat py-[7px] pr-3 pl-8 text-[13px] text-text outline-0 transition-[border-color] duration-150 placeholder:text-muted placeholder:opacity-50 focus:border-accent"
              />
            </div>
            <button
              type="button"
              id="msg-features-open"
              onClick={() => setFeaturesOpen(true)}
              className="mapping-msg-features-btn inline-flex shrink-0 cursor-pointer items-center gap-1.5 rounded-input border border-border bg-white/[0.03] px-3 py-[7px] text-xs font-medium whitespace-nowrap text-muted transition-colors duration-150 hover:border-white/[0.18] hover:bg-white/[0.06] hover:text-text"
            >
              <svg xmlns="http://www.w3.org/2000/svg" width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true" className="text-accent">
                <path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z" />
              </svg>
              Message Features
            </button>
          </div>
          {noResults ? (
            <div id="mapping-no-results" className="mapping-no-results py-10 text-center text-[13px] text-muted opacity-60">
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
                className={SECTION}
              >
                <SectionSummary>{s.name}</SectionSummary>
                <div className="mapping-section-body pt-1">
                  {s.shown.map((k) => {
                    const reason = DISABLED_SETTINGS[k];
                    return (
                      <ToggleRow
                        key={k}
                        id={`map_${k}`}
                        settingKey={k.toLowerCase()}
                        name={FRIENDLY_NAMES[k] ?? k}
                        desc={SETTING_TIPS[k]}
                        note={reason}
                        disabled={Boolean(reason)}
                        checked={form.settings[k] === true}
                        onChange={(v) => setSetting(k, v)}
                      />
                    );
                  })}
                </div>
              </details>
            );
          })}

          <UserTokensBlock
            mappingId={isEdit ? form.mapping_id : null}
            settings={form.settings}
            setSetting={setSetting}
            search={search}
          />

          <div className="mapping-form-footer sticky bottom-0 z-[3] -mx-5 -mb-5 flex justify-end gap-2 rounded-b-panel bg-[#0f0820] px-5 py-3.5">
            {dirty ? (
              <GhostButton type="button" id="mapping-cancel-btn" className="gap-[normal]!" onClick={() => setForm(baseline)}>
                Cancel
              </GhostButton>
            ) : null}
            <GhostButton type="submit" id="mapping-save-btn" className="gap-[normal]!" disabled={saving}>
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
