// The rule create/edit modal of the old forwarding.html (#fwdModal). It keeps
// the old ids and the old "show" class on #fwdModal for the e2e specs, so it
// carries its own shell instead of components/Modal (which takes no id): the
// same focus trap, Escape and overlay close, and focus back to the opener.
// The look is the old .modal-content: uppercase section titles with the
// gradient bar, 12px labels, the .dd guild dropdown, the More filters row of
// label toggles and the footer at the end of the scroll, not sticky.
// Saving runs the old checks, asks POST /api/forwarding/validate for Telegram
// and Pushover, then posts the rule to POST /api/forwarding.

import { useEffect, useRef, useState } from "react";
import type { FormEvent, ReactNode } from "react";
import { createPortal } from "react-dom";
import { MODAL_BACKDROP, MODAL_CARD, MODAL_CLOSE, MODAL_HEADER, MODAL_OVERLAY, MODAL_TITLE, useScrollLock } from "../../components/Modal";
import { Button, ChipInput, cx, Field, Input } from "../../components/ui";
import { FieldSelect } from "../../components/ui/Field";
import { useApi } from "../../hooks/useApi";
import type { ClientGuildsResponse, ForwardingRule } from "../../types/forwarding";
import { useNotify } from "../system/providers";
import type { RuleEditorRequest } from "./editor";
import { ProviderFields, ProviderPicker } from "./providers";
import {
  buildPayload,
  CLIENT_GUILDS_URL,
  EMPTY_FORM,
  formFromRequest,
  hasAdvancedFilters,
  localProblem,
  modalTitle,
  needsServerValidation,
  saveFailure,
  saveRule,
  validateFailure,
  validateProvider,
} from "./ruleForm";
import type { RuleForm } from "./ruleForm";

const FOCUSABLE =
  'a[href], button:not([disabled]), input:not([disabled]):not([type="hidden"]), select:not([disabled]), textarea:not([disabled]), summary, [tabindex]:not([tabindex="-1"])';

/** .form-group: 4px between label, control and help, 12px below; p.help-text kept the browser's 1em margins. */
const GROUP = "form-group mb-3 gap-1! [&>p]:my-3";
/** .chip-input-wrap and .chip-text-input over the shared ChipInput. */
const CHIP_WRAP =
  "[&>div]:transition-[border-color,box-shadow] [&>div]:duration-200 [&>div:hover]:border-accent/30 [&>div:focus-within]:border-accent [&>div:focus-within]:ring-0 " +
  "[&_li>button]:border-primary-a30 [&_li>button]:bg-primary-a15 [&_li>button]:py-1 [&_li>button]:pr-2 [&_li>button]:pl-2.5 " +
  "[&_input]:min-w-[120px] [&_input]:py-1 [&_input]:placeholder:text-muted-a40";

export interface RuleModalProps {
  /** null keeps the modal closed. */
  request: RuleEditorRequest | null;
  onClose: () => void;
  /** Called after a save with the server's item, or null when it sent only the id. */
  onSaved: (rule: ForwardingRule | null) => void;
}

function Section({ title, children }: { title: string; children: ReactNode }) {
  return (
    <div className="form-section mb-5 pb-4">
      <h4 className="form-section-title mt-0 mr-0 mb-3.5 ml-0 flex items-center gap-2 text-[13px] font-semibold tracking-[0.04em] text-accent uppercase before:h-3.5 before:w-[3px] before:rounded-[2px] before:bg-[linear-gradient(180deg,var(--color-primary),var(--color-primary-700))] before:content-['']">
        {title}
      </h4>
      {children}
    </div>
  );
}

/** label.toggle with its .toggle-label; `enabled` is the larger green .toggle--enabled. */
function LabelToggle({ id, label, title, checked, enabled, onChange }: {
  id: string;
  label: string;
  title?: string;
  checked: boolean;
  enabled?: boolean;
  onChange: (on: boolean) => void;
}) {
  return (
    <label
      title={title}
      className={cx("toggle group/tg relative inline-flex cursor-pointer items-center gap-2.5 select-none", enabled && "toggle--enabled")}
    >
      <input
        id={id}
        type="checkbox"
        checked={checked}
        onChange={(e) => onChange(e.target.checked)}
        className="peer absolute size-0 opacity-0"
      />
      <span
        className={cx(
          "toggle-track relative rounded-full border border-white/12 bg-white/8 transition duration-200 group-hover/tg:border-white/20 peer-checked:border-transparent peer-focus-visible:border-accent",
          enabled
            ? "h-6 w-11 peer-checked:border-0 peer-checked:bg-[linear-gradient(135deg,var(--color-good)_0%,var(--color-good-strong)_100%)]"
            : "h-[22px] w-[38px] peer-checked:bg-[linear-gradient(135deg,var(--color-primary)_0%,var(--color-primary-700)_100%)]",
        )}
      >
        <span
          className={cx(
            "toggle-thumb absolute top-0.5 left-0.5 rounded-full bg-muted transition duration-200 [.peer:checked+.toggle-track>&]:bg-white",
            enabled ? "size-[18px] [.peer:checked+.toggle-track>&]:translate-x-5" : "size-4 [.peer:checked+.toggle-track>&]:translate-x-4",
          )}
        />
      </span>
      <span className="toggle-label text-[13px] text-muted transition-colors duration-150 peer-checked:text-text">{label}</span>
    </label>
  );
}

/** The old .dd toggle the guild <select> was turned into: 34px, 13px, purple caret; FieldSelect adds the caret room. */
const GUILD_TOGGLE =
  "input dd-toggle min-h-[34px] rounded-btn border border-border bg-control py-2 pl-2.5 font-sans text-[13px] text-text outline-0 transition-[border,box-shadow,background] duration-200 focus-visible:border-accent";

export function RuleModal({ request, onClose, onSaved }: RuleModalProps) {
  const open = request !== null;
  useScrollLock(open);
  const notify = useNotify();
  const guilds = useApi<ClientGuildsResponse>(open ? CLIENT_GUILDS_URL : null);
  const [form, setForm] = useState<RuleForm>(EMPTY_FORM);
  const [advancedOpen, setAdvancedOpen] = useState(false);
  const [saving, setSaving] = useState(false);
  const dialogRef = useRef<HTMLDivElement>(null);
  const onCloseRef = useRef(onClose);
  onCloseRef.current = onClose;
  const savingRef = useRef(false);

  useEffect(() => {
    if (!request) return;
    const next = formFromRequest(request);
    setForm(next);
    setAdvancedOpen(hasAdvancedFilters(next));
  }, [request]);

  useEffect(() => {
    if (!open) return;
    const opener = document.activeElement as HTMLElement | null;
    const dialog = dialogRef.current;
    // The old page left focus on the opener; the dialog box takes it so Tab starts inside.
    dialog?.focus();
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") {
        e.stopPropagation();
        onCloseRef.current();
        return;
      }
      if (e.key !== "Tab" || !dialog) return;
      const items = Array.from(dialog.querySelectorAll<HTMLElement>(FOCUSABLE)).filter((el) => el.offsetParent !== null || el === document.activeElement);
      if (items.length === 0) return;
      const head = items[0]!;
      const tail = items[items.length - 1]!;
      if (e.shiftKey && (document.activeElement === head || document.activeElement === dialog)) {
        e.preventDefault();
        tail.focus();
      } else if (!e.shiftKey && document.activeElement === tail) {
        e.preventDefault();
        head.focus();
      }
    };
    document.addEventListener("keydown", onKey);
    return () => {
      document.removeEventListener("keydown", onKey);
      opener?.focus?.();
    };
  }, [open]);

  const patch = (next: Partial<RuleForm>) => setForm((f) => ({ ...f, ...next }));

  async function onSubmit(e: FormEvent) {
    e.preventDefault();
    if (savingRef.current) return;
    const payload = buildPayload(form);
    const problem = localProblem(payload);
    if (problem) {
      notify(problem, "error");
      return;
    }
    savingRef.current = true;
    setSaving(true);
    try {
      if (needsServerValidation(payload.provider)) {
        try {
          await validateProvider(payload);
        } catch (err) {
          notify(validateFailure(err), "error");
          return;
        }
      }
      try {
        const reply = await saveRule(payload);
        notify("Forwarding rule saved.", "success");
        onSaved(reply?.item ?? null);
        onClose();
      } catch (err) {
        notify(saveFailure(err), "error");
      }
    } finally {
      savingRef.current = false;
      setSaving(false);
    }
  }

  const guildItems = Array.isArray(guilds.data?.items) ? guilds.data.items : [];
  const knownGuild = form.guild_id === "" || guildItems.some((g) => String(g.id) === form.guild_id);

  return createPortal(
    <div
      id="fwdModal"
      className={open ? `modal show ${MODAL_OVERLAY}` : "modal hidden"}
      aria-hidden={!open}
      onMouseDown={(e) => {
        if (e.target === e.currentTarget) onClose();
      }}
    >
      {open ? (
        <>
          <div className={`modal-backdrop ${MODAL_BACKDROP} pointer-events-none`} />
          <div
            ref={dialogRef}
            role="dialog"
            aria-modal="true"
            aria-labelledby="fwdModalTitle"
            tabIndex={-1}
            className={`modal-content ${MODAL_CARD} w-[min(900px,92vw)]`}
          >
            <header className={`modal-header shrink-0 ${MODAL_HEADER}`}>
              <h3 id="fwdModalTitle" className={`modal-title ${MODAL_TITLE}`}>
                {modalTitle(request)}
              </h3>
              <button id="fwdModalCloseBtn" type="button" aria-label="Close" onClick={onClose} className={`verify-close ${MODAL_CLOSE}`}>
                ✕
              </button>
            </header>

            <div className="modal-body relative min-h-0 flex-auto overflow-x-hidden overflow-y-auto px-4 pt-3 pb-0">
              <form id="forwarding-form" autoComplete="off" noValidate onSubmit={(e) => void onSubmit(e)}>
                <input type="hidden" id="fwd_id" value={form.rule_id} />
                <Section title="Basic Information">
                  <Field label="Name" id="fwd_label" help="A short name so you can recognize this rule later." className={GROUP}>
                    <Input
                      type="text"
                      required
                      placeholder="e.g. Discord: #support keyword forwards"
                      value={form.label}
                      onChange={(e) => patch({ label: e.target.value })}
                    />
                  </Field>
                  <Field
                    label="Guild scope"
                    id="fwd_guild_id"
                    help="If set, this rule only applies to messages from the selected guild."
                    className={GROUP}
                  >
                    <FieldSelect
                      value={form.guild_id}
                      onChange={(guild_id) => patch({ guild_id })}
                      toggleClassName={GUILD_TOGGLE}
                      options={[
                        { value: "", label: "All guilds" },
                        ...guildItems.map((g) => ({ value: String(g.id), label: `${g.name || "Unknown guild"} (${String(g.id)})` })),
                        ...(knownGuild ? [] : [{ value: form.guild_id, label: form.guild_id }]),
                      ]}
                    />
                  </Field>
                  {guilds.error ? (
                    <p role="alert" className="text-xs text-danger">
                      Could not load the guild list; the rule can still apply to all guilds.
                    </p>
                  ) : null}
                </Section>

                <Section title="Provider Configuration">
                  <div className={`flex flex-col ${GROUP}`}>
                    <label id="fwd_provider_label" htmlFor="providerSelectBtn" className="form-label ml-0.5 text-xs text-muted">
                      Provider
                    </label>
                    <ProviderPicker value={form.provider} onChange={(provider) => patch({ provider })} />
                    <p className="help-text text-xs text-muted opacity-85">
                      Choose where Copycord should forward messages when this rule matches.
                    </p>
                  </div>
                  <ProviderFields form={form} patch={patch} />
                </Section>

                <Section title="Filters">
                  <Field label="Match any of these words / phrases" id="fwd_keywords_any" className={GROUP}>
                    <ChipInput
                      value={form.keywords_any}
                      onChange={(keywords_any) => patch({ keywords_any })}
                      placeholder="Type and press Enter..."
                      className={CHIP_WRAP}
                    />
                  </Field>
                  <Field label="Require all of these words (optional)" id="fwd_keywords_all" className={GROUP}>
                    <ChipInput
                      value={form.keywords_all}
                      onChange={(keywords_all) => patch({ keywords_all })}
                      placeholder="Type and press Enter..."
                      className={CHIP_WRAP}
                    />
                  </Field>
                  <Field label="Limit to channel IDs (optional)" id="fwd_channels" className={GROUP}>
                    <ChipInput
                      value={form.channel_ids}
                      onChange={(channel_ids) => patch({ channel_ids })}
                      placeholder="Type channel ID and press Enter..."
                      className={CHIP_WRAP}
                    />
                  </Field>
                  <Field label="Limit to user IDs (optional)" id="fwd_users" className={GROUP}>
                    <ChipInput
                      value={form.user_ids}
                      onChange={(user_ids) => patch({ user_ids })}
                      placeholder="Type user ID and press Enter..."
                      className={CHIP_WRAP}
                    />
                  </Field>

                  <details
                    id="fwdFiltersMore"
                    open={advancedOpen}
                    onToggle={(e) => setAdvancedOpen(e.currentTarget.open)}
                    className="filters-advanced group mt-2.5 rounded-control bg-card-solid p-2.5"
                  >
                    <summary className="filters-advanced-summary flex cursor-pointer list-none items-center justify-between gap-2.5 rounded-btn bg-card-solid px-2.5 py-2 select-none focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-white/12 [&::-webkit-details-marker]:hidden">
                      <span className="filters-advanced-title">More filters</span>
                      <span className="filters-advanced-chev transition-transform duration-150 group-open:rotate-180" aria-hidden="true">
                        ▾
                      </span>
                    </summary>
                    <div className="filters-advanced-body mt-2.5">
                      <div className="form-row flex flex-wrap gap-2.5">
                        <LabelToggle
                          id="fwd_case_sensitive"
                          label="Case sensitive"
                          title="Only match if the message text matches your keywords with the exact same capitalization"
                          checked={form.case_sensitive}
                          onChange={(case_sensitive) => patch({ case_sensitive })}
                        />
                        <LabelToggle
                          id="fwd_include_embeds"
                          label="Search text in embeds"
                          title="Also search inside embed"
                          checked={form.include_embeds}
                          onChange={(include_embeds) => patch({ include_embeds })}
                        />
                        <LabelToggle
                          id="fwd_bot_messages"
                          label="Bot messages"
                          title="Allow this rule to match messages sent by bots"
                          checked={form.include_bots}
                          onChange={(include_bots) => patch({ include_bots })}
                        />
                        <LabelToggle
                          id="fwd_system_messages"
                          label="System messages"
                          title="Forward Discord's own notices in this channel: members joining, boosts, pins and thread creation. Off by default."
                          checked={form.include_system}
                          onChange={(include_system) => patch({ include_system })}
                        />
                        <LabelToggle
                          id="fwd_has_attachments"
                          label="Has attachments"
                          title="Only match messages that include attachments (images, videos, audio, or files)"
                          checked={form.has_attachments}
                          onChange={(has_attachments) => patch({ has_attachments })}
                        />
                      </div>
                    </div>
                  </details>
                </Section>

                <div className="modal-form-footer -mx-4 -mb-3.5 flex items-center justify-between gap-3 rounded-b-panel border-t border-white/8 bg-[linear-gradient(180deg,var(--color-surface-deep-a95),var(--color-surface-deep))] px-4 py-3.5 max-[480px]:flex-col max-[480px]:items-stretch">
                  <LabelToggle id="fwd_enabled" label="Rule enabled" enabled checked={form.enabled} onChange={(enabled) => patch({ enabled })} />
                  <div className="form-actions flex gap-2.5 max-[480px]:w-full max-[480px]:[&>button]:flex-1">
                    <Button id="fwdCancelBtn" variant="ghost" type="button" onClick={onClose}>
                      Cancel
                    </Button>
                    <Button id="fwdSaveBtn" variant="primary" type="submit" disabled={saving}>
                      {saving ? "Saving…" : "Save Rule"}
                    </Button>
                  </div>
                </div>
              </form>
            </div>
          </div>
        </>
      ) : null}
    </div>,
    document.body,
  );
}
