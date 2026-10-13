// The rule create/edit modal of the old forwarding.html (#fwdModal). It keeps
// the old ids and the old "show" class on #fwdModal for the e2e specs, so it
// carries its own shell instead of components/Modal (which takes no id): the
// same focus trap, Escape and overlay close, and focus back to the opener.
// Saving runs the old checks, asks POST /api/forwarding/validate for Telegram
// and Pushover, then posts the rule to POST /api/forwarding.

import { useEffect, useRef, useState } from "react";
import type { FormEvent, ReactNode } from "react";
import { createPortal } from "react-dom";
import { Button, ChipInput, cx, Field, Input, Select, Toggle } from "../../components/ui";
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

export interface RuleModalProps {
  /** null keeps the modal closed. */
  request: RuleEditorRequest | null;
  onClose: () => void;
  /** Called after a save with the server's item, or null when it sent only the id. */
  onSaved: (rule: ForwardingRule | null) => void;
}

function Section({ title, children }: { title: string; children: ReactNode }) {
  return (
    <section className="form-section flex flex-col gap-3.5 border-b border-border pb-4 last:border-b-0">
      <h4 className="form-section-title text-xs font-bold tracking-wide text-text-2 uppercase">{title}</h4>
      {children}
    </section>
  );
}

function Switch({ id, label, title, checked, onChange }: {
  id: string;
  label: string;
  title: string;
  checked: boolean;
  onChange: (on: boolean) => void;
}) {
  return (
    <div title={title} className="rounded-control border border-border bg-white/3 px-3 py-2">
      <Toggle id={id} label={label} checked={checked} onChange={onChange} />
    </div>
  );
}

export function RuleModal({ request, onClose, onSaved }: RuleModalProps) {
  const open = request !== null;
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
    (dialog?.querySelector<HTMLElement>("#fwd_label") ?? dialog)?.focus();
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
      className={cx("modal fixed inset-0 z-40 items-center justify-center bg-black/60 p-3 sm:p-4", open ? "show flex" : "hidden")}
      aria-hidden={!open}
      onMouseDown={(e) => {
        if (e.target === e.currentTarget) onClose();
      }}
    >
      {open ? (
        <div
          ref={dialogRef}
          role="dialog"
          aria-modal="true"
          aria-labelledby="fwdModalTitle"
          tabIndex={-1}
          className="modal-content flex max-h-[calc(100dvh-24px)] w-full max-w-[640px] flex-col rounded-card border border-border bg-card-solid text-text shadow-card outline-none"
        >
          <header className="modal-header flex items-center justify-between border-b border-border px-5 py-4">
            <h3 id="fwdModalTitle" className="modal-title text-lg font-semibold">
              {modalTitle(request)}
            </h3>
            <button
              id="fwdModalCloseBtn"
              type="button"
              aria-label="Close"
              onClick={onClose}
              className="verify-close cursor-pointer rounded-control px-2 py-1 text-muted outline-none hover:text-text focus-visible:ring-2 focus-visible:ring-accent/60"
            >
              ✕
            </button>
          </header>

          <form id="forwarding-form" autoComplete="off" noValidate onSubmit={(e) => void onSubmit(e)} className="flex min-h-0 flex-1 flex-col">
            <input type="hidden" id="fwd_id" value={form.rule_id} />
            <div className="modal-body flex min-h-0 flex-1 flex-col gap-4 overflow-y-auto px-5 py-4">
              <Section title="Basic Information">
                <Field label="Name" id="fwd_label" help="A short name so you can recognize this rule later.">
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
                >
                  <Select value={form.guild_id} onChange={(e) => patch({ guild_id: e.target.value })}>
                    <option value="" className="bg-card-solid">
                      All guilds
                    </option>
                    {guildItems.map((g) => (
                      <option key={String(g.id)} value={String(g.id)} className="bg-card-solid">
                        {`${g.name || "Unknown guild"} (${String(g.id)})`}
                      </option>
                    ))}
                    {knownGuild ? null : (
                      <option value={form.guild_id} className="bg-card-solid">
                        {form.guild_id}
                      </option>
                    )}
                  </Select>
                </Field>
                {guilds.error ? (
                  <p role="alert" className="text-xs text-danger">
                    Could not load the guild list; the rule can still apply to all guilds.
                  </p>
                ) : null}
              </Section>

              <Section title="Provider Configuration">
                <div className="flex flex-col gap-1.5">
                  <label id="fwd_provider_label" htmlFor="providerSelectBtn" className="text-sm font-semibold text-text-1">
                    Provider
                  </label>
                  <ProviderPicker value={form.provider} onChange={(provider) => patch({ provider })} />
                  <p className="text-xs text-text-2">Choose where Copycord should forward messages when this rule matches.</p>
                </div>
                <ProviderFields form={form} patch={patch} />
              </Section>

              <Section title="Filters">
                <Field label="Match any of these words / phrases" id="fwd_keywords_any">
                  <ChipInput
                    value={form.keywords_any}
                    onChange={(keywords_any) => patch({ keywords_any })}
                    placeholder="Type and press Enter..."
                  />
                </Field>
                <Field label="Require all of these words (optional)" id="fwd_keywords_all">
                  <ChipInput
                    value={form.keywords_all}
                    onChange={(keywords_all) => patch({ keywords_all })}
                    placeholder="Type and press Enter..."
                  />
                </Field>
                <Field label="Limit to channel IDs (optional)" id="fwd_channels">
                  <ChipInput
                    value={form.channel_ids}
                    onChange={(channel_ids) => patch({ channel_ids })}
                    placeholder="Type channel ID and press Enter..."
                  />
                </Field>
                <Field label="Limit to user IDs (optional)" id="fwd_users">
                  <ChipInput
                    value={form.user_ids}
                    onChange={(user_ids) => patch({ user_ids })}
                    placeholder="Type user ID and press Enter..."
                  />
                </Field>

                <details
                  id="fwdFiltersMore"
                  open={advancedOpen}
                  onToggle={(e) => setAdvancedOpen(e.currentTarget.open)}
                  className="filters-advanced group rounded-control border border-border"
                >
                  <summary className="filters-advanced-summary flex cursor-pointer list-none items-center justify-between px-3 py-2.5 text-sm font-semibold outline-none focus-visible:ring-2 focus-visible:ring-accent/60 [&::-webkit-details-marker]:hidden">
                    <span className="filters-advanced-title">More filters</span>
                    <span className="filters-advanced-chev text-text-2 transition-transform group-open:rotate-180" aria-hidden="true">
                      ▾
                    </span>
                  </summary>
                  <div className="filters-advanced-body grid grid-cols-1 gap-2 px-3 pb-3 sm:grid-cols-2">
                    <Switch
                      id="fwd_case_sensitive"
                      label="Case sensitive"
                      title="Only match if the message text matches your keywords with the exact same capitalization"
                      checked={form.case_sensitive}
                      onChange={(case_sensitive) => patch({ case_sensitive })}
                    />
                    <Switch
                      id="fwd_include_embeds"
                      label="Search text in embeds"
                      title="Also search inside embed"
                      checked={form.include_embeds}
                      onChange={(include_embeds) => patch({ include_embeds })}
                    />
                    <Switch
                      id="fwd_bot_messages"
                      label="Bot messages"
                      title="Allow this rule to match messages sent by bots"
                      checked={form.include_bots}
                      onChange={(include_bots) => patch({ include_bots })}
                    />
                    <Switch
                      id="fwd_system_messages"
                      label="System messages"
                      title="Forward Discord's own notices in this channel: members joining, boosts, pins and thread creation. Off by default."
                      checked={form.include_system}
                      onChange={(include_system) => patch({ include_system })}
                    />
                    <Switch
                      id="fwd_has_attachments"
                      label="Has attachments"
                      title="Only match messages that include attachments (images, videos, audio, or files)"
                      checked={form.has_attachments}
                      onChange={(has_attachments) => patch({ has_attachments })}
                    />
                  </div>
                </details>
              </Section>
            </div>

            <footer className="modal-form-footer flex flex-wrap items-center justify-between gap-3 border-t border-border px-5 py-4">
              <Toggle
                id="fwd_enabled"
                label="Rule enabled"
                checked={form.enabled}
                onChange={(enabled) => patch({ enabled })}
                className="toggle--enabled flex-row-reverse justify-end"
              />
              <div className="form-actions flex gap-2">
                <Button id="fwdCancelBtn" variant="ghost" type="button" onClick={onClose}>
                  Cancel
                </Button>
                <Button id="fwdSaveBtn" variant="primary" type="submit" disabled={saving}>
                  {saving ? "Saving…" : "Save Rule"}
                </Button>
              </div>
            </footer>
          </form>
        </div>
      ) : null}
    </div>,
    document.body,
  );
}
