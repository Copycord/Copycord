// Start form of Export messages: one of the client account's servers (from the type's options, typed into
// the server box to filter by name or ID), channel and user filters, the content filters with the
// attachment types, a webhook to forward to, a date range and the media to download. The body is the one
// the Guilds page export dialog sent, built by buildExportPayload.

import { useState } from "react";
import type { ChangeEvent, FormEvent, ReactNode } from "react";
import { Modal } from "../../../../components/Modal";
import { Button } from "../../../../components/ui/Button";
import { Checkbox } from "../../../../components/ui/Checkbox";
import { Field, FieldSelect, Input } from "../../../../components/ui/Field";
import { useApi } from "../../../../hooks/useApi";
import { DEFAULT_EXPORT_FORM, MEDIA_LABELS, attachmentSummary, buildExportPayload } from "../../../guilds/model";
import type { ExportForm } from "../../../guilds/model";
import { useStartRun, type TaskFormProps } from "../start";

export const EXPORT_MESSAGES_EXPLAINER =
  "Reads a server's messages with the client account, keeps the ones that pass the filters and saves them to a " +
  "JSON file you can download from the run page. Messages can also be forwarded to a webhook.";

export interface ExportGuild {
  id: string;
  name: string;
  icon_url?: string | null;
}

export interface ExportMessagesOptions {
  guilds: ExportGuild[];
  error?: string;
}

// The form fields the server's error keys land on; any other key shows under the form.
const FIELD_KEYS = new Set([
  "guild_id",
  "channel_id",
  "user_id",
  "webhook_url",
  "after_iso",
  "before_iso",
  "filters.word",
  "filters.min_length",
  "filters.min_reactions",
]);

/** The server's "Value error, Enter ..." in the form's words. */
function serverText(msg: string): string {
  return msg.replace(/^Value error, /, "");
}

function Check({ label, checked, onChange, disabled }: { label: string; checked: boolean; onChange: (v: boolean) => void; disabled?: boolean }) {
  return (
    <label className="flex cursor-pointer items-center gap-2 text-sm text-text has-[:disabled]:cursor-not-allowed has-[:disabled]:opacity-60">
      <Checkbox checked={checked} disabled={disabled} onChange={(e) => onChange(e.target.checked)} />
      {label}
    </label>
  );
}

function Group({ legend, help, children }: { legend: string; help?: ReactNode; children: ReactNode }) {
  return (
    <fieldset className="m-0 flex flex-col gap-2 border-0 p-0">
      <legend className="mb-1 p-0 text-sm font-medium text-text">{legend}</legend>
      {children}
      {help ? <p className="m-0 text-xs text-muted">{help}</p> : null}
    </fieldset>
  );
}

export function ExportMessagesForm({ type, open, onClose, initial }: TaskFormProps) {
  const opts = useApi<ExportMessagesOptions>(open ? `/api/tasks/types/${type.key}/options` : null);
  const guilds = opts.data?.guilds ?? [];
  const pending = open && !opts.data && !opts.error;
  const loadError = opts.data?.error || (opts.error ? "Could not load the servers" : undefined);
  const [guildId, setGuildId] = useState(initial?.guild ?? "");
  const [form, setForm] = useState<ExportForm>(DEFAULT_EXPORT_FORM);
  const [errors, setErrors] = useState<Record<string, string>>({});
  const { start, busy } = useStartRun(type);

  const set = <K extends keyof ExportForm>(key: K, value: ExportForm[K]) => {
    setForm((f) => ({ ...f, [key]: value }));
    setErrors({});
  };
  const text = (key: "channelId" | "userId" | "word" | "minLength" | "minReactions" | "webhookUrl" | "after" | "before") => ({
    value: form[key],
    onChange: (e: ChangeEvent<HTMLInputElement>) => set(key, e.target.value),
  });
  const check = (key: keyof ExportForm, label: string) => (
    <Check label={label} checked={form[key] as boolean} onChange={(v) => set(key, v)} />
  );

  // A preselected server the list does not have still shows by its ID.
  const listed = guilds.some((g) => g.id === guildId);

  const onSubmit = async (e: FormEvent) => {
    e.preventDefault();
    if (!guildId) {
      setErrors({ guild_id: "Pick a server." });
      return;
    }
    setErrors({});
    const res = await start({ ...buildExportPayload(guildId, form) });
    if (res && Object.keys(res).length) setErrors(Object.fromEntries(Object.entries(res).map(([k, v]) => [k, serverText(v)])));
    else if (!res) onClose();
  };

  const other = Object.entries(errors)
    .filter(([k]) => !FIELD_KEYS.has(k))
    .map(([, v]) => v);
  const summary = attachmentSummary(form);

  const footer = (
    <>
      <Button variant="ghost" onClick={onClose}>
        Cancel
      </Button>
      <Button variant="primary" type="submit" form="export-messages-form" loading={busy}>
        Export messages
      </Button>
    </>
  );

  return (
    <Modal open={open} onClose={onClose} title={`Run ${type.title}`} className="w-[min(640px,92vw)]" footer={footer}>
      <form id="export-messages-form" noValidate onSubmit={(e) => void onSubmit(e)} className="flex flex-col gap-4">
        <p className="m-0 text-sm text-muted">{EXPORT_MESSAGES_EXPLAINER}</p>

        <Field label="Server" help={loadError ? undefined : "Servers the client account is in."} error={errors.guild_id || loadError}>
          <FieldSelect
            searchable
            value={guildId}
            onChange={(v) => {
              setGuildId(v);
              setErrors({});
            }}
            pending={pending}
            pendingLabel="Pick a server"
            options={[
              { value: "", label: "Pick a server", disabled: true },
              ...(guildId && !listed && opts.data ? [{ value: guildId, label: guildId }] : []),
              ...guilds.map((g) => ({ value: g.id, label: g.name })),
            ]}
          />
        </Field>

        <div className="grid gap-3 sm:grid-cols-2">
          <Field label="Channel ID" help="Optional: only this channel." error={errors.channel_id}>
            <Input inputMode="numeric" autoComplete="off" placeholder="123456789012345678" {...text("channelId")} />
          </Field>
          <Field label="User ID" help="Optional: only this user's messages." error={errors.user_id}>
            <Input inputMode="numeric" autoComplete="off" placeholder="123456789012345678" {...text("userId")} />
          </Field>
        </div>

        <Group legend="Include">
          <div className="grid grid-cols-2 gap-2">
            {check("hasContent", "Text content")}
            {check("embeds", "Embeds")}
            {check("attachments", "Attachments")}
            {check("links", "Links")}
            {check("emojis", "Emojis")}
            {check("replies", "Replies")}
            {check("bots", "Bot messages")}
            {check("system", "System messages")}
            {check("pinned", "Pinned messages")}
            {check("stickers", "Stickers")}
            {check("mentions", "Mentions")}
            {check("threads", "Threads")}
          </div>
        </Group>

        <Group legend="Attachment types" help={form.attachments ? summary || "All types." : "Attachments are not included."}>
          <div className="grid grid-cols-2 gap-2 sm:grid-cols-4">
            {MEDIA_LABELS.map(([key, label]) => (
              <Check
                key={key}
                label={label}
                disabled={!form.attachments}
                checked={form.attTypes[key]}
                onChange={(v) => set("attTypes", { ...form.attTypes, [key]: v })}
              />
            ))}
          </div>
        </Group>

        <div className="flex flex-col gap-2">
          {check("wordOn", "Only messages with a word")}
          <Field label="Word" error={errors["filters.word"]}>
            <Input autoComplete="off" placeholder="must have this word" disabled={!form.wordOn} {...text("word")} />
          </Field>
        </div>

        <div className="grid gap-3 sm:grid-cols-2">
          <Field label="Minimum length" help="Characters; 0 for any." error={errors["filters.min_length"]}>
            <div className="w-28">
              <Input type="number" min={0} step={1} {...text("minLength")} />
            </div>
          </Field>
          <Field label="Minimum reactions" help="All reactions together; 0 for any." error={errors["filters.min_reactions"]}>
            <div className="w-28">
              <Input type="number" min={0} step={1} {...text("minReactions")} />
            </div>
          </Field>
        </div>

        <Field label="Webhook URL" help="Optional: every exported message is also posted to this webhook." error={errors.webhook_url}>
          <Input type="url" autoComplete="off" placeholder="https://discord.com/api/webhooks/..." {...text("webhookUrl")} />
        </Field>

        <div className="grid gap-3 sm:grid-cols-2">
          <Field label="After" help="Optional, in your local time." error={errors.after_iso}>
            <Input type="datetime-local" step={1} {...text("after")} />
          </Field>
          <Field label="Before" help="Optional, in your local time." error={errors.before_iso}>
            <Input type="datetime-local" step={1} {...text("before")} />
          </Field>
        </div>

        <Group legend="Download media" help="Files are saved next to the JSON file, in media/<type>/.">
          <div className="grid grid-cols-2 gap-2 sm:grid-cols-4">
            {MEDIA_LABELS.map(([key, label]) => (
              <Check
                key={key}
                label={label}
                checked={form.download[key]}
                onChange={(v) => set("download", { ...form.download, [key]: v })}
              />
            ))}
          </div>
        </Group>

        {other.length ? (
          <p role="alert" className="m-0 text-xs text-danger">
            {other.join(" ")}
          </p>
        ) : null}
      </form>
    </Modal>
  );
}
