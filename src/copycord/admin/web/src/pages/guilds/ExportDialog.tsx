// "Export messages" (.export-modal): channel and user filters, content
// filters with an attachment types dialog, webhook forwarding, a date range
// and media download, then POST /api/export/messages. Control ids match the
// old dialog so selectors keep working.

import { useRef, useState } from "react";
import type { ChangeEvent, InputHTMLAttributes, ReactNode, Ref } from "react";
import { useToast } from "../../components/Toast";
import { Button, Tooltip, cx } from "../../components/ui";
import { api } from "../../lib/api";
import type { ExportMessagesResponse, GuildSummary, MediaTypes } from "../../types/guilds";
import { GuildDialog } from "./GuildDialog";
import { DEFAULT_EXPORT_FORM, MEDIA_LABELS, attachmentSummary, buildExportPayload, exportError } from "./model";
import type { ExportForm } from "./model";

const BOX =
  "relative m-[3px_3px_3px_4px] size-[18px] shrink-0 cursor-pointer appearance-none rounded-md border border-border bg-white/4 outline-none transition " +
  "hover:border-accent focus-visible:shadow-[0_0_0_4px_var(--color-accent-a18)] checked:border-primary-600 checked:bg-primary " +
  "disabled:cursor-not-allowed disabled:opacity-60";

const INPUT =
  "h-9 w-full rounded-lg border border-white/8 bg-white/4 px-2.5 text-sm leading-9 text-inherit outline-none [color-scheme:dark] " +
  "placeholder:text-text-2/70 focus:border-accent disabled:cursor-not-allowed disabled:opacity-55";

// label.form-field around span.label and the .input; SMALL is the 28px .input of the word and metric rows.
const LABEL = "label mb-1.5 block font-semibold";
const SMALL = "h-7! px-2! text-[13px]! leading-7!";
const HINT = "hint mt-1.5 block text-xs opacity-75";
const TWO_COL = "grid gap-3 min-[720px]:grid-cols-2";

function Check({
  id,
  label,
  checked,
  onChange,
}: {
  id: string;
  label: string;
  checked: boolean;
  onChange: (v: boolean) => void;
}) {
  return (
    <label htmlFor={id} className="inline-flex min-h-8 cursor-pointer items-center gap-2 text-sm text-text">
      <input id={id} type="checkbox" className={BOX} checked={checked} onChange={(e) => onChange(e.target.checked)} />
      <span>{label}</span>
    </label>
  );
}

function TextInput({
  label,
  hint,
  small,
  ref,
  className,
  ...rest
}: InputHTMLAttributes<HTMLInputElement> & { label: string; hint?: ReactNode; small?: boolean; ref?: Ref<HTMLInputElement> }) {
  return (
    <label className="form-field block">
      <span className={cx(LABEL, small && "small text-xs opacity-85")}>{label}</span>
      <input ref={ref} className={cx(INPUT, small && SMALL, className)} {...rest} />
      {hint ? <span className={HINT}>{hint}</span> : null}
    </label>
  );
}

function Section({
  id,
  title,
  tinted,
  onOpen,
  children,
}: {
  id: string;
  title: string;
  tinted?: boolean;
  onOpen?: () => void;
  children: ReactNode;
}) {
  return (
    <details
      id={id}
      onToggle={(e) => {
        if (e.currentTarget.open) onOpen?.();
      }}
      className={cx(
        "export-advanced group rounded-[10px] border px-2.5 [&+details]:mt-2.5",
        tinted ? "border-accent/18 bg-linear-to-b from-accent/6 to-accent/3" : "border-white/8 bg-white/2",
      )}
    >
      <summary className="label flex cursor-pointer list-none items-center px-1 py-2.5 text-base font-semibold select-none [&::-webkit-details-marker]:hidden">
        <span aria-hidden="true" className="mr-2 inline-block opacity-90 transition-transform group-open:rotate-90">
          ▸
        </span>
        {title}
      </summary>
      <div className="pb-3">{children}</div>
    </details>
  );
}

function AttachmentTypesDialog({
  initial,
  onApply,
  onClose,
}: {
  initial: MediaTypes;
  onApply: (v: MediaTypes) => void;
  onClose: () => void;
}) {
  const [types, setTypes] = useState<MediaTypes>(initial);
  return (
    <GuildDialog
      title="Attachment types"
      placement="center"
      width="sm"
      initialFocus="#att-ui-images"
      onClose={onClose}
      testId="att-types"
      footer={
        <>
          <Button variant="ghost" onClick={onClose}>
            Cancel
          </Button>
          <Button
            variant="ghost"
            onClick={() => {
              onApply(types);
              onClose();
            }}
          >
            Apply
          </Button>
        </>
      }
    >
      <div className="grid grid-cols-2 gap-x-3.5 gap-y-2.5 min-[720px]:grid-cols-4">
        {MEDIA_LABELS.map(([key, label]) => (
          <Check
            key={key}
            id={`att-ui-${key}`}
            label={label}
            checked={types[key]}
            onChange={(v) => setTypes((t) => ({ ...t, [key]: v }))}
          />
        ))}
      </div>
    </GuildDialog>
  );
}

export function ExportDialog({ guild, onClose }: { guild: GuildSummary; onClose: () => void }) {
  const toast = useToast();
  const [form, setForm] = useState<ExportForm>(DEFAULT_EXPORT_FORM);
  const [attOpen, setAttOpen] = useState(false);
  const [busy, setBusy] = useState(false);
  const webhookRef = useRef<HTMLInputElement>(null);

  const set = <K extends keyof ExportForm>(key: K, value: ExportForm[K]) => setForm((f) => ({ ...f, [key]: value }));
  const text = (key: "channelId" | "userId" | "word" | "minLength" | "minReactions" | "webhookUrl" | "after" | "before") =>
    ({ value: form[key], onChange: (e: ChangeEvent<HTMLInputElement>) => set(key, e.target.value) });
  const check = (id: string, key: keyof ExportForm, label: string) => (
    <Check id={id} label={label} checked={form[key] as boolean} onChange={(v) => set(key, v)} />
  );

  const submit = async () => {
    setBusy(true);
    try {
      const res = await api.post<ExportMessagesResponse>("/api/export/messages", buildExportPayload(guild.id, form));
      if (res && res.ok === false) throw new Error(res.error || "");
      toast.show("Export started. You’ll see progress in logs.", { kind: "success" });
      onClose();
    } catch (e) {
      toast.show(exportError(e), { kind: "error" });
      setBusy(false);
    }
  };

  const summary = attachmentSummary(form);

  return (
    <GuildDialog
      title="Export messages"
      subtitle="Export messages to a JSON file and optionally forward via webhook."
      placement="center"
      onClose={onClose}
      testId="export"
      footer={
        <>
          <Button variant="ghost" onClick={onClose}>
            Cancel
          </Button>
          <Button variant="ghost" loading={busy} onClick={() => void submit()} data-ex-go="">
            Start export
          </Button>
        </>
      }
    >
      <div className="export-body-wrap grid gap-3.5">
        <div className={TWO_COL}>
          <TextInput
            id="ex-channel"
            label="Channel ID (optional)"
            type="text"
            inputMode="numeric"
            placeholder="e.g. 123456789012345678"
            {...text("channelId")}
          />
          <TextInput
            id="ex-user"
            label="Filter by User ID (optional)"
            type="text"
            inputMode="numeric"
            placeholder="e.g. 123456789012345678"
            {...text("userId")}
          />
        </div>

        <Section id="ex-filters" title="Filters" tinted>
          <div className="mt-2 grid grid-cols-2 gap-2">
            {check("ex-f-hascontent", "hasContent", "Include text content")}
            {check("ex-f-embeds", "embeds", "Include embeds")}
            <div className="flex min-h-8 flex-wrap items-center gap-x-2">
              {check("ex-f-attachments", "attachments", "Include attachments")}
              <Tooltip content="Choose attachment types (images, videos, audio, other)">
                <button
                  type="button"
                  id="ex-f-att-tip"
                  aria-label="Attachment types"
                  disabled={!form.attachments}
                  onClick={() => setAttOpen(true)}
                  className={
                    "ml-1.5 grid size-4 cursor-pointer place-items-center rounded border border-accent/28 bg-accent/14 " +
                    "text-[11px] leading-none font-semibold text-accent outline-none hover:border-accent/42 hover:bg-accent/20 " +
                    "focus-visible:shadow-[0_0_0_3px_var(--color-accent-a22)] disabled:cursor-not-allowed disabled:bg-accent/8 disabled:opacity-55"
                  }
                >
                  i
                </button>
              </Tooltip>
              <span id="ex-f-att-summary" aria-live="polite" className="text-xs text-muted">
                {summary}
              </span>
            </div>
            {check("ex-f-links", "links", "Include links")}
            {check("ex-f-emojis", "emojis", "Include emojis")}
            <div className={cx(TWO_COL, "items-center")}>
              {check("ex-f-word-on", "wordOn", "Includes word")}
              <input
                id="ex-f-word"
                type="text"
                aria-label="Word to include"
                placeholder="must have this word..."
                disabled={!form.wordOn}
                className={cx(INPUT, SMALL, "disabled:border-white/6 disabled:bg-white/3 disabled:text-white/65 disabled:opacity-50!")}
                {...text("word")}
              />
            </div>
            {check("ex-f-replies", "replies", "Include replies")}
            {check("ex-f-bots", "bots", "Include bot messages")}
            {check("ex-f-system", "system", "Include system messages")}
            {check("ex-f-pinned", "pinned", "Include pinned messages")}
            {check("ex-f-stickers", "stickers", "Include stickers")}
            {check("ex-f-mentions", "mentions", "Include mentions")}
            {check("ex-f-threads", "threads", "Include threads")}
            <div className={cx(TWO_COL, "ex-metrics")}>
              <TextInput id="ex-f-minlen" small className="max-w-[120px]" label="Min message length" type="number" min={0} step={1} {...text("minLength")} />
              <TextInput
                id="ex-f-minreacts"
                small
                className="max-w-[120px]"
                label="Min total reactions"
                type="number"
                min={0}
                step={1}
                {...text("minReactions")}
              />
            </div>
          </div>
        </Section>

        <Section
          id="ex-whwrap"
          title="Forward to a webhook (optional)"
          onOpen={() => setTimeout(() => webhookRef.current?.focus(), 0)}
        >
          <div className="mt-2">
            <TextInput
              ref={webhookRef}
              id="ex-webhook"
              label="Webhook URL"
              type="url"
              placeholder="https://discord.com/api/webhooks/..."
              hint="Messages will be forwarded to this Discord webhook."
              {...text("webhookUrl")}
            />
          </div>
        </Section>

        <Section id="ex-range" title="Date Range (optional)">
          <div className={cx(TWO_COL, "mt-2")}>
            <TextInput id="ex-after" className="pr-9! text-[13px]!" label="After" type="datetime-local" step={1} {...text("after")} />
            <TextInput id="ex-before" className="pr-9! text-[13px]!" label="Before" type="datetime-local" step={1} {...text("before")} />
          </div>
          <p className={HINT}>Times use your local timezone; they’re converted to UTC.</p>
        </Section>

        <Section id="ex-dlwrap" title="Download media (optional)">
          <div className="mt-2 grid grid-cols-2 gap-2">
            {MEDIA_LABELS.map(([key, label]) => (
              <Check
                key={key}
                id={`ex-dl-${key}`}
                label={label}
                checked={form.download[key]}
                onChange={(v) => set("download", { ...form.download, [key]: v })}
              />
            ))}
            <p className="text-xs text-text-2">
              Files are saved to{" "}
              <code className="font-mono text-text-1">/data/exports/&lt;guild&gt;/&lt;timestamp&gt;/media/&lt;type&gt;/</code>.
            </p>
          </div>
        </Section>
      </div>

      {attOpen ? (
        <AttachmentTypesDialog
          initial={form.attTypes}
          onApply={(v) => set("attTypes", v)}
          onClose={() => setAttOpen(false)}
        />
      ) : null}
    </GuildDialog>
  );
}
