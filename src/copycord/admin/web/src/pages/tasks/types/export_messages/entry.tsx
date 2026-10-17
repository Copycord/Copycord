// Export messages in the task type registry: its form, inputs rows, summary words, hero count and the
// run page's download card, the Export DMs card read from this type's file route.

import type { InputRow, InputsRun, TaskTypeEntry } from "..";
import type { TaskRun } from "../../../../types/tasks";
import { MEDIA_LABELS } from "../../../guilds/model";
import { ExportFileCard, maskWebhook } from "../export_dms/entry";
import { ExportMessagesForm } from "./ExportMessagesForm";

export const EXPORT_MESSAGES_BASE = "/api/tasks/export-messages";

export function ExportMessagesDownloadCard({ run }: { run: TaskRun }) {
  return <ExportFileCard run={run} base={EXPORT_MESSAGES_BASE} />;
}

function picked(v: unknown): string {
  const types = v && typeof v === "object" ? (v as Record<string, unknown>) : {};
  const on = MEDIA_LABELS.filter(([k]) => types[k]).map(([, label]) => label);
  return on.length ? on.join(", ") : "None";
}

function when(v: unknown): string {
  if (typeof v !== "string" || !v) return "Any";
  const d = new Date(v);
  return Number.isNaN(d.getTime()) ? v : d.toLocaleString();
}

export function describeExportMessages(inputs: Record<string, unknown>, run?: InputsRun): InputRow[] {
  const f = inputs.filters && typeof inputs.filters === "object" ? (inputs.filters as Record<string, unknown>) : {};
  const rows: InputRow[] = [
    { label: "Server", value: run?.context?.label || String(inputs.guild_id ?? "") },
    { label: "Channel ID", value: inputs.channel_id ? String(inputs.channel_id) : "All channels" },
    { label: "User ID", value: inputs.user_id ? String(inputs.user_id) : "All users" },
    { label: "After", value: when(inputs.after_iso) },
    { label: "Before", value: when(inputs.before_iso) },
    { label: "Webhook", value: maskWebhook(inputs.webhook_url) },
  ];
  if (f.word_on && f.word) rows.push({ label: "Includes word", value: String(f.word) });
  if (f.attachments === false) rows.push({ label: "Attachments", value: "Not included" });
  else if (f.att_types) rows.push({ label: "Attachment types", value: picked(f.att_types) });
  rows.push({ label: "Download media", value: picked(f.download_media) });
  return rows;
}

const ExportMessagesIcon = (
  <svg viewBox="0 0 24 24" className="size-4" fill="none" stroke="currentColor" strokeWidth="2" aria-hidden="true">
    <path d="M4 4h16v12H5.5L4 17.5z" />
    <path d="M8 8h8M8 12h5" />
    <path d="M17 18v4M15 20l2 2 2-2" />
  </svg>
);

export const EXPORT_MESSAGES_ENTRY: TaskTypeEntry = {
  icon: ExportMessagesIcon,
  Form: ExportMessagesForm,
  describeInputs: describeExportMessages,
  summaryLabels: {
    scanned: "Messages scanned",
    exported: "Messages exported",
    failed: "Messages failed",
    forwarded: "Messages forwarded",
    downloaded: "Files downloaded",
    download_errors: "Files failed",
  },
  hero: { key: "exported", label: "messages exported" },
  RunCard: ExportMessagesDownloadCard,
};
