// Export DMs in the task type registry: its form, inputs rows, summary words and hero count.

import type { InputRow, TaskTypeEntry } from "..";
import { ExportDmsForm } from "./ExportDmsForm";

/** The webhook without its token: "https://discord.com/api/webhooks/123/...". */
export function maskWebhook(url: unknown): string {
  if (typeof url !== "string" || !url) return "None";
  const cut = url.replace(/\/+$/, "").lastIndexOf("/");
  const head = cut > 0 ? url.slice(0, cut) : "";
  return (head.match(/\//g)?.length ?? 0) >= 3 ? `${head}/...` : url;
}

export function describeExportDms(inputs: Record<string, unknown>): InputRow[] {
  return [
    { label: "User ID", value: String(inputs.user_id ?? "") },
    { label: "JSON file", value: inputs.json_file === false ? "No" : "Yes" },
    { label: "Webhook", value: maskWebhook(inputs.webhook_url) },
  ];
}

const ExportDmsIcon = (
  <svg viewBox="0 0 24 24" className="size-4" fill="none" stroke="currentColor" strokeWidth="2" aria-hidden="true">
    <path d="M4 5h16v11H8l-4 4z" />
    <path d="M12 8v5M9.5 10.5 12 13l2.5-2.5" />
  </svg>
);

export const EXPORT_DMS_ENTRY: TaskTypeEntry = {
  icon: ExportDmsIcon,
  Form: ExportDmsForm,
  describeInputs: describeExportDms,
  summaryLabels: { exported: "Messages exported", forwarded: "Messages forwarded", failed: "Messages failed" },
  hero: { key: "exported", label: "messages exported" },
};
