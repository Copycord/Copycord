// Export assets in the task type registry: its form, inputs rows, summary words, hero count and the
// run page's download card, the Export DMs card read from this type's file route ("Download archive").

import type { InputRow, InputsRun, TaskTypeEntry } from "..";
import type { TaskRun } from "../../../../types/tasks";
import { formatCount } from "../../StepList";
import { sizeWords } from "../download_avatars/entry";
import { ExportFileCard, type ExportFile } from "../export_dms/entry";
import { ASSET_OPTIONS, ExportAssetsForm } from "./ExportAssetsForm";

export const EXPORT_ASSETS_BASE = "/api/tasks/export-assets";

/** The saved archive's path from the run report, or null when the run saved none. */
export function archivePathOf(run: Pick<TaskRun, "report">): string | null {
  const report = run.report && typeof run.report === "object" ? (run.report as { output?: unknown }) : null;
  const output = report?.output && typeof report.output === "object" ? (report.output as { archive_path?: unknown }) : null;
  return typeof output?.archive_path === "string" && output.archive_path ? output.archive_path : null;
}

/** "12 emojis, 1 sticker, 2.0 MB": the counts of the kinds exported and the size, each left out when not known. */
export function archiveFacts(file: ExportFile & { emojis?: number | null; stickers?: number | null }): string[] {
  return [
    file.emojis != null ? `${formatCount(file.emojis)} ${file.emojis === 1 ? "emoji" : "emojis"}` : null,
    file.stickers != null ? `${formatCount(file.stickers)} ${file.stickers === 1 ? "sticker" : "stickers"}` : null,
    file.size != null ? sizeWords(file.size) : null,
  ].filter((f): f is string => Boolean(f));
}

export function ExportAssetsDownloadCard({ run }: { run: TaskRun }) {
  return (
    <ExportFileCard
      run={run}
      base={EXPORT_ASSETS_BASE}
      pathOf={archivePathOf}
      noun="Archive"
      button="Download archive"
      facts={archiveFacts}
    />
  );
}

export function describeExportAssets(inputs: Record<string, unknown>, run?: InputsRun): InputRow[] {
  const asset = ASSET_OPTIONS.find((o) => o.value === inputs.asset);
  return [
    { label: "Server", value: run?.context?.label || String(inputs.guild_id ?? "") },
    { label: "Assets", value: asset?.label ?? String(inputs.asset ?? "") },
  ];
}

const ExportAssetsIcon = (
  <svg viewBox="0 0 24 24" className="size-4" fill="none" stroke="currentColor" strokeWidth="2" aria-hidden="true">
    <circle cx="9" cy="9" r="5" />
    <path d="M7.5 10.5c.8.8 2.2.8 3 0" />
    <path d="M17 13v8M14 18l3 3 3-3" />
  </svg>
);

export const EXPORT_ASSETS_ENTRY: TaskTypeEntry = {
  icon: ExportAssetsIcon,
  Form: ExportAssetsForm,
  describeInputs: describeExportAssets,
  summaryLabels: {
    emojis_saved: "Emojis saved",
    emojis_total: "Emojis found",
    stickers_saved: "Stickers saved",
    stickers_total: "Stickers found",
  },
  hero: { key: "emojis_saved", label: "emojis saved" },
  RunCard: ExportAssetsDownloadCard,
};
