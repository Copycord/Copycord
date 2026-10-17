// Sync assets in the task type registry: its form, inputs rows, summary words and hero. Kept beside
// the form so the registry itself only names it.

import type { InputRow, InputsRun, TaskTypeEntry } from "..";
import { SYNC_KINDS, SYNC_KIND_LABELS, SyncAssetsForm } from "./SyncAssetsForm";

/** "Roles, emojis and stickers" in the order of the form. */
export function syncKindsWords(v: unknown): string {
  if (!Array.isArray(v) || !v.length) return "None";
  const names = SYNC_KINDS.filter((k) => v.includes(k)).map((k, i) =>
    i === 0 ? SYNC_KIND_LABELS[k] : SYNC_KIND_LABELS[k].toLowerCase(),
  );
  return names.length === 1 ? names[0]! : `${names.slice(0, -1).join(", ")} and ${names[names.length - 1]}`;
}

export function describeSyncAssets(inputs: Record<string, unknown>, run?: InputsRun): InputRow[] {
  return [
    { label: "Mapping", value: run?.context?.label || String(inputs.mapping_id ?? "") },
    { label: "Assets", value: syncKindsWords(inputs.kinds) },
    { label: "Delete extras", value: inputs.delete_extras ? "Yes" : "No" },
  ];
}

const SyncAssetsIcon = (
  <svg viewBox="0 0 24 24" className="size-4" fill="none" stroke="currentColor" strokeWidth="2" aria-hidden="true">
    <path d="M4 12a8 8 0 0 1 13.7-5.6L20 9" />
    <path d="M20 4v5h-5" />
    <path d="M20 12a8 8 0 0 1-13.7 5.6L4 15" />
    <path d="M4 20v-5h5" />
  </svg>
);

export const SYNC_ASSETS: TaskTypeEntry = {
  icon: SyncAssetsIcon,
  Form: SyncAssetsForm,
  describeInputs: describeSyncAssets,
  summaryLabels: {
    created: "Assets created",
    updated: "Assets updated",
    deleted: "Assets deleted",
    reordered: "Roles reordered",
    skipped: "Assets skipped",
    failed: "Assets failed",
  },
  hero: { key: "created", label: "assets created" },
};
