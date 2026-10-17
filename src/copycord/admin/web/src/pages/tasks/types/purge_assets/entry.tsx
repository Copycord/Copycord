// Purge assets in the task type registry: its form, inputs rows, summary words and hero. Kept beside
// the form so the registry itself only names it.

import type { InputRow, InputsRun, TaskTypeEntry } from "..";
import { KINDS, KIND_LABELS, PurgeAssetsForm, SCOPE_OPTIONS, type AssetKind } from "./PurgeAssetsForm";

/** "Emojis, stickers and roles" in the order of the form. */
export function kindsWords(v: unknown): string {
  if (!Array.isArray(v) || !v.length) return "None";
  const names = KINDS.filter((k) => v.includes(k)).map((k, i) => (i === 0 ? KIND_LABELS[k] : KIND_LABELS[k].toLowerCase()));
  return names.length === 1 ? names[0]! : `${names.slice(0, -1).join(", ")} and ${names[names.length - 1]}`;
}

export function describePurgeAssets(inputs: Record<string, unknown>, run?: InputsRun): InputRow[] {
  const scope = SCOPE_OPTIONS.find((o) => o.value === inputs.scope)?.label ?? String(inputs.scope ?? "");
  return [
    { label: "Mapping", value: run?.context?.label || String(inputs.mapping_id ?? "") },
    { label: "Assets", value: kindsWords(inputs.kinds as AssetKind[] | undefined) },
    { label: "Which assets", value: scope },
    { label: "Dry run", value: inputs.dry_run ? "Yes, nothing deleted" : "No" },
  ];
}

const PurgeAssetsIcon = (
  <svg viewBox="0 0 24 24" className="size-4" fill="none" stroke="currentColor" strokeWidth="2" aria-hidden="true">
    <circle cx="9" cy="9" r="5" />
    <path d="M7 8h.01M11 8h.01M7 11c1 1 3 1 4 0" />
    <path d="m15 15 6 6M21 15l-6 6" />
  </svg>
);

export const PURGE_ASSETS: TaskTypeEntry = {
  icon: PurgeAssetsIcon,
  Form: PurgeAssetsForm,
  describeInputs: describePurgeAssets,
  summaryLabels: { matched: "Assets matched", deleted: "Assets deleted", failed: "Assets failed", skipped: "Assets skipped" },
  hero: { key: "deleted", label: "assets deleted" },
};
