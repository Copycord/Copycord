// Start form of Purge messages: how old (N hours, days or weeks, or all records), which mapping, Dry run, Preview
// count and a Start that confirms with the count before anything is deleted. The records are
// Copycord's own message mappings, never Discord messages.

import { useState } from "react";
import type { FormEvent } from "react";
import { Modal } from "../../../../components/Modal";
import { Button } from "../../../../components/ui/Button";
import { Checkbox, Radio } from "../../../../components/ui/Checkbox";
import { useConfirm } from "../../../../components/ui/ConfirmDialog";
import { Field, FieldSelect, Input } from "../../../../components/ui/Field";
import { useApi } from "../../../../hooks/useApi";
import { api, errorText } from "../../../../lib/api";
import type { GuildMappingListResponse } from "../../../../types/channels";
import type { PurgePreviewResponse } from "../../../../types/tasks";
import { formatCount } from "../../StepList";
import { useStartRun, type TaskFormProps } from "../start";

export type PurgeUnit = "hours" | "days" | "weeks";

export const UNIT_OPTIONS: { value: PurgeUnit; label: string }[] = [
  { value: "hours", label: "hours" },
  { value: "days", label: "days" },
  { value: "weeks", label: "weeks" },
];

export const PURGE_EXPLAINER =
  "Deletes Copycord's stored message records: the links from each source message to its clone that " +
  "mirror edits and deletes and let a backfill skip messages already cloned. Messages on Discord are not touched.";

const ALL = "";

/** "Enter a whole number above 0." unless the text is a positive whole number. */
export function olderThanError(text: string): string | undefined {
  return /^\s*\d+\s*$/.test(text) && Number(text) > 0 ? undefined : "Enter a whole number above 0.";
}

/** "1,234 message records" with the singular for one. */
export function recordWords(n: number): string {
  return `${formatCount(n)} message record${n === 1 ? "" : "s"}`;
}

export function PurgeForm({ type, open, onClose }: TaskFormProps) {
  const mappings = useApi<GuildMappingListResponse>(open ? "/api/guild-mappings" : null);
  const list = mappings.data?.mappings ?? [];
  const mappingsPending = open && !mappings.data && !mappings.error;
  const [olderThan, setOlderThan] = useState("30");
  const [unit, setUnit] = useState<PurgeUnit>("days");
  // Every record whatever its age: the age fields are disabled and not sent.
  const [allRecords, setAllRecords] = useState(false);
  const [mappingId, setMappingId] = useState(ALL);
  const [dryRun, setDryRun] = useState(false);
  const [preview, setPreview] = useState<PurgePreviewResponse | null>(null);
  const [previewing, setPreviewing] = useState(false);
  const [errors, setErrors] = useState<Record<string, string>>({});
  const { start, busy } = useStartRun(type);
  const confirm = useConfirm();

  const changed = (fn: () => void) => {
    fn();
    setPreview(null);
    setErrors({});
  };

  const inputs = () =>
    allRecords
      ? { all_records: true, mapping_id: mappingId || null, dry_run: dryRun }
      : { older_than: Number(olderThan), unit, mapping_id: mappingId || null, dry_run: dryRun };

  const ageError = () => (allRecords ? undefined : olderThanError(olderThan));

  /** The count a run with these inputs would delete; null after an error (shown on its field). */
  const count = async (): Promise<PurgePreviewResponse | null> => {
    const bad = ageError();
    if (bad) {
      setErrors({ older_than: bad });
      return null;
    }
    setPreviewing(true);
    try {
      const qs = new URLSearchParams(allRecords ? { all_records: "true" } : { older_than: String(Number(olderThan)), unit });
      if (mappingId) qs.set("mapping_id", mappingId);
      const res = await api.get<PurgePreviewResponse>(`/api/tasks/types/${type.key}/options?${qs}`);
      if (res.errors && Object.keys(res.errors).length) {
        setErrors(res.errors);
        return null;
      }
      setPreview(res);
      return res;
    } catch (e) {
      setErrors({ general: errorText(e, "Could not count the records") });
      return null;
    } finally {
      setPreviewing(false);
    }
  };

  const onSubmit = async (e: FormEvent) => {
    e.preventDefault();
    const bad = ageError();
    if (bad) {
      setErrors({ older_than: bad });
      return;
    }
    if (!dryRun) {
      const p = preview ?? (await count());
      if (!p) return;
      const ok = await confirm({
        title: `Delete ${allRecords ? "all " : ""}${recordWords(p.matched)}?`,
        body: "Copycord forgets these links for good: edits and deletes of those messages are no longer mirrored, and a backfill with Ignore cloned may copy them again. Discord messages are not touched.",
        confirmLabel: "Delete",
        cancelLabel: "Keep",
        confirmVariant: "ghost-red",
      });
      if (!ok) return;
    }
    const res = await start(inputs());
    if (res && Object.keys(res).length) setErrors(res);
    else if (!res) onClose();
  };

  const footer = (
    <>
      <Button variant="ghost" onClick={onClose}>
        Cancel
      </Button>
      <Button variant="ghost" onClick={() => void count()} loading={previewing}>
        Preview count
      </Button>
      {dryRun ? (
        <Button variant="primary" type="submit" form="purge-form" loading={busy}>
          Start dry run
        </Button>
      ) : (
        <Button variant="danger-outline" type="submit" form="purge-form" loading={busy || previewing}>
          Delete records
        </Button>
      )}
    </>
  );

  const scopeName = mappingId ? (list.find((m) => m.mapping_id === mappingId)?.mapping_name || "this mapping") : "all mappings";

  return (
    <Modal open={open} onClose={onClose} title={`Run ${type.title}`} className="w-[min(560px,92vw)]" footer={footer}>
      <form id="purge-form" noValidate onSubmit={(e) => void onSubmit(e)} className="flex flex-col gap-4">
        <p className="m-0 text-sm text-muted">{PURGE_EXPLAINER}</p>

        <fieldset className="m-0 flex min-w-0 flex-col gap-2 border-0 p-0">
          <legend className="mb-1.5 ml-0.5 text-xs text-muted">Delete message records</legend>
          <div className="flex flex-wrap items-center gap-2">
            <label className="flex cursor-pointer items-center gap-2 text-sm text-text">
              <Radio name="purge-scope" checked={!allRecords} onChange={() => changed(() => setAllRecords(false))} />
              Older than
            </label>
            <div className="w-28">
              <Input
                type="number"
                min={1}
                step={1}
                aria-label="Older than"
                value={olderThan}
                disabled={allRecords}
                aria-invalid={errors.older_than ? true : undefined}
                onChange={(e) => changed(() => setOlderThan(e.target.value))}
              />
            </div>
            <div className="w-32">
              <label htmlFor="purge-unit" className="sr-only">
                Unit
              </label>
              <FieldSelect id="purge-unit" value={unit} disabled={allRecords} onChange={(v) => changed(() => setUnit(v as PurgeUnit))} options={UNIT_OPTIONS} />
            </div>
          </div>
          {errors.older_than || errors.unit ? (
            <p role="alert" className="m-0 text-xs text-danger">
              {errors.older_than || errors.unit}
            </p>
          ) : null}
          <label className="flex cursor-pointer items-center gap-2 text-sm text-text">
            <Radio name="purge-scope" checked={allRecords} onChange={() => changed(() => setAllRecords(true))} />
            All records
          </label>
        </fieldset>

        <Field label="Mapping" error={errors.mapping_id} id="purge-mapping">
          <FieldSelect
            value={mappingId}
            onChange={(v) => changed(() => setMappingId(v))}
            pending={mappingsPending}
            pendingLabel="All mappings"
            options={[
              { value: ALL, label: "All mappings" },
              ...list.map((m) => ({
                value: m.mapping_id,
                label: m.mapping_name || `${m.original_guild_name ?? m.original_guild_id} to ${m.cloned_guild_name ?? m.cloned_guild_id}`,
              })),
            ]}
          />
        </Field>

        <label className="flex cursor-pointer items-center gap-2 text-sm text-text">
          <Checkbox checked={dryRun} onChange={(e) => changed(() => setDryRun(e.target.checked))} />
          Dry run: count the records, delete nothing
        </label>

        {preview ? (
          <div role="status" className="rounded-md bg-surface-deep px-3 py-2 text-sm text-text">
            <p className="m-0">
              <span className="font-semibold tabular-nums">{recordWords(preview.matched)}</span>
              {allRecords ? "" : ` older than ${formatCount(Number(olderThan))} ${unit}`} in {scopeName}.
            </p>
            {preview.mappings.length > 1 ? (
              <ul className="m-0 mt-1 list-none p-0 text-xs text-muted">
                {preview.mappings.map((m) => (
                  <li key={m.mapping_id ?? "other"} className="flex justify-between gap-4">
                    <span className="truncate">{m.name}</span>
                    <span className="tabular-nums">{formatCount(m.matched)}</span>
                  </li>
                ))}
              </ul>
            ) : null}
          </div>
        ) : null}

        {errors.general || errors.inputs ? (
          <p role="alert" className="m-0 text-xs text-danger">
            {errors.general || errors.inputs}
          </p>
        ) : null}
      </form>
    </Modal>
  );
}
