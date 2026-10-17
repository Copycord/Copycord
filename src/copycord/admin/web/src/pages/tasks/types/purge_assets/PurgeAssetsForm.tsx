// Start form of Purge assets: which mapping, which kinds (emojis, stickers, roles, each with the count a
// run would delete), which assets (only what Copycord cloned, only what it did not, or everything), Dry
// run, Preview count and a danger "Purge assets" that confirms with the count before anything is deleted.
// The server bot deletes in the mapping's clone server; the host server is never touched.

import { useEffect, useRef, useState } from "react";
import type { FormEvent } from "react";
import { Modal } from "../../../../components/Modal";
import { Button } from "../../../../components/ui/Button";
import { Checkbox } from "../../../../components/ui/Checkbox";
import { useConfirm } from "../../../../components/ui/ConfirmDialog";
import { Field, FieldSelect } from "../../../../components/ui/Field";
import { useApi } from "../../../../hooks/useApi";
import { api, errorText } from "../../../../lib/api";
import { formatCount } from "../../StepList";
import { useStartRun, type TaskFormProps } from "../start";

export type AssetKind = "emojis" | "stickers" | "roles";
export type AssetScope = "cloned" | "unmapped" | "all";

export const KINDS: AssetKind[] = ["emojis", "stickers", "roles"];
export const KIND_LABELS: Record<AssetKind, string> = { emojis: "Emojis", stickers: "Stickers", roles: "Roles" };
const KIND_ONE: Record<AssetKind, string> = { emojis: "emoji", stickers: "sticker", roles: "role" };

export const SCOPE_OPTIONS: { value: AssetScope; label: string }[] = [
  { value: "cloned", label: "Only what Copycord cloned" },
  { value: "unmapped", label: "Only what Copycord did not clone" },
  { value: "all", label: "Everything" },
];

export const PURGE_ASSETS_EXPLAINER =
  "Deletes emojis, stickers and roles in the mapping's clone server through the server bot, and forgets the " +
  "mappings of the cloned ones. The host server is never touched.";

/** GET /api/tasks/types/purge_assets/options: the mappings, and with a mapping the counts a run would delete. */
export interface PurgeAssetsOptions {
  mappings: { mapping_id: string; name: string; cloned_guild_id: string | null; cloned_guild_name?: string | null }[];
  counts: Partial<Record<AssetKind, number | null>>;
  live: boolean;
  notes?: Partial<Record<AssetKind, string>>;
  error?: string | null;
}

/** "1 emoji", "12 roles". */
export function assetWords(n: number, kind: AssetKind): string {
  return `${formatCount(n)} ${n === 1 ? KIND_ONE[kind] : kind}`;
}

/** "12 emojis, 1 sticker and 3 roles"; null while any picked kind has no count. */
export function countWords(kinds: AssetKind[], counts: PurgeAssetsOptions["counts"]): string | null {
  const parts: string[] = [];
  for (const k of kinds) {
    const n = counts[k];
    if (typeof n !== "number") return null;
    parts.push(assetWords(n, k));
  }
  if (!parts.length) return null;
  return parts.length === 1 ? parts[0]! : `${parts.slice(0, -1).join(", ")} and ${parts[parts.length - 1]}`;
}

/** The total over the picked kinds; null while any of them has no count. */
export function countTotal(kinds: AssetKind[], counts: PurgeAssetsOptions["counts"]): number | null {
  let total = 0;
  for (const k of kinds) {
    const n = counts[k];
    if (typeof n !== "number") return null;
    total += n;
  }
  return total;
}

export function PurgeAssetsForm({ type, open, onClose }: TaskFormProps) {
  const base = useApi<PurgeAssetsOptions>(open ? `/api/tasks/types/${type.key}/options` : null);
  const list = base.data?.mappings ?? [];
  const mappingsPending = open && !base.data && !base.error;
  const [picked, setPicked] = useState<string | null>(null);
  const mappingId = picked ?? list[0]?.mapping_id ?? "";
  const [kinds, setKinds] = useState<AssetKind[]>(KINDS);
  const [scope, setScope] = useState<AssetScope>("cloned");
  const [dryRun, setDryRun] = useState(false);
  const [preview, setPreview] = useState<PurgeAssetsOptions | null>(null);
  const [previewing, setPreviewing] = useState(false);
  const [shown, setShown] = useState(false);
  const [errors, setErrors] = useState<Record<string, string>>({});
  const { start, busy } = useStartRun(type);
  const confirm = useConfirm();
  const ask = useRef(0);

  /** The counts for this mapping and scope; the latest ask wins, an older answer is dropped. */
  const count = async (): Promise<PurgeAssetsOptions | null> => {
    if (!mappingId) {
      setErrors({ mapping_id: "Pick a mapping." });
      return null;
    }
    const n = ++ask.current;
    setPreviewing(true);
    try {
      const qs = new URLSearchParams({ mapping_id: mappingId, scope });
      const res = await api.get<PurgeAssetsOptions>(`/api/tasks/types/${type.key}/options?${qs}`);
      if (n !== ask.current) return null;
      setPreview(res);
      return res;
    } catch (e) {
      if (n === ask.current) setErrors({ general: errorText(e, "Could not count the assets") });
      return null;
    } finally {
      if (n === ask.current) setPreviewing(false);
    }
  };

  // The counts beside the kinds follow the mapping and scope.
  useEffect(() => {
    if (open && mappingId) void count();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open, mappingId, scope]);

  const changed = (fn: () => void) => {
    fn();
    setErrors({});
  };
  const pick = (fn: () => void) => {
    changed(fn);
    setPreview(null);
    setShown(false);
  };
  const toggle = (k: AssetKind, on: boolean) =>
    changed(() => setKinds((prev) => KINDS.filter((x) => (x === k ? on : prev.includes(x)))));

  const check = (): boolean => {
    const bad: Record<string, string> = {};
    if (!mappingId) bad.mapping_id = "Pick a mapping.";
    if (!kinds.length) bad.kinds = "Pick at least one kind of asset.";
    setErrors(bad);
    return !Object.keys(bad).length;
  };

  const mapping = list.find((m) => m.mapping_id === mappingId);
  const where = mapping?.cloned_guild_name || mapping?.name || "the clone server";

  const onSubmit = async (e: FormEvent) => {
    e.preventDefault();
    if (!check()) return;
    if (!dryRun) {
      const p = preview ?? (await count());
      if (!p) return;
      const total = countTotal(kinds, p.counts);
      if (total === 0) {
        setShown(true);
        setErrors({ general: "Nothing to delete with these choices." });
        return;
      }
      const words = countWords(kinds, p.counts);
      const ok = await confirm({
        title: total === null ? `Delete the selected assets in ${where}?` : `Delete ${formatCount(total)} asset${total === 1 ? "" : "s"} in ${where}?`,
        body:
          `${words ? `${words} go for good. ` : ""}The server bot deletes them on Discord and Copycord forgets the ` +
          "mappings of the cloned ones, so the next sync may clone them again. This cannot be undone.",
        confirmLabel: "Delete",
        cancelLabel: "Keep",
        confirmVariant: "ghost-red",
      });
      if (!ok) return;
    }
    const res = await start({ mapping_id: mappingId, kinds, scope, dry_run: dryRun });
    if (res && Object.keys(res).length) setErrors(res);
    else if (!res) onClose();
  };

  const footer = (
    <>
      <Button variant="ghost" onClick={onClose}>
        Cancel
      </Button>
      <Button
        variant="ghost"
        onClick={() => {
          if (!check()) return;
          setShown(true);
          void count();
        }}
        loading={previewing && shown}
      >
        Preview count
      </Button>
      {dryRun ? (
        <Button variant="primary" type="submit" form="purge-assets-form" loading={busy}>
          Start dry run
        </Button>
      ) : (
        <Button variant="danger-outline" type="submit" form="purge-assets-form" loading={busy}>
          Purge assets
        </Button>
      )}
    </>
  );

  const counts = preview?.counts ?? {};
  const words = preview ? countWords(kinds, counts) : null;
  const scopeWords = scope === "cloned" ? " cloned by Copycord" : scope === "unmapped" ? " not cloned by Copycord" : "";
  const notes = KINDS.filter((k) => kinds.includes(k) && preview?.notes?.[k]).map((k) => preview!.notes![k]!);

  return (
    <Modal open={open} onClose={onClose} title={`Run ${type.title}`} className="w-[min(560px,92vw)]" footer={footer}>
      <form id="purge-assets-form" noValidate onSubmit={(e) => void onSubmit(e)} className="flex flex-col gap-4">
        <p className="m-0 text-sm text-muted">{PURGE_ASSETS_EXPLAINER}</p>

        <Field label="Mapping" error={errors.mapping_id} id="purge-assets-mapping">
          <FieldSelect
            value={mappingId}
            onChange={(v) => pick(() => setPicked(v))}
            pending={mappingsPending}
            pendingLabel="Main mapping"
            options={list.map((m) => ({ value: m.mapping_id, label: m.name }))}
          />
        </Field>

        <div className="flex flex-col gap-1.5">
          <span id="purge-assets-kinds" className="ml-0.5 text-xs text-muted">
            Assets
          </span>
          <div role="group" aria-labelledby="purge-assets-kinds" className="flex flex-wrap gap-x-6 gap-y-2">
            {KINDS.map((k) => {
              const n = counts[k];
              return (
                <label key={k} className="flex cursor-pointer items-center gap-2 text-sm text-text">
                  <Checkbox checked={kinds.includes(k)} onChange={(e) => toggle(k, e.target.checked)} />
                  {KIND_LABELS[k]}
                  <span className="min-w-[2ch] text-xs tabular-nums text-muted" data-testid={`count-${k}`}>
                    {typeof n === "number" ? formatCount(n) : ""}
                  </span>
                </label>
              );
            })}
          </div>
          {errors.kinds ? (
            <p role="alert" className="m-0 text-xs text-danger">
              {errors.kinds}
            </p>
          ) : null}
        </div>

        <Field label="Which assets" error={errors.scope} id="purge-assets-scope">
          <FieldSelect value={scope} onChange={(v) => pick(() => setScope(v as AssetScope))} options={SCOPE_OPTIONS} />
        </Field>

        <label className="flex cursor-pointer items-center gap-2 text-sm text-text">
          <Checkbox checked={dryRun} onChange={(e) => changed(() => setDryRun(e.target.checked))} />
          Dry run: count the assets, delete nothing
        </label>

        {shown && preview ? (
          <div role="status" className="rounded-md bg-surface-deep px-3 py-2 text-sm text-text">
            <p className="m-0">
              {words ? (
                <>
                  <span className="font-semibold tabular-nums">{words}</span>
                  {scopeWords} in {where}.
                </>
              ) : (
                "Start the bots to count these assets."
              )}
            </p>
            {notes.length || preview.error ? (
              <ul className="m-0 mt-1 list-none p-0 text-xs text-muted">
                {preview.error ? <li>{preview.error}</li> : null}
                {notes.map((n) => (
                  <li key={n}>{n}</li>
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
