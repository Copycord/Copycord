// Start form of Sync assets: which mapping, which kinds (roles, emojis, stickers; at least one), Delete
// extras (off by default) and a primary "Sync assets". Only with Delete extras on does it confirm first,
// since that deletes the clone server's assets the host does not have. The server bot does the work in
// the mapping's clone server; the host server is never touched.

import { useState } from "react";
import type { FormEvent } from "react";
import { Modal } from "../../../../components/Modal";
import { Button } from "../../../../components/ui/Button";
import { Checkbox } from "../../../../components/ui/Checkbox";
import { useConfirm } from "../../../../components/ui/ConfirmDialog";
import { Field, FieldSelect } from "../../../../components/ui/Field";
import { useApi } from "../../../../hooks/useApi";
import type { GuildMappingListResponse } from "../../../../types/channels";
import { useStartRun, type TaskFormProps } from "../start";

export type SyncKind = "roles" | "emojis" | "stickers";

export const SYNC_KINDS: SyncKind[] = ["roles", "emojis", "stickers"];
export const SYNC_KIND_LABELS: Record<SyncKind, string> = { roles: "Roles", emojis: "Emojis", stickers: "Stickers" };

export const SYNC_ASSETS_EXPLAINER =
  "Brings the clone server's roles, emojis and stickers in line with the host through the server bot: " +
  "creates what is missing, updates what changed and puts the roles in the host's order. The host server is never touched.";

export function SyncAssetsForm({ type, open, onClose }: TaskFormProps) {
  const mappings = useApi<GuildMappingListResponse>(open ? "/api/guild-mappings" : null);
  const list = mappings.data?.mappings ?? [];
  const mappingsPending = open && !mappings.data && !mappings.error;
  const [picked, setPicked] = useState<string | null>(null);
  const mappingId = picked ?? list[0]?.mapping_id ?? "";
  const [kinds, setKinds] = useState<SyncKind[]>(SYNC_KINDS);
  const [deleteExtras, setDeleteExtras] = useState(false);
  const [errors, setErrors] = useState<Record<string, string>>({});
  const { start, busy } = useStartRun(type);
  const confirm = useConfirm();

  const changed = (fn: () => void) => {
    fn();
    setErrors({});
  };
  const toggle = (k: SyncKind, on: boolean) =>
    changed(() => setKinds((prev) => SYNC_KINDS.filter((x) => (x === k ? on : prev.includes(x)))));

  const mapping = list.find((m) => m.mapping_id === mappingId);
  const where = mapping?.cloned_guild_name || mapping?.mapping_name || "the clone server";

  const onSubmit = async (e: FormEvent) => {
    e.preventDefault();
    const bad: Record<string, string> = {};
    if (!mappingId) bad.mapping_id = "Pick a mapping.";
    if (!kinds.length) bad.kinds = "Pick at least one kind of asset.";
    setErrors(bad);
    if (Object.keys(bad).length) return;
    if (deleteExtras) {
      const words = kinds.map((k) => SYNC_KIND_LABELS[k].toLowerCase());
      const named = words.length === 1 ? words[0] : `${words.slice(0, -1).join(", ")} and ${words[words.length - 1]}`;
      const ok = await confirm({
        title: `Delete extra ${named} in ${where}?`,
        body:
          `The sync also deletes the ${named} in ${where} that the host does not have. ` +
          "They go for good on Discord. This cannot be undone.",
        confirmLabel: "Sync and delete",
        cancelLabel: "Keep",
        confirmVariant: "ghost-red",
      });
      if (!ok) return;
    }
    const res = await start({ mapping_id: mappingId, kinds, delete_extras: deleteExtras });
    if (res && Object.keys(res).length) setErrors(res);
    else if (!res) onClose();
  };

  const footer = (
    <>
      <Button variant="ghost" onClick={onClose}>
        Cancel
      </Button>
      <Button variant="primary" type="submit" form="sync-assets-form" loading={busy}>
        Sync assets
      </Button>
    </>
  );

  return (
    <Modal open={open} onClose={onClose} title={`Run ${type.title}`} className="w-[min(560px,92vw)]" footer={footer}>
      <form id="sync-assets-form" noValidate onSubmit={(e) => void onSubmit(e)} className="flex flex-col gap-4">
        <p className="m-0 text-sm text-muted">{SYNC_ASSETS_EXPLAINER}</p>

        <Field label="Mapping" error={errors.mapping_id} id="sync-assets-mapping">
          <FieldSelect
            searchable
            value={mappingId}
            onChange={(v) => changed(() => setPicked(v))}
            pending={mappingsPending}
            pendingLabel="Main mapping"
            options={list.map((m) => ({
              value: m.mapping_id,
              label: m.mapping_name || `${m.original_guild_name ?? m.original_guild_id} to ${m.cloned_guild_name ?? m.cloned_guild_id}`,
            }))}
          />
        </Field>

        <div className="flex flex-col gap-1.5">
          <span id="sync-assets-kinds" className="ml-0.5 text-xs text-muted">
            Assets
          </span>
          <div role="group" aria-labelledby="sync-assets-kinds" className="flex flex-wrap gap-x-6 gap-y-2">
            {SYNC_KINDS.map((k) => (
              <label key={k} className="flex cursor-pointer items-center gap-2 text-sm text-text">
                <Checkbox checked={kinds.includes(k)} onChange={(e) => toggle(k, e.target.checked)} />
                {SYNC_KIND_LABELS[k]}
              </label>
            ))}
          </div>
          {errors.kinds ? (
            <p role="alert" className="m-0 text-xs text-danger">
              {errors.kinds}
            </p>
          ) : null}
        </div>

        <label className="flex cursor-pointer items-center gap-2 text-sm text-text">
          <Checkbox checked={deleteExtras} onChange={(e) => changed(() => setDeleteExtras(e.target.checked))} />
          Delete extras: remove what the clone has and the host does not
        </label>

        {errors.general || errors.inputs ? (
          <p role="alert" className="m-0 text-xs text-danger">
            {errors.general || errors.inputs}
          </p>
        ) : null}
      </form>
    </Modal>
  );
}
