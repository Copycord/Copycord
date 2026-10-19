// Start form of Export assets: one of the client account's servers (from the type's options, typed into
// the server box to filter by name or ID) and which assets to save, emojis and stickers by default. Nothing
// is deleted, so the run starts without a confirm.

import { useState } from "react";
import type { FormEvent } from "react";
import { Modal } from "../../../../components/Modal";
import { Button } from "../../../../components/ui/Button";
import { Field, FieldSelect } from "../../../../components/ui/Field";
import { useApi } from "../../../../hooks/useApi";
import { useStartRun, type TaskFormProps } from "../start";

export const EXPORT_ASSETS_EXPLAINER =
  "Saves the emojis and stickers of one of the client account's servers as a .tar.gz archive in the data " +
  "folder, ready to download from the run page.";

export type AssetChoice = "both" | "emojis" | "stickers";

export const ASSET_OPTIONS: { value: AssetChoice; label: string }[] = [
  { value: "both", label: "Emojis and stickers" },
  { value: "emojis", label: "Emojis" },
  { value: "stickers", label: "Stickers" },
];

export interface AssetGuild {
  id: string;
  name: string;
  icon?: string | null;
}

export interface ExportAssetsOptions {
  guilds: AssetGuild[];
  error?: string;
}

/** The server's "Value error, Pick ..." in the form's words. */
function serverText(msg: string): string {
  return msg.replace(/^Value error, /, "");
}

export function ExportAssetsForm({ type, open, onClose }: TaskFormProps) {
  const opts = useApi<ExportAssetsOptions>(open ? `/api/tasks/types/${type.key}/options` : null);
  const guilds = opts.data?.guilds ?? [];
  const pending = open && !opts.data && !opts.error;
  const loadError = opts.data?.error || (opts.error ? "Could not load the servers" : undefined);
  const [guildId, setGuildId] = useState("");
  const [asset, setAsset] = useState<AssetChoice>("both");
  const [errors, setErrors] = useState<Record<string, string>>({});
  const { start, busy } = useStartRun(type);

  const changed = (fn: () => void) => {
    fn();
    setErrors({});
  };

  const onSubmit = async (e: FormEvent) => {
    e.preventDefault();
    if (!guildId) {
      setErrors({ guild_id: "Pick a server." });
      return;
    }
    setErrors({});
    const res = await start({ guild_id: guildId, asset });
    if (res && Object.keys(res).length) setErrors(Object.fromEntries(Object.entries(res).map(([k, v]) => [k, serverText(v)])));
    else if (!res) onClose();
  };

  const footer = (
    <>
      <Button variant="ghost" onClick={onClose}>
        Cancel
      </Button>
      <Button variant="primary" type="submit" form="export-assets-form" loading={busy}>
        Export assets
      </Button>
    </>
  );

  return (
    <Modal open={open} onClose={onClose} title={`Run ${type.title}`} className="w-[min(560px,92vw)]" footer={footer}>
      <form id="export-assets-form" noValidate onSubmit={(e) => void onSubmit(e)} className="flex flex-col gap-4">
        <p className="m-0 text-sm text-muted">{EXPORT_ASSETS_EXPLAINER}</p>

        <Field label="Server" help={loadError ? undefined : "Servers the client account is in."} error={errors.guild_id || loadError}>
          <FieldSelect
            searchable
            value={guildId}
            onChange={(v) => changed(() => setGuildId(v))}
            pending={pending}
            pendingLabel="Pick a server"
            options={[
              { value: "", label: "Pick a server", disabled: true },
              ...guilds.map((g) => ({ value: g.id, label: g.name })),
            ]}
          />
        </Field>

        <Field label="Assets" error={errors.asset}>
          <FieldSelect value={asset} onChange={(v) => changed(() => setAsset(v as AssetChoice))} options={ASSET_OPTIONS} />
        </Field>

        {errors.inputs || errors.general ? (
          <p role="alert" className="m-0 text-xs text-danger">
            {errors.inputs || errors.general}
          </p>
        ) : null}
      </form>
    </Modal>
  );
}
