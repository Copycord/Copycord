// Start form of Download avatars: one of the client account's servers (from the type's options,
// typed into the server box to filter by name or ID), how long the link lives and how many avatars download at once. Nothing is deleted, so the
// run starts without a confirm.

import { useState } from "react";
import type { FormEvent } from "react";
import { Modal } from "../../../../components/Modal";
import { Button } from "../../../../components/ui/Button";
import { Field, FieldSelect, Input } from "../../../../components/ui/Field";
import { useApi } from "../../../../hooks/useApi";
import { useStartRun, type TaskFormProps } from "../start";

export const AVATARS_EXPLAINER =
  "Lists the members the client account can see in one of its servers, downloads each profile picture " +
  "and zips them behind a download link that expires.";

export interface AvatarGuild {
  id: string;
  name: string;
}

export interface AvatarOptions {
  guilds: AvatarGuild[];
  error?: string;
}

/** "Enter a whole number of hours from 1 to 168." unless the text is one. */
export function hoursError(text: string): string | undefined {
  const n = Number(text);
  return /^\s*\d+\s*$/.test(text) && n >= 1 && n <= 168 ? undefined : "Enter a whole number of hours from 1 to 168.";
}

export const CONCURRENCY_DEFAULT = 8;
export const CONCURRENCY_MAX = 32;

/** "Enter a whole number of parallel downloads from 1 to 32." unless the text is one. */
export function concurrencyError(text: string): string | undefined {
  const n = Number(text);
  return /^\s*\d+\s*$/.test(text) && n >= 1 && n <= CONCURRENCY_MAX
    ? undefined
    : `Enter a whole number of parallel downloads from 1 to ${CONCURRENCY_MAX}.`;
}

/** The server's "Value error, Pick ..." in the form's words. */
function serverText(msg: string): string {
  return msg.replace(/^Value error, /, "");
}

export function AvatarsForm({ type, open, onClose }: TaskFormProps) {
  const opts = useApi<AvatarOptions>(open ? `/api/tasks/types/${type.key}/options` : null);
  const guilds = opts.data?.guilds ?? [];
  const pending = open && !opts.data && !opts.error;
  const loadError = opts.data?.error || (opts.error ? "Could not load the servers" : undefined);
  const [guildId, setGuildId] = useState("");
  const [hours, setHours] = useState("24");
  const [parallel, setParallel] = useState(String(CONCURRENCY_DEFAULT));
  const [errors, setErrors] = useState<Record<string, string>>({});
  const { start, busy } = useStartRun(type);

  const changed = (fn: () => void) => {
    fn();
    setErrors({});
  };

  const chosen = guilds.find((g) => g.id === guildId);

  const onSubmit = async (e: FormEvent) => {
    e.preventDefault();
    const found: Record<string, string> = {};
    if (!guildId) found.guild_id = "Pick a server.";
    const badHours = hoursError(hours);
    if (badHours) found.expires_hours = badHours;
    const badParallel = concurrencyError(parallel);
    if (badParallel) found.concurrency = badParallel;
    setErrors(found);
    if (Object.keys(found).length) return;
    const res = await start({
      guild_id: guildId,
      guild_name: chosen?.name ?? null,
      expires_hours: Number(hours),
      concurrency: Number(parallel),
    });
    if (res && Object.keys(res).length) setErrors(Object.fromEntries(Object.entries(res).map(([k, v]) => [k, serverText(v)])));
    else if (!res) onClose();
  };

  const footer = (
    <>
      <Button variant="ghost" onClick={onClose}>
        Cancel
      </Button>
      <Button variant="primary" type="submit" form="avatars-form" loading={busy}>
        Download avatars
      </Button>
    </>
  );

  return (
    <Modal open={open} onClose={onClose} title={`Run ${type.title}`} className="w-[min(560px,92vw)]" footer={footer}>
      <form id="avatars-form" noValidate onSubmit={(e) => void onSubmit(e)} className="flex flex-col gap-4">
        <p className="m-0 text-sm text-muted">{AVATARS_EXPLAINER}</p>

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

        <Field label="Link expires after (hours)" help="From 1 hour to 7 days (168 hours)." error={errors.expires_hours}>
          <div className="w-28">
            <Input type="number" min={1} max={168} step={1} value={hours} onChange={(e) => changed(() => setHours(e.target.value))} />
          </div>
        </Field>

        <Field label="Parallel downloads" help="Higher is faster; lower it if Discord rate limits the run." error={errors.concurrency}>
          <div className="w-28">
            <Input
              type="number"
              min={1}
              max={CONCURRENCY_MAX}
              step={1}
              value={parallel}
              onChange={(e) => changed(() => setParallel(e.target.value))}
            />
          </div>
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
