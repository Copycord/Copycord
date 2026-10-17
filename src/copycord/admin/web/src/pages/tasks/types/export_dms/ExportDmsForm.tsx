// Start form of Export DMs: the user ID, Save a JSON file and an optional webhook URL, the inputs the
// /export_dms slash command took. Nothing is deleted, so Export DMs starts without a confirm.

import { useState } from "react";
import type { FormEvent } from "react";
import { Modal } from "../../../../components/Modal";
import { Button } from "../../../../components/ui/Button";
import { Checkbox } from "../../../../components/ui/Checkbox";
import { Field, Input } from "../../../../components/ui/Field";
import { useStartRun, type TaskFormProps } from "../start";

export const EXPORT_DMS_EXPLAINER =
  "Reads the DM history between the client account and this user, oldest first, then saves it as a JSON " +
  "file in the data folder, forwards it to a webhook, or both.";

export const JSON_PATH_HELP = "Saved as exports/dm/<user id>/<time>/messages.json in the data folder.";

/** "Enter a numeric Discord user ID." unless the text is all digits. */
export function userIdError(text: string): string | undefined {
  return /^\s*\d{5,}\s*$/.test(text) ? undefined : "Enter a numeric Discord user ID.";
}

/** An empty webhook is fine; anything else starts with https:// or http://. */
export function webhookError(text: string): string | undefined {
  const s = text.trim();
  return !s || /^https?:\/\/\S+$/.test(s) ? undefined : "Enter a webhook URL starting with https://";
}

export function destinationError(jsonFile: boolean, webhook: string): string | undefined {
  return jsonFile || webhook.trim() ? undefined : "Pick a destination: a JSON file, a webhook or both.";
}

/** The server's "Value error, Enter ..." in the form's words. */
function serverText(msg: string): string {
  return msg.replace(/^Value error, /, "");
}

export function ExportDmsForm({ type, open, onClose }: TaskFormProps) {
  const [userId, setUserId] = useState("");
  const [jsonFile, setJsonFile] = useState(true);
  const [webhook, setWebhook] = useState("");
  const [errors, setErrors] = useState<Record<string, string>>({});
  const { start, busy } = useStartRun(type);

  const changed = (fn: () => void) => {
    fn();
    setErrors({});
  };

  const onSubmit = async (e: FormEvent) => {
    e.preventDefault();
    const found: Record<string, string> = {};
    const badUser = userIdError(userId);
    const badHook = webhookError(webhook);
    const badDest = destinationError(jsonFile, webhook);
    if (badUser) found.user_id = badUser;
    if (badHook) found.webhook_url = badHook;
    if (badDest) found.inputs = badDest;
    setErrors(found);
    if (Object.keys(found).length) return;
    const res = await start({ user_id: userId.trim(), json_file: jsonFile, webhook_url: webhook.trim() || null });
    if (res && Object.keys(res).length) setErrors(Object.fromEntries(Object.entries(res).map(([k, v]) => [k, serverText(v)])));
    else if (!res) onClose();
  };

  const footer = (
    <>
      <Button variant="ghost" onClick={onClose}>
        Cancel
      </Button>
      <Button variant="primary" type="submit" form="export-dms-form" loading={busy}>
        Export DMs
      </Button>
    </>
  );

  return (
    <Modal open={open} onClose={onClose} title={`Run ${type.title}`} className="w-[min(560px,92vw)]" footer={footer}>
      <form id="export-dms-form" noValidate onSubmit={(e) => void onSubmit(e)} className="flex flex-col gap-4">
        <p className="m-0 text-sm text-muted">{EXPORT_DMS_EXPLAINER}</p>

        <Field label="User ID" help="The client account needs an open DM with this user." error={errors.user_id}>
          <Input
            inputMode="numeric"
            autoComplete="off"
            placeholder="123456789012345678"
            value={userId}
            onChange={(e) => changed(() => setUserId(e.target.value))}
          />
        </Field>

        <div className="flex flex-col gap-1">
          <label className="flex cursor-pointer items-center gap-2 text-sm text-text">
            <Checkbox checked={jsonFile} onChange={(e) => changed(() => setJsonFile(e.target.checked))} />
            Save a JSON file
          </label>
          <p className="m-0 ml-6 text-xs text-muted">{JSON_PATH_HELP}</p>
        </div>

        <Field label="Webhook URL" help="Optional: every message is also posted to this webhook." error={errors.webhook_url}>
          <Input
            type="url"
            autoComplete="off"
            placeholder="https://discord.com/api/webhooks/..."
            value={webhook}
            onChange={(e) => changed(() => setWebhook(e.target.value))}
          />
        </Field>

        {errors.inputs || errors.json_file || errors.general ? (
          <p role="alert" className="m-0 text-xs text-danger">
            {errors.inputs || errors.json_file || errors.general}
          </p>
        ) : null}
      </form>
    </Modal>
  );
}
