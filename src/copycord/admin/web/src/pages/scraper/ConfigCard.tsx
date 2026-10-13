// Configuration card: guild ID, include fields, Validate, Add to Queue,
// Start Scrape and Cancel. The proxy list is read fresh for each start or
// queue add, as the old page did.

import { useEffect, useState } from "react";
import { Button, Card, cx, Input } from "../../components/ui";
import type { ScraperPayload } from "../../types/scraper";
import type { Notify } from "../system/providers";
import { PlayIcon, QueueIcon } from "./icons";
import {
  cancelScrape,
  getProxies,
  plural,
  queueAdd,
  queueStart,
  reason,
  startScrape,
  validateSetup,
} from "./scraperApi";
import { TITLE } from "./TokensCard";

export interface IncludeOpts {
  username: boolean;
  avatar: boolean;
  roles: boolean;
  bio: boolean;
}

export const DEFAULT_OPTS: IncludeOpts = { username: true, avatar: true, roles: false, bio: false };

const OPTS: Array<{ key: keyof IncludeOpts; id: string; label: string }> = [
  { key: "username", id: "sc-opt-username", label: "Username" },
  { key: "avatar", id: "sc-opt-avatar", label: "Avatar URL" },
  { key: "roles", id: "sc-opt-roles", label: "Roles" },
  { key: "bio", id: "sc-opt-bio", label: "Bio" },
];

/** How long the validation result stays up, as on the old page. */
export const RESULT_MS = 5000;

export function buildPayload(guildId: string, opts: IncludeOpts, proxies: string[]): ScraperPayload {
  return {
    guild_id: guildId,
    include_username: opts.username,
    include_avatar_url: opts.avatar,
    include_bio: opts.bio,
    include_roles: opts.roles,
    proxies: proxies.length > 0 ? proxies : undefined,
  };
}

interface Result {
  ok: boolean;
  text: string;
  warn?: string;
}

export interface ConfigCardProps {
  tokenCount: number;
  queueLength: number;
  running: boolean;
  onStarted: () => void;
  onCancelled: () => void;
  notify: Notify;
}

export function ConfigCard({ tokenCount, queueLength, running, onStarted, onCancelled, notify }: ConfigCardProps) {
  const [guildId, setGuildId] = useState("");
  const [opts, setOpts] = useState<IncludeOpts>(DEFAULT_OPTS);
  const [result, setResult] = useState<Result | null>(null);
  const [busy, setBusy] = useState<"validate" | "queue" | "start" | "cancel" | null>(null);

  useEffect(() => {
    if (!result) return;
    const t = setTimeout(() => setResult(null), RESULT_MS);
    return () => clearTimeout(t);
  }, [result]);

  async function onValidate() {
    const gid = guildId.trim();
    if (!gid) {
      notify("Enter a Guild ID", "info");
      return;
    }
    setBusy("validate");
    try {
      const j = await validateSetup(gid);
      if (!j?.ok) throw new Error(j?.error || "Validation failed");
      const missing = j.tokens_missing ?? 0;
      setResult({
        ok: true,
        text: `${plural(j.tokens_in_guild ?? 0, "token")} have access`,
        warn: missing ? `${missing} missing access` : undefined,
      });
    } catch (e) {
      setResult({ ok: false, text: reason(e, "Validation failed") });
    } finally {
      setBusy(null);
    }
  }

  async function onQueue() {
    const gid = guildId.trim();
    if (!gid) return notify("Enter a Guild ID", "info");
    if (!tokenCount) return notify("Add tokens first", "info");
    setBusy("queue");
    try {
      const j = await queueAdd(buildPayload(gid, opts, await getProxies()));
      if (!j?.ok) throw new Error(j?.error || "Failed to add to queue");
      notify(`Guild ${gid} added to queue (position ${j.queue_position ?? "?"})`, "success");
      setGuildId("");
    } catch (e) {
      notify(reason(e, "Failed to add to queue"), "error");
    } finally {
      setBusy(null);
    }
  }

  async function onStart() {
    const gid = guildId.trim();
    if (!gid && queueLength > 0) {
      setBusy("start");
      try {
        const j = await queueStart();
        if (!j?.ok) throw new Error(j?.error || "Failed to start queue");
        notify("Queue started", "success");
        onStarted();
      } catch (e) {
        notify(reason(e, "Failed to start queue"), "error");
      } finally {
        setBusy(null);
      }
      return;
    }
    if (!gid) return notify("Enter a Guild ID or add guilds to the queue", "info");
    if (!tokenCount) return notify("Add tokens first", "info");
    setBusy("start");
    try {
      const j = await startScrape(buildPayload(gid, opts, await getProxies()));
      if (!j?.ok) throw new Error(j?.error || "Failed to start");
      const used = j.tokens_used ?? 0;
      notify(
        (j.queue_position ?? 0) > 1
          ? `Queued, position ${j.queue_position} (${used} token(s))`
          : `Scraper started, ${used} token(s)`,
        "success",
      );
      setGuildId("");
      onStarted();
    } catch (e) {
      notify(reason(e, "Failed to start"), "error");
    } finally {
      setBusy(null);
    }
  }

  async function onCancel() {
    setBusy("cancel");
    try {
      const j = await cancelScrape();
      if (!j?.ok) throw new Error(j?.error || "Cancel failed");
      notify("Cancel requested", "info");
      onCancelled();
    } catch (e) {
      notify(reason(e, "Cancel failed"), "error");
    } finally {
      setBusy(null);
    }
  }

  return (
    <Card id="sc-config-card" aria-label="Configuration" className="flex min-w-0 flex-col">
      <div className="mb-3 flex items-center justify-between gap-3">
        <h3 className={TITLE}>Configuration</h3>
      </div>

      <div className="flex flex-col gap-3.5">
        <div className="flex flex-col gap-1.5">
          <label htmlFor="sc-guild-id" className="text-xs font-semibold tracking-wider text-text-1 uppercase">
            Server (Guild) ID
          </label>
          <Input
            id="sc-guild-id"
            placeholder="e.g. 123456789012345678"
            autoComplete="off"
            inputMode="numeric"
            value={guildId}
            onChange={(e) => setGuildId(e.target.value)}
          />
        </div>

        <fieldset className="m-0 rounded-control border border-border px-4 pt-1 pb-3">
          <legend className="px-1.5 text-xs font-semibold tracking-wider text-text-1 uppercase">Include fields</legend>
          <div className="grid grid-cols-2 gap-x-4 gap-y-2 py-2">
            {OPTS.map((o) => (
              <label key={o.key} className="inline-flex cursor-pointer items-center gap-2 text-sm">
                <input
                  id={o.id}
                  type="checkbox"
                  className="size-4 cursor-pointer accent-primary"
                  checked={opts[o.key]}
                  onChange={(e) => setOpts((p) => ({ ...p, [o.key]: e.target.checked }))}
                />
                <span>{o.label}</span>
              </label>
            ))}
          </div>
          <p className="m-0 text-xs text-text-2">
            Enabling <b className="text-text">Bio</b> requires extra API calls and will be slower.
          </p>
        </fieldset>

        {result ? (
          <div
            id="sc-validation-result"
            role="status"
            className={cx(
              "rounded-control border px-3 py-2 text-sm",
              result.ok ? "border-good/40 bg-good/10 text-good" : "border-danger/40 bg-danger/10 text-danger",
            )}
          >
            <span aria-hidden="true" className="mr-1.5 font-bold">
              {result.ok ? "✓" : "✗"}
            </span>
            {result.text}
            {result.warn ? (
              <>
                {" · "}
                <span className="text-custom-ink">{result.warn}</span>
              </>
            ) : null}
          </div>
        ) : null}

        <div className="flex flex-wrap items-center justify-between gap-2.5">
          <Button
            id="sc-validate-btn"
            variant="ghost"
            loading={busy === "validate"}
            onClick={() => void onValidate()}
          >
            {busy === "validate" ? "Validating…" : "Validate"}
          </Button>
          <div className="flex flex-wrap gap-2.5">
            <Button
              id="sc-queue-btn"
              variant="outline"
              title="Add to queue"
              disabled={busy === "queue"}
              onClick={() => void onQueue()}
            >
              <QueueIcon />
              Add to Queue
            </Button>
            {running ? (
              <Button id="sc-cancel-btn" variant="danger" disabled={busy === "cancel"} onClick={() => void onCancel()}>
                Cancel
              </Button>
            ) : (
              <Button id="sc-start-btn" variant="primary" disabled={busy === "start"} onClick={() => void onStart()}>
                <PlayIcon />
                Start Scrape
              </Button>
            )}
          </div>
        </div>
      </div>
    </Card>
  );
}
