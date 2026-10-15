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
import { CHECK_BOX, CHECK_SPAN, DEFAULT_OPTS, INCLUDE_OPTS, OLD_BTN, SC_CARD, SC_CHECK, SC_LABEL, TITLE, TITLEBAR } from "./ScraperSkeleton";
import type { IncludeOpts } from "./ScraperSkeleton";

export { CHECK_BOX, CHECK_SPAN, DEFAULT_OPTS, SC_CHECK };
export type { IncludeOpts };

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
    <Card id="sc-config-card" aria-label="Configuration" className={cx(SC_CARD, "min-w-0")}>
      <div className={TITLEBAR}>
        <h3 className={TITLE}>Configuration</h3>
      </div>

      <div className="mt-3.5 flex flex-col gap-4">
        <div className="flex flex-col gap-1.5">
          <label htmlFor="sc-guild-id" className={SC_LABEL}>
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

        <fieldset className="m-0 rounded-control border border-border bg-[rgba(20,11,38,0.35)] px-4 py-3.5">
          <legend className={cx(SC_LABEL, "px-1.5")}>Include fields</legend>
          <div className="mt-2 grid grid-cols-2 gap-2.5">
            {INCLUDE_OPTS.map((o) => (
              <label key={o.key} className={SC_CHECK}>
                <input
                  id={o.id}
                  type="checkbox"
                  className={CHECK_BOX}
                  checked={opts[o.key]}
                  onChange={(e) => setOpts((p) => ({ ...p, [o.key]: e.target.checked }))}
                />
                <span>{o.label}</span>
              </label>
            ))}
          </div>
          <p className="m-0 mt-2.5 text-xs text-muted opacity-70">
            Enabling <b>Bio</b> requires extra API calls and will be slower.
          </p>
        </fieldset>

        {result ? (
          <div
            id="sc-validation-result"
            role="status"
            className={cx(
              "flex items-center gap-2 rounded-btn border px-3.5 py-2.5 text-[13px]",
              result.ok
                ? "border-[rgba(16,185,129,0.3)] bg-[rgba(16,185,129,0.1)] text-[#34d399]"
                : "border-[rgba(239,68,68,0.3)] bg-[rgba(239,68,68,0.1)] text-[#f87171]",
            )}
          >
            <span aria-hidden="true" className="text-base font-bold">
              {result.ok ? "✓" : "✗"}
            </span>
            <span>
              {result.text}
              {result.warn ? (
                <>
                  {" · "}
                  <span className="text-[#fbbf24]">{result.warn}</span>
                </>
              ) : null}
            </span>
          </div>
        ) : null}

        <div className="flex items-center gap-2.5 pt-1">
          <Button
            id="sc-validate-btn"
            variant="ghost"
            loading={busy === "validate"}
            onClick={() => void onValidate()}
          >
            {busy === "validate" ? "Validating…" : "Validate"}
          </Button>
          <div className="ml-auto flex gap-2.5">
            <Button
              id="sc-queue-btn"
              variant="outline"
              title="Add to queue"
              className={OLD_BTN}
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
              <Button
                id="sc-start-btn"
                variant="primary"
                className={OLD_BTN}
                disabled={busy === "start"}
                onClick={() => void onStart()}
              >
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
