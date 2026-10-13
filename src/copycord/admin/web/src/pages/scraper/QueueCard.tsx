// Queue card: pending and running guilds with remove, clear and cancel all,
// as renderQueue and its handlers in the old scraper.js. Hidden while the
// queue is empty. The list itself changes through queue_update messages.

import { useState } from "react";
import { Button, Card, cx } from "../../components/ui";
import type { ScraperQueueItem } from "../../types/scraper";
import type { Confirm, Notify } from "../system/providers";
import { ClockIcon, RefreshIcon, XIcon } from "./icons";
import { cancelAll, plural, queueClear, queueRemove, reason } from "./scraperApi";
import { ICON_BUTTON, TITLE } from "./TokensCard";

export interface QueueCardProps {
  queue: ScraperQueueItem[];
  notify: Notify;
  confirm: Confirm;
}

export function QueueCard({ queue, notify, confirm }: QueueCardProps) {
  const [removing, setRemoving] = useState<string | null>(null);
  const [busy, setBusy] = useState<"clear" | "cancel" | null>(null);

  async function onRemove(id: string) {
    setRemoving(id);
    try {
      const j = await queueRemove(id);
      if (!j?.ok) throw new Error(j?.error || "Failed to remove");
      notify("Removed from queue", "success");
    } catch (e) {
      notify(reason(e, "Failed to remove"), "error");
    } finally {
      setRemoving(null);
    }
  }

  async function onClear() {
    setBusy("clear");
    try {
      const j = await queueClear();
      if (!j?.ok) throw new Error(j?.error || "Failed to clear queue");
      notify("Queue cleared", "success");
    } catch (e) {
      notify(reason(e, "Failed to clear queue"), "error");
    } finally {
      setBusy(null);
    }
  }

  async function onCancelAll() {
    const ok = await confirm({
      title: "Cancel all?",
      body: "Cancel the current scrape and clear the entire queue?",
      confirmLabel: "Cancel All",
      cancelLabel: "Keep",
      danger: true,
    });
    if (!ok) return;
    setBusy("cancel");
    try {
      const j = await cancelAll();
      if (!j?.ok) throw new Error(j?.error || "Cancel failed");
      notify("All cancelled", "info");
    } catch (e) {
      notify(reason(e, "Cancel failed"), "error");
    } finally {
      setBusy(null);
    }
  }

  return (
    <Card id="sc-queue-card" aria-label="Queue" hidden={queue.length === 0}>
      <div className="mb-3 flex flex-wrap items-center justify-between gap-3">
        <h3 className={TITLE}>Queue</h3>
        <div className="flex items-center gap-2">
          <span id="sc-queue-count" className="text-[13px] text-text-1">
            {plural(queue.length, "item")}
          </span>
          <Button id="sc-queue-clear" size="sm" variant="ghost" title="Clear pending" disabled={busy !== null} onClick={onClear}>
            Clear
          </Button>
          <Button
            id="sc-cancel-all-btn"
            size="sm"
            variant="ghost"
            title="Cancel all"
            className="text-danger enabled:hover:border-danger"
            disabled={busy !== null}
            onClick={onCancelAll}
          >
            Cancel All
          </Button>
        </div>
      </div>
      <div id="sc-queue-body">
        <div id="sc-queue-empty" hidden={queue.length > 0} className="py-4 text-center text-sm text-text-2">
          <p className="m-0">Queue is empty</p>
        </div>
        <div id="sc-queue-list" className="flex flex-col gap-2">
          {queue.map((item) => {
            const id = String(item.id);
            const isRunning = item.status === "running";
            return (
              <div
                key={id}
                data-id={id}
                className="sc-queue-item flex items-center justify-between gap-3 rounded-control border border-border bg-white/3 px-3 py-2"
              >
                <div className="flex min-w-0 items-center gap-3">
                  <span
                    className={cx(
                      "sc-queue-badge inline-flex size-7 shrink-0 items-center justify-center rounded-full",
                      isRunning ? "running bg-good/15 text-good" : "pending bg-white/6 text-text-1",
                    )}
                  >
                    {isRunning ? <RefreshIcon className="animate-spin" /> : <ClockIcon />}
                  </span>
                  <div className="flex min-w-0 flex-col">
                    <span className="sc-queue-guild truncate font-mono text-sm">{String(item.guild_id)}</span>
                    <span className="sc-queue-status text-xs text-text-2">{isRunning ? "Scraping…" : "Pending"}</span>
                  </div>
                </div>
                {isRunning ? null : (
                  <button
                    type="button"
                    data-id={id}
                    title="Remove from queue"
                    aria-label={`Remove ${String(item.guild_id)} from queue`}
                    className={cx("sc-queue-remove", ICON_BUTTON, "enabled:hover:border-danger enabled:hover:text-danger")}
                    disabled={removing === id}
                    onClick={() => void onRemove(id)}
                  >
                    <XIcon />
                  </button>
                )}
              </div>
            );
          })}
        </div>
      </div>
    </Card>
  );
}
