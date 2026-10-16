// Queue card: pending and running guilds with remove, clear and cancel all,
// as renderQueue and its handlers in the old scraper.js. Hidden while the
// queue is empty. The list itself changes through queue_update messages.

import { useState } from "react";
import { Button, Card, cx } from "../../components/ui";
import type { ScraperQueueItem } from "../../types/scraper";
import type { Confirm, Notify } from "../system/providers";
import { ClockIcon, RefreshIcon, XIcon } from "./icons";
import { cancelAll, plural, queueClear, queueRemove, reason } from "./scraperApi";
import { ICON_BUTTON, SC_CARD, TITLE, TITLEBAR } from "./TokensCard";

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
      confirmVariant: "ghost-red",
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
    <Card id="sc-queue-card" aria-label="Queue" className={SC_CARD} hidden={queue.length === 0}>
      <div className={TITLEBAR}>
        <h3 className={TITLE}>Queue</h3>
        <div className="flex shrink-0 items-center gap-2">
          <span id="sc-queue-count" className="rounded-lg bg-primary-a12 px-2 py-0.5 text-xs text-muted">
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
            className="enabled:hover:border-danger enabled:hover:text-danger"
            disabled={busy !== null}
            onClick={onCancelAll}
          >
            Cancel All
          </Button>
        </div>
      </div>
      <div id="sc-queue-body" className="flex flex-col gap-1.5">
        <div id="sc-queue-empty" hidden={queue.length > 0} className="p-5 text-center text-[13px] text-muted">
          <p className="m-0">Queue is empty</p>
        </div>
        <div id="sc-queue-list" className="flex flex-col gap-1.5">
          {queue.map((item) => {
            const id = String(item.id);
            const isRunning = item.status === "running";
            return (
              <div
                key={id}
                data-id={id}
                className="sc-queue-item flex items-center justify-between gap-3 rounded-btn border border-border bg-control-a40 px-3 py-2.5 transition-colors duration-150 hover:border-primary-a25 hover:bg-control-a70"
              >
                <div className="flex min-w-0 items-center gap-2.5">
                  <span
                    className={cx(
                      "sc-queue-badge inline-flex size-7 shrink-0 items-center justify-center rounded-lg",
                      isRunning ? "running bg-info-a15 text-info-soft" : "pending bg-primary-a12 text-muted",
                    )}
                  >
                    {isRunning ? <RefreshIcon className="animate-spin" /> : <ClockIcon />}
                  </span>
                  <div className="flex min-w-0 flex-col gap-px">
                    <span title={String(item.guild_id)} className="sc-queue-guild truncate font-mono text-[13px] font-semibold text-text">{String(item.guild_id)}</span>
                    <span className="sc-queue-status text-[11px] text-muted">{isRunning ? "Scraping…" : "Pending"}</span>
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
