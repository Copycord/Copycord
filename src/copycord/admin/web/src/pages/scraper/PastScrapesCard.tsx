// Past scrapes card: saved scrape files from GET /api/scraper/scrapes with
// download and a confirmed delete, as loadScrapes and its handlers in the old
// scraper.js. The page owns the list request so a finished run can reload it.

import { useState } from "react";
import { Card, cx } from "../../components/ui";
import type { ScraperScrape, ScraperScrapesResponse } from "../../types/scraper";
import type { Confirm, Notify } from "../system/providers";
import { DownloadIcon, RefreshIcon, TrashIcon, UsersIcon } from "./icons";
import { deleteScrape, reason, scrapeUrl } from "./scraperApi";
import { ICON_BUTTON, TITLE } from "./TokensCard";

/** "Oct 13, 2026 09:41" in the browser's locale, as fmtDate. */
export function fmtDate(iso: string): string {
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso;
  return (
    d.toLocaleDateString(undefined, { year: "numeric", month: "short", day: "numeric" }) +
    " " +
    d.toLocaleTimeString(undefined, { hour: "2-digit", minute: "2-digit" })
  );
}

export function fmtBytes(b: number): string {
  if (b < 1024) return `${b} B`;
  if (b < 1024 * 1024) return `${(b / 1024).toFixed(1)} KB`;
  return `${(b / (1024 * 1024)).toFixed(1)} MB`;
}

export interface PastScrapesCardProps {
  data: ScraperScrapesResponse | undefined;
  reload: () => void;
  notify: Notify;
  confirm: Confirm;
}

export function PastScrapesCard({ data, reload, notify, confirm }: PastScrapesCardProps) {
  const [deleting, setDeleting] = useState<string | null>(null);
  const all = data?.ok ? (data.scrapes ?? []) : [];
  const list = all.filter((s) => !s.error);

  async function onDelete(s: ScraperScrape) {
    const path = s.path || s.filename;
    const ok = await confirm({
      title: "Delete scrape?",
      body: `Delete scrape file "${path}"?`,
      confirmLabel: "Delete",
      danger: true,
    });
    if (!ok) return;
    setDeleting(path);
    try {
      const j = await deleteScrape(path);
      if (!j?.ok) throw new Error(j?.error || "Delete failed");
      notify("Scrape deleted", "success");
      reload();
    } catch (e) {
      notify(reason(e, "Delete failed"), "error");
    } finally {
      setDeleting(null);
    }
  }

  return (
    <Card id="sc-scrapes-card" aria-label="Past Scrapes">
      <div className="mb-3 flex items-center justify-between gap-3">
        <h3 className={TITLE}>Past Scrapes</h3>
        <button id="sc-scrapes-refresh" type="button" title="Refresh" aria-label="Refresh past scrapes" className={ICON_BUTTON} onClick={reload}>
          <RefreshIcon />
        </button>
      </div>
      <div id="sc-scrapes-body">
        <div
          id="sc-scrapes-empty"
          hidden={all.length > 0}
          className="flex flex-col items-center gap-1 rounded-panel border border-dashed border-border px-6 py-8 text-center"
        >
          <p className="m-0 font-semibold text-text-1">No saved scrapes yet</p>
          <small className="text-text-2">Completed scrapes are automatically saved here</small>
        </div>
        <div id="sc-scrapes-list" className="flex flex-col gap-2">
          {list.map((s) => {
            const path = s.path || s.filename;
            const name = s.guild_name || (s.guild_id != null ? String(s.guild_id) : "") || "Unknown";
            const method = s.metadata?.method || "gateway";
            return (
              <div
                key={path}
                data-path={path}
                className="sc-scrape-item flex items-center justify-between gap-3 rounded-control border border-border bg-white/3 px-3 py-2"
              >
                <div className="flex min-w-0 flex-col gap-0.5">
                  <span
                    className="sc-scrape-guild flex min-w-0 items-center gap-1.5 font-semibold"
                    title={`Guild ID: ${s.guild_id != null ? String(s.guild_id) : "?"}`}
                  >
                    <UsersIcon className="shrink-0 text-accent" />
                    <span className="truncate">{name}</span>
                  </span>
                  <span className="sc-scrape-meta text-xs text-text-1">
                    {`${s.total_count ?? 0} members · ${method} · ${fmtBytes(s.file_size || 0)}`}
                  </span>
                  <span className="sc-scrape-date text-xs text-text-2">
                    {s.scraped_at ? fmtDate(s.scraped_at) : "Unknown date"}
                  </span>
                </div>
                <div className="flex shrink-0 items-center gap-2">
                  <a
                    href={scrapeUrl(path)}
                    download
                    title="Download"
                    aria-label={`Download ${name}`}
                    className={cx(
                      "sc-scrape-download inline-flex size-8 items-center justify-center rounded-control border border-border",
                      "bg-linear-to-b from-primary to-primary-700 text-white transition duration-200 hover:-translate-y-0.5",
                    )}
                  >
                    <DownloadIcon />
                  </a>
                  <button
                    type="button"
                    data-path={path}
                    title="Delete"
                    aria-label={`Delete ${name}`}
                    className={cx("sc-scrape-delete", ICON_BUTTON, "enabled:hover:border-danger enabled:hover:text-danger")}
                    disabled={deleting === path}
                    onClick={() => void onDelete(s)}
                  >
                    <TrashIcon />
                  </button>
                </div>
              </div>
            );
          })}
        </div>
      </div>
    </Card>
  );
}
