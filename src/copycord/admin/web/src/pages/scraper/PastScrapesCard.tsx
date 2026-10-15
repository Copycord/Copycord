// Past scrapes card: saved scrape files from GET /api/scraper/scrapes with
// download and a confirmed delete, as loadScrapes and its handlers in the old
// scraper.js. The page owns the list request so a finished run can reload it.

import { useState } from "react";
import { Button, Card, cx } from "../../components/ui";
import { useLastCount } from "../../components/ui/lastCount";
import type { ScraperScrape, ScraperScrapesResponse } from "../../types/scraper";
import type { Confirm, Notify } from "../system/providers";
import { DownloadIcon, RefreshIcon, TrashIcon, UsersIcon } from "./icons";
import { deleteScrape, reason, scrapeUrl } from "./scraperApi";
import {
  DELETE_BUTTON,
  DOWNLOAD_LINK,
  SCRAPE_ACTIONS,
  SCRAPE_DATE,
  SCRAPE_GUILD,
  SCRAPE_LIST,
  SCRAPE_META,
  SCRAPE_ROW,
  SCRAPE_TEXT,
  SCRAPES_COUNT_KEY,
  ScrapesPlaceholder,
} from "./ScraperSkeleton";
import { SC_CARD, TITLE, TITLEBAR } from "./TokensCard";

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
  /** First load still running: skeleton rows instead of the empty state. */
  loading?: boolean;
}

export function PastScrapesCard({ data, reload, notify, confirm, loading = false }: PastScrapesCardProps) {
  const [deleting, setDeleting] = useState<string | null>(null);
  const all = data?.ok ? (data.scrapes ?? []) : [];
  const list = all.filter((s) => !s.error);
  useLastCount(SCRAPES_COUNT_KEY, data ? list.length : undefined);

  async function onDelete(s: ScraperScrape) {
    const path = s.path || s.filename;
    const ok = await confirm({
      title: "Delete scrape?",
      body: `Delete scrape file "${path}"?`,
      confirmLabel: "Delete",
      confirmVariant: "ghost-red",
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
    <Card id="sc-scrapes-card" aria-label="Past Scrapes" className={SC_CARD}>
      <div className={TITLEBAR}>
        <h3 className={TITLE}>Past Scrapes</h3>
        <Button id="sc-scrapes-refresh" size="sm" variant="ghost" title="Refresh" aria-label="Refresh" onClick={reload}>
          <RefreshIcon />
        </Button>
      </div>
      <div id="sc-scrapes-body" className="mt-3">
        {loading ? <ScrapesPlaceholder /> : null}
        <div id="sc-scrapes-empty" hidden={loading || all.length > 0} className="px-4 py-6 text-center text-muted">
          <p className="m-0 mb-1 text-sm font-semibold">No saved scrapes yet</p>
          <small className="text-xs">Completed scrapes are automatically saved here</small>
        </div>
        <div id="sc-scrapes-list" className={SCRAPE_LIST}>
          {list.map((s) => {
            const path = s.path || s.filename;
            const name = s.guild_name || (s.guild_id != null ? String(s.guild_id) : "") || "Unknown";
            const method = s.metadata?.method || "gateway";
            return (
              <div key={path} data-path={path} className={cx("sc-scrape-item", SCRAPE_ROW)}>
                <div className={SCRAPE_TEXT}>
                  <span
                    className={cx("sc-scrape-guild", SCRAPE_GUILD)}
                    title={`Guild ID: ${s.guild_id != null ? String(s.guild_id) : "?"}`}
                  >
                    <UsersIcon className="shrink-0 text-accent" />
                    <span className="truncate">{name}</span>
                  </span>
                  <span className={cx("sc-scrape-meta", SCRAPE_META)}>
                    {`${s.total_count ?? 0} members · ${method} · ${fmtBytes(s.file_size || 0)}`}
                  </span>
                  <span className={cx("sc-scrape-date", SCRAPE_DATE)}>
                    {s.scraped_at ? fmtDate(s.scraped_at) : "Unknown date"}
                  </span>
                </div>
                <div className={SCRAPE_ACTIONS}>
                  <a
                    href={scrapeUrl(path)}
                    download
                    title="Download"
                    aria-label={`Download ${name}`}
                    className={cx("sc-scrape-download", DOWNLOAD_LINK)}
                  >
                    <DownloadIcon />
                  </a>
                  <button
                    type="button"
                    data-path={path}
                    title="Delete"
                    aria-label={`Delete ${name}`}
                    className={cx("sc-scrape-delete", DELETE_BUTTON)}
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
