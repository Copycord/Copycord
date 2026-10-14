// Past scrapes card: saved scrape files from GET /api/scraper/scrapes with
// download and a confirmed delete, as loadScrapes and its handlers in the old
// scraper.js. The page owns the list request so a finished run can reload it.

import { useState } from "react";
import { Button, Card, cx } from "../../components/ui";
import type { ScraperScrape, ScraperScrapesResponse } from "../../types/scraper";
import type { Confirm, Notify } from "../system/providers";
import { DownloadIcon, RefreshIcon, TrashIcon, UsersIcon } from "./icons";
import { deleteScrape, reason, scrapeUrl } from "./scraperApi";
import { ScrapesPlaceholder } from "./ScraperSkeleton";
import { ICON_BUTTON, SC_CARD, TITLE, TITLEBAR } from "./TokensCard";

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
        <div id="sc-scrapes-list" className="flex flex-col gap-1.5">
          {list.map((s) => {
            const path = s.path || s.filename;
            const name = s.guild_name || (s.guild_id != null ? String(s.guild_id) : "") || "Unknown";
            const method = s.metadata?.method || "gateway";
            return (
              <div
                key={path}
                data-path={path}
                className="sc-scrape-item flex items-center justify-between gap-3 rounded-btn border border-border bg-[rgba(20,11,38,0.4)] px-3 py-2.5 transition-colors duration-150 hover:border-[rgba(139,92,246,0.35)]"
              >
                <div className="flex min-w-0 flex-col gap-0.5">
                  <span
                    className="sc-scrape-guild flex min-w-0 items-center gap-1.5 font-mono text-[13px] font-semibold text-text"
                    title={`Guild ID: ${s.guild_id != null ? String(s.guild_id) : "?"}`}
                  >
                    <UsersIcon className="shrink-0 text-accent" />
                    <span className="truncate">{name}</span>
                  </span>
                  <span className="sc-scrape-meta text-xs text-muted">
                    {`${s.total_count ?? 0} members · ${method} · ${fmtBytes(s.file_size || 0)}`}
                  </span>
                  <span className="sc-scrape-date text-[11px] text-muted opacity-70">
                    {s.scraped_at ? fmtDate(s.scraped_at) : "Unknown date"}
                  </span>
                </div>
                <div className="flex shrink-0 items-center gap-1.5">
                  <a
                    href={scrapeUrl(path)}
                    download
                    title="Download"
                    aria-label={`Download ${name}`}
                    className={cx(
                      "sc-scrape-download inline-flex min-h-7 items-center justify-center rounded-btn border border-border px-2.5 py-1.5",
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
