// Scraper page: the tokens, configuration, proxy, queue, progress, log and
// past scrapes cards from the old scraper.html and scraper.js. Element ids
// match the old page for the e2e specs.

import { useApi } from "../hooks/useApi";
import type { ScraperScrapesResponse, ScraperStatus, ScraperTokensResponse } from "../types/scraper";
import { ConfigCard } from "./scraper/ConfigCard";
import { SearchIcon } from "./scraper/icons";
import { PastScrapesCard } from "./scraper/PastScrapesCard";
import { ProgressCard } from "./scraper/ProgressCard";
import { ProxyCard } from "./scraper/ProxyCard";
import { QueueCard } from "./scraper/QueueCard";
import { ScraperLogsCard } from "./scraper/ScraperLogsCard";
import { SCRAPES_URL, STATUS_URL, TOKENS_URL } from "./scraper/scraperApi";
import { TokensCard } from "./scraper/TokensCard";
import { useScraperLive } from "./scraper/useScraperLive";
import { useNotify, useSafeConfirm } from "./system/providers";

export function Scraper() {
  const tokens = useApi<ScraperTokensResponse>(TOKENS_URL);
  const status = useApi<ScraperStatus>(STATUS_URL);
  const scrapes = useApi<ScraperScrapesResponse>(SCRAPES_URL);
  const notify = useNotify();
  const confirm = useSafeConfirm();
  const live = useScraperLive(status.data, { notify, onSaved: scrapes.reload });

  const list = tokens.data?.ok ? (tokens.data.tokens ?? []) : [];
  const { running } = live;
  const busy = live.pill === "running";

  return (
    <div id="scraper-root" className="mx-auto max-w-[1120px] px-3 pt-3.5 md:px-0 md:pt-[18px] xl:max-w-[1240px]">
      <h1 className="sr-only">Scraper</h1>
      <div className="mb-5 flex items-start justify-between gap-3">
        <div>
          <h2 className="m-0 flex items-center gap-2 text-2xl font-bold text-accent">
            <SearchIcon />
            Member Scraper
          </h2>
          <p className="m-0 mt-0.5 text-sm text-text-1">Scrape Discord server members</p>
        </div>
        <span
          id="sc-status-pill"
          data-status={live.pill}
          className="inline-flex items-center gap-1.5 rounded-full border border-border bg-white/4 px-3 py-1 text-[13px] font-bold"
        >
          <span
            aria-hidden="true"
            className={
              busy
                ? "size-1.5 animate-pulse rounded-full bg-good"
                : live.pill === "done"
                  ? "size-1.5 rounded-full bg-good"
                  : live.pill === "error"
                    ? "size-1.5 rounded-full bg-danger"
                    : "size-1.5 rounded-full bg-text"
            }
          />
          <span id="sc-status-label">{live.pillLabel}</span>
        </span>
      </div>

      <div className="grid grid-cols-1 items-start gap-[18px] lg:grid-cols-2">
        <TokensCard tokens={list} reload={tokens.reload} notify={notify} confirm={confirm} />
        <ConfigCard
          tokenCount={list.length}
          queueLength={live.queue.length}
          running={running}
          onStarted={live.start}
          onCancelled={status.reload}
          notify={notify}
        />
      </div>

      <div className="mt-[18px] flex flex-col gap-[18px]">
        <ProxyCard notify={notify} />
        <QueueCard queue={live.queue} notify={notify} confirm={confirm} />
        <ProgressCard progress={live.progress} />
        <ScraperLogsCard signal={live.logSignal} />
        <PastScrapesCard data={scrapes.data} reload={scrapes.reload} notify={notify} confirm={confirm} />
      </div>
    </div>
  );
}
