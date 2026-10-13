// Scraper page, top half: the tokens, configuration and proxy cards from the
// old scraper.html and scraper.js. The queue, progress, log and past scrapes
// cards come in a later lane and go in the marked slot below. Element ids
// match the old page for the e2e specs.

import { useState } from "react";
import { useApi } from "../hooks/useApi";
import type { ScraperStatus, ScraperTokensResponse } from "../types/scraper";
import { ConfigCard } from "./scraper/ConfigCard";
import { SearchIcon } from "./scraper/icons";
import { ProxyCard } from "./scraper/ProxyCard";
import { STATUS_URL, TOKENS_URL } from "./scraper/scraperApi";
import { TokensCard } from "./scraper/TokensCard";
import { useNotify, useSafeConfirm } from "./system/providers";

export function Scraper() {
  const tokens = useApi<ScraperTokensResponse>(TOKENS_URL);
  const status = useApi<ScraperStatus>(STATUS_URL);
  const notify = useNotify();
  const confirm = useSafeConfirm();
  // Set by Start and Cancel until the next status reply says otherwise.
  const [runningOverride, setRunningOverride] = useState<boolean | null>(null);

  const list = tokens.data?.ok ? (tokens.data.tokens ?? []) : [];
  const running = runningOverride ?? Boolean(status.data?.running);
  const queueLength = status.data?.queue?.length ?? 0;

  return (
    <div id="scraper-root" className="mx-auto max-w-[1120px] px-3 pt-3.5 md:px-0 md:pt-[18px] xl:max-w-[1240px]">
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
          data-status={running ? "running" : "idle"}
          className="inline-flex items-center gap-1.5 rounded-full border border-border bg-white/4 px-3 py-1 text-[13px] font-bold"
        >
          <span
            aria-hidden="true"
            className={running ? "size-1.5 animate-pulse rounded-full bg-good" : "size-1.5 rounded-full bg-text"}
          />
          <span id="sc-status-label">{running ? "Running" : "Idle"}</span>
        </span>
      </div>

      <div className="grid grid-cols-1 items-start gap-[18px] lg:grid-cols-2">
        <TokensCard tokens={list} reload={tokens.reload} notify={notify} confirm={confirm} />
        <ConfigCard
          tokenCount={list.length}
          queueLength={queueLength}
          running={running}
          onStarted={() => setRunningOverride(true)}
          onCancelled={() => {
            setRunningOverride(null);
            status.reload();
          }}
          notify={notify}
        />
      </div>

      <div className="mt-[18px] flex flex-col gap-[18px]">
        <ProxyCard notify={notify} />

        {/* LANE 12: queue (#sc-queue-card), progress (#sc-progress-card), scraper logs
            (#sc-log-card) and past scrapes (#sc-scrapes-card) cards go here. */}
      </div>
    </div>
  );
}
