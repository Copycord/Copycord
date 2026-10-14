// Scraper page: the tokens, configuration, proxy, queue, progress, log and
// past scrapes cards from the old scraper.html and scraper.js. Element ids
// match the old page for the e2e specs.

import { cx } from "../components/ui";
import { useApi } from "../hooks/useApi";
import type { ScraperScrapesResponse, ScraperStatus, ScraperTokensResponse } from "../types/scraper";
import { ConfigCard } from "./scraper/ConfigCard";
import { PastScrapesCard } from "./scraper/PastScrapesCard";
import { ProgressCard } from "./scraper/ProgressCard";
import { ProxyCard } from "./scraper/ProxyCard";
import { QueueCard } from "./scraper/QueueCard";
import { ScraperLogsCard } from "./scraper/ScraperLogsCard";
import { SCRAPES_URL, STATUS_URL, TOKENS_URL } from "./scraper/scraperApi";
import { IDLE_PILL, PILL_BOX, ScraperTitle } from "./scraper/ScraperSkeleton";
import { TokensCard } from "./scraper/TokensCard";
import { useScraperLive } from "./scraper/useScraperLive";
import { useNotify, useSafeConfirm } from "./system/providers";

// .scraper-status-pill and its .pill-dot per data-status.
const PILL: Record<string, { box: string; dot: string }> = {
  idle: IDLE_PILL,
  running: {
    box: "border-[rgba(16,185,129,0.4)] bg-[rgba(16,185,129,0.08)] text-[#34d399]",
    dot: "bg-[#34d399] shadow-[0_0_8px_rgba(16,185,129,0.5)]",
  },
  done: { box: "border-[rgba(139,92,246,0.4)] bg-[rgba(139,92,246,0.08)] text-primary", dot: "bg-primary" },
  error: { box: "border-[rgba(239,68,68,0.4)] bg-[rgba(239,68,68,0.08)] text-[#f87171]", dot: "bg-[#ef4444]" },
};

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

  const pill = PILL[live.pill] ?? IDLE_PILL;

  // min-w-fit: like the old .scraper-wide column, the page is never narrower than its widest
  // unbreakable row (the log card toolbar), so on phones it keeps the old one-line title.
  return (
    <div id="scraper-root" className="min-w-fit">
      <h1 className="sr-only">Scraper</h1>
      <div className="mt-7 mb-1 flex flex-wrap items-start justify-between gap-4">
        <ScraperTitle />
        <span
          id="sc-status-pill"
          data-status={live.pill}
          className={cx(PILL_BOX, pill.box)}
        >
          <span aria-hidden="true" className={cx("size-2 rounded-full transition-all duration-[250ms]", pill.dot, busy && "animate-pulse")} />
          <span id="sc-status-label">{live.pillLabel}</span>
        </span>
      </div>

      <div className="grid grid-cols-[1fr] items-start gap-[18px] min-[961px]:grid-cols-[1fr_1fr]">
        <TokensCard tokens={list} loading={tokens.loading && !tokens.data} reload={tokens.reload} notify={notify} confirm={confirm} />
        <ConfigCard
          tokenCount={list.length}
          queueLength={live.queue.length}
          running={running}
          onStarted={live.start}
          onCancelled={status.reload}
          notify={notify}
        />
      </div>

      <ProxyCard notify={notify} />
      <QueueCard queue={live.queue} notify={notify} confirm={confirm} />
      <ProgressCard progress={live.progress} />
      <ScraperLogsCard signal={live.logSignal} />
      <PastScrapesCard data={scrapes.data} loading={scrapes.loading && !scrapes.data} reload={scrapes.reload} notify={notify} confirm={confirm} />
    </div>
  );
}
