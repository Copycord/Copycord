// Scraper logs card: tails /logs/stream/scraper, filters by a search box,
// and clears on demand (also emptying scraper.out) or by itself around each
// run when Auto-clear is on, as the log half of the old scraper.js.

import { useEffect, useLayoutEffect, useRef, useState } from "react";
import { Button, Card, cx, Input } from "../../components/ui";
import { connectSse } from "../../lib/sse";
import type { ScraperLogEvent } from "../../types/scraper";
import { clearLogFile, LOG_STREAM_URL } from "./scraperApi";
import { AUTOCLEAR_INPUT, AUTOCLEAR_KEY, CHECK_SPAN, LOG_BODY, readAutoclear, SC_CARD, TITLE } from "./ScraperSkeleton";
import type { LogSignal } from "./useScraperLive";

export const MAX_LINES = 10000;
export { AUTOCLEAR_KEY };
/** Search keystrokes settle this long before the view filters. */
export const SEARCH_DEBOUNCE_MS = 80;
/** Within this many pixels of the bottom the view keeps following new lines. */
const STICK_PX = 24;

/** The lines one stream event carries: a tail batch, a new line, or the raw text when not JSON. */
export function parseLogEvent(raw: string): string[] {
  try {
    const obj = JSON.parse(raw) as ScraperLogEvent | null;
    if (obj && Array.isArray(obj.lines)) return obj.lines.map((l) => String(l ?? ""));
    if (obj && typeof obj.line === "string") return [obj.line];
    return [];
  } catch {
    return [raw];
  }
}

export function filterLines(lines: string[], query: string): string[] {
  const q = query.trim().toLowerCase();
  return q ? lines.filter((l) => l.toLowerCase().includes(q)) : lines;
}

export interface ScraperLogsCardProps {
  signal: LogSignal | null;
  /** For tests. */
  EventSourceImpl?: typeof EventSource;
}

export function ScraperLogsCard({ signal, EventSourceImpl }: ScraperLogsCardProps) {
  const [lines, setLines] = useState<string[]>([]);
  const [search, setSearch] = useState("");
  const [query, setQuery] = useState("");
  const [autoclear, setAutoclear] = useState(readAutoclear);
  const body = useRef<HTMLPreElement>(null);
  const stick = useRef(true);
  const autoclearRef = useRef(autoclear);
  autoclearRef.current = autoclear;

  useEffect(() => {
    const Impl = EventSourceImpl ?? (typeof EventSource === "undefined" ? undefined : EventSource);
    if (!Impl) return;
    const client = connectSse<string>(LOG_STREAM_URL, {
      json: false,
      EventSourceImpl: Impl,
      onMessage: (raw) => {
        const got = parseLogEvent(raw);
        if (!got.length) return;
        setLines((prev) => {
          const next = prev.concat(got);
          return next.length > MAX_LINES ? next.slice(next.length - MAX_LINES) : next;
        });
      },
    });
    return () => client.close();
  }, [EventSourceImpl]);

  useEffect(() => {
    const t = setTimeout(() => setQuery(search), SEARCH_DEBOUNCE_MS);
    return () => clearTimeout(t);
  }, [search]);

  // Run start clears the view; run end also empties the file, both only with Auto-clear on.
  useEffect(() => {
    if (!signal || !autoclearRef.current) return;
    setLines([]);
    if (signal.kind === "done") {
      setSearch("");
      setQuery("");
      clearLogFile();
    }
  }, [signal]);

  const view = filterLines(lines, query);
  const text = view.length ? view.join("\n") + "\n" : "";

  useLayoutEffect(() => {
    const el = body.current;
    if (el && stick.current) el.scrollTop = el.scrollHeight;
  }, [text]);

  function onScroll() {
    const el = body.current;
    if (el) stick.current = el.scrollHeight - el.scrollTop - el.clientHeight <= STICK_PX;
  }

  function onClear() {
    setLines([]);
    setSearch("");
    setQuery("");
    stick.current = true;
    clearLogFile();
  }

  function onAutoclear(on: boolean) {
    setAutoclear(on);
    try {
      localStorage.setItem(AUTOCLEAR_KEY, on ? "true" : "false");
    } catch {
      // Private mode: the choice lasts until reload.
    }
  }

  return (
    <Card id="sc-log-card" aria-label="Scraper Logs" className={cx(SC_CARD, "sc-log-card")}>
      <div className="mb-1.5 flex flex-nowrap items-center justify-between gap-2.5">
        <h3 className={TITLE}>Scraper Logs</h3>
        <div className="ml-auto flex min-w-0 shrink flex-nowrap items-center gap-2">
          <label
            className="flex cursor-pointer items-center gap-[5px] text-[11px] whitespace-nowrap text-muted select-none"
            title="Automatically clear logs after each scrape completes"
          >
            <input
              id="sc-log-autoclear"
              type="checkbox"
              className={AUTOCLEAR_INPUT}
              checked={autoclear}
              onChange={(e) => onAutoclear(e.target.checked)}
            />
            <span aria-hidden="true" className={CHECK_SPAN} />
            <span>Auto-clear</span>
          </label>
          <Input
            id="sc-log-search"
            type="text"
            aria-label="Search logs"
            placeholder={"Search logs…"}
            className="block min-w-[60px] shrink"
            value={search}
            onChange={(e) => {
              stick.current = true;
              setSearch(e.target.value);
            }}
            onKeyDown={(e) => {
              if (e.key !== "Escape") return;
              e.preventDefault();
              setSearch("");
              setQuery("");
            }}
          />
          <Button id="sc-log-clear" size="sm" variant="ghost" className="shrink-0" onClick={onClear}>
            Clear
          </Button>
        </div>
      </div>
      <pre
        id="sc-log-body"
        ref={body}
        onScroll={onScroll}
        aria-live="polite"
        aria-label="Scraper log output"
        className={LOG_BODY}
      >
        {text}
      </pre>
    </Card>
  );
}
