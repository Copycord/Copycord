// Scraper logs card: tails /logs/stream/scraper, filters by a search box,
// and clears on demand (also emptying scraper.out) or by itself around each
// run when Auto-clear is on, as the log half of the old scraper.js.

import { useEffect, useLayoutEffect, useRef, useState } from "react";
import { Button, Card, cx, Input } from "../../components/ui";
import { connectSse } from "../../lib/sse";
import type { ScraperLogEvent } from "../../types/scraper";
import { clearLogFile, LOG_STREAM_URL } from "./scraperApi";
import { TITLE } from "./TokensCard";
import type { LogSignal } from "./useScraperLive";

export const MAX_LINES = 10000;
export const AUTOCLEAR_KEY = "scraper_log_autoclear";
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

function readAutoclear(): boolean {
  try {
    return localStorage.getItem(AUTOCLEAR_KEY) !== "false";
  } catch {
    return true;
  }
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
    <Card id="sc-log-card" aria-label="Scraper Logs" className="sc-log-card">
      <div className="flex items-center justify-between gap-3">
        <h3 className={cx(TITLE, "shrink-0")}>Scraper Logs</h3>
        <div className="flex min-w-0 items-center gap-2">
          <label
            className="inline-flex shrink-0 cursor-pointer items-center gap-2 text-[13px] text-text-1"
            title="Automatically clear logs after each scrape completes"
          >
            <input
              id="sc-log-autoclear"
              type="checkbox"
              className="size-4 cursor-pointer accent-primary"
              checked={autoclear}
              onChange={(e) => onAutoclear(e.target.checked)}
            />
            <span>Auto-clear</span>
          </label>
          <div className="w-40 min-w-0 shrink">
            <Input
              id="sc-log-search"
              type="text"
              aria-label="Search logs"
              placeholder={"Search logs…"}
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
          </div>
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
        className="m-0 mt-3 h-[420px] overflow-y-auto overscroll-contain whitespace-pre-wrap rounded-b-2xl bg-[#0e0720] px-4 py-3 font-mono text-xs leading-normal text-[#e6e1ff]"
      >
        {text}
      </pre>
    </Card>
  );
}
