// Server-sent events for /bus/stream and /logs/stream/{which}. EventSource
// reconnects by itself; this adds JSON parsing and a close handle.

import type { BusMessage, LogStream } from "../types/api";

export interface SseOptions<T> {
  onMessage: (msg: T) => void;
  onError?: (ev: Event) => void;
  /** Parse each data line as JSON (default true). */
  json?: boolean;
  /** For tests. */
  EventSourceImpl?: typeof EventSource;
}

export interface SseClient {
  close: () => void;
}

export function connectSse<T = BusMessage>(url: string, opts: SseOptions<T>): SseClient {
  const Impl = opts.EventSourceImpl ?? EventSource;
  const es = new Impl(url, { withCredentials: true });
  const json = opts.json ?? true;
  es.onmessage = (ev: MessageEvent) => {
    const raw = String(ev.data);
    if (!json) {
      opts.onMessage(raw as T);
      return;
    }
    try {
      opts.onMessage(JSON.parse(raw) as T);
    } catch {
      // Heartbeats and other non JSON lines are skipped.
    }
  };
  es.onerror = (ev: Event) => opts.onError?.(ev);
  return { close: () => es.close() };
}

export const busStream = (opts: SseOptions<BusMessage>) => connectSse("/bus/stream", opts);

export const logStream = (which: LogStream, opts: SseOptions<string>) =>
  connectSse<string>(`/logs/stream/${encodeURIComponent(which)}`, { json: false, ...opts });
