// Websocket client for /ws/ui and /ws/out: parses JSON messages and
// reconnects with a capped backoff until closed.

import type { BusMessage, WsPath } from "../types/api";

export interface WsOptions<T> {
  onMessage: (msg: T) => void;
  onOpen?: () => void;
  onClose?: () => void;
  /** First retry delay in ms, doubled per failure up to maxDelay. */
  baseDelay?: number;
  maxDelay?: number;
  /** For tests. */
  WebSocketImpl?: typeof WebSocket;
}

export interface WsClient {
  send: (data: unknown) => boolean;
  close: () => void;
}

export function wsUrl(path: string, loc: Location = window.location): string {
  const proto = loc.protocol === "https:" ? "wss:" : "ws:";
  return `${proto}//${loc.host}${path}`;
}

export function connectWs<T = BusMessage>(path: WsPath, opts: WsOptions<T>): WsClient {
  const Impl = opts.WebSocketImpl ?? WebSocket;
  const base = opts.baseDelay ?? 1000;
  const max = opts.maxDelay ?? 15000;
  let attempt = 0;
  let closed = false;
  let timer: ReturnType<typeof setTimeout> | undefined;
  let sock: WebSocket | undefined;

  const open = () => {
    if (closed) return;
    const ws = new Impl(wsUrl(path));
    sock = ws;
    ws.onopen = () => {
      attempt = 0;
      opts.onOpen?.();
    };
    ws.onmessage = (ev: MessageEvent) => {
      if (typeof ev.data !== "string") return;
      try {
        opts.onMessage(JSON.parse(ev.data) as T);
      } catch {
        // Not JSON: ignore, the server only sends JSON.
      }
    };
    ws.onclose = () => {
      opts.onClose?.();
      if (closed) return;
      const delay = Math.min(max, base * 2 ** attempt);
      attempt += 1;
      timer = setTimeout(open, delay);
    };
  };

  open();

  return {
    send: (data: unknown) => {
      if (!sock || sock.readyState !== Impl.OPEN) return false;
      sock.send(typeof data === "string" ? data : JSON.stringify(data));
      return true;
    },
    close: () => {
      closed = true;
      if (timer) clearTimeout(timer);
      sock?.close();
    },
  };
}
