// Subscribes a component to /ws/ui or /ws/out for its lifetime.
//   const { connected, send } = useWsMessages("/ws/ui", (m) => ..., { types: ["clone_progress"] });

import { useCallback, useEffect, useRef, useState } from "react";
import { connectWs } from "../lib/ws";
import type { WsClient, WsOptions } from "../lib/ws";
import type { BusMessage, WsPath } from "../types/api";

export interface UseWsOptions {
  /** Only deliver messages whose `type` (or `kind`) is in this list. */
  types?: readonly string[];
  /** For tests. */
  WebSocketImpl?: WsOptions<BusMessage>["WebSocketImpl"];
  enabled?: boolean;
}

export function useWsMessages<T extends BusMessage = BusMessage>(
  path: WsPath,
  onMessage: (msg: T) => void,
  opts: UseWsOptions = {},
): { connected: boolean; send: (data: unknown) => boolean } {
  const [connected, setConnected] = useState(false);
  const handler = useRef(onMessage);
  handler.current = onMessage;
  const types = useRef(opts.types);
  types.current = opts.types;
  const client = useRef<WsClient | null>(null);
  const enabled = opts.enabled ?? true;
  const Impl = opts.WebSocketImpl;

  useEffect(() => {
    if (!enabled) return;
    const c = connectWs<T>(path, {
      WebSocketImpl: Impl,
      onOpen: () => setConnected(true),
      onClose: () => setConnected(false),
      onMessage: (msg) => {
        const want = types.current;
        if (want && !want.includes(String(msg.type ?? msg.kind ?? ""))) return;
        handler.current(msg);
      },
    });
    client.current = c;
    return () => {
      c.close();
      client.current = null;
      setConnected(false);
    };
  }, [path, enabled, Impl]);

  const send = useCallback((data: unknown) => client.current?.send(data) ?? false, []);
  return { connected, send };
}
