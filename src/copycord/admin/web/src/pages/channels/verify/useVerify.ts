// Verify scan transport: sends {kind:"verify", payload:{action:"list"}} on /ws/in
// and writes the "orphans" event of /ws/out into the shared orphan store.

import { useCallback, useEffect, useRef, useState } from "react";
import { useWsMessages } from "../../../hooks/useWsMessages";
import { wsUrl } from "../../../lib/ws";
import type { BusMessage } from "../../../types/api";
import type { OrphansEvent } from "../../../types/channels";
import { verifyEnvelope } from "../actions/logic";
import { orphanStore } from "./orphanStore";
import type { OrphanStore } from "./orphanStore";
import { parseOrphans } from "./logic";

export interface Verify {
  /** Asks the client for the orphan list. */
  fetch: () => void;
  scanning: boolean;
  /** True once an orphans event arrived for this mapping. */
  scanned: boolean;
}

export function useVerify(mappingId: string, store: OrphanStore = orphanStore): Verify {
  const [scanning, setScanning] = useState(false);
  const [scanned, setScanned] = useState(false);
  const sock = useRef<WebSocket | null>(null);

  useEffect(() => () => sock.current?.close(), []);
  useEffect(() => {
    setScanned(false);
    setScanning(false);
  }, [mappingId]);

  const send = useCallback(() => {
    let s = sock.current;
    if (!s || s.readyState > WebSocket.OPEN) {
      s = new WebSocket(wsUrl("/ws/in"));
      sock.current = s;
    }
    const frame = JSON.stringify(verifyEnvelope(mappingId, { action: "list" }));
    if (s.readyState === WebSocket.OPEN) s.send(frame);
    else s.addEventListener("open", () => s.send(frame), { once: true });
  }, [mappingId]);

  const fetchOrphans = useCallback(() => {
    setScanning(true);
    send();
  }, [send]);

  // The old page asked for the list once at boot so the tree shows orphans right away.
  useEffect(() => {
    if (mappingId) send();
  }, [mappingId, send]);

  useWsMessages(
    "/ws/out",
    (msg: BusMessage) => {
      const p = (msg.payload ?? msg) as Partial<OrphansEvent>;
      const next = parseOrphans(p, mappingId);
      if (!next) return;
      store.set(next);
      setScanned(true);
      setScanning(false);
    },
    { types: ["verify"] },
  );

  return { fetch: fetchOrphans, scanning, scanned };
}
