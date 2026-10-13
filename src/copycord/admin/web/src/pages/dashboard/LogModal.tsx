// The log modal from index.html: live tail of /logs/stream/{which} over SSE,
// a search box that filters lines, Clear logs (asks first, then POSTs
// /logs/clear/{which}) and close by X, backdrop or Escape. Lines render as
// plain text. Keeps the old ids and the .show class for the e2e specs.

import { useEffect, useLayoutEffect, useRef, useState } from "react";
import { createPortal } from "react-dom";
import { cx } from "../../components/ui";
import { logStream } from "../../lib/sse";
import type { SseOptions } from "../../lib/sse";
import type { LogStream } from "../../types/api";
import { GhostButton } from "../system/buttons";
import { useNotify, useSafeConfirm } from "../system/providers";
import { appendCapped, clearLogs, filterLines, parseLogData } from "./model";

const STICK_PX = 24;
const RETRY_MS = 1500;

export interface LogModalProps {
  /** The open stream, or null when closed. */
  which: LogStream | null;
  onClose: () => void;
  /** For tests. */
  EventSourceImpl?: SseOptions<string>["EventSourceImpl"];
}

export function LogModal({ which, onClose, EventSourceImpl }: LogModalProps) {
  const notify = useNotify();
  const confirm = useSafeConfirm();
  const [lines, setLines] = useState<string[]>([]);
  const [query, setQuery] = useState("");
  const bodyRef = useRef<HTMLPreElement>(null);
  const searchRef = useRef<HTMLInputElement>(null);
  const follow = useRef(true);
  const notifyRef = useRef(notify);
  notifyRef.current = notify;
  const onCloseRef = useRef(onClose);
  onCloseRef.current = onClose;
  const open = which !== null;

  // One stream per open; a dropped stream reconnects after RETRY_MS like the old page.
  useEffect(() => {
    if (!which) return;
    setLines([]);
    setQuery("");
    follow.current = true;
    let closed = false;
    let retry: ReturnType<typeof setTimeout> | undefined;
    let client: { close: () => void } | undefined;
    const start = () => {
      client = logStream(which, {
        EventSourceImpl,
        onMessage: (raw) => setLines((prev) => appendCapped(prev, parseLogData(raw))),
        onError: () => {
          client?.close();
          if (closed) return;
          notifyRef.current("Log stream temporarily unavailable, retrying", "info");
          retry = setTimeout(start, RETRY_MS);
        },
      });
    };
    start();
    return () => {
      closed = true;
      if (retry) clearTimeout(retry);
      client?.close();
    };
    // notifyRef keeps the stream from restarting when the toast hook rebuilds.
  }, [which, EventSourceImpl]);

  useEffect(() => {
    if (!open) return;
    const opener = document.activeElement as HTMLElement | null;
    searchRef.current?.focus();
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") onCloseRef.current();
    };
    document.addEventListener("keydown", onKey);
    return () => {
      document.removeEventListener("keydown", onKey);
      opener?.focus?.();
    };
  }, [open]);

  const view = filterLines(lines, query);

  useLayoutEffect(() => {
    const el = bodyRef.current;
    if (el && follow.current) el.scrollTop = el.scrollHeight;
  }, [view.length, query]);

  if (!open) return null;

  const label = which === "server" ? "Server" : "Client";

  const clear = async () => {
    const ok = await confirm({
      title: "Clear logs?",
      body: `This will permanently delete all ${label.toLowerCase()} logs. This cannot be undone.`,
      confirmLabel: "Clear logs",
      danger: true,
    });
    if (!ok) return;
    try {
      await clearLogs(which);
      setLines([]);
      notify(`${label} logs cleared`, "success");
    } catch {
      notify("Failed to clear logs", "error");
    }
  };

  return createPortal(
    <div id="log-modal" className="show fixed inset-0 z-40 flex items-center justify-center p-4" aria-hidden="false">
      <div className="modal-backdrop absolute inset-0 bg-black/60" data-testid="log-backdrop" onClick={onClose} />
      <div
        role="dialog"
        aria-modal="true"
        aria-labelledby="log-title"
        className="relative flex h-[min(80vh,720px)] w-full max-w-5xl flex-col overflow-hidden rounded-2xl border border-border bg-card-solid text-text shadow-card"
      >
        <div className="flex items-center gap-3 border-b border-border px-4 py-3">
          <h4 id="log-title" className="shrink-0 text-base font-semibold">
            {label} logs
          </h4>
          <input
            ref={searchRef}
            id="log-search-input"
            type="text"
            placeholder="Search…"
            aria-label="Search logs"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === "Escape" && query) {
                e.preventDefault();
                e.nativeEvent.stopImmediatePropagation();
                setQuery("");
              }
            }}
            className="min-w-0 flex-1 rounded-control border border-border bg-bg-2 px-3 py-1.5 text-sm text-text outline-none focus:border-accent"
          />
          <button
            type="button"
            id="log-close"
            aria-label="Close"
            onClick={onClose}
            className="verify-close rounded-control px-2 py-1 text-muted hover:text-text"
          >
            ✕
          </button>
        </div>
        <pre
          ref={bodyRef}
          id="log-body"
          onScroll={(e) => {
            const el = e.currentTarget;
            follow.current = el.scrollHeight - el.scrollTop - el.clientHeight <= STICK_PX;
          }}
          className={cx(
            "m-0 flex-1 overflow-auto overscroll-contain bg-[#0e0720] px-4 py-3 font-mono text-xs leading-normal",
            "whitespace-pre-wrap text-[#e6e1ff]",
          )}
        >
          {view.length ? view.join("\n") + "\n" : ""}
        </pre>
        <div className="flex justify-end px-4 py-2">
          <GhostButton id="log-clear-btn" onClick={() => void clear()}>
            Clear logs
          </GhostButton>
        </div>
      </div>
    </div>,
    document.body,
  );
}
