// The log modal: live tail of /logs/stream/{which} over SSE,
// a search box that filters lines, Clear logs (asks first, then POSTs
// /logs/clear/{which}) and close by X, backdrop or Escape. Lines render as
// plain text. Its ids and the .show class are stable; tests select by them.

import { useEffect, useLayoutEffect, useRef, useState } from "react";
import { createPortal } from "react-dom";
import { cx, Skeleton } from "../../components/ui";
import { MODAL_BACKDROP, MODAL_CARD, MODAL_CLOSE, MODAL_OVERLAY, MODAL_TITLE, useScrollLock } from "../../components/Modal";
import { errorText } from "../../lib/api";
import { logStream } from "../../lib/sse";
import type { SseOptions } from "../../lib/sse";
import type { LogStream } from "../../types/api";
import { GhostButton } from "../system/buttons";
import { useNotify, useSafeConfirm } from "../system/providers";
import { appendCapped, clearLogs, filterLines, isLogReady, parseLogData } from "./model";

const STICK_PX = 24;
const RETRY_MS = 1500;
// The server sends the backlog in 50-line batches, then a {"ready": true} marker
// (also when the log is empty). The body stays hidden until the marker, or until
// batches stop arriving (quiet for SETTLE_MS) from a server without the marker,
// so it appears once, already at the bottom. SETTLE_MAX_MS ends the wait regardless.
const SETTLE_MS = 150;
const SETTLE_MAX_MS = 2500;
// Log lines shimmer in the body while the backlog loads, one per 18 px log line (text-xs, leading-normal),
// enough rows to fill the tallest body (80vh); the overflow is clipped. Widths repeat like real lines.
const PLACEHOLDER_WIDTHS = ["72%", "58%", "84%", "46%", "77%", "63%", "88%", "52%", "69%", "41%", "80%", "57%"];
const PLACEHOLDER_ROWS = Array.from({ length: 60 }, (_, i) => PLACEHOLDER_WIDTHS[i % PLACEHOLDER_WIDTHS.length]!);

export interface LogModalProps {
  /** The open stream, or null when closed. */
  which: LogStream | null;
  onClose: () => void;
  /** For tests. */
  EventSourceImpl?: SseOptions<string>["EventSourceImpl"];
}

export function LogModal({ which, onClose, EventSourceImpl }: LogModalProps) {
  // Dashboard keeps this mounted while closed (which is null), so lock only while open.
  useScrollLock(which !== null);
  const notify = useNotify();
  const confirm = useSafeConfirm();
  const [lines, setLines] = useState<string[]>([]);
  const [query, setQuery] = useState("");
  const [settled, setSettled] = useState(false);
  const bodyRef = useRef<HTMLPreElement>(null);
  const searchRef = useRef<HTMLInputElement>(null);
  const follow = useRef(true);
  const notifyRef = useRef(notify);
  notifyRef.current = notify;
  const onCloseRef = useRef(onClose);
  onCloseRef.current = onClose;
  const open = which !== null;

  // One stream per open; a dropped stream reconnects after RETRY_MS.
  useEffect(() => {
    if (!which) return;
    follow.current = true;
    let closed = false;
    let retry: ReturnType<typeof setTimeout> | undefined;
    let client: { close: () => void } | undefined;
    let quiet: ReturnType<typeof setTimeout> | undefined;
    const settle = () => setSettled(true);
    const cap = setTimeout(settle, SETTLE_MAX_MS);
    const start = () => {
      client = logStream(which, {
        EventSourceImpl,
        onMessage: (raw) => {
          if (quiet) clearTimeout(quiet);
          if (isLogReady(raw)) {
            settle();
            return;
          }
          setLines((prev) => appendCapped(prev, parseLogData(raw)));
          quiet = setTimeout(settle, SETTLE_MS);
        },
        onError: () => {
          client?.close();
          if (closed) return;
          settle();
          notifyRef.current("Log stream temporarily unavailable, retrying", "info");
          retry = setTimeout(start, RETRY_MS);
        },
      });
    };
    start();
    return () => {
      closed = true;
      if (retry) clearTimeout(retry);
      if (quiet) clearTimeout(quiet);
      clearTimeout(cap);
      client?.close();
      setSettled(false);
      // Clear on close, not on open: the next open then never paints the previous tail at the top.
      setLines([]);
      setQuery("");
      follow.current = true;
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

  // Before paint, so the body opens already at the bottom and new lines keep it
  // there only while the reader has not scrolled up. Plain assignment, no smooth scroll.
  useLayoutEffect(() => {
    const el = bodyRef.current;
    if (el && follow.current) el.scrollTop = el.scrollHeight;
  }, [which, view.length, query, settled]);

  if (!open) return null;

  const label = which === "server" ? "Server" : "Client";

  const clear = async () => {
    const ok = await confirm({
      title: "Clear logs?",
      body: `This will permanently delete all ${label.toLowerCase()} logs. This cannot be undone.`,
      confirmLabel: "Clear logs",
      confirmVariant: "ghost-red",
    });
    if (!ok) return;
    try {
      await clearLogs(which);
      setLines([]);
      notify(`${label} logs cleared`, "success");
    } catch (e) {
      notify(errorText(e, "Failed to clear logs"), "error");
    }
  };

  return createPortal(
    <div id="log-modal" className={`show ${MODAL_OVERLAY}`} aria-hidden="false">
      <div className={`modal-backdrop ${MODAL_BACKDROP}`} data-testid="log-backdrop" onClick={onClose} />
      <div
        role="dialog"
        aria-modal="true"
        aria-labelledby="log-title"
        className={`${MODAL_CARD} h-[80vh] w-[min(900px,92vw)]`}
      >
        <div className="flex shrink-0 items-center justify-center gap-2 px-4 py-3">
          <h4 id="log-title" className={MODAL_TITLE}>
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
            className="ml-0 min-w-[220px] rounded-input border border-log-input-edge bg-log-input-bg px-2.5 py-1.5 text-text-soft caret-log-input-focus outline-none transition-[border-color,box-shadow,background-color] duration-150 placeholder:text-[color-mix(in_srgb,var(--color-text-soft)_55%,transparent)] placeholder:opacity-75 hover:border-log-input-focus hover:shadow-[0_0_0_0.25px_var(--color-log-input-focus)] focus:border-log-input-focus focus:shadow-[0_0_0_0.25px_var(--color-log-input-focus)]"
          />
          <button type="button" id="log-close" aria-label="Close" onClick={onClose} className={`verify-close ${MODAL_CLOSE}`}>
            ✕
          </button>
        </div>
        <div className="relative flex min-h-0 flex-1 flex-col">
        {!settled && (
          <div data-testid="log-loading" aria-hidden="true" className="absolute inset-0 z-10 overflow-hidden bg-log-bg px-4 py-3">
            {PLACEHOLDER_ROWS.map((w, i) => (
              <div key={i} className="flex h-[18px] items-center">
                <Skeleton.Line width={w} height={10} className="my-0" />
              </div>
            ))}
          </div>
        )}
        {settled && lines.length === 0 && (
          <p id="log-empty" className="pointer-events-none absolute inset-x-0 top-0 m-0 px-4 py-3 font-mono text-xs leading-normal text-muted">
            No log lines yet.
          </p>
        )}
        <pre
          ref={bodyRef}
          id="log-body"
          onScroll={(e) => {
            const el = e.currentTarget;
            follow.current = el.scrollHeight - el.scrollTop - el.clientHeight <= STICK_PX;
          }}
          className={cx(
            "m-0 flex-1 overflow-auto overscroll-contain bg-log-bg px-4 py-3 font-mono text-xs leading-normal",
            "whitespace-pre-wrap text-text-soft",
            !settled && "invisible",
          )}
        >
          {view.length ? view.join("\n") + "\n" : ""}
        </pre>
        </div>
        <footer className="mt-3 flex flex-wrap justify-end gap-2.5 px-4 py-2">
          <GhostButton id="log-clear-btn" className="gap-[normal]!" onClick={() => void clear()}>
            Clear logs
          </GhostButton>
        </footer>
      </div>
    </div>,
    document.body,
  );
}
