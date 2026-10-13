// The proxy list: one chip per proxy with a favorites star, an optional test
// badge and a remove cross, then a text entry. Enter or paste commits; Backspace
// on an empty entry removes the last chip. A local list rather than the shared
// ChipInput because proxy strings are longer than its 100 character cap and
// each chip carries a star and a test result.

import { useState } from "react";
import type { ClipboardEvent, KeyboardEvent } from "react";
import { cx } from "../../../components/ui";
import type { ProxyChipResult } from "../../../types/dashboard-proxy";

export interface ProxyChipsProps {
  proxies: readonly string[];
  favorites: ReadonlySet<string>;
  results: Readonly<Record<string, ProxyChipResult>>;
  onAdd: (text: string) => void;
  onRemove: (proxy: string) => void;
  onRemoveLast: () => void;
  onToggleFavorite: (proxy: string) => void;
}

const TONE: Record<string, string> = {
  loading: "border-accent/50 opacity-80",
  pass: "border-good/60",
  slow: "border-[#ff9800]/70",
  fail: "border-danger/70",
};

function Star() {
  return (
    <svg viewBox="0 0 24 24" fill="currentColor" aria-hidden="true" className="size-3.5">
      <polygon points="12 2 15.09 8.26 22 9.27 17 14.14 18.18 21.02 12 17.77 5.82 21.02 7 14.14 2 9.27 8.91 8.26 12 2" />
    </svg>
  );
}

export function ProxyChips({ proxies, favorites, results, onAdd, onRemove, onRemoveLast, onToggleFavorite }: ProxyChipsProps) {
  const [text, setText] = useState("");

  const commit = () => {
    const v = text.trim();
    if (!v) return;
    onAdd(v);
    setText("");
  };

  const onKeyDown = (e: KeyboardEvent<HTMLInputElement>) => {
    if (e.key === "Enter") {
      e.preventDefault();
      commit();
    } else if (e.key === "Backspace" && !text && proxies.length) {
      onRemoveLast();
    }
  };

  const onPaste = (e: ClipboardEvent<HTMLInputElement>) => {
    const pasted = e.clipboardData.getData("text");
    if (!pasted) return;
    e.preventDefault();
    onAdd(pasted);
    setText("");
  };

  return (
    <div
      id="srv-proxy-chips"
      className="srv-proxy-chips flex min-h-[44px] flex-wrap items-center gap-1.5 rounded-control border border-border bg-bg-2 p-2"
    >
      {proxies.map((p) => {
        const fav = favorites.has(p);
        const res = results[p];
        return (
          <span
            key={p}
            data-proxy={p}
            className={cx(
              "srv-proxy-chip-item inline-flex max-w-full items-center gap-1.5 rounded-full border bg-white/4 py-0.5 pl-1.5 pr-2 text-xs",
              res ? `test-${res.state}` : "",
              res ? TONE[res.state] : "border-border",
              fav && "is-favorite",
            )}
          >
            <span
              role="button"
              tabIndex={0}
              aria-pressed={fav}
              title={fav ? "Favorite: preferred for the gateway" : "Mark as favorite"}
              className={cx(
                "srv-proxy-chip-star inline-flex cursor-pointer",
                fav ? "text-custom-ink" : "text-muted/60 hover:text-custom-ink",
              )}
              onClick={(e) => {
                e.stopPropagation();
                onToggleFavorite(p);
              }}
              onKeyDown={(e) => {
                if (e.key === "Enter" || e.key === " ") {
                  e.preventDefault();
                  e.stopPropagation();
                  onToggleFavorite(p);
                }
              }}
            >
              <Star />
            </span>
            <span className="srv-proxy-chip-text min-w-0 truncate font-mono">{p}</span>
            {res ? (
              <span className="srv-proxy-chip-ms text-[11px] text-muted">
                {res.state === "loading" ? "testing…" : res.label}
              </span>
            ) : null}
            <span
              role="button"
              tabIndex={0}
              aria-label={`Remove ${p}`}
              className="srv-proxy-chip-x cursor-pointer text-muted hover:text-danger"
              onClick={() => onRemove(p)}
              onKeyDown={(e) => {
                if (e.key === "Enter" || e.key === " ") {
                  e.preventDefault();
                  onRemove(p);
                }
              }}
            >
              ×
            </span>
          </span>
        );
      })}
      <input
        id="srv-proxy-chips-input"
        type="text"
        spellCheck={false}
        placeholder="Paste or type a proxy and press Enter"
        value={text}
        onChange={(e) => setText(e.target.value)}
        onKeyDown={onKeyDown}
        onPaste={onPaste}
        className="srv-proxy-chips-input min-w-[200px] flex-1 bg-transparent px-1 py-1 text-sm text-text outline-none"
      />
    </div>
  );
}
