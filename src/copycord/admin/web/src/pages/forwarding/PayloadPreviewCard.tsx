// A window style card showing a JSON value: a title bar with three decorative
// dots, a centred file name and a Copy button, over a scrollable code body.

import { useEffect, useState } from "react";
import type { ReactNode } from "react";
import { cx } from "../../components/ui";

const DOTS = ["#ff5f57", "#febc2e", "#28c840"];

const COPY_BTN =
  "relative ml-auto inline-flex cursor-pointer items-center rounded-md border border-border bg-transparent px-2 py-0.5 text-[11px] font-semibold text-text-2 outline-none hover:bg-rail-hover hover:text-text focus-visible:ring-2 focus-visible:ring-accent/60";

// Keys in the accent tint and string values in green; everything else stays plain.
function tint(text: string): ReactNode[] {
  const out: ReactNode[] = [];
  const re = /("(?:[^"\\]|\\.)*")(\s*:)?/g;
  let at = 0;
  for (const m of text.matchAll(re)) {
    const i = m.index ?? 0;
    if (i > at) out.push(text.slice(at, i));
    out.push(
      <span key={i} className={m[2] ? "text-accent-soft" : "text-good"}>
        {m[1]}
      </span>,
    );
    if (m[2]) out.push(m[2]);
    at = i + m[0].length;
  }
  if (at < text.length) out.push(text.slice(at));
  return out;
}

export function PayloadPreviewCard({
  value,
  title = "payload.json",
  className,
}: {
  value: unknown;
  title?: string;
  className?: string;
}) {
  const text = JSON.stringify(value, null, 2);
  const [copied, setCopied] = useState(false);

  useEffect(() => {
    if (!copied) return;
    const t = setTimeout(() => setCopied(false), 1500);
    return () => clearTimeout(t);
  }, [copied]);

  const copy = () => {
    void navigator.clipboard?.writeText(text).then(
      () => setCopied(true),
      () => setCopied(false),
    );
  };

  return (
    <div className={cx("payload-preview overflow-hidden rounded-control border border-border bg-control shadow-card", className)}>
      <div className="relative flex h-8 items-center border-b border-border bg-wash-4 px-3">
        <span className="relative flex gap-1.5" aria-hidden="true">
          {DOTS.map((c) => (
            <span key={c} className="block size-2.5 rounded-full" style={{ backgroundColor: c }} />
          ))}
        </span>
        <span className="pointer-events-none absolute inset-x-0 text-center font-mono text-xs text-muted">{title}</span>
        <button type="button" onClick={copy} className={COPY_BTN}>
          {copied ? "Copied" : "Copy"}
        </button>
      </div>
      <pre
        aria-label={`Example ${title}`}
        className="m-0 max-h-[280px] overflow-auto p-3 font-mono text-xs leading-5 whitespace-pre text-text-1"
      >
        <code>{tint(text)}</code>
      </pre>
    </div>
  );
}
