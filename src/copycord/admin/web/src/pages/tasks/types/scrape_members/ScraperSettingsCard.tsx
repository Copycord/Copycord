// "Scraper tokens and proxies" card on the Scrape members type page, moved off the old Scraper page:
// the saved tokens and the proxy list, each a plain textbox with one entry per line, saved as a whole
// list (JSON PUT, a text file on the server) by itself shortly after the last edit. Collapsed by default.

import { useCallback, useEffect, useRef, useState, useSyncExternalStore } from "react";
import { cx, Skeleton, Textarea, useReveal } from "../../../../components/ui";
import { api, errorText } from "../../../../lib/api";
import { useNotify } from "../../../system/providers";
import { formatCount } from "../../StepList";
import { DISCLOSURE_SUMMARY, DisclosureTitle, RUN_CARD } from "../../TasksSkeleton";

export const SETTINGS_ID = "scraper-settings";
export const TOKENS_URL = "/api/scraper/tokens";
export const PROXIES_URL = "/api/scraper/proxies";
const PANEL_ID = "scraper-settings-panel";
const OPEN_EVENT = "copycord:open-scraper-settings";
/** How long after the last keystroke a list saves itself. */
export const AUTOSAVE_MS = 800;

/** Opens the card and scrolls to it (the start form's link when no tokens are saved). */
export function openScraperSettings() {
  window.dispatchEvent(new Event(OPEN_EVENT));
  document.getElementById(SETTINGS_ID)?.scrollIntoView({ behavior: "smooth", block: "start" });
}

// The saved token count while the card is mounted, so the start form opens straight to the right
// layout: set on load and after each save, null while unknown (loading, failed, or no card on the page).
let tokenCount: number | null = null;
const tokenListeners = new Set<() => void>();

function setTokenCount(n: number | null) {
  if (n === tokenCount) return;
  tokenCount = n;
  for (const fn of tokenListeners) fn();
}

function subscribeTokenCount(fn: () => void) {
  tokenListeners.add(fn);
  return () => tokenListeners.delete(fn);
}

const getTokenCount = () => tokenCount;

/** How many scraper tokens the settings card holds, or null when it does not know. */
export function useSavedTokenCount(): number | null {
  return useSyncExternalStore(subscribeTokenCount, getTokenCount, getTokenCount);
}

interface Ok {
  ok?: boolean;
  error?: string;
}

function checked<T extends Ok>(j: T, fallback: string): T {
  if (!j?.ok) throw new Error(j?.error || fallback);
  return j;
}

const plural = (n: number, one: string, many: string) => `${formatCount(n)} ${n === 1 ? one : many}`;

const SECTION_TITLE = "m-0 text-sm font-semibold text-text-1";

/** The textbox's lines as a list: trimmed, blank lines and repeats dropped. */
export function textLines(text: string): string[] {
  return [
    ...new Set(
      text
        .split(/\r?\n/)
        .map((l) => l.trim())
        .filter(Boolean),
    ),
  ];
}

type Field = "tokens" | "proxies";
/** A list's saved count for the card head: missing while it loads, null when the load failed. */
type Counts = Partial<Record<Field, number | null>>;

interface LineListProps {
  /** The list's key in the GET reply and the PUT body, "tokens" or "proxies". */
  field: Field;
  url: string;
  title: string;
  /** The saved count, "1 token saved" or "3 tokens saved". */
  one: string;
  many: string;
  placeholder: string;
  rows: number;
  className?: string;
  onCount: (field: Field, n: number | null) => void;
}

/**
 * One section of the card: a textbox with one entry per line and its saved count. Edits save themselves
 * AUTOSAVE_MS after the last keystroke, at once on blur and on unmount; the text is never rewritten.
 */
function LineList({ field, url, title, one, many, placeholder, rows, className, onCount }: LineListProps) {
  const notify = useNotify();
  const noun = title.toLowerCase();
  const [text, setText] = useState("");
  const [saved, setSaved] = useState<string[] | null>(null);
  const [loadError, setLoadError] = useState<string | null>(null);
  const [saving, setSaving] = useState(false);
  const loading = saved === null && loadError === null;
  const reveal = useReveal(!loading);
  // The autosave reads these outside renders (timer, blur, unmount). `sent` is the list the file holds
  // once the last PUT lands, null until the load succeeded so a failed load never overwrites the file.
  const latest = useRef("");
  const sent = useRef<string | null>(null);
  const confirmed = useRef<string | null>(null);
  const seq = useRef(0);
  const timer = useRef<ReturnType<typeof setTimeout>>(undefined);

  useEffect(() => {
    let live = true;
    api
      .get<Ok & Record<string, unknown>>(url)
      .then((j) => {
        const list = checked(j, `Could not load the ${noun}`)[field];
        return Array.isArray(list) ? list.filter((x): x is string => typeof x === "string") : [];
      })
      .then(
        (list) => {
          if (!live) return;
          latest.current = list.join("\n");
          sent.current = confirmed.current = textLines(latest.current).join("\n");
          setText(latest.current);
          setSaved(list);
        },
        (e) => {
          if (live) setLoadError(errorText(e, `Could not load the ${noun}`));
        },
      );
    return () => {
      live = false;
    };
  }, [url, field, noun]);

  useEffect(() => {
    if (saved !== null) onCount(field, saved.length);
    else if (loadError !== null) onCount(field, null);
  }, [field, saved, loadError, onCount]);

  function flush() {
    clearTimeout(timer.current);
    timer.current = undefined;
    if (sent.current === null) return;
    const lines = textLines(latest.current);
    const key = lines.join("\n");
    if (key === sent.current) return;
    sent.current = key;
    const mine = ++seq.current;
    setSaving(true);
    api
      .put<Ok & Record<string, unknown>>(url, { [field]: lines })
      .then((j) => checked(j, `Could not save the ${noun}`))
      .then(
        (j) => {
          if (mine !== seq.current) return;
          const next = Array.isArray(j[field]) ? (j[field] as string[]) : lines;
          confirmed.current = key;
          setSaved(next);
          setSaving(false);
        },
        (e) => {
          if (mine !== seq.current) return;
          // Back to what the file holds, so the next edit or blur tries again with the text as typed.
          sent.current = confirmed.current;
          setSaving(false);
          notify(errorText(e, `Could not save the ${noun}`), "error");
        },
      );
  }
  const flushRef = useRef(flush);
  flushRef.current = flush;
  useEffect(() => () => flushRef.current(), []);

  function onEdit(value: string) {
    setText(value);
    latest.current = value;
    if (sent.current === null) return;
    clearTimeout(timer.current);
    timer.current = setTimeout(() => flushRef.current(), AUTOSAVE_MS);
  }

  const count = plural(saved?.length ?? 0, one, many);
  return (
    <div className={cx("flex flex-col gap-2.5 px-4 py-3", className)}>
      <div className="flex items-center justify-between gap-3">
        <h3 className={SECTION_TITLE}>{title}</h3>
        {loading ? (
          <Skeleton.Text className="text-xs">{plural(12, one, many)}</Skeleton.Text>
        ) : loadError ? (
          <span className="text-xs text-danger">{loadError}</span>
        ) : (
          // The count and "Saving" share one cell, so the row keeps the wider one's width either way.
          <span className={cx("grid text-right text-xs text-muted", reveal)}>
            <span className={cx("[grid-area:1/1]", saving && "invisible")}>{count}</span>
            <span className={cx("[grid-area:1/1]", !saving && "invisible")} aria-hidden={!saving}>
              Saving
            </span>
          </span>
        )}
      </div>
      <div className="relative" aria-busy={loading || undefined}>
        <Textarea
          aria-label={title}
          rows={rows}
          spellCheck={false}
          autoComplete="off"
          readOnly={saved === null}
          value={text}
          onChange={(e) => onEdit(e.target.value)}
          onBlur={() => flush()}
          placeholder={loading ? undefined : placeholder}
          className={cx("max-h-64 overflow-y-auto font-mono text-xs leading-5 whitespace-pre", reveal)}
        />
        {loading ? (
          <div className="pointer-events-none absolute inset-x-3.5 top-3">
            <Skeleton.Line width="62%" />
            <Skeleton.Line width="48%" />
            <Skeleton.Line width="55%" />
          </div>
        ) : null}
      </div>
    </div>
  );
}

/** The collapsed card head's "2 tokens, 1 proxy": shimmers until both lists answered, nothing if one failed. */
function CountsSummary({ counts }: { counts: Counts }) {
  const { tokens, proxies } = counts;
  const settled = tokens !== undefined && proxies !== undefined;
  const reveal = useReveal(settled);
  const words = (t: number, p: number) => `${plural(t, "token", "tokens")}, ${plural(p, "proxy", "proxies")}`;
  if (!settled) return <Skeleton.Text className="ml-auto text-xs">{words(2, 1)}</Skeleton.Text>;
  if (tokens === null || proxies === null) return null;
  return <span className={cx("ml-auto text-xs font-normal text-muted", reveal)}>{words(tokens, proxies)}</span>;
}

export function ScraperSettingsCard() {
  const [open, setOpen] = useState(false);
  const [counts, setCounts] = useState<Counts>({});
  const onCount = useCallback((field: Field, n: number | null) => {
    setCounts((c) => ({ ...c, [field]: n }));
    if (field === "tokens") setTokenCount(n);
  }, []);

  useEffect(() => () => setTokenCount(null), []);

  useEffect(() => {
    const show = () => setOpen(true);
    window.addEventListener(OPEN_EVENT, show);
    return () => window.removeEventListener(OPEN_EVENT, show);
  }, []);

  return (
    <section id={SETTINGS_ID} aria-label="Scraper tokens and proxies" className={cx(RUN_CARD, "mb-4 scroll-mt-4")}>
      <h2 className="m-0">
        <button
          type="button"
          aria-expanded={open}
          aria-controls={PANEL_ID}
          onClick={() => setOpen((v) => !v)}
          className={cx(
            DISCLOSURE_SUMMARY,
            "w-full rounded-card text-left outline-none focus-visible:ring-2 focus-visible:ring-accent/60 focus-visible:ring-inset",
          )}
        >
          <DisclosureTitle title="Scraper tokens and proxies" open={open} />
          {open ? null : <CountsSummary counts={counts} />}
        </button>
      </h2>
      {/* Kept mounted while collapsed: the lists load once and an edit in flight still saves. */}
      <div id={PANEL_ID} hidden={!open} className="border-t border-border">
        <LineList
          field="tokens"
          url={TOKENS_URL}
          title="Tokens"
          one="token saved"
          many="tokens saved"
          placeholder="One Discord token per line"
          rows={6}
          className="border-b border-border"
          onCount={onCount}
        />
        <LineList
          field="proxies"
          url={PROXIES_URL}
          title="Proxies"
          one="proxy saved"
          many="proxies saved"
          placeholder="One proxy per line: host:port or scheme://user:pass@host:port"
          rows={8}
          onCount={onCount}
        />
      </div>
    </section>
  );
}
