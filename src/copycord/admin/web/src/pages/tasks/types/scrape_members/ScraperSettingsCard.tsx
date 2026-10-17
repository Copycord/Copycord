// "Scraper tokens and proxies" card on the Scrape members type page, moved off the old Scraper page:
// the saved tokens and the proxy list, each a plain textbox with one entry per line, saved as a whole
// list (JSON PUT, a text file on the server). Tokens are not validated here; a run skips a token that
// fails or is not in the server.

import { useEffect, useState, type ReactNode } from "react";
import { Button, cx, Skeleton, Textarea, useReveal } from "../../../../components/ui";
import { api, errorText } from "../../../../lib/api";
import { useNotify } from "../../../system/providers";
import { formatCount } from "../../StepList";
import { CARD_HEAD, RUN_CARD } from "../../TasksSkeleton";

export const SETTINGS_ID = "scraper-settings";
export const TOKENS_URL = "/api/scraper/tokens";
export const PROXIES_URL = "/api/scraper/proxies";

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

interface LineListProps {
  /** The list's key in the GET reply and the PUT body, "tokens" or "proxies". */
  field: "tokens" | "proxies";
  url: string;
  title: string;
  /** The saved count, "1 token saved" or "3 tokens saved". */
  one: string;
  many: string;
  help?: ReactNode;
  placeholder: string;
  rows: number;
  className?: string;
}

/** One section of the card: a textbox with one entry per line, its saved count and a Save button. */
function LineList({ field, url, title, one, many, help, placeholder, rows, className }: LineListProps) {
  const notify = useNotify();
  const noun = title.toLowerCase();
  const [text, setText] = useState("");
  const lines = textLines(text);
  const setLines = (list: string[]) => setText(list.join("\n"));
  const [saved, setSaved] = useState<string[] | null>(null);
  const [loadError, setLoadError] = useState<string | null>(null);
  const [saving, setSaving] = useState(false);
  const loading = saved === null && loadError === null;
  const reveal = useReveal(!loading);

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
          setText(list.join("\n"));
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

  const dirty = saved !== null && lines.join("\n") !== saved.join("\n");

  async function onSave() {
    setSaving(true);
    try {
      const j = checked(await api.put<Ok & Record<string, unknown>>(url, { [field]: lines }), `Could not save the ${noun}`);
      const next = Array.isArray(j[field]) ? (j[field] as string[]) : lines;
      setLines(next);
      setSaved(next);
      notify(`${title} saved`, "success");
    } catch (e) {
      notify(errorText(e, `Could not save the ${noun}`), "error");
    } finally {
      setSaving(false);
    }
  }

  return (
    <div className={cx("flex flex-col gap-2.5 px-4 py-3", className)}>
      <div className="flex items-center justify-between gap-3">
        <h3 className={SECTION_TITLE}>{title}</h3>
        {loading ? (
          <Skeleton.Text className="text-xs">{plural(0, one, many)}</Skeleton.Text>
        ) : loadError ? (
          <span className="text-xs text-danger">{loadError}</span>
        ) : (
          <span className={cx("text-xs text-muted", reveal)}>{plural(saved?.length ?? 0, one, many)}</span>
        )}
      </div>
      {help ? <p className="m-0 text-xs text-muted">{help}</p> : null}
      <div className="relative" aria-busy={loading || undefined}>
        <Textarea
          aria-label={title}
          rows={rows}
          spellCheck={false}
          autoComplete="off"
          readOnly={saved === null}
          value={text}
          onChange={(e) => setText(e.target.value)}
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
      <div className="flex justify-end">
        <Button variant="ghost" loading={saving} disabled={!dirty} onClick={() => void onSave()}>
          {`Save ${noun}`}
        </Button>
      </div>
    </div>
  );
}

export function ScraperSettingsCard() {
  return (
    <section id={SETTINGS_ID} aria-label="Scraper tokens and proxies" className={cx(RUN_CARD, "mb-4 scroll-mt-4")}>
      <div className={CARD_HEAD}>
        <h2 className="m-0 text-sm font-semibold text-text-1">Scraper tokens and proxies</h2>
      </div>
      <LineList
        field="tokens"
        url={TOKENS_URL}
        title="Tokens"
        one="token saved"
        many="tokens saved"
        help="Discord user tokens the scrape uses. They are not validated: one that fails or is not in the server is skipped."
        placeholder="One Discord token per line"
        rows={6}
        className="border-b border-border"
      />
      <LineList
        field="proxies"
        url={PROXIES_URL}
        title="Proxies"
        one="proxy saved"
        many="proxies saved"
        placeholder="One proxy per line: host:port or scheme://user:pass@host:port"
        rows={8}
      />
    </section>
  );
}
