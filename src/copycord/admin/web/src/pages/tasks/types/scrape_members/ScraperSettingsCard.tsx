// "Scraper tokens and proxies" card on the Scrape members type page, moved off the old Scraper page:
// the saved tokens (masked, add, rename, delete) and the proxy list. Tokens are not validated here; a
// run skips a token that fails or is not in the server. Token forms are multipart, as the FastAPI
// Form(...) parameters expect; proxies are JSON.

import { useEffect, useState } from "react";
import { Button, ChipInput, cx, Input } from "../../../../components/ui";
import { useApi } from "../../../../hooks/useApi";
import { api, errorText } from "../../../../lib/api";
import { useNotify, useSafeConfirm } from "../../../system/providers";
import { formatCount } from "../../StepList";
import { CARD_HEAD, RUN_CARD } from "../../TasksSkeleton";

export const SETTINGS_ID = "scraper-settings";
export const TOKENS_URL = "/api/scraper/tokens";
export const PROXIES_URL = "/api/scraper/proxies";

export interface ScraperToken {
  token_id: string;
  label?: string | null;
  masked?: string | null;
  username?: string | null;
}

interface Ok {
  ok?: boolean;
  error?: string;
}

export const ELLIPSIS = "…";

/** app.py _mask_token: up to 10 chars keep 2, longer keep the first 6 and last 4. */
export function maskToken(tok: string | null | undefined): string {
  const t = String(tok ?? "");
  if (!t) return "";
  if (t.length <= 10) return `${t.slice(0, 2)}${ELLIPSIS}`;
  return `${t.slice(0, 6)}${ELLIPSIS}${t.slice(-4)}`;
}

/** The server's mask when it looks masked, otherwise masked here, so no more than 6 + 4 characters show. */
export function displayMask(masked: string | null | undefined): string {
  const m = String(masked ?? "");
  return m.includes(ELLIPSIS) && m.length <= 11 ? m : maskToken(m);
}

export function tokenUrl(id: string): string {
  return `${TOKENS_URL}/${encodeURIComponent(id)}`;
}

function form(fields: Record<string, string>): FormData {
  const fd = new FormData();
  for (const [key, value] of Object.entries(fields)) fd.append(key, value);
  return fd;
}

function checked<T extends Ok>(j: T, fallback: string): T {
  if (!j?.ok) throw new Error(j?.error || fallback);
  return j;
}

const plural = (n: number, one: string, many: string) => `${formatCount(n)} ${n === 1 ? one : many}`;

const ICON_BUTTON =
  "inline-flex size-8 cursor-pointer items-center justify-center rounded-btn border border-border bg-wash-4 text-muted " +
  "outline-none transition-colors focus-visible:ring-2 focus-visible:ring-accent/60 enabled:hover:text-text " +
  "disabled:cursor-not-allowed disabled:opacity-55";
const SECTION_TITLE = "m-0 text-sm font-semibold text-text-1";

const PencilIcon = () => (
  <svg viewBox="0 0 24 24" className="size-4" fill="none" stroke="currentColor" strokeWidth="2" aria-hidden="true">
    <path d="M4 20h4L19 9l-4-4L4 16z" />
    <path d="m13.5 6.5 4 4" />
  </svg>
);

const TrashIcon = () => (
  <svg viewBox="0 0 24 24" className="size-4" fill="none" stroke="currentColor" strokeWidth="2" aria-hidden="true">
    <path d="M4 7h16" />
    <path d="M6 7l1 13h10l1-13" />
    <path d="M9 7V4h6v3" />
  </svg>
);

function TokenRow({ token, onChanged }: { token: ScraperToken; onChanged: () => void }) {
  const notify = useNotify();
  const confirm = useSafeConfirm();
  const name = token.username || token.label || "Unlabeled";
  const [editing, setEditing] = useState(false);
  const [label, setLabel] = useState(token.label ?? "");
  const [busy, setBusy] = useState(false);

  async function onSave() {
    setBusy(true);
    try {
      checked(await api.post<Ok>(`${tokenUrl(token.token_id)}/update`, form({ label: label.trim() })), "Could not rename the token");
      notify("Token renamed", "success");
      setEditing(false);
      onChanged();
    } catch (e) {
      notify(errorText(e, "Could not rename the token"), "error");
    } finally {
      setBusy(false);
    }
  }

  async function onDelete() {
    const ok = await confirm({ title: "Delete token?", body: `Delete the token ${name}?`, confirmLabel: "Delete", confirmVariant: "ghost-red" });
    if (!ok) return;
    setBusy(true);
    try {
      checked(await api.del<Ok>(tokenUrl(token.token_id)), "Could not delete the token");
      notify("Token deleted", "success");
      onChanged();
    } catch (e) {
      notify(errorText(e, "Could not delete the token"), "error");
      setBusy(false);
    }
  }

  return (
    <li className="flex items-center justify-between gap-3 border-b border-border px-4 py-2.5 last:border-b-0">
      {editing ? (
        <form
          className="flex min-w-0 flex-1 items-center gap-2"
          onSubmit={(e) => {
            e.preventDefault();
            void onSave();
          }}
        >
          <Input aria-label={`Label of ${name}`} autoFocus className="min-w-0 flex-1" value={label} onChange={(e) => setLabel(e.target.value)} />
          <Button type="submit" variant="primary" size="sm" loading={busy}>
            Save
          </Button>
          <Button
            variant="ghost"
            size="sm"
            onClick={() => {
              setLabel(token.label ?? "");
              setEditing(false);
            }}
          >
            Cancel
          </Button>
        </form>
      ) : (
        <>
          <div className="flex min-w-0 flex-col gap-0.5">
            <span className="truncate text-sm font-semibold text-text" title={name}>
              {name}
            </span>
            <span className="truncate font-mono text-xs text-muted">{displayMask(token.masked)}</span>
          </div>
          <div className="flex shrink-0 gap-1.5">
            <button type="button" className={ICON_BUTTON} title="Edit" aria-label={`Edit ${name}`} disabled={busy} onClick={() => setEditing(true)}>
              <PencilIcon />
            </button>
            <button
              type="button"
              className={cx(ICON_BUTTON, "enabled:hover:border-danger enabled:hover:text-danger")}
              title="Delete"
              aria-label={`Delete ${name}`}
              disabled={busy}
              onClick={() => void onDelete()}
            >
              <TrashIcon />
            </button>
          </div>
        </>
      )}
    </li>
  );
}

function AddToken({ onAdded }: { onAdded: () => void }) {
  const notify = useNotify();
  const [value, setValue] = useState("");
  const [label, setLabel] = useState("");
  const [adding, setAdding] = useState(false);

  async function onAdd() {
    const val = value.trim();
    if (!val) {
      notify("Enter a token", "info");
      return;
    }
    setAdding(true);
    try {
      checked(await api.post<Ok>(`${TOKENS_URL}/add`, form({ token_value: val, label: label.trim() })), "Could not add the token");
      notify("Token added", "success");
      setValue("");
      setLabel("");
      onAdded();
    } catch (e) {
      notify(errorText(e, "Could not add the token"), "error");
    } finally {
      setAdding(false);
    }
  }

  return (
    <form
      className="flex items-center gap-2 px-4 py-3 max-[600px]:flex-col max-[600px]:items-stretch"
      onSubmit={(e) => {
        e.preventDefault();
        void onAdd();
      }}
    >
      <Input
        type="text"
        aria-label="Discord token"
        placeholder="Paste a Discord token"
        autoComplete="off"
        spellCheck={false}
        className="min-w-0 flex-[2]"
        value={value}
        onChange={(e) => setValue(e.target.value)}
      />
      <Input aria-label="Token label" placeholder="Label (optional)" className="min-w-0 flex-1" value={label} onChange={(e) => setLabel(e.target.value)} />
      <Button type="submit" variant="primary" size="sm" loading={adding}>
        Add token
      </Button>
    </form>
  );
}

function Proxies() {
  const notify = useNotify();
  const [proxies, setProxies] = useState<string[]>([]);
  const [saved, setSaved] = useState<string[] | null>(null);
  const [saving, setSaving] = useState(false);

  useEffect(() => {
    let live = true;
    api
      .get<Ok & { proxies?: string[] }>(PROXIES_URL)
      .then((j) => (j?.ok ? (j.proxies ?? []) : []))
      .catch(() => [] as string[])
      .then((list) => {
        if (!live) return;
        setProxies(list);
        setSaved(list);
      });
    return () => {
      live = false;
    };
  }, []);

  const dirty = saved !== null && proxies.join("\n") !== saved.join("\n");

  async function onSave() {
    setSaving(true);
    try {
      const j = checked(await api.put<Ok & { proxies?: string[] }>(PROXIES_URL, { proxies }), "Could not save the proxies");
      const next = j.proxies ?? proxies;
      setProxies(next);
      setSaved(next);
      notify("Proxies saved", "success");
    } catch (e) {
      notify(errorText(e, "Could not save the proxies"), "error");
    } finally {
      setSaving(false);
    }
  }

  return (
    <div className="flex flex-col gap-2.5 px-4 py-3">
      <div className="flex items-center justify-between gap-3">
        <h3 className={SECTION_TITLE}>Proxies</h3>
        <span className="text-xs text-muted">{saved === null ? "" : plural(saved.length, "proxy saved", "proxies saved")}</span>
      </div>
      <ChipInput
        aria-label="Proxies"
        value={proxies}
        onChange={setProxies}
        placeholder="host:port or scheme://user:pass@host:port, Enter to add"
      />
      <div className="flex justify-end">
        <Button variant="primary" size="sm" loading={saving} disabled={!dirty} onClick={() => void onSave()}>
          Save proxies
        </Button>
      </div>
    </div>
  );
}

export function ScraperSettingsCard() {
  const { data, error, loading, reload } = useApi<Ok & { tokens?: ScraperToken[] }>(TOKENS_URL);
  const tokens = data?.tokens ?? [];
  const first = loading && !data;

  return (
    <section id={SETTINGS_ID} aria-label="Scraper tokens and proxies" className={cx(RUN_CARD, "mb-4 scroll-mt-4")}>
      <div className={CARD_HEAD}>
        <h2 className="m-0 text-sm font-semibold text-text-1">Scraper tokens and proxies</h2>
        <span className="text-xs text-muted">{first ? "" : plural(tokens.length, "token", "tokens")}</span>
      </div>
      <div className="border-b border-border">
        <div className="px-4 pt-3">
          <h3 className={SECTION_TITLE}>Tokens</h3>
          <p className="m-0 mt-1 text-xs text-muted">Discord user tokens the scrape uses. One that fails or is not in the server is skipped.</p>
        </div>
        <AddToken onAdded={reload} />
        {first ? null : error && !data ? (
          <p className="m-0 px-4 pb-3 text-sm text-danger">{errorText(error, "Could not load the tokens")}</p>
        ) : tokens.length === 0 ? (
          <p className="m-0 px-4 pb-3 text-sm text-muted">No tokens saved. Add one to start scraping members.</p>
        ) : (
          <ul aria-label="Scraper tokens" className="m-0 max-h-[300px] list-none overflow-y-auto border-t border-border p-0">
            {tokens.map((t) => (
              <TokenRow key={t.token_id} token={t} onChanged={reload} />
            ))}
          </ul>
        )}
      </div>
      <Proxies />
    </section>
  );
}
