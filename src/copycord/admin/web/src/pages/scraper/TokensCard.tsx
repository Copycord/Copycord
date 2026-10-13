// Tokens card: list, add (value + optional label), re-validate and delete.
// Only masked values are ever rendered; the value input clears after a save.

import { useState } from "react";
import { Button, Card, cx, Input } from "../../components/ui";
import type { ScraperToken } from "../../types/scraper";
import type { Confirm, Notify } from "../system/providers";
import { CheckIcon, KeyIcon, RefreshIcon, TrashIcon, XIcon } from "./icons";
import { displayMask } from "./mask";
import { addToken, deleteToken, plural, reason, validateToken } from "./scraperApi";

export const TITLE = "m-0 text-lg font-bold text-accent";

export const ICON_BUTTON =
  "inline-flex size-8 cursor-pointer items-center justify-center rounded-control border border-border bg-white/4 " +
  "text-accent outline-none transition duration-200 focus-visible:ring-2 focus-visible:ring-accent/60 " +
  "enabled:hover:-translate-y-0.5 enabled:hover:border-accent/30 enabled:hover:bg-white/8 disabled:cursor-not-allowed disabled:opacity-55";

export interface TokensCardProps {
  tokens: ScraperToken[];
  reload: () => void;
  notify: Notify;
  confirm: Confirm;
}

export function TokensCard({ tokens, reload, notify, confirm }: TokensCardProps) {
  const [value, setValue] = useState("");
  const [label, setLabel] = useState("");
  const [adding, setAdding] = useState(false);
  const [busyId, setBusyId] = useState<string | null>(null);

  async function onAdd() {
    const val = value.trim();
    if (!val) {
      notify("Enter a token", "info");
      return;
    }
    setAdding(true);
    try {
      const j = await addToken(val, label.trim());
      if (!j?.ok) throw new Error(j?.error || "Failed to add token");
      notify(`Token added${j.is_valid ? " ✓ valid" : ", invalid"}`, j.is_valid ? "success" : "info");
      setValue("");
      setLabel("");
      reload();
    } catch (e) {
      notify(reason(e, "Failed to add token"), "error");
    } finally {
      setAdding(false);
    }
  }

  async function onValidate(id: string) {
    setBusyId(id);
    try {
      const j = await validateToken(id);
      if (!j?.ok) throw new Error(j?.error || "Validation failed");
      notify(`Token ${j.is_valid ? "valid ✓" : "invalid ✗"}`, j.is_valid ? "success" : "info");
      reload();
    } catch (e) {
      notify(reason(e, "Validation failed"), "error");
    } finally {
      setBusyId(null);
    }
  }

  async function onDelete(t: ScraperToken) {
    const ok = await confirm({
      title: "Delete token?",
      body: "Delete this token?",
      confirmLabel: "Delete",
      danger: true,
    });
    if (!ok) return;
    setBusyId(t.token_id);
    try {
      const j = await deleteToken(t.token_id);
      if (!j?.ok) throw new Error(j?.error || "Delete failed");
      notify("Token deleted", "success");
      reload();
    } catch (e) {
      notify(reason(e, "Delete failed"), "error");
    } finally {
      setBusyId(null);
    }
  }

  return (
    <Card id="sc-tokens-card" aria-label="Tokens" className="flex min-w-0 flex-col">
      <div className="mb-3 flex items-center justify-between gap-3">
        <h3 className={TITLE}>Tokens</h3>
        <span
          id="sc-token-count"
          className="rounded-full border border-accent/40 bg-accent/15 px-2.5 py-0.5 text-[11px] font-bold text-accent"
        >
          {plural(tokens.length, "token")}
        </span>
      </div>

      <form
        id="sc-add-form"
        className="rounded-control border border-border bg-white/2 p-3.5"
        onSubmit={(e) => {
          e.preventDefault();
          void onAdd();
        }}
      >
        <div className="flex flex-col gap-2 sm:flex-row sm:items-center">
          <Input
            id="sc-token-value"
            type="password"
            aria-label="Discord token"
            placeholder="Paste Discord token…"
            autoComplete="off"
            spellCheck={false}
            className="min-w-0 sm:flex-[2]"
            value={value}
            onChange={(e) => setValue(e.target.value)}
          />
          <Input
            id="sc-token-label"
            aria-label="Token label"
            placeholder="Label (optional)"
            className="min-w-0 sm:flex-1"
            value={label}
            onChange={(e) => setLabel(e.target.value)}
          />
          <Button id="sc-add-submit" type="submit" variant="primary" size="sm" loading={adding}>
            Add
          </Button>
        </div>
      </form>

      <div id="sc-token-list" className="mt-3 flex flex-col gap-2">
        {tokens.length === 0 ? (
          <div id="sc-token-empty" className="flex flex-col items-center gap-1 py-6 text-center text-text-2">
            <KeyIcon className="mb-1 opacity-60" />
            <p className="m-0 text-text">No tokens configured</p>
            <small className="text-xs">Add Discord tokens to start scraping members</small>
          </div>
        ) : (
          tokens.map((t) => {
            const busy = busyId === t.token_id;
            return (
              <div
                key={t.token_id}
                data-id={t.token_id}
                className="sc-token-item flex items-center justify-between gap-3 rounded-control border border-border bg-white/3 px-3 py-2.5"
              >
                <div className="flex min-w-0 items-center gap-2.5">
                  <span
                    title={t.is_valid ? "Valid" : "Invalid"}
                    className={cx(
                      "inline-flex size-6 shrink-0 items-center justify-center rounded-full",
                      t.is_valid ? "bg-good/15 text-good" : "bg-danger/15 text-danger",
                    )}
                  >
                    {t.is_valid ? <CheckIcon /> : <XIcon />}
                  </span>
                  <div className="flex min-w-0 flex-col">
                    <span className="sc-token-name truncate font-semibold">{t.username || t.label || "Unlabeled"}</span>
                    <span className="sc-token-masked truncate font-mono text-xs text-text-2">{displayMask(t.masked)}</span>
                  </div>
                </div>
                <div className="flex shrink-0 gap-1.5">
                  <button
                    type="button"
                    className={cx("sc-token-revalidate", ICON_BUTTON)}
                    data-id={t.token_id}
                    title="Re-validate"
                    aria-label="Re-validate"
                    disabled={busy}
                    onClick={() => void onValidate(t.token_id)}
                  >
                    <RefreshIcon className={busy ? "animate-spin" : undefined} />
                  </button>
                  <button
                    type="button"
                    className={cx("sc-token-delete", ICON_BUTTON, "enabled:hover:border-danger enabled:hover:text-danger")}
                    data-id={t.token_id}
                    title="Delete"
                    aria-label="Delete"
                    disabled={busy}
                    onClick={() => void onDelete(t)}
                  >
                    <TrashIcon />
                  </button>
                </div>
              </div>
            );
          })
        )}
      </div>
    </Card>
  );
}
