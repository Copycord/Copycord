// Tokens card: list, add (value + optional label), re-validate and delete.
// Only masked values are ever rendered; the value input clears after a save.

import { useState } from "react";
import { Button, Card, cx, Input, useReveal } from "../../components/ui";
import { useLastCount } from "../../components/ui/lastCount";
import type { ScraperToken } from "../../types/scraper";
import type { Confirm, Notify } from "../system/providers";
import { CheckIcon, KeyIcon, RefreshIcon, TrashIcon, XIcon } from "./icons";
import { displayMask } from "./mask";
import { addToken, deleteToken, plural, reason, validateToken } from "./scraperApi";
import {
  DELETE_BUTTON,
  ICON_BUTTON,
  SC_CARD,
  TITLE,
  TITLEBAR,
  TOKEN_ACTIONS,
  TOKEN_BADGE,
  TOKEN_MAIN,
  TOKEN_MASK,
  TOKEN_NAME,
  TOKEN_ROW,
  TOKEN_TEXT,
  TokenCount,
  TOKENS_COUNT_KEY,
  TokenListPlaceholder,
} from "./ScraperSkeleton";

export { ICON_BUTTON, SC_CARD, TITLE, TITLEBAR };

export interface TokensCardProps {
  tokens: ScraperToken[];
  reload: () => void;
  notify: Notify;
  confirm: Confirm;
  /** First load still running: skeleton rows instead of the empty state. */
  loading?: boolean;
}

export function TokensCard({ tokens, reload, notify, confirm, loading = false }: TokensCardProps) {
  const [value, setValue] = useState("");
  const [label, setLabel] = useState("");
  const [adding, setAdding] = useState(false);
  const [busyId, setBusyId] = useState<string | null>(null);
  useLastCount(TOKENS_COUNT_KEY, loading ? undefined : tokens.length);

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
      confirmVariant: "ghost-red",
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

  const placeholder = loading && tokens.length === 0;
  const reveal = useReveal(!placeholder);

  return (
    <Card id="sc-tokens-card" aria-label="Tokens" className={cx(SC_CARD, "min-w-0")}>
      <div className={TITLEBAR}>
        <h3 className={TITLE}>Tokens</h3>
        <TokenCount text={placeholder ? null : plural(tokens.length, "token")} className={reveal} />
      </div>

      <form
        id="sc-add-form"
        className="mt-3.5 mb-2 rounded-control border border-border bg-control-a60 p-3.5"
        onSubmit={(e) => {
          e.preventDefault();
          void onAdd();
        }}
      >
        <div className="flex items-center gap-2 max-[600px]:flex-col">
          <Input
            id="sc-token-value"
            type="text"
            aria-label="Discord token"
            placeholder="Paste Discord token…"
            autoComplete="off"
            spellCheck={false}
            className="min-w-0 flex-[2]"
            value={value}
            onChange={(e) => setValue(e.target.value)}
          />
          <Input
            id="sc-token-label"
            aria-label="Token label"
            placeholder="Label (optional)"
            className="min-w-0 flex-1"
            value={label}
            onChange={(e) => setLabel(e.target.value)}
          />
          <Button id="sc-add-submit" type="submit" variant="primary" size="sm" loading={adding}>
            Add
          </Button>
        </div>
      </form>

      <div id="sc-token-list" className={cx("mt-3 max-h-[340px] overflow-y-auto", reveal)}>
        {placeholder ? (
          <TokenListPlaceholder />
        ) : tokens.length === 0 ? (
          <div id="sc-token-empty" className="flex flex-col items-center gap-1 px-4 py-8 text-center text-muted">
            <KeyIcon className="mb-1.5 text-muted opacity-35" />
            <p className="m-0 text-sm">No tokens configured</p>
            <small className="text-xs opacity-65">Add Discord tokens to start scraping members</small>
          </div>
        ) : (
          tokens.map((t) => {
            const busy = busyId === t.token_id;
            return (
              <div key={t.token_id} data-id={t.token_id} className={cx("sc-token-item", TOKEN_ROW)}>
                <div className={TOKEN_MAIN}>
                  <span
                    title={t.is_valid ? "Valid" : "Invalid"}
                    className={cx(
                      TOKEN_BADGE,
                      t.is_valid
                        ? "bg-good-a15 text-good-soft"
                        : "bg-danger-a15 text-danger-soft",
                    )}
                  >
                    {t.is_valid ? <CheckIcon /> : <XIcon />}
                  </span>
                  <div className={TOKEN_TEXT}>
                    <span title={t.username || t.label || "Unlabeled"} className={cx("sc-token-name", TOKEN_NAME)}>{t.username || t.label || "Unlabeled"}</span>
                    <span className={cx("sc-token-masked", TOKEN_MASK)}>{displayMask(t.masked)}</span>
                  </div>
                </div>
                <div className={TOKEN_ACTIONS}>
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
                    className={cx("sc-token-delete", DELETE_BUTTON)}
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
