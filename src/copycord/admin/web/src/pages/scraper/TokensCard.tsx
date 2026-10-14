// Tokens card: list, add (value + optional label), re-validate and delete.
// Only masked values are ever rendered; the value input clears after a save.

import { useState } from "react";
import { Button, Card, cx, Input } from "../../components/ui";
import type { ScraperToken } from "../../types/scraper";
import type { Confirm, Notify } from "../system/providers";
import { CheckIcon, KeyIcon, RefreshIcon, TrashIcon, XIcon } from "./icons";
import { displayMask } from "./mask";
import { addToken, deleteToken, plural, reason, validateToken } from "./scraperApi";

// .scraper-card .card-titlebar h3 and the .card-titlebar row; every scraper card sits 18px below the last.
export const TITLE = "m-0 text-[15px] font-bold whitespace-nowrap text-accent";
export const TITLEBAR = "mb-1.5 flex items-center justify-between gap-2.5";
export const SC_CARD = "mt-[18px] max-w-full";

// .btn.btn-ghost.btn-xs holding a 14px icon.
export const ICON_BUTTON =
  "inline-flex min-h-7 cursor-pointer items-center justify-center rounded-btn border border-border bg-[rgba(255,255,255,0.04)] " +
  "px-2.5 py-1.5 leading-none " +
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

  return (
    <Card id="sc-tokens-card" aria-label="Tokens" className={cx(SC_CARD, "min-w-0")}>
      <div className={TITLEBAR}>
        <h3 className={TITLE}>Tokens</h3>
        <span
          id="sc-token-count"
          className="rounded-xl border border-[rgba(139,92,246,0.25)] bg-[rgba(139,92,246,0.12)] px-2.5 py-[3px] text-[11px] font-semibold whitespace-nowrap text-primary"
        >
          {plural(tokens.length, "token")}
        </span>
      </div>

      <form
        id="sc-add-form"
        className="mt-3.5 mb-2 rounded-control border border-border bg-[rgba(20,11,38,0.6)] p-3.5"
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

      <div id="sc-token-list" className="mt-3 max-h-[340px] overflow-y-auto [scrollbar-color:var(--color-accent)_transparent] [scrollbar-width:thin]">
        {tokens.length === 0 ? (
          <div id="sc-token-empty" className="flex flex-col items-center gap-1 px-4 py-8 text-center text-muted">
            <KeyIcon className="mb-1.5 text-muted opacity-35" />
            <p className="m-0 text-sm">No tokens configured</p>
            <small className="text-xs opacity-65">Add Discord tokens to start scraping members</small>
          </div>
        ) : (
          tokens.map((t) => {
            const busy = busyId === t.token_id;
            return (
              <div
                key={t.token_id}
                data-id={t.token_id}
                className="sc-token-item mb-1.5 flex items-center justify-between gap-2.5 rounded-btn border border-border bg-[rgba(20,11,38,0.4)] px-3 py-2.5 transition-colors duration-200 last:mb-0 hover:border-[rgba(139,92,246,0.25)] hover:bg-[rgba(20,11,38,0.7)]"
              >
                <div className="flex min-w-0 items-center gap-2.5">
                  <span
                    title={t.is_valid ? "Valid" : "Invalid"}
                    className={cx(
                      "inline-flex size-7 shrink-0 items-center justify-center rounded-lg border",
                      t.is_valid
                        ? "border-[rgba(16,185,129,0.3)] bg-[rgba(16,185,129,0.15)] text-[#34d399]"
                        : "border-[rgba(239,68,68,0.3)] bg-[rgba(239,68,68,0.15)] text-[#f87171]",
                    )}
                  >
                    {t.is_valid ? <CheckIcon /> : <XIcon />}
                  </span>
                  <div className="flex min-w-0 flex-col gap-0.5">
                    <span className="sc-token-name truncate text-[13px] font-semibold text-text">{t.username || t.label || "Unlabeled"}</span>
                    <span className="sc-token-masked truncate font-mono text-[11px] text-muted">{displayMask(t.masked)}</span>
                  </div>
                </div>
                <div className="flex shrink-0 gap-1">
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
