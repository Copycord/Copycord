// Provider half of the rule modal: the old custom provider dropdown
// (#providerSelectBtn / #providerSelectMenu) and the three field groups
// (#provider_pushover, #provider_discord, #provider_telegram).

import { useEffect, useRef, useState } from "react";
import type { KeyboardEvent } from "react";
import { cx, Field, Input } from "../../components/ui";
import { PROVIDERS } from "./ruleForm";
import type { RuleForm } from "./ruleForm";
import { ProviderIcon } from "./RuleCard";

type Patch = (next: Partial<RuleForm>) => void;

// .provider-opt-icon: an 18 px box, kept when nothing is chosen so the button is 36 px tall.
const OPT_ICON = "provider-opt-icon inline-flex size-[18px] shrink-0 items-center justify-center [&>svg]:size-[18px]";

export function ProviderPicker({ value, onChange }: { value: string; onChange: (provider: string) => void }) {
  const [open, setOpen] = useState(false);
  const wrapRef = useRef<HTMLDivElement>(null);
  const btnRef = useRef<HTMLButtonElement>(null);
  const menuRef = useRef<HTMLDivElement>(null);
  const selected = PROVIDERS.find((p) => p.value === value);

  useEffect(() => {
    if (!open) return;
    const onDown = (e: MouseEvent) => {
      if (!wrapRef.current?.contains(e.target as Node)) setOpen(false);
    };
    menuRef.current?.querySelector<HTMLButtonElement>('[aria-selected="true"], [role=option]')?.focus();
    document.addEventListener("mousedown", onDown);
    return () => document.removeEventListener("mousedown", onDown);
  }, [open]);

  const choose = (v: string) => {
    onChange(v);
    setOpen(false);
    btnRef.current?.focus();
  };

  const onMenuKey = (e: KeyboardEvent<HTMLDivElement>) => {
    const items = Array.from(e.currentTarget.querySelectorAll<HTMLButtonElement>("[role=option]"));
    const at = items.indexOf(document.activeElement as HTMLButtonElement);
    if (e.key === "Escape") {
      e.preventDefault();
      e.stopPropagation();
      setOpen(false);
      btnRef.current?.focus();
    } else if (e.key === "ArrowDown" || e.key === "ArrowUp") {
      e.preventDefault();
      const step = e.key === "ArrowDown" ? 1 : -1;
      items[(at + step + items.length) % items.length]?.focus();
    }
  };

  return (
    <div ref={wrapRef} className="provider-select-wrap relative w-[240px] max-w-full">
      <input type="hidden" id="fwd_provider" value={value} />
      <button
        ref={btnRef}
        type="button"
        id="providerSelectBtn"
        aria-haspopup="listbox"
        aria-expanded={open}
        aria-labelledby="fwd_provider_label providerSelectBtn"
        onClick={() => setOpen((o) => !o)}
        onKeyDown={(e) => {
          if (e.key === "ArrowDown" && !open) {
            e.preventDefault();
            setOpen(true);
          }
        }}
        className={cx(
          "provider-select-btn input flex w-full max-w-[240px] min-w-[160px] cursor-pointer items-center gap-2.5 rounded-btn border border-border-input bg-control px-2.5 py-2 text-left font-sans text-[13px] text-text",
          "outline-none focus:border-accent",
        )}
      >
        <span className={cx(OPT_ICON, !selected && "invisible")} aria-hidden="true">
          {selected ? <ProviderIcon provider={selected.value} /> : null}
        </span>
        <span className="provider-opt-label flex-1">{selected ? selected.label : "Choose provider…"}</span>
        <span className="provider-opt-caret text-text-2" aria-hidden="true">
          ▾
        </span>
      </button>
      {open ? (
        <div
          id="providerSelectMenu"
          role="listbox"
          tabIndex={-1}
          aria-label="Provider"
          onKeyDown={onMenuKey}
          ref={menuRef}
          className="provider-select-menu absolute inset-x-0 top-full z-10 mt-1 overflow-hidden rounded-control border border-border bg-card-solid py-1 shadow-card"
        >
          {PROVIDERS.map((p) => (
            <button
              key={p.value}
              type="button"
              role="option"
              aria-selected={p.value === value}
              data-value={p.value}
              onClick={() => choose(p.value)}
              className={cx(
                "provider-opt flex w-full cursor-pointer items-center gap-2 px-3 py-2 text-left text-sm text-text outline-none",
                "hover:bg-white/6 focus-visible:bg-white/8",
                p.value === value && "bg-primary/20",
              )}
            >
              <span className={OPT_ICON} aria-hidden="true">
                <ProviderIcon provider={p.value} />
              </span>
              <span className="provider-opt-label">{p.label}</span>
            </button>
          ))}
        </div>
      ) : null}
    </div>
  );
}

// .form-row of two .form-group cells, and a .form-group on its own.
const ROW = "form-row flex flex-wrap gap-2.5";
const CELL = "form-group flex-[1_1_200px] gap-1!";
const GROUP = "form-group mb-3 gap-1! [&>p]:my-3";

export function PushoverFields({ form, patch }: { form: RuleForm; patch: Patch }) {
  return (
    <div id="provider_pushover" className={cx("provider-fields", ROW)}>
      <Field label="App token" id="pushover_app_token" className={CELL}>
        <Input
          type="text"
          autoComplete="off"
          spellCheck={false}
          placeholder="Pushover application token"
          value={form.pushover_app_token}
          onChange={(e) => patch({ pushover_app_token: e.target.value })}
        />
      </Field>
      <Field label="User / group key" id="pushover_user_key" className={CELL}>
        <Input
          type="text"
          autoComplete="off"
          spellCheck={false}
          placeholder="User or group key"
          value={form.pushover_user_key}
          onChange={(e) => patch({ pushover_user_key: e.target.value })}
        />
      </Field>
    </div>
  );
}

export function TelegramFields({ form, patch }: { form: RuleForm; patch: Patch }) {
  return (
    <div id="provider_telegram" className={cx("provider-fields", ROW)}>
      <Field label="Bot token" id="telegram_bot_token" className={CELL}>
        <Input
          type="text"
          autoComplete="off"
          spellCheck={false}
          placeholder="123456:ABC-DEF..."
          value={form.telegram_bot_token}
          onChange={(e) => patch({ telegram_bot_token: e.target.value })}
        />
      </Field>
      <Field label="Chat ID" id="telegram_chat_id" className={CELL}>
        <Input
          type="text"
          autoComplete="off"
          spellCheck={false}
          placeholder="Chat or channel ID"
          value={form.telegram_chat_id}
          onChange={(e) => patch({ telegram_chat_id: e.target.value })}
        />
      </Field>
    </div>
  );
}

/**
 * Webhook URL chips. ChipInput caps words at 100 characters and webhook URLs
 * run longer, so this keeps the old chip rules (Enter, comma or blur commits,
 * Backspace on empty drops the last one) without the cap.
 */
function UrlChips({ value, onChange }: { value: string[]; onChange: (urls: string[]) => void }) {
  const [text, setText] = useState("");
  const entryRef = useRef<HTMLInputElement>(null);

  const commit = (raw: string) => {
    const next = [...value];
    for (const part of raw.split(",").map((s) => s.trim()).filter(Boolean)) {
      if (!next.includes(part)) next.push(part);
    }
    if (next.length !== value.length) onChange(next);
    setText("");
  };

  return (
    <div
      data-chip-input="discord_webhook_urls"
      onClick={(e) => {
        if (e.target === e.currentTarget) entryRef.current?.focus();
      }}
      className={cx(
        "chip-input-wrap flex min-h-[46px] cursor-text flex-wrap items-center gap-1.5 rounded-control border border-border-input px-3 py-2",
        "bg-linear-to-b from-control-top to-control transition-[border-color,box-shadow] duration-200 hover:border-accent/30 focus-within:border-accent",
      )}
    >
      <ul className="contents" aria-label="Webhook URLs">
        {value.map((url) => (
          <li key={url} className="contents">
            <button
              type="button"
              aria-label={`Remove ${url}`}
              title={url}
              onClick={() => {
                onChange(value.filter((v) => v !== url));
                entryRef.current?.focus();
              }}
              className="chip max-w-full cursor-pointer truncate rounded-[6px] border border-transparent bg-primary-a15 py-1 pr-2 pl-2.5 text-[13px] leading-snug text-text outline-none hover:bg-danger/10 focus-visible:ring-2 focus-visible:ring-accent/60"
            >
              {url}
            </button>
          </li>
        ))}
      </ul>
      <input
        ref={entryRef}
        id="discord_webhook_urls"
        type="text"
        autoComplete="off"
        spellCheck={false}
        value={text}
        placeholder={value.length ? undefined : "Paste a webhook URL and press Enter..."}
        aria-describedby="discord_webhook_urls-help"
        onChange={(e) => setText(e.target.value)}
        onBlur={() => commit(text)}
        onKeyDown={(e) => {
          if (e.key === "Enter" || e.key === ",") {
            e.preventDefault();
            commit(text);
          } else if (e.key === "Backspace" && text === "" && value.length) {
            e.preventDefault();
            onChange(value.slice(0, -1));
          }
        }}
        className="chip-text-input min-w-[120px] flex-1 bg-transparent py-1 text-sm text-text outline-none placeholder:text-muted-a40"
      />
    </div>
  );
}

export function DiscordFields({ form, patch }: { form: RuleForm; patch: Patch }) {
  return (
    <div id="provider_discord" className="provider-fields">
      <div className={cx("flex flex-col", GROUP)}>
        <label htmlFor="discord_webhook_urls" className="form-label ml-0.5 text-xs text-muted">
          Discord webhook URLs
        </label>
        <UrlChips value={form.discord_urls} onChange={(discord_urls) => patch({ discord_urls })} />
        <p id="discord_webhook_urls-help" className="help-text text-xs text-muted opacity-85">
          Copycord forwards the matched message to every webhook URL listed here.
        </p>
      </div>
      <Field label="Bot Username (optional)" id="discord_webhook_username" className={GROUP}>
        <Input
          type="text"
          autoComplete="off"
          placeholder="Override webhook username (e.g., Copycord)"
          value={form.discord_username}
          onChange={(e) => patch({ discord_username: e.target.value })}
        />
      </Field>
      <Field label="Bot Avatar URL (optional)" id="discord_webhook_avatar_url" className={GROUP}>
        <Input
          type="text"
          autoComplete="off"
          placeholder="https://… (image URL)"
          value={form.discord_avatar_url}
          onChange={(e) => patch({ discord_avatar_url: e.target.value })}
        />
      </Field>
    </div>
  );
}

export function ProviderFields({ form, patch }: { form: RuleForm; patch: Patch }) {
  if (form.provider === "pushover") return <PushoverFields form={form} patch={patch} />;
  if (form.provider === "discord") return <DiscordFields form={form} patch={patch} />;
  if (form.provider === "telegram") return <TelegramFields form={form} patch={patch} />;
  return null;
}
