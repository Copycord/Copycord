// Start form of Scrape members: one of the servers the scraper tokens are in (from the type's options,
// with a filter box once the list is long) or a server ID typed in, what to include for each member,
// and whether to use the saved proxies. Tokens are not checked here: one that fails or is not in the
// server is skipped when the run starts. With no tokens saved the form points at the settings card.

import { useState } from "react";
import type { FormEvent } from "react";
import { Modal } from "../../../../components/Modal";
import { Button } from "../../../../components/ui/Button";
import { EmptyState } from "../../../../components/ui/EmptyState";
import { Field, FieldSelect, Input } from "../../../../components/ui/Field";
import { Toggle } from "../../../../components/ui/Toggle";
import { useApi } from "../../../../hooks/useApi";
import { formatCount } from "../../StepList";
import { FILTER_FROM, filterGuilds } from "../download_avatars/AvatarsForm";
import { useStartRun, type TaskFormProps } from "../start";
import { SETTINGS_ID } from "./ScraperSettingsCard";

export const SCRAPE_EXPLAINER =
  "Lists the members of a server with the saved scraper tokens and saves them to a JSON file. " +
  "A token that does not work or is not in the server is skipped.";

export interface ScrapeGuild {
  id: string;
  name: string;
  tokens?: number;
}

export interface ScrapeOptions {
  guilds?: ScrapeGuild[];
  /** {total, skipped}; a bare count is read the same. */
  tokens?: { total?: number; skipped?: number; valid?: number } | number;
  proxies?: number;
  error?: string;
}

/** How many scraper tokens are saved, by whichever count the options carry. */
export function tokenTotal(opts: ScrapeOptions | undefined): number {
  const t = opts?.tokens;
  if (typeof t === "number") return t;
  return t?.total ?? t?.valid ?? 0;
}

export const INCLUDE_FIELDS = [
  { key: "include_username", label: "Username", initial: true },
  { key: "include_avatar_url", label: "Avatar URL", initial: true },
  { key: "include_bio", label: "Bio", initial: false, help: "Needs an extra call per member, so the scrape is slower." },
  { key: "include_roles", label: "Roles", initial: false },
] as const;

type IncludeKey = (typeof INCLUDE_FIELDS)[number]["key"];

/** "Enter a numeric server ID." unless the text is all digits (blank is fine: the picker is used). */
export function guildIdError(text: string): string | undefined {
  const t = text.trim();
  return !t || /^\d+$/.test(t) ? undefined : "Enter a numeric server ID.";
}

/** "Server name (2 tokens)". */
export function guildOptionLabel(g: ScrapeGuild): string {
  if (g.tokens == null) return g.name;
  return `${g.name} (${formatCount(g.tokens)} ${g.tokens === 1 ? "token" : "tokens"})`;
}

function serverText(msg: string): string {
  return msg.replace(/^Value error, /, "");
}

export function ScrapeMembersForm({ type, open, onClose }: TaskFormProps) {
  const opts = useApi<ScrapeOptions>(open ? `/api/tasks/types/${type.key}/options` : null);
  const guilds = opts.data?.guilds ?? [];
  const pending = open && !opts.data && !opts.error;
  const noTokens = Boolean(opts.data) && tokenTotal(opts.data) === 0;
  const proxies = opts.data?.proxies ?? 0;
  const loadError = opts.data?.error || (opts.error ? "Could not load the servers" : undefined);
  const [guildId, setGuildId] = useState("");
  const [typedId, setTypedId] = useState("");
  const [filter, setFilter] = useState("");
  const [include, setInclude] = useState<Record<IncludeKey, boolean>>(
    () => Object.fromEntries(INCLUDE_FIELDS.map((f) => [f.key, f.initial])) as Record<IncludeKey, boolean>,
  );
  const [useProxies, setUseProxies] = useState(true);
  const [errors, setErrors] = useState<Record<string, string>>({});
  const { start, busy } = useStartRun(type);

  const changed = (fn: () => void) => {
    fn();
    setErrors({});
  };

  const shown = filterGuilds(guilds, filter);
  const chosen = guilds.find((g) => g.id === guildId);
  const listed = chosen && !shown.includes(chosen) ? [chosen, ...shown] : shown;

  const onSubmit = async (e: FormEvent) => {
    e.preventDefault();
    const found: Record<string, string> = {};
    const typed = typedId.trim();
    const badId = guildIdError(typed);
    if (badId) found.guild_id = badId;
    else if (!typed && !guildId) found.guild_id = "Pick a server or enter a server ID.";
    setErrors(found);
    if (Object.keys(found).length) return;
    const res = await start({ guild_id: typed || guildId, ...include, use_proxies: useProxies });
    if (res && Object.keys(res).length) setErrors(Object.fromEntries(Object.entries(res).map(([k, v]) => [k, serverText(v)])));
    else if (!res) onClose();
  };

  const toSettings = () => {
    onClose();
    document.getElementById(SETTINGS_ID)?.scrollIntoView({ behavior: "smooth", block: "start" });
  };

  const footer = noTokens ? (
    <Button variant="ghost" onClick={onClose}>
      Close
    </Button>
  ) : (
    <>
      <Button variant="ghost" onClick={onClose}>
        Cancel
      </Button>
      <Button variant="primary" type="submit" form="scrape-form" loading={busy}>
        Scrape members
      </Button>
    </>
  );

  return (
    <Modal open={open} onClose={onClose} title={`Run ${type.title}`} className="w-[min(560px,92vw)]" footer={footer}>
      {noTokens ? (
        <EmptyState
          title="No scraper tokens saved"
          description="Add a Discord token under Scraper tokens and proxies on this page, then start the scrape."
          action={
            <Button variant="primary" onClick={toSettings}>
              Go to scraper tokens
            </Button>
          }
        />
      ) : (
        <form id="scrape-form" noValidate onSubmit={(e) => void onSubmit(e)} className="flex flex-col gap-4">
          <p className="m-0 text-sm text-muted">{SCRAPE_EXPLAINER}</p>

          {guilds.length > FILTER_FROM ? (
            <Field label="Find a server">
              <Input type="search" autoComplete="off" placeholder="Server name or ID" value={filter} onChange={(e) => setFilter(e.target.value)} />
            </Field>
          ) : null}

          <Field label="Server" help={loadError ? undefined : "Servers the scraper tokens are in."} error={typedId.trim() ? undefined : errors.guild_id || loadError}>
            <FieldSelect
              value={guildId}
              onChange={(v) =>
                changed(() => {
                  setGuildId(v);
                  setTypedId("");
                })
              }
              pending={pending}
              pendingLabel="Pick a server"
              options={[
                { value: "", label: listed.length || !filter ? "Pick a server" : "No server matches", disabled: true },
                ...listed.map((g) => ({ value: g.id, label: guildOptionLabel(g) })),
              ]}
            />
          </Field>

          <Field label="Or a server ID" help="For a server not listed. Tokens not in it are skipped." error={typedId.trim() ? errors.guild_id : undefined}>
            <Input
              inputMode="numeric"
              autoComplete="off"
              placeholder="123456789012345678"
              value={typedId}
              onChange={(e) =>
                changed(() => {
                  setTypedId(e.target.value);
                  if (e.target.value.trim()) setGuildId("");
                })
              }
            />
          </Field>

          <fieldset className="m-0 flex flex-col gap-3 border-0 p-0">
            <legend className="mb-2 text-sm font-semibold text-text-1">Include for each member</legend>
            {INCLUDE_FIELDS.map((f) => (
              <Toggle
                key={f.key}
                label={f.label}
                description={"help" in f ? f.help : undefined}
                checked={include[f.key]}
                onChange={(v) => changed(() => setInclude((prev) => ({ ...prev, [f.key]: v })))}
              />
            ))}
          </fieldset>

          <Toggle
            label="Use proxies"
            description={
              pending ? "Spreads the requests over the saved proxies." : proxies ? `${formatCount(proxies)} saved ${proxies === 1 ? "proxy" : "proxies"}.` : "No proxies saved, so none are used."
            }
            checked={useProxies}
            onChange={(v) => changed(() => setUseProxies(v))}
          />

          {errors.inputs || errors.general ? (
            <p role="alert" className="m-0 text-xs text-danger">
              {errors.inputs || errors.general}
            </p>
          ) : null}
        </form>
      )}
    </Modal>
  );
}
