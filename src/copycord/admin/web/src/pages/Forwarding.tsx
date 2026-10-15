// Forwarding page: the rule list from the old forwarding.html and
// forwarding.js. Search, the total forwarded count, one card per rule with
// its own count, Enable/Disable and Delete (asks first). New Forward, Edit
// and Duplicate open the rule modal (forwarding/RuleModal.tsx) through
// openRuleEditor.
// Element ids match the old page for the e2e specs.

import { useState } from "react";
import { Button, Card } from "../components/ui";
import { StatusGate } from "../components/ui/StatusGate";
import { useLastCount } from "../components/ui/lastCount";
import { useApi } from "../hooks/useApi";
import type { StatusGateState } from "../hooks/useStatusGate";
import { ApiError, errorText } from "../lib/api";
import type {
  ClientGuildsResponse,
  ForwardingCountByRuleResponse,
  ForwardingCountResponse,
  ForwardingListResponse,
  ForwardingRule,
} from "../types/forwarding";
import type { OpenRuleEditor, RuleEditorRequest } from "./forwarding/editor";
import {
  COUNT_BY_RULE_URL,
  COUNT_URL,
  deleteRule,
  filterRules,
  RULES_URL,
  setRuleEnabled,
  toCount,
} from "./forwarding/forwardingApi";
import { RuleCard } from "./forwarding/RuleCard";
import { FWD_EMPTY, FWD_LIST, FWD_SEARCH, FWD_TOOLBAR, RULES_COUNT_KEY, RuleSkeletons } from "./forwarding/ForwardingSkeleton";
import { RuleModal } from "./forwarding/RuleModal";

export { ForwardingSkeleton } from "./forwarding/ForwardingSkeleton";
import { CLIENT_GUILDS_URL } from "./forwarding/ruleForm";
import { useNotify, useSafeConfirm } from "./system/providers";

const NO_VALUE = "—";

// The old loadGuilds: without a working client token /api/client-guilds fails and the page
// stays in its loading state with this message (the old warning toast).
export const CLIENT_TOKEN_MESSAGE =
  "We couldn’t load any guilds yet. It looks like your Discord client token isn’t configured. Open the Configuration page, add your tokens, then come back here.";

function clientTokenGate(error: unknown): StatusGateState {
  let message = "";
  if (error instanceof ApiError) {
    const detail = String((error.body as { detail?: unknown } | null)?.detail ?? "");
    message =
      error.status === 400 || error.status === 404 || detail.startsWith("client-token")
        ? CLIENT_TOKEN_MESSAGE
        : `Failed to load guilds (${error.status}). Please check your configuration and try again.`;
  } else if (error) {
    message = "Failed to load guild list. Check your connection and token configuration, then try reloading.";
  }
  return { gated: message !== "", ready: !message, serverReady: false, clientReady: !message, message };
}

interface RowBusy {
  id: string;
  action: "toggle" | "delete";
}

function failure(what: string, e: unknown): string {
  return errorText(e, what);
}

export interface ForwardingProps {
  /** Replaces the built in rule modal opener (tests). */
  openRuleEditor?: OpenRuleEditor;
}

export function Forwarding({ openRuleEditor: openOverride }: ForwardingProps) {
  const rules = useApi<ForwardingListResponse>(RULES_URL);
  const total = useApi<ForwardingCountResponse>(COUNT_URL);
  const byRule = useApi<ForwardingCountByRuleResponse>(COUNT_BY_RULE_URL);
  const clientGuilds = useApi<ClientGuildsResponse>(CLIENT_GUILDS_URL);
  const tokenGate = clientTokenGate(clientGuilds.error);
  const notify = useNotify();
  const confirm = useSafeConfirm();

  const [query, setQuery] = useState("");
  const [rowBusy, setRowBusy] = useState<RowBusy | null>(null);
  const [editor, setEditor] = useState<RuleEditorRequest | null>(null);
  const openRuleEditor: OpenRuleEditor = openOverride ?? setEditor;

  const loaded = rules.data !== undefined || rules.error !== undefined;
  const items: ForwardingRule[] = Array.isArray(rules.data?.items) ? rules.data.items : [];
  const shown = filterRules(items, query);
  const totalLabel = total.data?.ok ? toCount(total.data.count).toLocaleString() : NO_VALUE;
  const counts = byRule.data?.ok && byRule.data.by_rule && typeof byRule.data.by_rule === "object" ? byRule.data.by_rule : {};

  const replaceItems = (next: ForwardingRule[]) => {
    rules.setData({ ok: true, ...rules.data, items: next });
  };

  async function onToggle(rule: ForwardingRule) {
    setRowBusy({ id: rule.rule_id, action: "toggle" });
    try {
      const reply = await setRuleEnabled(rule, !rule.enabled);
      const saved = reply?.item;
      if (saved) replaceItems(items.map((r) => (r.rule_id === saved.rule_id ? saved : r)));
      else rules.reload();
      notify("Forwarding rule saved.", "success");
    } catch (e) {
      notify(failure("Failed to save forwarding rule", e), "error");
    } finally {
      setRowBusy(null);
    }
  }

  async function onDelete(rule: ForwardingRule) {
    const ok = await confirm({
      title: "Delete forwarding rule",
      body: "Are you sure you want to delete this forwarding rule?",
      confirmLabel: "Delete",
      confirmVariant: "ghost-red",
    });
    if (!ok) return;
    setRowBusy({ id: rule.rule_id, action: "delete" });
    try {
      await deleteRule(rule.rule_id);
      replaceItems(items.filter((r) => r.rule_id !== rule.rule_id));
      notify("Forwarding rule deleted.", "success");
    } catch (e) {
      notify(failure("Failed to delete forwarding rule", e), "error");
    } finally {
      setRowBusy(null);
    }
  }

  // The old saveForwardingRule: swap the saved rule in, or put a new one first; reload when only the id came back.
  function onSaved(saved: ForwardingRule | null) {
    if (!saved) {
      rules.reload();
      return;
    }
    const known = items.some((r) => r.rule_id === saved.rule_id);
    replaceItems(known ? items.map((r) => (r.rule_id === saved.rule_id ? saved : r)) : [saved, ...items]);
  }

  const empty = loaded && items.length === 0;
  useLastCount(RULES_COUNT_KEY, rules.data ? items.length : undefined);

  return (
    <div id="fwd-root" className="forwarding-page w-full">
      <StatusGate gate={tokenGate}>
        <Card className="mt-[18px]">
          <header>
            <h2 className="mb-1 text-[18px] font-bold">Forwarding</h2>
          </header>

          <div className="mt-1.5">
            <div
              id="forwarding-empty"
              hidden={!empty}
              className={FWD_EMPTY}
            >
              <svg
                className="size-12 text-accent"
                xmlns="http://www.w3.org/2000/svg"
                fill="none"
                viewBox="0 0 24 24"
                stroke="currentColor"
                strokeWidth="1.5"
                aria-hidden="true"
              >
                <path strokeLinecap="round" strokeLinejoin="round" d="m3.75 13.5 10.5-11.25L12 10.5h8.25L9.75 21.75 12 13.5H3.75Z" />
              </svg>
              <h3 className="text-base font-semibold text-text">No forwarding rules yet</h3>
              <p className="text-sm text-text-2">Create rules to forward messages to external services.</p>
              <Button id="fwdCreateBtn" variant="primary" className="mt-2" onClick={() => openRuleEditor({ mode: "create" })}>
                + Create your first rule
              </Button>
            </div>

            <div
              id="forwarding-list"
              className={FWD_LIST}
            >
              {!loaded ? <RuleSkeletons /> : null}
              {items.length > 0 ? (
                <>
                  <div className={FWD_TOOLBAR}>
                    <div className="flex flex-col">
                      <div className="flex min-w-0 flex-auto items-center gap-2.5">
                        <input
                          id="fwdSearchInput"
                          type="text"
                          value={query}
                          placeholder="Search rules…"
                          autoComplete="off"
                          spellCheck={false}
                          aria-label="Search forwarding rules"
                          onChange={(e) => setQuery(e.target.value)}
                          onKeyDown={(e) => {
                            if (e.key === "Escape") {
                              e.preventDefault();
                              setQuery("");
                            }
                          }}
                          className={FWD_SEARCH}
                        />
                      </div>
                      <div id="forwarding-count" role="status" aria-live="polite" className="mt-2 mb-3">
                        <span className="opacity-60">Total Forwarded:</span>{" "}
                        <span id="fwdTotalCount" className="opacity-60">
                          {totalLabel}
                        </span>
                      </div>
                    </div>
                    <div className="flex items-center">
                      <Button variant="primary" className="fwd-add-btn" onClick={() => openRuleEditor({ mode: "create" })}>
                        New Forward
                      </Button>
                    </div>
                  </div>

                  {shown.map((rule) => (
                    <RuleCard
                      key={rule.rule_id}
                      rule={rule}
                      count={toCount(counts[rule.rule_id])}
                      busy={rowBusy?.id === rule.rule_id ? rowBusy.action : null}
                      onEdit={() => openRuleEditor({ mode: "edit", rule })}
                      onToggle={() => void onToggle(rule)}
                      onDuplicate={() => openRuleEditor({ mode: "duplicate", rule })}
                      onDelete={() => void onDelete(rule)}
                    />
                  ))}

                  {shown.length === 0 ? (
                    <div
                      role="status"
                      aria-live="polite"
                      className="fwd-no-match col-span-full flex min-h-40 flex-col items-center justify-center gap-1.5 p-2.5 text-center"
                    >
                      <div className="font-bold text-text">No rules matched</div>
                      <div className="text-[13px] text-muted">
                        No rules match <strong>{query}</strong>
                      </div>
                    </div>
                  ) : null}
                </>
              ) : null}
            </div>

            {rules.error ? (
              <p role="alert" className="py-2.5 text-danger">
                Could not load forwarding rules.
              </p>
            ) : null}
          </div>
        </Card>
      </StatusGate>
      {/* After the card so the visible "Forwarding" h2 comes first, as on the old page. */}
      <h1 className="sr-only">Forwarding</h1>
      {openOverride ? null : <RuleModal request={editor} onClose={() => setEditor(null)} onSaved={onSaved} />}
    </div>
  );
}
