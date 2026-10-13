// Forwarding page: the rule list from the old forwarding.html and
// forwarding.js. Search, the total forwarded count, one card per rule with
// its own count, Enable/Disable and Delete (asks first). The create/edit
// modal is plan lane 10: New Forward, Edit and Duplicate go through
// openRuleEditor, a no-op stub for now (forwarding/editor.ts).
// Element ids match the old page for the e2e specs.

import { useState } from "react";
import { Button, Card, cx } from "../components/ui";
import { useApi } from "../hooks/useApi";
import { ApiError } from "../lib/api";
import type {
  ForwardingCountByRuleResponse,
  ForwardingCountResponse,
  ForwardingListResponse,
  ForwardingRule,
} from "../types/forwarding";
import { openRuleEditorStub } from "./forwarding/editor";
import type { OpenRuleEditor } from "./forwarding/editor";
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
import { useNotify, useSafeConfirm } from "./system/providers";

const NO_VALUE = "—";

interface RowBusy {
  id: string;
  action: "toggle" | "delete";
}

function failure(what: string, e: unknown): string {
  if (e instanceof ApiError) return `${what} (${e.status})`;
  return `${what} (network error)`;
}

function Skeletons() {
  return (
    <>
      {[0, 1, 2].map((i) => (
        <div
          key={i}
          data-testid="fwd-skeleton"
          aria-hidden="true"
          className="h-[118px] animate-pulse rounded-[14px] border border-border bg-white/4"
        />
      ))}
    </>
  );
}

export interface ForwardingProps {
  /** Lane 10 passes the real modal opener here. */
  openRuleEditor?: OpenRuleEditor;
}

export function Forwarding({ openRuleEditor = openRuleEditorStub }: ForwardingProps) {
  const rules = useApi<ForwardingListResponse>(RULES_URL);
  const total = useApi<ForwardingCountResponse>(COUNT_URL);
  const byRule = useApi<ForwardingCountByRuleResponse>(COUNT_BY_RULE_URL);
  const notify = useNotify();
  const confirm = useSafeConfirm();

  const [query, setQuery] = useState("");
  const [rowBusy, setRowBusy] = useState<RowBusy | null>(null);

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
      danger: true,
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

  const empty = loaded && items.length === 0;

  return (
    <div id="fwd-root" className="forwarding-page w-full px-3 pt-3.5 md:px-5 md:pt-0">
      <h1 className="sr-only">Forwarding</h1>
      <Card className="md:mt-[18px]">
        <header className="mb-3">
          <h2 className="text-[1.17em] font-bold">Forwarding</h2>
        </header>

        <div>
          <div
            id="forwarding-empty"
            hidden={!empty}
            className="flex flex-col items-center justify-center gap-2 px-6 py-10 text-center"
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
            className={cx(
              "fwd-list grid grid-cols-1 items-start gap-3.5",
              "min-[701px]:grid-cols-[repeat(2,minmax(260px,1fr))]",
              "min-[1101px]:grid-cols-[repeat(3,minmax(260px,1fr))]",
              "min-[1501px]:grid-cols-[repeat(4,minmax(260px,1fr))]",
            )}
          >
            {!loaded ? <Skeletons /> : null}
            {items.length > 0 ? (
              <>
                <div className="col-span-full mb-2 flex items-start justify-between gap-3">
                  <div className="flex min-w-0 flex-1 flex-col">
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
                      className={cx(
                        "w-[min(420px,100%)] rounded-control border border-border bg-bg-2 px-3 py-2 text-sm text-text",
                        "placeholder:text-text-2 outline-none focus:border-primary",
                      )}
                    />
                    <div id="forwarding-count" role="status" aria-live="polite" className="mt-2 mb-3 opacity-60">
                      <span>Total Forwarded:</span> <span id="fwdTotalCount">{totalLabel}</span>
                    </div>
                  </div>
                  <Button variant="primary" className="fwd-add-btn" onClick={() => openRuleEditor({ mode: "create" })}>
                    New Forward
                  </Button>
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
    </div>
  );
}
