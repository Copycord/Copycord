// The three dialogs of the Proxy card: Proxy Settings, the favorites help and
// the test results. Same shell as the log modal: old ids, the .show class,
// close by X, backdrop or Escape. The look is the old .proxy-settings-content
// (480px) with its groups, rows and the ghost Reset button.

import { useEffect, useRef } from "react";
import type { ReactNode } from "react";
import { createPortal } from "react-dom";
import type { ProxySettingKey, ProxyTestResult } from "../../../types/dashboard-proxy";
import { MODAL_BACKDROP, MODAL_CARD, MODAL_CLOSE, MODAL_HEADER, MODAL_OVERLAY, MODAL_TITLE } from "../../../components/Modal";
import { GhostButton } from "../../system/buttons";
import { PS_FIELD_INPUT } from "../parts";
import { maskProxy, PS_DEFAULTS } from "./model";

interface ShellProps {
  id: string;
  closeId: string;
  title: string;
  onClose: () => void;
  children: ReactNode;
}

function Shell({ id, closeId, title, onClose, children }: ShellProps) {
  const closeRef = useRef(onClose);
  closeRef.current = onClose;
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") closeRef.current();
    };
    document.addEventListener("keydown", onKey);
    return () => document.removeEventListener("keydown", onKey);
  }, []);
  return createPortal(
    <div id={id} className={`modal show ${MODAL_OVERLAY}`} aria-hidden="false">
      <div className={`modal-backdrop ${MODAL_BACKDROP}`} onClick={onClose} />
      <div role="dialog" aria-modal="true" aria-label={title} className={`${MODAL_CARD} w-[min(480px,92vw)]`}>
        <div className={`modal-header ${MODAL_HEADER}`}>
          <h4 className={MODAL_TITLE}>{title}</h4>
          <button type="button" id={closeId} aria-label="Close" onClick={onClose} className={`verify-close ${MODAL_CLOSE}`}>
            ✕
          </button>
        </div>
        <div className="min-h-0 overflow-y-auto px-5 pt-1 pb-5">{children}</div>
      </div>
    </div>,
    document.body,
  );
}

// .srv-proxy-hint and .srv-proxy-fav-list.
const HINT = "m-0 mb-2.5 text-xs leading-normal text-muted";

export function FavHelpModal({ onClose }: { onClose: () => void }) {
  return (
    <Shell id="proxy-fav-help-modal" closeId="proxy-fav-help-close" title="Favorite proxies" onClose={onClose}>
      <p className={HINT}>
        Click the <span className="text-[#ffc83d]">★</span> on any proxy to mark it a favorite. Favorites apply{" "}
        <strong>only</strong> to how the client connects to the Discord gateway. Nothing else uses them.
      </p>
      <ul className="mt-2.5 mb-3 list-disc pl-[18px] text-[13px] leading-[1.6] text-muted [&>li]:mb-1">
        <li>The client logs in through a favorite whenever one is available.</li>
        <li>If that proxy drops, it moves to another favorite first.</li>
        <li>With no favorite working, it falls back to a normal proxy so the client stays connected rather than waiting.</li>
        <li>While on a fallback, you can enable the option to return to a favorite if the proxy is working again.</li>
      </ul>
      <p className={HINT}>Starring nothing keeps the old behaviour, where every proxy is treated equally.</p>
    </Shell>
  );
}

interface FieldRowProps {
  id: string;
  label: string;
  sub: string;
  unit: string;
  min: number;
  max: number;
  value: string;
  onChange: (v: string) => void;
  onCommit: () => void;
}

function FieldRow({ id, label, sub, unit, min, max, value, onChange, onCommit }: FieldRowProps) {
  return (
    <label className="flex items-center justify-between gap-4 rounded-input bg-white/[0.02] px-2.5 py-1.5 text-[13px] text-text transition-colors hover:bg-white/[0.04]">
      <div className="flex min-w-0 flex-col gap-px">
        <span className="font-medium">{label}</span>
        <span className="text-[11px] text-muted opacity-70">{sub}</span>
      </div>
      <div className="flex shrink-0 items-center gap-1.5">
        <input
          id={id}
          type="number"
          min={min}
          max={max}
          value={value}
          onChange={(e) => onChange(e.target.value)}
          onBlur={onCommit}
          className={PS_FIELD_INPUT}
        />
        <span className="text-[11px] text-muted">{unit}</span>
      </div>
    </label>
  );
}

function Group({ icon, title, desc, children }: { icon: ReactNode; title: string; desc: ReactNode; children: ReactNode }) {
  return (
    <div className="border-b border-border py-3.5">
      <div className="mb-1 flex items-center gap-2 text-accent">
        <svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
          {icon}
        </svg>
        <h5 className="m-0 text-[13px] font-semibold text-text">{title}</h5>
      </div>
      <p className="m-0 mb-3 text-xs leading-[1.4] text-muted">{desc}</p>
      <div className="flex flex-col gap-3">{children}</div>
    </div>
  );
}

export interface SettingsModalProps {
  values: Record<ProxySettingKey, string>;
  onChange: (key: ProxySettingKey, value: string) => void;
  /** Called on blur or Enter, and once after Reset, with the values to save. */
  onCommit: (next?: Record<ProxySettingKey, string>) => void;
  onClose: () => void;
}

export function SettingsModal({ values, onChange, onCommit, onClose }: SettingsModalProps) {
  const reset = () => {
    const next = Object.fromEntries(
      (Object.keys(PS_DEFAULTS) as ProxySettingKey[]).map((k) => [k, String(PS_DEFAULTS[k])]),
    ) as Record<ProxySettingKey, string>;
    for (const k of Object.keys(next) as ProxySettingKey[]) onChange(k, next[k]);
    onCommit(next);
  };
  return (
    <Shell id="proxy-settings-modal" closeId="proxy-settings-close" title="Proxy Settings" onClose={onClose}>
      <Group
        title="Failover"
        icon={<path d="M22 12h-2.48a2 2 0 0 0-1.93 1.46l-2.35 8.36a.25.25 0 0 1-.48 0L9.24 2.18a.25.25 0 0 0-.48 0l-2.35 8.36A2 2 0 0 1 4.49 12H2" />}
        desc="If your proxy drops or goes down, the client detects it automatically and switches to the next available proxy. A failed proxy is temporarily blocked to avoid reconnecting to it immediately."
      >
        <FieldRow
          id="ps-suspend-duration"
          label="Suspend duration"
          sub="How long a failed proxy stays blocked before retrying"
          unit="sec"
          min={10}
          max={3600}
          value={values.PROXY_SUSPEND_DURATION}
          onChange={(v) => onChange("PROXY_SUSPEND_DURATION", v)}
          onCommit={() => onCommit()}
        />
      </Group>
      <Group
        title="Proxy Testing"
        icon={
          <>
            <path d="m15 6 2 2 4-4" />
            <path d="M2 12h20A10 10 0 1 1 12 2a14.5 14.5 0 0 0 0 20 14.5 14.5 0 0 0 4-10" />
          </>
        }
        desc={
          <>
            When you click &quot;Test All&quot;, every proxy in your list is checked by connecting to Discord through it.
            These settings control how many are tested at once and what counts as too slow.
          </>
        }
      >
        <FieldRow
          id="ps-test-batch"
          label="Batch size"
          sub="Proxies tested at the same time"
          unit="concurrent"
          min={5}
          max={500}
          value={values.PROXY_TEST_BATCH_SIZE}
          onChange={(v) => onChange("PROXY_TEST_BATCH_SIZE", v)}
          onCommit={() => onCommit()}
        />
        <FieldRow
          id="ps-slow-threshold"
          label="Slow threshold"
          sub="Flag proxies slower than this"
          unit="sec"
          min={1}
          max={30}
          value={values.PROXY_SLOW_THRESHOLD}
          onChange={(v) => onChange("PROXY_SLOW_THRESHOLD", v)}
          onCommit={() => onCommit()}
        />
      </Group>
      <div className="mt-4 flex justify-end">
        <button
          type="button"
          id="ps-reset-defaults"
          onClick={reset}
          className="inline-flex cursor-pointer items-center gap-1.5 rounded-input border border-white/[0.08] bg-white/[0.03] px-3.5 py-1.5 text-xs font-medium text-muted transition-colors hover:border-white/15 hover:bg-white/[0.06] hover:text-text"
        >
          <svg xmlns="http://www.w3.org/2000/svg" width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
            <path d="M3 12a9 9 0 1 0 9-9 9.75 9.75 0 0 0-6.74 2.74L3 8" />
            <path d="M3 3v5h5" />
          </svg>
          Reset to Defaults
        </button>
      </div>
    </Shell>
  );
}

function ResultRows({ rows, tone }: { rows: ProxyTestResult[]; tone: "fail" | "slow" }) {
  const color = tone === "fail" ? "text-[#ff6b6b]" : "text-[#ff9800]";
  return (
    <>
      {rows.map((r) => (
        <div key={r.proxy} className="flex items-center gap-2 py-1 font-mono text-xs">
          <span className={color}>{tone === "fail" ? "✗" : "⚠"}</span>
          <span className="min-w-0 flex-1 truncate">{maskProxy(r.proxy)}</span>
          <span className={`${color} text-[11px]`}>{r.ok ? `${r.ms}ms` : r.error || "failed"}</span>
        </div>
      ))}
    </>
  );
}

export interface ResultsModalProps {
  good: number;
  slow: ProxyTestResult[];
  failed: ProxyTestResult[];
  slowSec: number;
  removeFailed: boolean;
  removeSlow: boolean;
  onRemoveFailed: (v: boolean) => void;
  onRemoveSlow: (v: boolean) => void;
  onConfirm: () => void;
  onClose: () => void;
}

export function ResultsModal(p: ResultsModalProps) {
  return (
    <Shell id="proxy-results-modal" closeId="proxy-results-close" title="Proxy Test Results" onClose={p.onClose}>
      <div className="flex flex-col gap-4 pt-3">
        <div className="flex flex-wrap gap-2.5">
          <Stat n={p.good} label="healthy" cls="bg-[rgba(76,175,80,0.1)] text-[#4caf50]" />
          {p.slow.length ? <Stat n={p.slow.length} label="slow" cls="bg-[rgba(255,152,0,0.1)] text-[#ff9800]" /> : null}
          {p.failed.length ? (
            <Stat n={p.failed.length} label="failed" cls="bg-[rgba(255,80,80,0.1)] text-[#ff6b6b]" />
          ) : null}
        </div>
        <div className="max-h-60 overflow-y-auto">
          {p.failed.length ? (
            <>
              <div className="mb-1 text-[11px] font-semibold uppercase tracking-wide text-[#ff6b6b]">Failed</div>
              <ResultRows rows={p.failed} tone="fail" />
            </>
          ) : null}
          {p.slow.length ? (
            <>
              <div className="mb-1 mt-2.5 text-[11px] font-semibold uppercase tracking-wide text-[#ff9800]">
                Slow (&gt;{p.slowSec}s)
              </div>
              <ResultRows rows={p.slow} tone="slow" />
            </>
          ) : null}
        </div>
        <div className="flex flex-col gap-2 text-sm">
          {p.failed.length ? (
            <label className="flex cursor-pointer items-center gap-2">
              <input
                id="ptr-remove-failed"
                type="checkbox"
                checked={p.removeFailed}
                onChange={(e) => p.onRemoveFailed(e.target.checked)}
              />
              <span>
                Remove <strong>{p.failed.length}</strong> failed {p.failed.length === 1 ? "proxy" : "proxies"}
              </span>
            </label>
          ) : null}
          {p.slow.length ? (
            <label className="flex cursor-pointer items-center gap-2">
              <input
                id="ptr-remove-slow"
                type="checkbox"
                checked={p.removeSlow}
                onChange={(e) => p.onRemoveSlow(e.target.checked)}
              />
              <span>
                Remove <strong>{p.slow.length}</strong> slow {p.slow.length === 1 ? "proxy" : "proxies"}
              </span>
            </label>
          ) : null}
        </div>
        <div className="flex justify-end gap-2">
          <GhostButton id="ptr-cancel" compact onClick={p.onClose}>
            Cancel
          </GhostButton>
          <GhostButton id="ptr-confirm" compact danger onClick={p.onConfirm}>
            Remove Selected
          </GhostButton>
        </div>
      </div>
    </Shell>
  );
}

function Stat({ n, label, cls }: { n: number; label: string; cls: string }) {
  return (
    <div className={`min-w-[70px] flex-1 rounded-[10px] px-2 py-2.5 text-center ${cls}`}>
      <div className="text-[22px] font-bold">{n}</div>
      <div className="text-[11px] text-muted">{label}</div>
    </div>
  );
}
