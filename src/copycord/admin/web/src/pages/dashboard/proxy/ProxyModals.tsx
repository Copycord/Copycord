// The three dialogs of the Proxy card: Proxy Settings, the favorites help and
// the test results. Same shell as the log modal: old ids, the .show class,
// close by X, backdrop or Escape.

import { useEffect, useRef } from "react";
import type { ReactNode } from "react";
import { createPortal } from "react-dom";
import type { ProxySettingKey, ProxyTestResult } from "../../../types/dashboard-proxy";
import { GhostButton } from "../../system/buttons";
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
    <div id={id} className="modal show fixed inset-0 z-40 flex items-center justify-center p-4" aria-hidden="false">
      <div className="modal-backdrop absolute inset-0 bg-black/60" onClick={onClose} />
      <div
        role="dialog"
        aria-modal="true"
        aria-label={title}
        className="relative max-h-[85vh] w-full max-w-lg overflow-y-auto rounded-2xl border border-border bg-card-solid text-text shadow-card"
      >
        <div className="flex items-center justify-between border-b border-border px-4 py-3">
          <h4 className="text-base font-semibold">{title}</h4>
          <button
            type="button"
            id={closeId}
            aria-label="Close"
            onClick={onClose}
            className="verify-close rounded-control px-2 py-1 text-muted hover:text-text"
          >
            ✕
          </button>
        </div>
        <div className="flex flex-col gap-4 p-4">{children}</div>
      </div>
    </div>,
    document.body,
  );
}

const HINT = "text-xs leading-relaxed text-muted";

export function FavHelpModal({ onClose }: { onClose: () => void }) {
  return (
    <Shell id="proxy-fav-help-modal" closeId="proxy-fav-help-close" title="Favorite proxies" onClose={onClose}>
      <p className={HINT}>
        Click the <span className="text-custom-ink">★</span> on any proxy to mark it a favorite. Favorites apply{" "}
        <strong>only</strong> to how the client connects to the Discord gateway. Nothing else uses them.
      </p>
      <ul className={`${HINT} list-disc pl-5`}>
        <li>The client logs in through a favorite whenever one is available.</li>
        <li>If that proxy drops, it moves to another favorite first.</li>
        <li>With no favorite working, it falls back to a normal proxy so the client stays connected rather than waiting.</li>
        <li>While on a fallback, you can enable the option to return to a favorite if the proxy is working again.</li>
      </ul>
      <p className={HINT}>Starring nothing keeps the old behaviour, where every proxy is treated equally.</p>
    </Shell>
  );
}

const INPUT =
  "w-24 rounded-control border border-border bg-bg-2 px-3 py-1.5 text-sm text-text outline-none focus:border-accent";

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
    <label className="flex items-center justify-between gap-3">
      <span className="flex flex-col">
        <span className="text-sm font-semibold">{label}</span>
        <span className={HINT}>{sub}</span>
      </span>
      <span className="flex items-center gap-2">
        <input
          id={id}
          type="number"
          min={min}
          max={max}
          value={value}
          onChange={(e) => onChange(e.target.value)}
          onBlur={onCommit}
          className={INPUT}
        />
        <span className={HINT}>{unit}</span>
      </span>
    </label>
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
      <section className="flex flex-col gap-3">
        <h5 className="text-sm font-semibold text-accent">Failover</h5>
        <p className={HINT}>
          If your proxy drops or goes down, the client detects it automatically and switches to the next available
          proxy. A failed proxy is temporarily blocked to avoid reconnecting to it immediately.
        </p>
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
      </section>
      <section className="flex flex-col gap-3">
        <h5 className="text-sm font-semibold text-accent">Proxy Testing</h5>
        <p className={HINT}>
          When you click &quot;Test All&quot;, every proxy in your list is checked by connecting to Discord through it.
          These settings control how many are tested at once and what counts as too slow.
        </p>
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
      </section>
      <div className="flex justify-end">
        <GhostButton id="ps-reset-defaults" compact onClick={reset}>
          Reset to Defaults
        </GhostButton>
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
