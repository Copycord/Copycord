// The Proxy card from index.html: enable toggle, proxy chips saved on a short
// delay, favorites, rotation and favorite re-check, Test All with live progress
// from the bus (proxy_test messages), the Settings modal and the favorites help.

import { useCallback, useEffect, useRef, useState } from "react";
import { cx } from "../../components/ui";
import { useWsMessages } from "../../hooks/useWsMessages";
import type { UseWsOptions } from "../../hooks/useWsMessages";
import type {
  IntervalUnit,
  ProxyChipResult,
  ProxySettingKey,
  ProxyTestEvent,
  ProxyTestResult,
} from "../../types/dashboard-proxy";
import { useNotify, useSafeConfirm } from "../system/providers";
import { CARD, Chevron, DocLink, GHOST_XS_GREEN, GHOST_XS_RED, SUMMARY, SUMMARY_TITLE } from "./parts";
import { ProxyChips } from "./proxy/ProxyChips";
import { FavHelpModal, ResultsModal, SettingsModal } from "./proxy/ProxyModals";
import {
  addProxies,
  chipResult,
  groupResults,
  isProxyTestEvent,
  loadProxies,
  loadSettings,
  PROXY_CARD_OPEN_KEY,
  PS_DEFAULTS,
  pluralProxy,
  recheckSplit,
  rotationSeconds,
  saveEnabled,
  saveFavorites,
  saveProxyList,
  saveRecheck,
  saveRotation,
  saveSettings,
  settingsPayload,
  splitInterval,
  startTest,
  statusChip,
  stopTest,
  testStatus,
  toSeconds,
} from "./proxy/model";

const SAVE_MS = 400;
const SETTINGS_SAVE_MS = 600;
/** The bus replays recent history on connect; test events in that window are old. */
const REPLAY_MS = 1500;

/** .srv-proxy-hint.srv-proxy-rotation-hint, and the code samples in the paste hint. */
const ROTATION_HINT = "m-0 mt-1.5 text-xs leading-normal text-muted";
const CODE = "[&_code]:rounded-[4px] [&_code]:bg-white/6 [&_code]:px-[5px] [&_code]:py-px [&_code]:font-[monospace] [&_code]:text-[11px]";
/** .input.srv-proxy-interval-input: the full width .input look at 34px, so it shrinks on phones like the old one. */
const NUM =
  "h-[34px] w-full rounded-control border border-border bg-(image:--control-gradient) px-3.5 py-3 text-center text-sm text-text outline-0 transition-[border-color] [appearance:textfield] " +
  "hover:border-accent/30 hover:bg-[linear-gradient(180deg,#1f1237_0%,#160b2a_100%)] focus:border-accent focus:bg-[linear-gradient(180deg,#1f1237_0%,#160b2a_100%)] disabled:opacity-50 " +
  "[&::-webkit-inner-spin-button]:appearance-none [&::-webkit-outer-spin-button]:appearance-none";
/** The .dd toggle the old page turned select.srv-proxy-interval-select into; the caret sits on top. */
const UNIT =
  "dd-toggle relative min-h-[34px] cursor-pointer appearance-none rounded-btn border border-border bg-control py-2 pr-9 pl-2.5 text-left font-sans text-[13px] " +
  "text-text outline-0 transition-[border,box-shadow,background] duration-200 focus-visible:border-accent disabled:opacity-50";
/** .srv-proxy-rotation-section */
const ROTATION_SECTION = "mt-4 border-t border-t-border pt-3.5";

function UnitSelect({ id, value, disabled, onChange }: { id: string; value: string; disabled: boolean; onChange: (v: IntervalUnit) => void }) {
  return (
    <div className="dd relative">
      <select id={id} disabled={disabled} value={value} className={UNIT} onChange={(e) => onChange(e.target.value as IntervalUnit)}>
        <option value="60">minutes</option>
        <option value="3600">hours</option>
      </select>
      <span aria-hidden="true" className="dd-caret pointer-events-none absolute top-1/2 right-[11px] -translate-y-1/2 text-sm leading-none text-accent">
        ▾
      </span>
    </div>
  );
}
/** .srv-proxy-check and its 18px box. */
const CHECK_LABEL = "srv-proxy-check inline-flex cursor-pointer items-center gap-2 text-[13px] font-medium text-text select-none";
const CHECK =
  "relative m-0 size-[18px] shrink-0 cursor-pointer appearance-none rounded-[5px] border-[1.5px] border-border bg-[rgba(255,255,255,0.04)] " +
  "transition hover:border-accent focus-visible:shadow-[0_0_0_3px_rgba(167,139,250,0.18)] focus-visible:outline-0 checked:border-primary checked:bg-primary " +
  "checked:bg-[url(\"data:image/svg+xml,%3Csvg_xmlns='http://www.w3.org/2000/svg'_viewBox='0_0_24_24'_fill='none'_stroke='white'_stroke-width='3'_stroke-linecap='round'_stroke-linejoin='round'%3E%3Cpath_d='M20_6_9_17l-5-5'/%3E%3C/svg%3E\")] checked:bg-size-[12px] checked:bg-center checked:bg-no-repeat disabled:cursor-not-allowed";
/** .srv-proxy-chip and its .is-on / .is-warn colors. */
const CHIP_TONE = {
  on: "bg-[rgba(76,175,80,0.12)] text-[#4caf50]",
  warn: "bg-[rgba(255,152,0,0.12)] text-[#ff9800]",
  off: "bg-[rgba(255,255,255,0.06)] text-muted",
} as const;
const DOCS_URL = "https://copycord.github.io/Copycord/docs/configuration/advanced#proxy-support";
const GEAR =
  "M9.594 3.94c.09-.542.56-.94 1.11-.94h2.593c.55 0 1.02.398 1.11.94l.213 1.281c.063.374.313.686.645.87.074.04.147.083.22.127.325.196.72.257 1.075.124l1.217-.456a1.125 1.125 0 0 1 1.37.49l1.296 2.247a1.125 1.125 0 0 1-.26 1.431l-1.003.827c-.293.241-.438.613-.43.992a7.723 7.723 0 0 1 0 .255c-.008.378.137.75.43.991l1.004.827c.424.35.534.955.26 1.43l-1.298 2.247a1.125 1.125 0 0 1-1.369.491l-1.217-.456c-.355-.133-.75-.072-1.076.124a6.47 6.47 0 0 1-.22.128c-.331.183-.581.495-.644.869l-.213 1.281c-.09.543-.56.94-1.11.94h-2.594c-.55 0-1.019-.398-1.11-.94l-.213-1.281c-.062-.374-.312-.686-.644-.87a6.52 6.52 0 0 1-.22-.127c-.325-.196-.72-.257-1.076-.124l-1.217.456a1.125 1.125 0 0 1-1.369-.49l-1.297-2.247a1.125 1.125 0 0 1 .26-1.431l1.004-.827c.292-.24.437-.613.43-.991a6.932 6.932 0 0 1 0-.255c.007-.38-.138-.751-.43-.992l-1.004-.827a1.125 1.125 0 0 1-.26-1.43l1.297-2.247a1.125 1.125 0 0 1 1.37-.491l1.216.456c.356.133.751.072 1.076-.124.072-.044.146-.086.22-.128.332-.183.582-.495.644-.869l.214-1.28Z";

const initialSettings = () =>
  Object.fromEntries(
    (Object.keys(PS_DEFAULTS) as ProxySettingKey[]).map((k) => [k, String(PS_DEFAULTS[k])]),
  ) as Record<ProxySettingKey, string>;

function readOpen(): boolean {
  try {
    return localStorage.getItem(PROXY_CARD_OPEN_KEY) === "1";
  } catch {
    return false;
  }
}

export interface ProxySectionProps {
  /** For tests. */
  WebSocketImpl?: UseWsOptions["WebSocketImpl"];
  /** Ignore test events this long after mount (default 1500). */
  replayMs?: number;
}

export function ProxySection({ WebSocketImpl, replayMs = REPLAY_MS }: ProxySectionProps = {}) {
  const notify = useNotify();
  const confirm = useSafeConfirm();
  const [open, setOpen] = useState(readOpen);
  const [proxies, setProxies] = useState<string[]>([]);
  const [favorites, setFavorites] = useState<Set<string>>(new Set());
  const [enabled, setEnabled] = useState(false);
  const [rotOn, setRotOn] = useState(false);
  const [rotValue, setRotValue] = useState("30");
  const [rotUnit, setRotUnit] = useState<IntervalUnit>("60");
  const [recheckOn, setRecheckOn] = useState(false);
  const [recheckValue, setRecheckValue] = useState("5");
  const [recheckUnit, setRecheckUnit] = useState<IntervalUnit>("60");

  const [results, setResults] = useState<Record<string, ProxyChipResult>>({});
  const [running, setRunning] = useState(false);
  const [stopping, setStopping] = useState(false);
  const [progress, setProgress] = useState<{ current: number; total: number; text?: string } | null>(null);
  const [report, setReport] = useState<ProxyTestResult[] | null>(null);
  const [removeFailed, setRemoveFailed] = useState(true);
  const [removeSlow, setRemoveSlow] = useState(false);

  const [settingsOpen, setSettingsOpen] = useState(false);
  const [helpOpen, setHelpOpen] = useState(false);
  const [psValues, setPsValues] = useState(initialSettings);
  const [slowMs, setSlowMs] = useState(PS_DEFAULTS.PROXY_SLOW_THRESHOLD * 1000);
  const psLoaded = useRef(false);

  // The save timers read the latest values when they fire, after the render.
  const latest = useRef({ proxies, rotOn, rotValue, rotUnit, recheckOn, recheckValue, recheckUnit, psValues });
  latest.current = { proxies, rotOn, rotValue, rotUnit, recheckOn, recheckValue, recheckUnit, psValues };
  const timers = useRef<Record<string, ReturnType<typeof setTimeout> | undefined>>({});
  const collected = useRef<ProxyTestResult[]>([]);
  const slowRef = useRef(slowMs);
  slowRef.current = slowMs;
  const readyAt = useRef(Date.now() + replayMs);

  const fail = useCallback((e: unknown, fallback: string) => notify(e instanceof Error && e.message ? e.message : fallback, "error"), [notify]);

  const applyConfig = useCallback((j: Awaited<ReturnType<typeof loadProxies>>) => {
    setFavorites(new Set(j.favorites ?? []));
    setProxies(j.proxies ?? []);
    setEnabled(!!j.enabled);
    const rot = j.rotation_interval ?? 0;
    setRotOn(rot > 0);
    if (rot > 0) {
      const s = splitInterval(rot);
      setRotValue(String(s.value));
      setRotUnit(s.unit);
    }
    setRecheckOn(!!j.favorite_recheck);
    const rc = recheckSplit(j.favorite_recheck_interval);
    setRecheckValue(String(rc.value));
    setRecheckUnit(rc.unit);
  }, []);

  useEffect(() => {
    let live = true;
    loadProxies()
      .then((j) => live && j.ok && applyConfig(j))
      .catch(() => undefined);
    // A test started before a reload keeps running on the server.
    testStatus()
      .then((j) => {
        if (live && j.ok && j.running) {
          setRunning(true);
          setProgress({ current: 0, total: 0, text: "resuming…" });
        }
      })
      .catch(() => undefined);
    const t = timers.current;
    return () => {
      live = false;
      for (const k of Object.keys(t)) clearTimeout(t[k]);
    };
  }, [applyConfig]);

  const later = (key: string, ms: number, fn: () => void) => {
    clearTimeout(timers.current[key]);
    timers.current[key] = setTimeout(fn, ms);
  };

  const saveList = useCallback(
    async (list: string[]) => {
      const l = latest.current;
      try {
        const [a, b] = await Promise.all([
          saveProxyList(list),
          saveRotation(rotationSeconds(l.rotOn, l.rotValue, l.rotUnit)),
        ]);
        if (!a.ok) notify(a.error || "Failed to save proxies", "error");
        if (!b.ok) notify(b.error || "Failed to save rotation", "error");
      } catch (e) {
        fail(e, "Failed to save proxy settings");
      }
    },
    [notify, fail],
  );
  const scheduleSave = () => later("list", SAVE_MS, () => void saveList(latest.current.proxies));

  const changeProxies = (next: string[]) => {
    setProxies(next);
    latest.current = { ...latest.current, proxies: next };
    scheduleSave();
  };

  const toggleFavorite = async (proxy: string) => {
    const next = new Set(favorites);
    if (!next.delete(proxy)) next.add(proxy);
    setFavorites(next);
    try {
      const j = await saveFavorites([...next]);
      if (!j.ok) return notify(j.error || "Failed to save favorites", "error");
      // The server drops favorites naming a proxy it does not have, so its answer is the truth.
      if (j.favorites) setFavorites(new Set(j.favorites));
    } catch (e) {
      fail(e, "Failed to save favorites");
    }
  };

  const toggleEnabled = async (on: boolean) => {
    setEnabled(on);
    try {
      const j = await saveEnabled(on);
      if (j.ok) notify(on ? `Client proxies enabled (${pluralProxy(proxies.length)})` : "Client proxies disabled", "success");
      else notify(j.error || "Failed to toggle", "error");
    } catch (e) {
      fail(e, "Failed to toggle client proxies");
    }
  };

  const saveRecheckNow = useCallback(
    async (on?: boolean) => {
      const l = latest.current;
      try {
        const j = await saveRecheck(on ?? l.recheckOn, toSeconds(l.recheckValue, l.recheckUnit, 5));
        if (!j.ok) notify(j.error || "Failed to save", "error");
      } catch (e) {
        fail(e, "Failed to save favorite re-check");
      }
    },
    [notify, fail],
  );

  const clearAll = async () => {
    if (!proxies.length) return;
    const ok = await confirm({
      title: "Clear all proxies?",
      body: "This will remove all proxies from the list.",
      confirmLabel: "Clear",
      confirmVariant: "ghost-red",
    });
    if (!ok) return;
    setProxies([]);
    latest.current = { ...latest.current, proxies: [] };
    void saveList([]);
  };

  // Live test progress.
  const onTestEvent = (ev: ProxyTestEvent) => {
    if (ev.type === "started") {
      collected.current = [];
      setRunning(true);
      setStopping(false);
      setProgress({ current: 0, total: ev.total ?? 0 });
      setResults(Object.fromEntries(latest.current.proxies.map((p) => [p, { state: "loading", label: "" }])));
    } else if (ev.type === "progress") {
      setProgress({ current: ev.current ?? 0, total: ev.total ?? 0 });
      if (ev.results?.length) {
        collected.current.push(...ev.results);
        const batch = ev.results;
        setResults((r) => {
          const next = { ...r };
          for (const x of batch) if (x.proxy in next) next[x.proxy] = chipResult(x, slowRef.current);
          return next;
        });
      }
    } else {
      setRunning(false);
      setStopping(false);
      setProgress(null);
      const all = ev.type === "complete" ? (ev.results ?? collected.current) : collected.current;
      if (ev.type === "stopped") {
        // Chips still waiting for a result go back to plain.
        setResults((r) => Object.fromEntries(Object.entries(r).filter(([, v]) => v.state !== "loading")));
      }
      showReport(all, ev.type === "stopped");
    }
  };

  const showReport = (all: ProxyTestResult[], stopped: boolean) => {
    if (!all.length) {
      if (stopped) notify("Proxy test stopped", "success");
      return;
    }
    const g = groupResults(all, slowRef.current);
    if (!g.failed.length && !g.slow.length) {
      notify(`All ${g.good.length} proxies passed`, "success");
      return;
    }
    setRemoveFailed(true);
    setRemoveSlow(false);
    setReport(all);
  };

  useWsMessages(
    "/ws/ui",
    (msg) => {
      if (Date.now() < readyAt.current) return;
      if (isProxyTestEvent(msg)) onTestEvent(msg.payload);
    },
    { WebSocketImpl },
  );

  const runTest = async () => {
    if (running) {
      setStopping(true);
      try {
        await stopTest();
      } catch {
        setStopping(false);
        setRunning(false);
        setProgress(null);
      }
      return;
    }
    if (!proxies.length) return notify("No proxies to test", "error");
    setStopping(true);
    try {
      const j = await startTest(proxies);
      if (!j.ok) notify(j.error || "Failed to start test", "error");
    } catch (e) {
      fail(e, "Failed to start proxy test");
    } finally {
      setStopping(false);
    }
  };

  const confirmRemoval = () => {
    if (!report) return;
    const g = groupResults(report, slowMs);
    const drop = new Set<string>();
    if (removeFailed) g.failed.forEach((r) => drop.add(r.proxy));
    if (removeSlow) g.slow.forEach((r) => drop.add(r.proxy));
    setReport(null);
    if (!drop.size) return;
    const next = proxies.filter((p) => !drop.has(p));
    setProxies(next);
    setFavorites((f) => new Set([...f].filter((p) => !drop.has(p))));
    void saveList(next);
    notify(`Removed ${drop.size} ${drop.size === 1 ? "proxy" : "proxies"}`, "success");
  };

  // Settings modal.
  const openSettings = () => {
    setSettingsOpen(true);
    if (psLoaded.current) return;
    loadSettings()
      .then((j) => {
        if (!j.ok || !j.settings) return;
        psLoaded.current = true;
        setPsValues((v) => {
          const next = { ...v };
          for (const k of Object.keys(PS_DEFAULTS) as ProxySettingKey[]) {
            const x = j.settings?.[k];
            if (x !== undefined) next[k] = String(x);
          }
          return next;
        });
        if (j.settings.PROXY_SLOW_THRESHOLD) setSlowMs(j.settings.PROXY_SLOW_THRESHOLD * 1000);
      })
      .catch(() => undefined);
  };

  const commitSettings = (override?: Record<ProxySettingKey, string>) =>
    later("settings", override ? 0 : SETTINGS_SAVE_MS, async () => {
      const payload = settingsPayload(override ?? latest.current.psValues);
      setSlowMs(payload.PROXY_SLOW_THRESHOLD * 1000);
      try {
        const j = await saveSettings(payload);
        if (!j.ok) notify(j.error || "Failed to save settings", "error");
      } catch (e) {
        fail(e, "Failed to save proxy settings");
      }
    });

  const chip = statusChip(enabled, proxies.length);
  const locked = !enabled;
  const grouped = report ? groupResults(report, slowMs) : null;

  const lockedProps = {
    "aria-disabled": locked,
    title: locked ? "Enable proxies for client to use this" : undefined,
  };

  return (
    <>
      <details
        id="srv-proxy-card"
        className={CARD}
        open={open}
        onToggle={(e) => {
          const o = e.currentTarget.open;
          setOpen(o);
          try {
            localStorage.setItem(PROXY_CARD_OPEN_KEY, o ? "1" : "0");
          } catch {
            // Storage can be blocked; the card just forgets its state.
          }
        }}
      >
        <summary className={SUMMARY}>
          <h3 className={SUMMARY_TITLE}>Proxy</h3>
          <DocLink href={DOCS_URL} label="Proxy docs" />
          <span
            id="srv-proxy-status"
            className={cx(
              "srv-proxy-chip rounded-[20px] px-2.5 py-1 text-[11px] leading-none font-semibold empty:hidden",
              CHIP_TONE[chip.tone],
            )}
          >
            {chip.text}
          </span>
          <button
            type="button"
            id="srv-proxy-settings-btn"
            title="Proxy Settings"
            className="srv-proxy-settings-btn ml-1.5 inline-flex cursor-pointer items-center gap-[5px] rounded-lg border border-border bg-[rgba(255,255,255,0.04)] px-2.5 py-1 text-[11px] font-medium text-muted transition-colors duration-150 hover:border-[rgba(255,255,255,0.18)] hover:bg-[rgba(255,255,255,0.08)] hover:text-text"
            onClick={(e) => {
              e.preventDefault();
              e.stopPropagation();
              openSettings();
            }}
          >
            <svg xmlns="http://www.w3.org/2000/svg" width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
              <path d={GEAR} />
              <path d="M15 12a3 3 0 1 1-6 0 3 3 0 0 1 6 0Z" />
            </svg>
            Settings
          </button>
          <button
            type="button"
            id="srv-proxy-fav-help-btn"
            title="What do favorites do?"
            aria-label="What do favorites do?"
            className="srv-proxy-fav-help-btn inline-flex size-[26px] cursor-pointer items-center justify-center rounded-md border border-[rgba(255,255,255,0.1)] bg-transparent p-0 text-muted transition-colors duration-100 hover:border-[rgba(255,200,61,0.4)] hover:bg-[rgba(255,200,61,0.1)] hover:text-[#ffc83d]"
            onClick={(e) => {
              // The header is a <summary>; without this the click also toggles the card shut.
              e.preventDefault();
              e.stopPropagation();
              setHelpOpen(true);
            }}
          >
            <svg xmlns="http://www.w3.org/2000/svg" width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
              <polygon points="12 2 15.09 8.26 22 9.27 17 14.14 18.18 21.02 12 17.77 5.82 21.02 7 14.14 2 9.27 8.91 8.26 12 2" />
            </svg>
          </button>
          <Chevron />
        </summary>

        <div className="srv-proxy-body mt-3.5">
          <div className="srv-proxy-toggle-row mb-3">
            <label className={CHECK_LABEL} htmlFor="srv-proxy-toggle">
              <input
                id="srv-proxy-toggle"
                type="checkbox"
                className={CHECK}
                checked={enabled}
                onChange={(e) => void toggleEnabled(e.target.checked)}
              />
              <span>Enable proxies for client</span>
            </label>
          </div>

          <div className="srv-proxy-hint-row mb-2.5 flex items-start justify-between gap-3">
            <p className={cx("m-0 text-xs leading-normal text-muted", CODE)}>
              Paste one or more proxies &mdash; <code>host:port</code> &middot; <code>host:port:user:pass</code>{" "}
              &middot; <code>user:pass@host:port</code>
            </p>
            <div className="srv-proxy-hint-actions flex shrink-0 gap-1.5">
              <button
                id="srv-proxy-test"
                type="button"
                disabled={stopping}
                onClick={() => void runTest()}
                className={running ? GHOST_XS_RED : GHOST_XS_GREEN}
              >
                {running ? "Stop" : "Test All"}
              </button>
              <button id="srv-proxy-clear" type="button" onClick={() => void clearAll()} className={GHOST_XS_RED}>
                Clear
              </button>
            </div>
          </div>

          {progress ? (
            <div id="srv-proxy-progress" className="srv-proxy-progress mb-2.5 flex items-center gap-2.5">
              <div className="srv-proxy-progress-bar h-1.5 flex-1 overflow-hidden rounded-[3px] bg-white/6">
                <div
                  id="srv-proxy-progress-fill"
                  className="h-full rounded-[3px] bg-[linear-gradient(90deg,var(--color-primary),var(--color-accent))] transition-[width] duration-300"
                  style={{ width: `${progress.total > 0 ? Math.round((progress.current / progress.total) * 100) : 0}%` }}
                />
              </div>
              <span id="srv-proxy-progress-text" className="min-w-[70px] text-right text-[11px] whitespace-nowrap text-muted">
                {progress.text ?? `${progress.current} / ${progress.total}`}
              </span>
            </div>
          ) : null}

          <ProxyChips
            proxies={proxies}
            favorites={favorites}
            results={results}
            onAdd={(text) => changeProxies(addProxies(proxies, text))}
            onRemove={(p) => {
              setFavorites((f) => new Set([...f].filter((x) => x !== p)));
              changeProxies(proxies.filter((x) => x !== p));
            }}
            onRemoveLast={() => changeProxies(proxies.slice(0, -1))}
            onToggleFavorite={(p) => void toggleFavorite(p)}
          />

          <div
            id="srv-proxy-rotation-section"
            className={cx(ROTATION_SECTION, locked && "is-locked opacity-40")}
            {...lockedProps}
          >
            <div className="srv-proxy-rotation-row flex items-center gap-2.5">
              <label className={CHECK_LABEL} htmlFor="srv-proxy-rotation-toggle">
                <input
                  id="srv-proxy-rotation-toggle"
                  type="checkbox"
                  className={CHECK}
                  disabled={locked}
                  checked={rotOn}
                  onChange={(e) => {
                    setRotOn(e.target.checked);
                    latest.current = { ...latest.current, rotOn: e.target.checked };
                    scheduleSave();
                  }}
                />
                <span>Rotate every</span>
              </label>
              <div id="srv-proxy-rotation-controls" className={cx("srv-proxy-interval-inline flex items-center gap-1.5", !rotOn && "hidden")}>
                <input
                  id="srv-proxy-interval"
                  type="number"
                  min={1}
                  max={1440}
                  disabled={locked}
                  value={rotValue}
                  className={NUM}
                  onChange={(e) => {
                    setRotValue(e.target.value);
                    latest.current = { ...latest.current, rotValue: e.target.value };
                  }}
                  onBlur={() => {
                    const v = Math.max(1, parseInt(rotValue, 10) || 1);
                    setRotValue(String(v));
                    latest.current = { ...latest.current, rotValue: String(v) };
                    scheduleSave();
                  }}
                />
                <UnitSelect
                  id="srv-proxy-interval-unit"
                  disabled={locked}
                  value={rotUnit}
                  onChange={(unit) => {
                    setRotUnit(unit);
                    latest.current = { ...latest.current, rotUnit: unit };
                    scheduleSave();
                  }}
                />
              </div>
            </div>
            <p className={ROTATION_HINT}>
              Automatically switches to the next proxy after the set duration. When off, one proxy is used until it
              fails.
            </p>
          </div>

          <div
            id="srv-proxy-fav-section"
            className={cx(ROTATION_SECTION, locked && "is-locked opacity-40")}
            {...lockedProps}
          >
            <div className="srv-proxy-rotation-row flex items-center gap-2.5">
              <label className={CHECK_LABEL} htmlFor="srv-proxy-fav-recheck-toggle">
                <input
                  id="srv-proxy-fav-recheck-toggle"
                  type="checkbox"
                  className={CHECK}
                  disabled={locked}
                  checked={recheckOn}
                  onChange={(e) => {
                    setRecheckOn(e.target.checked);
                    void saveRecheckNow(e.target.checked);
                  }}
                />
                <span>Return to a favorite every</span>
              </label>
              <div
                id="srv-proxy-fav-recheck-controls"
                className={cx("srv-proxy-interval-inline flex items-center gap-1.5", !recheckOn && "pointer-events-none opacity-45")}
              >
                <input
                  id="srv-proxy-fav-recheck-interval"
                  type="number"
                  min={1}
                  max={1440}
                  disabled={locked}
                  value={recheckValue}
                  className={NUM}
                  onChange={(e) => {
                    setRecheckValue(e.target.value);
                    latest.current = { ...latest.current, recheckValue: e.target.value };
                    later("recheck", SAVE_MS, () => void saveRecheckNow());
                  }}
                />
                <UnitSelect
                  id="srv-proxy-fav-recheck-unit"
                  disabled={locked}
                  value={recheckUnit}
                  onChange={(unit) => {
                    setRecheckUnit(unit);
                    latest.current = { ...latest.current, recheckUnit: unit };
                    later("recheck", SAVE_MS, () => void saveRecheckNow());
                  }}
                />
              </div>
            </div>
            <p className={ROTATION_HINT}>
              While the client is on a proxy you have not starred, re-test the favorites on this interval and switch
              back as soon as one works again.
            </p>
          </div>
        </div>
      </details>

      {helpOpen ? <FavHelpModal onClose={() => setHelpOpen(false)} /> : null}
      {settingsOpen ? (
        <SettingsModal
          values={psValues}
          onChange={(k, v) => {
            setPsValues((s) => {
              const next = { ...s, [k]: v };
              latest.current = { ...latest.current, psValues: next };
              return next;
            });
          }}
          onCommit={commitSettings}
          onClose={() => setSettingsOpen(false)}
        />
      ) : null}
      {report && grouped ? (
        <ResultsModal
          good={grouped.good.length}
          slow={grouped.slow}
          failed={grouped.failed}
          slowSec={slowMs / 1000}
          removeFailed={removeFailed}
          removeSlow={removeSlow}
          onRemoveFailed={setRemoveFailed}
          onRemoveSlow={setRemoveSlow}
          onConfirm={confirmRemoval}
          onClose={() => setReport(null)}
        />
      ) : null}
    </>
  );
}
