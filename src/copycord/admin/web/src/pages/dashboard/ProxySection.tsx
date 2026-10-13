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
import { GhostButton } from "../system/buttons";
import { useNotify, useSafeConfirm } from "../system/providers";
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

const SECTION =
  "rounded-card border border-border bg-linear-to-b from-card to-[rgba(20,10,40,0.45)] text-text";
const HINT = "text-xs leading-relaxed text-muted";
const NUM =
  "w-20 rounded-control border border-border bg-bg-2 px-2 py-1.5 text-sm text-text outline-none focus:border-accent disabled:opacity-50";
const CHIP_TONE = {
  on: "border-good/50 text-good",
  warn: "border-[#ff9800]/60 text-[#ff9800]",
  off: "border-border text-muted",
} as const;

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
      danger: true,
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
        className={cx(SECTION, "group")}
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
        <summary className="flex cursor-pointer list-none items-center gap-3 px-[18px] py-3 [&::-webkit-details-marker]:hidden">
          <h2 className="text-base font-semibold">Proxy</h2>
          <a
            href="https://copycord.github.io/Copycord/docs/configuration/advanced#proxy-support"
            target="_blank"
            rel="noopener noreferrer"
            title="View docs"
            className="text-xs text-accent hover:underline"
            onClick={(e) => e.stopPropagation()}
          >
            Docs
          </a>
          <span
            id="srv-proxy-status"
            className={cx("srv-proxy-chip rounded-full border px-2.5 py-0.5 text-xs", CHIP_TONE[chip.tone])}
          >
            {chip.text}
          </span>
          <span className="flex-1" />
          <button
            type="button"
            id="srv-proxy-settings-btn"
            title="Proxy Settings"
            className="rounded-control border border-border px-2.5 py-1 text-xs text-accent hover:bg-white/8"
            onClick={(e) => {
              e.preventDefault();
              e.stopPropagation();
              openSettings();
            }}
          >
            Settings
          </button>
          <button
            type="button"
            id="srv-proxy-fav-help-btn"
            title="What do favorites do?"
            aria-label="What do favorites do?"
            className="rounded-control border border-border px-2 py-1 text-custom-ink hover:bg-white/8"
            onClick={(e) => {
              // The header is a <summary>; without this the click also toggles the card shut.
              e.preventDefault();
              e.stopPropagation();
              setHelpOpen(true);
            }}
          >
            ★
          </button>
          <span aria-hidden="true" className="text-muted transition group-open:rotate-180">
            ▾
          </span>
        </summary>

        <div className="flex flex-col gap-4 border-t border-border px-[18px] py-4">
          <label className="flex w-fit cursor-pointer items-center gap-2 text-sm" htmlFor="srv-proxy-toggle">
            <input
              id="srv-proxy-toggle"
              type="checkbox"
              checked={enabled}
              onChange={(e) => void toggleEnabled(e.target.checked)}
            />
            <span>Enable proxies for client</span>
          </label>

          <div className="flex flex-wrap items-center justify-between gap-2">
            <p className={HINT}>
              Paste one or more proxies &mdash; <code>host:port</code> &middot; <code>host:port:user:pass</code>{" "}
              &middot; <code>user:pass@host:port</code>
            </p>
            <div className="flex gap-2">
              <GhostButton
                id="srv-proxy-test"
                compact
                danger={running}
                disabled={stopping}
                onClick={() => void runTest()}
              >
                {running ? "Stop" : "Test All"}
              </GhostButton>
              <GhostButton id="srv-proxy-clear" compact danger onClick={() => void clearAll()}>
                Clear
              </GhostButton>
            </div>
          </div>

          {progress ? (
            <div id="srv-proxy-progress" className="flex items-center gap-3">
              <div className="h-2 flex-1 overflow-hidden rounded-full bg-white/8">
                <div
                  id="srv-proxy-progress-fill"
                  className="h-full bg-progress transition-[width]"
                  style={{ width: `${progress.total > 0 ? Math.round((progress.current / progress.total) * 100) : 0}%` }}
                />
              </div>
              <span id="srv-proxy-progress-text" className="text-xs text-muted">
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
            className={cx("flex flex-col gap-1.5", locked && "is-locked opacity-50")}
            {...lockedProps}
          >
            <div className="flex flex-wrap items-center gap-3">
              <label className="flex cursor-pointer items-center gap-2 text-sm" htmlFor="srv-proxy-rotation-toggle">
                <input
                  id="srv-proxy-rotation-toggle"
                  type="checkbox"
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
              <div id="srv-proxy-rotation-controls" className={cx("flex gap-2", !rotOn && "hidden")}>
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
                <select
                  id="srv-proxy-interval-unit"
                  disabled={locked}
                  value={rotUnit}
                  className={NUM}
                  onChange={(e) => {
                    setRotUnit(e.target.value as IntervalUnit);
                    latest.current = { ...latest.current, rotUnit: e.target.value as IntervalUnit };
                    scheduleSave();
                  }}
                >
                  <option value="60">minutes</option>
                  <option value="3600">hours</option>
                </select>
              </div>
            </div>
            <p className={HINT}>
              Automatically switches to the next proxy after the set duration. When off, one proxy is used until it
              fails.
            </p>
          </div>

          <div
            id="srv-proxy-fav-section"
            className={cx("flex flex-col gap-1.5", locked && "is-locked opacity-50")}
            {...lockedProps}
          >
            <div className="flex flex-wrap items-center gap-3">
              <label className="flex cursor-pointer items-center gap-2 text-sm" htmlFor="srv-proxy-fav-recheck-toggle">
                <input
                  id="srv-proxy-fav-recheck-toggle"
                  type="checkbox"
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
                className={cx("flex gap-2", !recheckOn && "pointer-events-none opacity-45")}
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
                <select
                  id="srv-proxy-fav-recheck-unit"
                  disabled={locked}
                  value={recheckUnit}
                  className={NUM}
                  onChange={(e) => {
                    setRecheckUnit(e.target.value as IntervalUnit);
                    latest.current = { ...latest.current, recheckUnit: e.target.value as IntervalUnit };
                    later("recheck", SAVE_MS, () => void saveRecheckNow());
                  }}
                >
                  <option value="60">minutes</option>
                  <option value="3600">hours</option>
                </select>
              </div>
            </div>
            <p className={HINT}>
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
