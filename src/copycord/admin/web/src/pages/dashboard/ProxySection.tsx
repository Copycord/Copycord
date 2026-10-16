// The Proxy card from index.html: enable toggle, proxy chips saved on a short
// delay, favorites, rotation and favorite re-check, Test All with live progress
// from the bus (proxy_test messages), the Settings modal and the favorites help.

import { useCallback, useEffect, useRef, useState } from "react";
import { useWsMessages } from "../../hooks/useWsMessages";
import type { UseWsOptions } from "../../hooks/useWsMessages";
import type {
  IntervalUnit,
  ProxyChipResult,
  ProxySettingKey,
  ProxyTestEvent,
  ProxyTestResult,
} from "../../types/dashboard-proxy";
import { errorText } from "../../lib/api";
import { useNotify, useSafeConfirm } from "../system/providers";
import { CARD, CARD_DOCS, CHECK, Chevron, DocLink, GHOST_XS_GREEN, GHOST_XS_RED, readCardOpen, SUMMARY, SUMMARY_TITLE } from "./parts";
import {
  CheckPending,
  CheckRow,
  HintRow,
  IntervalSection,
  NUM,
  RECHECK_TEXT,
  RecheckControls,
  ROTATION_TEXT,
  RotationControls,
  SummaryTools,
  UnitSelect,
} from "./proxy/chrome";
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

const initialSettings = () =>
  Object.fromEntries(
    (Object.keys(PS_DEFAULTS) as ProxySettingKey[]).map((k) => [k, String(PS_DEFAULTS[k])]),
  ) as Record<ProxySettingKey, string>;

export interface ProxySectionProps {
  /** For tests. */
  WebSocketImpl?: UseWsOptions["WebSocketImpl"];
  /** Ignore test events this long after mount (default 1500). */
  replayMs?: number;
}

export function ProxySection({ WebSocketImpl, replayMs = REPLAY_MS }: ProxySectionProps = {}) {
  const notify = useNotify();
  const confirm = useSafeConfirm();
  const [open, setOpen] = useState(() => readCardOpen(PROXY_CARD_OPEN_KEY));
  const [proxies, setProxies] = useState<string[]>([]);
  const [favorites, setFavorites] = useState<Set<string>>(new Set());
  const [enabled, setEnabled] = useState(false);
  // The status chip and the re-check interval are bars, like the page skeleton's, until the config answers.
  const [loaded, setLoaded] = useState(false);
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

  const fail = useCallback((e: unknown, fallback: string) => notify(errorText(e, fallback), "error"), [notify]);

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
      .catch(() => undefined)
      .finally(() => live && setLoaded(true));
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
          <DocLink href={CARD_DOCS.proxy} label="Proxy docs" />
          <SummaryTools chip={loaded ? chip : null} onSettings={openSettings} onHelp={() => setHelpOpen(true)} />
          <Chevron />
        </summary>

        <div className="srv-proxy-body mt-3.5">
          <div className="srv-proxy-toggle-row mb-3">
            <CheckRow id="srv-proxy-toggle" label="Enable proxies for client">
              {loaded ? (
                <input
                  id="srv-proxy-toggle"
                  type="checkbox"
                  className={CHECK}
                  checked={enabled}
                  onChange={(e) => void toggleEnabled(e.target.checked)}
                />
              ) : (
                <CheckPending />
              )}
            </CheckRow>
          </div>

          <HintRow>
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
          </HintRow>
          {progress ? (
            <div id="srv-proxy-progress" className="srv-proxy-progress mb-2.5 flex items-center gap-2.5">
              <div className="srv-proxy-progress-bar h-1.5 flex-1 overflow-hidden rounded-[3px] bg-white/6">
                <div
                  id="srv-proxy-progress-fill"
                  className="h-full rounded-[3px] bg-accent transition-[width] duration-300"
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

          <IntervalSection
            id="srv-proxy-rotation-section"
            locked={locked}
            hint={ROTATION_TEXT}
            check={
              <CheckRow id="srv-proxy-rotation-toggle" label="Rotate every">
                {loaded ? (
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
                ) : (
                  <CheckPending />
                )}
              </CheckRow>
            }
            controls={
              <RotationControls on={rotOn}>
                {/* Hidden until rotation is on; the skeleton draws none, so neither does the loading state. */}
                {loaded ? (
                  <>
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
                  </>
                ) : null}
              </RotationControls>
            }
          />

          <IntervalSection
            id="srv-proxy-fav-section"
            locked={locked}
            hint={RECHECK_TEXT}
            check={
              <CheckRow id="srv-proxy-fav-recheck-toggle" label="Return to a favorite every">
                {loaded ? (
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
                ) : (
                  <CheckPending />
                )}
              </CheckRow>
            }
            controls={
              <RecheckControls off={!recheckOn}>
                <input
                  id="srv-proxy-fav-recheck-interval"
                  type="number"
                  min={1}
                  max={1440}
                  disabled={locked}
                  value={loaded ? recheckValue : ""}
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
                  pending={!loaded}
                  value={recheckUnit}
                  onChange={(unit) => {
                    setRecheckUnit(unit);
                    latest.current = { ...latest.current, recheckUnit: unit };
                    later("recheck", SAVE_MS, () => void saveRecheckNow());
                  }}
                />
              </RecheckControls>
            }
          />
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
