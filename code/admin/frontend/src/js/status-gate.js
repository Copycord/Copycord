window.createStatusGate = function createStatusGate(opts = {}) {
  const {
    hideSelectors = [],
    gateDelayMs = 350,
    readyCacheKey = "bot:lastUpAt",
    readyTtlMs = 20000,
    onStatusChange = null,
    onUp = null,
    onDown = null,
    watchIntervalMsUp = 10000,
    watchIntervalMsDown = 5000,

    require = "either",
  } = opts;

  let gateTimer = null;
  let gateShown = false;
  let statusPoll = null;
  let watchTimer = null;
  let watchTimerMs = null;
  let lastIsUp = null;

  function fireStatus(isUp, json) {
    if (lastIsUp !== isUp) {
      if (isUp) {
        try {
          onUp && onUp(json);
        } catch {}
      } else {
        try {
          onDown && onDown(json);
        } catch {}
      }
    }
    try {
      onStatusChange && onStatusChange(isUp, json);
    } catch {}
    lastIsUp = isUp;
  }

  async function oneWatchTick() {
    const j = await fetchRuntime();
    if (!j) return;
    const st = normalizeRuntime(j);
    if (st.isUp) hideGateSafe();
    else showGateNow();
    fireStatus(st.isUp, j);
    const want = st.isUp ? watchIntervalMsUp : watchIntervalMsDown;
    if (watchTimer && watchTimerMs !== want) startWatch(want);
  }

  function startWatch(ms = watchIntervalMsUp) {
    stopWatch();
    watchTimer = setInterval(oneWatchTick, ms);
    watchTimerMs = ms;
    oneWatchTick().catch(() => {});
  }
  function stopWatch() {
    if (watchTimer) {
      clearInterval(watchTimer);
      watchTimer = null;
      watchTimerMs = null;
    }
  }

  (function injectStyles() {
    if (document.getElementById("status-gate-styles")) return;
    const css = document.createElement("style");
    css.id = "status-gate-styles";
    css.textContent = `
      html.await-status .container{filter:blur(4px);pointer-events:none;user-select:none;}
      #status-gate{position:fixed;left:0;right:0;bottom:0;top:var(--header-h,64px);
        display:none;align-items:center;justify-content:center;z-index:9998;
        background:rgba(0,0,0,.38)}
      html.await-status #status-gate{display:flex}
      #status-gate .sg-msg{margin:0;padding:0 16px;text-align:center;font:600 14px/1.35 system-ui,-apple-system,Segoe UI,Roboto,sans-serif;color:#cfd3ff;text-shadow:0 1px 10px rgba(0,0,0,.35);}
      ${
        hideSelectors.length
          ? `html.await-status ${hideSelectors.join(
              ", html.await-status "
            )}{display:none !important;}`
          : ""
      }
    `;
    document.head.appendChild(css);
    const setHeaderHeightVar = () => {
      const h = document.querySelector(".site-header");
      if (h)
        document.documentElement.style.setProperty(
          "--header-h",
          `${h.offsetHeight}px`
        );
    };
    setHeaderHeightVar();
    window.addEventListener("resize", setHeaderHeightVar, { passive: true });
  })();

  function ensureStatusGate() {
    if (document.getElementById("status-gate")) return;
    const gate = document.createElement("div");
    gate.id = "status-gate";
    gate.innerHTML = `
      <div class="sg-logo" aria-hidden="true">
        <img src="/static/logo.png" alt="" draggable="false">
      </div>
      <div class="sg-msg" role="status" aria-live="polite">Checking bot status...</div>
    `;
    document.body.appendChild(gate);
  }
  function setGateText(msg) {
    if (!gateShown) return;
    const el = document.querySelector("#status-gate .sg-msg");
    if (el) el.textContent = msg;
  }
  function gateOn() {
    document.documentElement.classList.add("await-status");
    ensureStatusGate();
  }
  function gateOff() {
    document.documentElement.classList.remove("await-status");
  }

  function lastUpIsFresh() {
    try {
      const t = Number(sessionStorage.getItem(readyCacheKey) || 0);
      return t && Date.now() - t < readyTtlMs;
    } catch {
      return false;
    }
  }
  function showGateSoon() {
    if (gateShown || gateTimer) return;
    gateTimer = setTimeout(() => {
      gateTimer = null;
      if (!gateShown) {
        gateOn();
        gateShown = true;
      }
    }, gateDelayMs);
  }
  function showGateNow() {
    if (gateTimer) {
      clearTimeout(gateTimer);
      gateTimer = null;
    }
    if (!gateShown) {
      gateOn();
      gateShown = true;
    }
  }
  function hideGateSafe() {
    if (gateTimer) {
      clearTimeout(gateTimer);
      gateTimer = null;
    }
    if (gateShown) {
      gateOff();
      gateShown = false;
    }
  }

  function normalizeReadySide(side) {
    const lower = (x) => String(x ?? "").toLowerCase();
    const statusStr = lower(side?.status ?? side?.state ?? "");
    return (
      side?.ready === true ||
      side?.discord?.ready === true ||
      side?.discord?.connected === true ||
      side?.discord?.online === true ||
      statusStr === "ready" ||
      statusStr.includes("logged in as")
    );
  }

  function normalizeRuntime(json) {
    const serverReady = normalizeReadySide(json?.server || {});
    const clientReady = normalizeReadySide(json?.client || {});
    let isUp;
    switch (require) {
      case "both":
        isUp = !!(serverReady && clientReady);
        break;
      case "server":
        isUp = !!serverReady;
        break;
      default:
        isUp = !!(serverReady || clientReady);
    }
    return { isUp, serverReady, clientReady };
  }

  async function fetchRuntime() {
    const ENDPOINTS = ["/api/status"];
    for (const url of ENDPOINTS) {
      try {
        const r = await fetch(url, { cache: "no-store" });
        if (!r.ok) continue;
        return await r.json();
      } catch {}
    }
    return null;
  }

  async function checkAndGate(onReady, fromRetry = false) {
    setGateText(fromRetry ? "Server or Client were disconnected..." : "Checking bot connection…");
    const j = await fetchRuntime();
    if (!j) {
      setGateText("Make sure Server and Client are started and connected to Discord.");
      if (!statusPoll)
        statusPoll = setInterval(() => checkAndGate(onReady, true), 5000);
      fireStatus(false, j);
      return false;
    }

    const st = normalizeRuntime(j);
    if (st.isUp) {
      if (statusPoll) {
        clearInterval(statusPoll);
        statusPoll = null;
      }
      hideGateSafe();
      try {
        sessionStorage.setItem(readyCacheKey, String(Date.now()));
      } catch {}
      if (typeof onReady === "function") onReady();
      fireStatus(true, j);
      return true;
    } else {
      if (require === "both") {
        if (!st.serverReady && !st.clientReady)
          setGateText("Waiting for Server and Client connection...");
        else if (!st.serverReady)
          setGateText("Waiting for Server to connect to Discord gateway...");
        else setGateText("Waiting for Client to connect to Discord gateway...");
      } else {
        setGateText("Bot is not connected to Discord yet…");
      }
      showGateNow();
      fireStatus(false, j);
      if (!statusPoll)
        statusPoll = setInterval(() => checkAndGate(onReady, true), 5000);
      return false;
    }
  }

  return {
    lastUpIsFresh,
    showGateSoon,
    checkAndGate,
    ensureStatusGate,
    startWatch,
    stopWatch,
  };
};
