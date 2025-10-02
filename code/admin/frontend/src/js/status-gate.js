window.createStatusGate = function createStatusGate(opts = {}) {
    const {
      hideSelectors = [],
      gateDelayMs = 350,
      readyCacheKey = "bot:lastUpAt",
      readyTtlMs = 20000,
    } = opts;
  
    let gateTimer = null;
    let gateShown = false;
    let statusPoll = null;
  
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
        ${hideSelectors.length ? `html.await-status ${hideSelectors.join(", html.await-status ")}{display:none !important;}` : ""}
      `;
      document.head.appendChild(css);
      const setHeaderHeightVar = () => {
        const h = document.querySelector(".site-header");
        if (h) document.documentElement.style.setProperty("--header-h", `${h.offsetHeight}px`);
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
        <div class="sg-msg" role="status" aria-live="polite">Connecting to bot…</div>
      `;
      document.body.appendChild(gate);
    }
    function setGateText(msg) {
      if (!gateShown) return;
      const el = document.querySelector("#status-gate .sg-msg");
      if (el) el.textContent = msg;
    }
    function gateOn(){ document.documentElement.classList.add("await-status"); ensureStatusGate(); }
    function gateOff(){ document.documentElement.classList.remove("await-status"); }
    function lastUpIsFresh(){
      try { const t = Number(sessionStorage.getItem(readyCacheKey) || 0);
        return t && Date.now() - t < readyTtlMs; } catch { return false; }
    }
    function showGateSoon(){
      if (gateShown || gateTimer) return;
      gateTimer = setTimeout(() => { gateTimer = null; if (!gateShown){ gateOn(); gateShown = true; } }, gateDelayMs);
    }
    function showGateNow(){
      if (gateTimer){ clearTimeout(gateTimer); gateTimer = null; }
      if (!gateShown){ gateOn(); gateShown = true; }
    }
    function hideGateSafe(){
      if (gateTimer){ clearTimeout(gateTimer); gateTimer = null; }
      if (gateShown){ gateOff(); gateShown = false; }
    }
  
    function normalizeRuntime(json){
      const s = json?.server || {};
      const lower = (x)=>String(x ?? "").toLowerCase();
      const statusStr = lower(s.status ?? s.state ?? "");
      const serverIsReady =
        s.ready === true ||
        (s.discord && (s.discord.ready || s.discord.connected || s.discord.online)) ||
        statusStr === "ready" ||
        statusStr.includes("logged in as");
      return { isUp: serverIsReady };
    }
    async function fetchRuntime(){
      const ENDPOINTS = ["/api/status"];
      for (const url of ENDPOINTS){
        try { const r = await fetch(url, { cache:"no-store" }); if (!r.ok) continue; return await r.json(); }
        catch{}
      }
      return null;
    }
  
    async function checkAndGate(onReady, fromRetry=false){
      setGateText(fromRetry ? "Rechecking…" : "Connecting to bot…");
      const j = await fetchRuntime();
      if (!j){
        setGateText("Can’t contact bot status. It may be offline.");
        if (!statusPoll) statusPoll = setInterval(() => checkAndGate(onReady, true), 5000);
        return false;
      }
      const st = normalizeRuntime(j);
      if (st.isUp){
        if (statusPoll){ clearInterval(statusPoll); statusPoll = null; }
        hideGateSafe();
        try { sessionStorage.setItem(readyCacheKey, String(Date.now())); } catch {}
        if (typeof onReady === "function") onReady();
        return true;
      } else {
        setGateText("Bot is not running. Start it on the Configuration page.");
        showGateNow();
        if (!statusPoll) statusPoll = setInterval(() => checkAndGate(onReady, true), 5000);
        return false;
      }
    }
  
    return {
      lastUpIsFresh, showGateSoon, checkAndGate, ensureStatusGate
    };
  };
  