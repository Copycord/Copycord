(() => {
  let uiSock = null;
  let toggleLocked = false;
  const RUNTIME_CACHE = {};
  let GUILD_MAPPINGS = [];
  let CURRENT_FILTER_PICKER = null;
  let ROLE_BLOCKS_ALL_ROLES = [];
  let FILTER_OBJECTS_ALL_ITEMS = [];
  let FILTER_OBJECTS_KIND = null;
  let FILTER_OBJECTS_CATMAP = null;
  let lastRunning = null;
  let tooltipEl = null;
  let cModal, cTitle, cBody, cBtnOk, cBtnX, cBtnCa, cBack;
  let confirmResolve = null;
  let confirmReject = null;
  const UPTIME_KEY = (role) => `cpc:uptime:${role}`;
  const ICONS = {
    trash: `
      <svg viewBox="0 0 24 24" width="20" height="20" fill="none" stroke="currentColor" stroke-width="1.5" aria-hidden="true">
        <path stroke-linecap="round" stroke-linejoin="round"
          d="m14.74 9-.346 9m-4.788 0L9.26 9m9.968-3.21c.342.052.682.107 1.022.166m-1.022-.165L18.16 19.673a2.25 2.25 0 0 1-2.244 2.077H8.084a2.25 2.25 0 0 1-2.244-2.077L4.772 5.79m14.456 0a48.108 48.108 0 0 0-3.478-.397m-12 .562c.34-.059.68-.114 1.022-.165m0 0a48.11 48.11 0 0 1 3.478-.397m7.5 0v-.916c0-1.18-.91-2.164-2.09-2.201a51.964 51.964 0 0 0-3.32 0c-1.18.037-2.09 1.022-2.09 2.201v.916m7.5 0a48.667 48.667 0 0 0-7.5 0" />
      </svg>
    `,
    settings: `
      <svg viewBox="0 0 24 24" width="20" height="20" fill="none" stroke="currentColor" stroke-width="1.5" aria-hidden="true">
        <path stroke-linecap="round" stroke-linejoin="round"
          d="M9.594 3.94c.09-.542.56-.94 1.11-.94h2.593c.55 0 1.02.398 1.11.94l.213 1.281c.063.374.313.686.645.87.074.04.147.083.22.127.325.196.72.257 1.075.124l1.217-.456a1.125 1.125 0 0 1 1.37.49l1.296 2.247a1.125 1.125 0 0 1-.26 1.431l-1.003.827c-.293.241-.438.613-.43.992a7.723 7.723 0 0 1 0 .255c-.008.378.137.75.43.991l1.004.827c.424.35.534.955.26 1.43l-1.298 2.247a1.125 1.125 0 0 1-1.369.491l-1.217-.456c-.355-.133-.75-.072-1.076.124a6.47 6.47 0 0 1-.22.128c-.331.183-.581.495-.644.869l-.213 1.281c-.09.543-.56.94-1.11.94h-2.594c-.55 0-1.019-.398-1.11-.94l-.213-1.281c-.062-.374-.312-.686-.644-.87a6.52 6.52 0 0 1-.22-.127c-.325-.196-.72-.257-1.076-.124l-1.217.456a1.125 1.125 0 0 1-1.369-.49l-1.297-2.247a1.125 1.125 0 0 1 .26-1.431l1.004-.827c.292-.24.437-.613.43-.991a6.932 6.932 0 0 1 0-.255c.007-.38-.138-.751-.43-.992l-1.004-.827a1.125 1.125 0 0 1-.26-1.43l1.297-2.247a1.125 1.125 0 0 1 1.37-.491l1.216.456c.356.133.751.072 1.076-.124.072-.044.146-.086.22-.128.332-.183.582-.495.644-.869l.214-1.28Z" />
        <path stroke-linecap="round" stroke-linejoin="round" d="M15 12a3 3 0 1 1-6 0 3 3 0 0 1 6 0Z" />
      </svg>
    `,
    filters: `
      <svg viewBox="0 0 24 24" width="20" height="20" fill="none" stroke="currentColor" stroke-width="1.5" aria-hidden="true">
        <path stroke-linecap="round" stroke-linejoin="round"
          d="M12 3c2.755 0 5.455.232 8.083.678.533.09.917.556.917 1.096v1.044a2.25 2.25 0 0 1-.659 1.591l-5.432 5.432a2.25 2.25 0 0 0-.659 1.591v2.927a2.25 2.25 0 0 1-1.244 2.013L9.75 21v-6.568a2.25 2.25 0 0 0-.659-1.591L3.659 7.409A2.25 2.25 0 0 1 3 5.818V4.774c0-.54.384-1.006.917-1.096A48.32 48.32 0 0 1 12 3Z" />
      </svg>
    `,
  };

  let mapValidated = false;

  function getMappingInputs() {
    return {
      name: document.getElementById("map_mapping_name"),
      host: document.getElementById("map_original_guild_id"),
      clone: document.getElementById("map_cloned_guild_id"),
    };
  }

  function validateMappingFields({ decorate = true } = {}) {
    const { name, host, clone } = getMappingInputs();

    const checks = [
      { el: name, bad: !name.value.trim() },
      { el: host, bad: !host.value.trim() },
      { el: clone, bad: !clone.value.trim() },
    ];

    let ok = true;
    let firstBad = null;

    for (const { el, bad } of checks) {
      if (decorate && mapValidated) {
        if (bad) {
          el.classList.add("flash");

          const handleAnimEnd = () => {
            el.classList.remove("flash");
            el.classList.remove("is-invalid");
            el.removeEventListener("animationend", handleAnimEnd);
          };
          el.addEventListener("animationend", handleAnimEnd, { once: true });
        } else {
          el.classList.remove("flash", "is-invalid");
        }
      } else if (decorate && !mapValidated) {
        el.classList.remove("flash", "is-invalid");
      }

      if (bad && ok) {
        ok = false;
        firstBad = el;
      }
    }

    return { ok, firstBad };
  }

  function bindMappingFieldListeners() {
    const { name, host, clone } = getMappingInputs();
    [name, host, clone].forEach((el) => {
      if (!el || el._mapValBound) return;
      el._mapValBound = true;

      el.addEventListener("input", () => {
        el.classList.remove("flash", "is-invalid");
      });
    });
  }

  let CURRENT_BLOCKED_ROLE_IDS = new Set();

  function parseBlockedRolesCsv(str) {
    const s = (str || "").trim();
    if (!s) return new Set();
    return new Set(
      s
        .split(",")
        .map((x) => x.trim())
        .filter(Boolean)
    );
  }

  const DEFAULT_MAPPING_SETTINGS = {
    DELETE_CHANNELS: true,
    CLONE_MESSAGES: true,
    DELETE_THREADS: true,
    DELETE_MESSAGES: true,
    EDIT_MESSAGES: true,
    REPOSITION_CHANNELS: true,
    DELETE_ROLES: true,
    UPDATE_ROLES: true,
    CLONE_EMOJI: true,
    CLONE_STICKER: true,
    CLONE_ROLES: true,
    MIRROR_ROLE_PERMISSIONS: false,
    MIRROR_CHANNEL_PERMISSIONS: false,
    ENABLE_CLONING: true,
    RENAME_CHANNELS: true,
  };

  let lastFocusLog = null;
  let lastFocusConfirm = null;
  let lastFocusMapping = null;
  let lastFocusFilters = null;
  let currentFilterMapping = null;
  let FILTERS_BASELINE = "";
  let MAPPING_BASELINE = "";

  function setInert(el, on) {
    if (!el) return;
    try {
      if (on) el.setAttribute("inert", "");
      else el.removeAttribute("inert");
    } catch {}
  }

  function saveUptime(role, sec) {
    try {
      sessionStorage.setItem(
        UPTIME_KEY(role),
        JSON.stringify({ sec: Number(sec || 0), t: Date.now() })
      );
    } catch {}
  }
  function loadUptime(role, maxAgeMs = 60_000) {
    try {
      const raw = sessionStorage.getItem(UPTIME_KEY(role));
      if (!raw) return null;
      const obj = JSON.parse(raw);
      if (!obj || typeof obj.sec !== "number" || typeof obj.t !== "number")
        return null;
      if (Date.now() - obj.t > maxAgeMs) return null;
      return obj;
    } catch {
      return null;
    }
  }

  async function refreshFooterVersion() {
    const wrap = document.getElementById("footer-version");
    if (!wrap) return;

    const link = document.getElementById("footer-version-link");
    const plain = document.getElementById("footer-version-text");

    try {
      const res = await fetch("/version", { credentials: "same-origin" });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const v = await res.json();

      if (v.update_available) {
        if (link) {
          link.textContent = v.current || "dev";
          link.classList.add("update-flash");
          link.href = v.url;
          link.setAttribute("aria-label", "New update available");
        } else if (plain) {
          plain.textContent = v.current ? `Version ${v.current}` : "dev";
          plain.classList.add("update-flash");
          plain.style.cursor = "pointer";
          plain.onclick = () => window.open(v.url, "_blank", "noopener");
        }

        const notice = document.getElementById("update-notice");
        if (notice) {
          notice.style.display = "block";
          notice.textContent = "New Update Available";
          notice.onclick = () => window.open(v.url, "_blank", "noopener");
        }
      } else {
        if (link) {
          link.textContent = v.current || "dev";
          link.classList.remove("update-flash");

          const def = link.getAttribute("data-default-href");
          link.href =
            def ||
            `https://github.com/Copycord/Copycord/releases/tag/${v.current}`;
          link.setAttribute("aria-label", `Copycord ${v.current}`);
        } else if (plain) {
          plain.textContent = v.current ? `Version ${v.current}` : "dev";
          plain.classList.remove("update-flash");
          plain.onclick = null;
          plain.style.cursor = "";
        }

        const notice = document.getElementById("update-notice");
        if (notice) {
          notice.style.display = "none";
          notice.onclick = null;
        }
      }
    } catch (err) {
      console.debug("Footer version check failed:", err);
    }
  }

  (function initToasts() {
    function ensureToastRoot() {
      let div = document.getElementById("toast-root");
      if (!div) {
        div = document.createElement("div");
        div.id = "toast-root";
        document.body.appendChild(div);
      }

      div.classList.remove("toast-top-center");

      div.classList.add("toast-root");

      div.style.position = "";
      div.style.top = "";
      div.style.right = "";
      div.style.left = "";
      div.style.transform = "";
      div.style.display = "";
      div.style.flexDirection = "";
      div.style.gap = "";
      div.style.zIndex = "";
    }

    function clearAllToasts() {
      const rootEl = document.getElementById("toast-root");
      if (rootEl) rootEl.innerHTML = "";
      if (window._toastOnceKeys?.clear) window._toastOnceKeys.clear();
    }

    (() => {
      const BOOT_TS = Date.now();
      let BOOT_MS = 900;

      const shouldAnnounceNow = () => Date.now() - BOOT_TS > BOOT_MS;

      function ssSet(key, value, ttlMs = 15000) {
        try {
          if (value == null) {
            sessionStorage.removeItem(key);
            return;
          }
          sessionStorage.setItem(
            key,
            JSON.stringify({ v: value, exp: Date.now() + ttlMs })
          );
        } catch {}
      }
      function ssGet(key) {
        try {
          const raw = sessionStorage.getItem(key);
          if (!raw) return null;
          const obj = JSON.parse(raw);
          if (!obj || typeof obj !== "object") return null;
          if (obj.exp && Date.now() > obj.exp) {
            sessionStorage.removeItem(key);
            return null;
          }
          return obj.v;
        } catch {
          return null;
        }
      }

      function once(key, message, opts = {}, ttlMs = 20000) {
        const k = `toast:once:${key}`;
        if (ssGet(k)) return;
        window.showToast?.(message, opts);
        ssSet(k, 1, ttlMs);
      }

      function markLaunched(key, ttlMs = 20000) {
        ssSet(`toast:launch:${key}`, 1, ttlMs);
      }
      function launchedHere(key) {
        return !!ssGet(`toast:launch:${key}`);
      }
      function clearLaunch(key) {
        ssSet(`toast:launch:${key}`, null, 1);
      }

      function wsGate({
        key,
        msg,
        type = "info",
        force = false,
        ttlMs = 20000,
      }) {
        if (force || shouldAnnounceNow() || launchedHere(key)) {
          once(`ws:${type}:${key}`, msg, { type }, ttlMs);
        }
      }

      window.toast = {
        once,

        markLaunched,
        launchedHere,
        clearLaunch,

        wsGate,

        setBootQuiet(ms) {
          BOOT_MS = Math.max(0, Number(ms) || 0);
        },
        shouldAnnounceNow,
      };

      window.addEventListener("pageshow", (e) => {
        if (e.persisted) window.clearAllToasts?.();
      });
    })();

    function escapeHtml(s) {
      return String(s).replace(
        /[&<>"']/g,
        (c) =>
          ({
            "&": "&amp;",
            "<": "&lt;",
            ">": "&gt;",
            '"': "&quot;",
            "'": "&#39;",
          }[c])
      );
    }

    function showToast(message, { type = "info", timeout = 4000 } = {}) {
      ensureToastRoot();
      const rootEl = document.getElementById("toast-root");
      if (!rootEl) return;

      const el = document.createElement("div");
      el.className = `toast toast-${type}`;
      el.role = "status";
      el.innerHTML = `
        <div class="toast-bar"></div>
        <div class="toast-msg">${escapeHtml(message)}</div>
      `;
      rootEl.appendChild(el);
      requestAnimationFrame(() => el.classList.add("show"));

      const close = () => {
        el.classList.remove("show");
        setTimeout(() => el.remove(), 200);
      };
      el.addEventListener("click", close);
      if (timeout > 0) setTimeout(close, timeout);
    }

    window.showToast = showToast;
    window.clearAllToasts = clearAllToasts;
    window.escapeHtml = escapeHtml;

    window.addEventListener("pageshow", (e) => {
      if (!e.persisted) return;
      window.clearAllToasts();
    });
  })();

  function getCurrentRunning(data) {
    return !!(data.server?.running || data.client?.running);
  }

  function renderStatusRow(role, s) {
    const elState = document.getElementById(`${role}-state`);
    const elUp = document.getElementById(`${role}-uptime`);
    const elStatus = document.getElementById(`${role}-status`);

    if (!elState || !elUp || !elStatus) return;

    const running = !!s.running;
    const rawStatus = (s.status || "").trim();

    elState.textContent = running ? "running" : "stopped";
    elState.classList.toggle("badge-ok", running);
    elState.classList.toggle("badge-stop", !running);

    if (running) {
      if (s.uptime_sec != null) {
        RUNTIME_CACHE[role] = {
          baseSec: Number(s.uptime_sec),
          lastUpdateMs: Date.now(),
        };

        saveUptime(role, s.uptime_sec);

        const elUp = document.getElementById(`${role}-uptime`);
        if (elUp) elUp.textContent = formatUptime(s.uptime_sec);
      }
    } else {
      delete RUNTIME_CACHE[role];
      saveUptime(role, 0);
      const elUp = document.getElementById(`${role}-uptime`);
      if (elUp) elUp.textContent = "";
    }

    const blocked = /^(active|stopped)$/i.test(rawStatus);
    const displayStatus = blocked ? "" : rawStatus;

    elStatus.textContent = displayStatus;
    elStatus.title = displayStatus || "";

    if (elStatus.dataset.expanded === "1") {
      elStatus.classList.add("expanded");
    } else {
      elStatus.classList.remove("expanded");
    }
    elStatus.style.whiteSpace = elStatus.classList.contains("expanded")
      ? "normal"
      : "nowrap";

    if (!elStatus._toggleBound) {
      elStatus._toggleBound = true;
      elStatus.style.cursor = "pointer";
      elStatus.addEventListener("click", () => {
        const expanded = elStatus.dataset.expanded === "1";
        elStatus.dataset.expanded = expanded ? "0" : "1";
        elStatus.classList.toggle("expanded", !expanded);
        elStatus.style.whiteSpace = !expanded ? "normal" : "nowrap";
      });
    }
  }

  function updateToggleButton(data) {
    const btn = document.getElementById("toggle-btn");
    const form = document.getElementById("toggle-form");
    if (!btn || !form) return;

    const running = !!(data.server?.running || data.client?.running);
    btn.textContent = running ? "Stop" : "Start";
    form.action = running ? "/stop" : "/start";

    validateConfigAndToggle({ decorate: false });
  }

  function formatUptime(sec) {
    const s = Math.max(0, Math.floor(Number(sec || 0)));
    const d = Math.floor(s / 86400);
    const h = Math.floor((s % 86400) / 3600);
    const m = Math.floor((s % 3600) / 60);
    const ss = s % 60;
    const parts = [];
    if (d) parts.push(`${d}d`);
    if (h) parts.push(`${h}h`);
    if (m) parts.push(`${m}m`);
    parts.push(`${ss}s`);
    return parts.join(" ");
  }

  setInterval(() => {
    for (const role of Object.keys(RUNTIME_CACHE)) {
      const elUp = document.getElementById(`${role}-uptime`);
      if (!elUp) continue;
      const r = RUNTIME_CACHE[role];
      const elapsed = Math.floor((Date.now() - r.lastUpdateMs) / 1000);
      elUp.textContent = formatUptime(r.baseSec + elapsed);
    }
  }, 1000);

  let statusTimer = null;
  let uiSockTimer = null;
  let currentInterval = 4000;

  function ensureTooltipEl() {
    if (!tooltipEl) {
      tooltipEl = document.createElement("div");
      tooltipEl.className = "lock-tooltip";
      tooltipEl.innerHTML = `
      <div class="lock-tooltip-text">Stop the bot to edit server mappings</div>
      <div class="lock-tooltip-arrow"></div>
    `;
      document.body.appendChild(tooltipEl);
    }
    return tooltipEl;
  }

  function showTooltip(x, y) {
    const tip = ensureTooltipEl();

    const offsetY = 16;
    tip.style.left = x + "px";
    tip.style.top = y - offsetY + "px";
    tip.style.opacity = "1";
  }

  function hideTooltip() {
    if (tooltipEl) {
      tooltipEl.style.opacity = "0";
    }
  }

  function attachLockOverlay(card) {
    if (card.querySelector(".guild-card-lock-overlay")) return;

    const cs = window.getComputedStyle(card);
    if (cs.position === "static") {
      card.style.position = "relative";
    }

    const overlay = document.createElement("div");
    overlay.className = "guild-card-lock-overlay";

    const kill = (e) => {
      e.preventDefault();
      e.stopPropagation();
    };
    overlay.addEventListener("click", kill);
    overlay.addEventListener("mousedown", kill);
    overlay.addEventListener("mouseup", kill);
    overlay.addEventListener("touchstart", kill);
    overlay.addEventListener("touchend", kill);

    overlay.addEventListener("mouseenter", (e) => {
      showTooltip(e.clientX, e.clientY);
    });

    overlay.addEventListener("mousemove", (e) => {
      showTooltip(e.clientX, e.clientY);
    });

    overlay.addEventListener("mouseleave", () => {
      hideTooltip();
    });

    card.appendChild(overlay);
  }

  function detachLockOverlay(card) {
    const overlay = card.querySelector(".guild-card-lock-overlay");
    if (overlay) overlay.remove();
  }

  function setGlobalConfigLocked(running) {
    const cfgForm = document.getElementById("cfg-form");
    const saveBtn = document.getElementById("cfg-save-btn");
    const cancelBtn = document.getElementById("cfg-cancel-btn");

    if (!cfgForm) return;

    if (running) {
      cfgForm.classList.add("cfg-locked");
    } else {
      cfgForm.classList.remove("cfg-locked");
    }

    cfgForm
      .querySelectorAll("input, select, textarea, button")
      .forEach((el) => {
        const id = el.id || "";
        const isSave = id === "cfg-save-btn";
        const isCancel = id === "cfg-cancel-btn";
        const isReveal = el.classList.contains("reveal-btn");

        if (isSave || isCancel) {
          el.disabled = running;
          el.classList.toggle("disabled-btn", running);
          el.title = running ? "Stop the bot to edit global configuration" : "";
          return;
        }

        if (isReveal) {
          el.disabled = running;
          el.classList.toggle("disabled-btn", running);
          el.title = running ? "Stop the bot to view token values" : "";
          return;
        }

        el.disabled = running;
        if (running) {
          el.classList.add("locked-field");
        } else {
          el.classList.remove("locked-field");
        }
      });

    cfgForm.querySelectorAll(".dd").forEach((dd) => {
      if (running) {
        dd.setAttribute("data-locked", "1");
      } else {
        dd.removeAttribute("data-locked");
      }
    });

    cfgForm.querySelectorAll(".chips").forEach((chipsEl) => {
      if (running) {
        chipsEl.setAttribute("data-locked", "1");
        chipsEl.classList.add("locked-field");
      } else {
        chipsEl.removeAttribute("data-locked");
        chipsEl.classList.remove("locked-field");
      }
    });
  }

  function setGuildCardsLocked(running) {
    const cards = document.querySelectorAll("#guild-mapping-list .guild-card");
    cards.forEach((card) => {
      if (running) {
        card.classList.add("locked");
        attachLockOverlay(card);
      } else {
        card.classList.remove("locked");
        detachLockOverlay(card);
      }
    });
    if (!running) hideTooltip();
  }

  async function fetchAndRenderStatus() {
    try {
      const res = await fetch("/api/status", { credentials: "same-origin" });
      if (!res.ok) return;
      const data = await res.json();

      renderStatusRow("server", data.server || {});
      renderStatusRow("client", data.client || {});
      updateToggleButton(data);

      const running = getCurrentRunning(data);

      setGuildCardsLocked(running);
      setGlobalConfigLocked(running);

      if (lastRunning === null) lastRunning = running;

      if (toggleLocked && running !== lastRunning) {
        toggleLocked = false;
        setToggleDisabled(false);
      }
      lastRunning = running;

      const srvOk =
        data.server && data.server.ok !== false && !data.server.error;
      const cliOk =
        data.client && data.client.ok !== false && !data.client.error;
      if (!srvOk || !cliOk) startStatusPoll(8000);
    } catch {
      startStatusPoll(Math.min(currentInterval * 2, 15000));
    }
  }

  function startStatusPoll(intervalMs) {
    if (intervalMs === currentInterval && statusTimer) return;
    currentInterval = intervalMs;
    if (statusTimer) clearInterval(statusTimer);
    statusTimer = setInterval(fetchAndRenderStatus, currentInterval);
  }

  function burstStatusPoll(fastMs = 800, durationMs = 12000, slowMs = 4000) {
    startStatusPoll(fastMs);
    fetchAndRenderStatus();
    setTimeout(() => startStatusPoll(slowMs), durationMs);
  }

  document.addEventListener("visibilitychange", () => {
    if (document.hidden) startStatusPoll(15000);
    else {
      fetchAndRenderStatus();
      startStatusPoll(4000);
    }
  });

  const modal = document.getElementById("log-modal");
  const logBody = document.getElementById("log-body");
  const logTitle = document.getElementById("log-title");
  const closeBtn = document.getElementById("log-close");
  const backdrop = modal ? modal.querySelector(".modal-backdrop") : null;
  let LOG_LINES = [];
  let LOG_QUERY = "";

  let es = null;
  let autoFollow = true;
  const THRESH = 24;

  function renderLogView({ preserveScroll = false } = {}) {
    if (!logBody) return;

    const shouldStick =
      logBody.scrollHeight - logBody.scrollTop - logBody.clientHeight <= THRESH;

    const q = LOG_QUERY.trim().toLowerCase();
    let view = LOG_LINES;

    if (q) {
      view = LOG_LINES.filter((l) => l.toLowerCase().includes(q));
    }

    logBody.textContent = view.length ? view.join("\n") + "\n" : "";

    if (shouldStick || !preserveScroll) {
      logBody.scrollTop = logBody.scrollHeight;
    }
  }

  function onScroll() {
    autoFollow =
      logBody.scrollHeight - logBody.scrollTop - logBody.clientHeight <= THRESH;
  }

  const MAX_LINES = 10000;

  function appendLines(lines) {
    if (!Array.isArray(lines) || lines.length === 0) return;
    for (const l of lines) {
      LOG_LINES.push(String(l ?? ""));
    }
    if (LOG_LINES.length > MAX_LINES) {
      LOG_LINES.splice(0, LOG_LINES.length - MAX_LINES);
    }
    renderLogView({ preserveScroll: true });
  }

  function appendLine(line) {
    LOG_LINES.push(String(line ?? ""));
    if (LOG_LINES.length > MAX_LINES) {
      LOG_LINES.splice(0, LOG_LINES.length - MAX_LINES);
    }
    renderLogView({ preserveScroll: true });
  }

  function openLogs(which) {
    if (!modal || !logBody) return;
    if (es) {
      try {
        es.close();
      } catch {}
      es = null;
    }
    lastFocusLog = document.activeElement;

    logTitle.textContent = which === "server" ? "Server logs" : "Client logs";
    logBody.textContent = "";
    modal.classList.add("show");
    setInert(modal, false);
    modal.setAttribute("aria-hidden", "false");

    document.body.classList.add("body-lock-scroll");

    LOG_LINES = [];
    LOG_QUERY = "";
    renderLogView();

    const qInput = document.getElementById("log-search-input");
    if (qInput) {
      qInput.value = "";
      setTimeout(() => qInput.focus(), 0);

      let t;
      qInput.oninput = () => {
        clearTimeout(t);
        const val = qInput.value || "";
        t = setTimeout(() => {
          LOG_QUERY = val;
          renderLogView();
        }, 60);
      };

      qInput.onkeydown = (e) => {
        if (e.key === "Escape") {
          qInput.value = "";
          LOG_QUERY = "";
          renderLogView();
          e.preventDefault();
        }
      };
    }

    autoFollow = true;
    logBody.addEventListener("scroll", onScroll, { passive: true });
    const firstFocusable =
      document.getElementById("log-search-input") ||
      modal.querySelector(
        'button, [href], input, select, textarea, [tabindex]:not([tabindex="-1"])'
      ) ||
      logBody;
    setTimeout(() => firstFocusable?.focus(), 0);

    let retryTimer = null;
    function startStream() {
      clearTimeout(retryTimer);
      if (es) {
        try {
          es.close();
        } catch {}
      }
      es = new EventSource(`/logs/stream/${which}`);

      es.onmessage = (ev) => {
        try {
          const obj = JSON.parse(ev.data);
          if (Array.isArray(obj.lines)) appendLines(obj.lines);
          else if (typeof obj.line === "string") appendLine(obj.line);
        } catch {
          appendLine(ev.data);
        }
      };

      es.onerror = () => {
        showToast("Log stream temporarily unavailable… retrying", {
          type: "warning",
          timeout: 2000,
        });
        try {
          es.close();
        } catch {}
        retryTimer = setTimeout(() => {
          if (modal.classList.contains("show")) startStream();
        }, 1500);
      };
    }

    startStream();
  }

  function closeLogs() {
    if (es) {
      try {
        es.close();
      } catch {}
      es = null;
    }
    logBody.removeEventListener("scroll", onScroll);
    const active = document.activeElement;
    if (active && modal.contains(active)) {
      try {
        active.blur();
      } catch {}
    }
    setInert(modal, true);
    modal.classList.remove("show");
    modal.setAttribute("aria-hidden", "true");

    document.body.classList.remove("body-lock-scroll");

    if (lastFocusLog && typeof lastFocusLog.focus === "function") {
      try {
        lastFocusLog.focus();
      } catch {}
    }
    lastFocusLog = null;
  }

  document.querySelectorAll("[data-log]").forEach((btn) => {
    btn.addEventListener("click", () => openLogs(btn.getAttribute("data-log")));
  });
  if (closeBtn) closeBtn.addEventListener("click", closeLogs);
  if (backdrop) backdrop.addEventListener("click", closeLogs);
  document.addEventListener("keydown", (e) => {
    if (e.key === "Escape" && modal && modal.classList.contains("show"))
      closeLogs();
  });

  function enhanceAllSelects() {
    document
      .querySelectorAll("select:not([multiple]):not([data-dd])")
      .forEach(initDropdown);
  }

  function initDropdown(sel) {
    sel.setAttribute("data-dd", "1");

    const isDisabled = sel.disabled;
    const dd = document.createElement("div");
    dd.className = "dd";
    if (isDisabled) dd.dataset.disabled = "true";

    sel.parentNode.insertBefore(dd, sel);
    dd.appendChild(sel);

    sel.classList.add("is-hidden-native");

    const btn = document.createElement("button");
    btn.type = "button";
    btn.className = "dd-toggle";
    btn.setAttribute("aria-has-popup", "listbox");
    btn.setAttribute("aria-expanded", "false");

    const lbl = dd.closest(".field")?.querySelector(`label[for="${sel.id}"]`);
    if (lbl) {
      if (!lbl.id) lbl.id = `${sel.id}-label`;
      btn.setAttribute("aria-label-by", lbl.id);
      lbl.addEventListener("click", (e) => {
        e.preventDefault();
        btn.focus();
      });
    }

    const menu = document.createElement("div");
    const listboxId = `${
      sel.id || Math.random().toString(36).slice(2)
    }-listbox`;
    menu.className = "dd-menu";
    menu.id = listboxId;
    menu.setAttribute("role", "listbox");
    btn.setAttribute("aria-controls", listboxId);

    dd.appendChild(btn);
    dd.appendChild(menu);

    let items = [];
    let focusIndex = Math.max(0, sel.selectedIndex);

    function optionLabel(opt) {
      return (opt?.textContent || "").trim();
    }
    function updateButtonLabel() {
      const current = sel.options[sel.selectedIndex];
      btn.innerHTML = `<span class="dd-label">${escapeHtml(
        optionLabel(current)
      )}</span><span class="dd-caret">▾</span>`;
    }
    function rebuildMenu() {
      menu.innerHTML = "";
      items = Array.from(sel.options).map((o, i) => {
        const el = document.createElement("div");
        el.className = "dd-option";
        el.setAttribute("role", "option");
        el.dataset.value = o.value;
        el.setAttribute("aria-selected", o.selected ? "true" : "false");
        el.textContent = optionLabel(o);
        el.addEventListener("mousedown", (e) => e.preventDefault());
        el.addEventListener("click", () => chooseIndex(i));
        menu.appendChild(el);
        return el;
      });
      focusIndex = Math.max(0, sel.selectedIndex);
      updateKbdHover();
    }

    function chooseIndex(i) {
      if (i < 0 || i >= sel.options.length) return;
      sel.selectedIndex = i;
      sel.dispatchEvent(new Event("change", { bubbles: true }));
      items.forEach((it, idx) =>
        it.setAttribute("aria-selected", idx === i ? "true" : "false")
      );
      updateButtonLabel();
      closeMenu();
    }

    function openMenu() {
      if (isDisabled) return;
      dd.classList.add("open");
      btn.setAttribute("aria-expanded", "true");
      updateKbdHover();
      requestAnimationFrame(() => {
        const el = items[sel.selectedIndex];
        if (el) {
          const r = el.getBoundingClientRect();
          const mr = menu.getBoundingClientRect();
          if (r.top < mr.top || r.bottom > mr.bottom)
            el.scrollIntoView({ block: "nearest" });
        }
      });
      window.addEventListener("click", onOutsideClick, { capture: true });
    }

    function closeMenu() {
      dd.classList.remove("open");
      btn.setAttribute("aria-expanded", "false");
      window.removeEventListener("click", onOutsideClick, { capture: true });
    }

    function onOutsideClick(e) {
      if (!dd.contains(e.target)) closeMenu();
    }

    function moveFocus(delta) {
      focusIndex = Math.min(items.length - 1, Math.max(0, focusIndex + delta));
      updateKbdHover(true);
    }
    function setFocus(i) {
      focusIndex = Math.min(items.length - 1, Math.max(0, i));
      updateKbdHover(true);
    }
    function updateKbdHover(scroll = false) {
      items.forEach((it, idx) =>
        it.classList.toggle("kbd-hover", idx === focusIndex)
      );
      if (scroll) items[focusIndex]?.scrollIntoView({ block: "nearest" });
    }

    btn.addEventListener("keydown", (e) => {
      if (isDisabled) return;
      switch (e.key) {
        case "ArrowDown":
          e.preventDefault();
          if (!dd.classList.contains("open")) openMenu();
          else moveFocus(1);
          break;
        case "ArrowUp":
          e.preventDefault();
          if (!dd.classList.contains("open")) openMenu();
          else moveFocus(-1);
          break;
        case "Home":
          e.preventDefault();
          if (!dd.classList.contains("open")) openMenu();
          setFocus(0);
          break;
        case "End":
          e.preventDefault();
          if (!dd.classList.contains("open")) openMenu();
          setFocus(items.length - 1);
          break;
        case "Enter":
        case " ":
          e.preventDefault();
          if (!dd.classList.contains("open")) openMenu();
          else chooseIndex(focusIndex);
          break;
        case "Escape":
          if (dd.classList.contains("open")) {
            e.preventDefault();
            closeMenu();
          }
          break;
        default:
          break;
      }
    });

    btn.addEventListener("click", () => {
      if (dd.classList.contains("open")) closeMenu();
      else openMenu();
    });

    sel.addEventListener("change", () => {
      rebuildMenu();
      updateButtonLabel();
    });
    const form = sel.closest("form");
    if (form) {
      form.addEventListener("reset", () => {
        setTimeout(() => {
          rebuildMenu();
          updateButtonLabel();
          closeMenu();
        }, 0);
      });
    }

    sel.addEventListener("focus", () => btn.focus());

    rebuildMenu();
    updateButtonLabel();
  }

  function initCollapsibleCards() {
    const cards = document.querySelectorAll(".card");

    cards.forEach((card, idx) => {
      const h = card.querySelector(":scope > h3");
      if (!h) return;

      const titleBar = document.createElement("div");
      titleBar.className = "card-titlebar";
      h.parentNode.insertBefore(titleBar, h);
      titleBar.appendChild(h);

      const body =
        card.querySelector(":scope > .card-body") ||
        (() => {
          const b = document.createElement("div");
          b.className = "card-body";
          card.appendChild(b);
          while (titleBar.nextSibling && titleBar.nextSibling !== b) {
            b.appendChild(titleBar.nextSibling);
          }
          return b;
        })();

      const slug = (h.textContent || `panel-${idx}`)
        .toLowerCase()
        .replace(/[^a-z0-9]+/g, "-")
        .replace(/(^-|-$)/g, "");
      body.id = body.id || `card-body-${slug}`;

      const btn = document.createElement("button");
      btn.type = "button";
      btn.className = "btn btn-ghost btn-icon card-toggle";
      btn.setAttribute("aria-controls", body.id);
      btn.setAttribute("aria-expanded", "true");
      btn.setAttribute("aria-label", "Collapse panel");
      btn.innerHTML = `<span class="chev" aria-hidden="true">▾</span>`;
      titleBar.appendChild(btn);

      const key = `cpc.collapsed.${slug}`;
      applyCollapse(card, btn, body, localStorage.getItem(key) === "1");

      const toggle = () => {
        const nowCollapsed = !card.classList.contains("collapsed");
        applyCollapse(card, btn, body, nowCollapsed);
        localStorage.setItem(key, nowCollapsed ? "1" : "0");
      };
      btn.addEventListener("click", toggle);
      titleBar.addEventListener("dblclick", toggle);
    });

    function applyCollapse(card, btn, body, collapsed) {
      card.classList.toggle("collapsed", collapsed);

      body.hidden = !!collapsed;

      btn.setAttribute("aria-expanded", collapsed ? "false" : "true");
      btn.setAttribute(
        "aria-label",
        collapsed ? "Expand panel" : "Collapse panel"
      );

      card.style.width = "100%";
      card.style.maxWidth = "100%";
      card.style.minWidth = "100%";

      const ev = new CustomEvent("card-toggled", {
        detail: { collapsed },
        bubbles: true,
      });
      card.dispatchEvent(ev);
    }
  }

  class ChipsInput {
    constructor(root, hidden) {
      this.root = root;
      this.hidden = hidden;

      this.entry = root.querySelector(".chip-input");

      this.entryWrap = this.entry
        ? this.entry.closest(".chip-input-wrap") || this.entry
        : null;

      this.values = [];

      if (this.entry) {
        this.entry.addEventListener("keydown", (e) => {
          if (this.root.getAttribute("data-locked") === "1") {
            e.preventDefault();
            return;
          }

          if (e.key === "Enter") {
            e.preventDefault();
            this.addFromText(this.entry.value);
            this.entry.value = "";
          } else if (
            e.key === "Backspace" &&
            this.entry.value === "" &&
            this.values.length
          ) {
            this.remove(this.values[this.values.length - 1]);
          }
        });

        this.entry.addEventListener("paste", (e) => {
          if (this.root.getAttribute("data-locked") === "1") {
            e.preventDefault();
            return;
          }

          const text =
            (e.clipboardData && e.clipboardData.getData("text")) || "";
          if (text) {
            e.preventDefault();
            this.addFromText(text);
          }
        });
      }
    }

    addFromText(text) {
      if (this.root.getAttribute("data-locked") === "1") {
        return;
      }

      const parts = String(text)
        .split(/[^\d]+/)
        .map((s) => s.trim())
        .filter(Boolean);

      const ids = [];
      for (const s of parts) {
        try {
          if (!/^\d+$/.test(s)) continue;
          const n = BigInt(s);
          if (n <= 0n) continue;
          ids.push(n.toString());
        } catch {}
      }
      this.addMany(ids);
    }

    addMany(arr) {
      if (this.root.getAttribute("data-locked") === "1") {
        return false;
      }

      let changed = false;
      for (const id of arr) {
        if (!this.values.includes(id)) {
          this.values.push(id);
          this.renderChip(id);
          changed = true;
        }
      }
      if (changed) this.syncHidden();
      return changed;
    }

    remove(id) {
      if (this.root.getAttribute("data-locked") === "1") {
        return;
      }

      const ix = this.values.indexOf(id);
      if (ix >= 0) {
        this.values.splice(ix, 1);
        const chipEl = this.root.querySelector(
          `.chip[data-id="${CSS.escape(id)}"]`
        );
        if (chipEl) chipEl.remove();
        this.syncHidden();
      }
    }

    renderChip(id) {
      const chip = document.createElement("span");
      chip.className = "chip";
      chip.dataset.id = id;
      chip.textContent = id;

      chip.addEventListener("click", () => {
        if (this.root.getAttribute("data-locked") === "1") {
          return;
        }
        this.remove(id);
      });

      if (this.entryWrap && this.entryWrap.parentNode === this.root) {
        this.root.insertBefore(chip, this.entryWrap);
      } else if (this.entry && this.entry.parentNode === this.root) {
        this.root.insertBefore(chip, this.entry);
      } else {
        this.root.appendChild(chip);
      }
    }

    syncHidden() {
      this.hidden.value = this.values.join(",");
      this.hidden.dispatchEvent(new Event("input", { bubbles: true }));
    }

    set(list) {
      this.values = [];

      Array.from(this.root.querySelectorAll(".chip")).forEach((el) =>
        el.remove()
      );

      const cleaned = (list || []).map(String);
      const changed = this.addMany(cleaned);

      if (this.entry) {
        this.entry.value = "";
      }

      if (!changed) {
        this.syncHidden();
      }
    }

    get() {
      return [...this.values];
    }
  }

  class WordChipsInput {
    constructor(rootEl, hiddenEl) {
      this.root = rootEl;
      this.hidden = hiddenEl;
      this.values = [];

      this.entryWrap = rootEl.querySelector(".chip-input-wrap") || rootEl;
      this.entry = rootEl.querySelector(".chip-input");

      this.root.addEventListener("click", (ev) => {
        const clickedRootItself =
          ev.target === this.root || ev.target === this.entryWrap;

        if (this.root.getAttribute("data-locked") === "1") {
          return;
        }

        if (clickedRootItself && this.entry) {
          this.entry.focus();
        }
      });

      if (this.entry) {
        this.entry.addEventListener("keydown", (ev) => {
          if (this.root.getAttribute("data-locked") === "1") {
            ev.preventDefault();
            return;
          }

          if (ev.key === "Enter" || ev.key === ",") {
            ev.preventDefault();
            this.addFromText(this.entry.value);
            this.entry.value = "";
            return;
          }

          if (
            (ev.key === "Backspace" || ev.key === "Delete") &&
            !this.entry.value
          ) {
            if (this.values.length) {
              this.remove(this.values[this.values.length - 1]);
            }
          }
        });

        this.entry.addEventListener("paste", (ev) => {
          if (this.root.getAttribute("data-locked") === "1") {
            ev.preventDefault();
            return;
          }

          const clip = ev.clipboardData?.getData("text") || "";
          if (!clip) return;
          ev.preventDefault();
          this.addFromText(clip);
          this.entry.value = "";
        });
      }
    }

    addFromText(text) {
      if (this.root.getAttribute("data-locked") === "1") {
        return;
      }

      if (!text) return;
      const parts = String(text)
        .split(/[,|\n]/)
        .map((s) => s.trim())
        .filter(Boolean);

      this.addMany(parts);
    }

    addMany(arr) {
      if (this.root.getAttribute("data-locked") === "1") {
        return false;
      }

      let changed = false;
      for (const raw of arr) {
        const word = raw.slice(0, 100);
        if (!this.values.includes(word)) {
          this.values.push(word);
          this._renderChip(word);
          changed = true;
        }
      }
      if (changed) {
        this._syncHidden();
      }
      return changed;
    }

    remove(word) {
      if (this.root.getAttribute("data-locked") === "1") {
        return;
      }

      const idx = this.values.indexOf(word);
      if (idx !== -1) {
        this.values.splice(idx, 1);
      }

      const sel = `.chip[data-id="${CSS.escape(word)}"]`;
      const chipEl = this.root.querySelector(sel);
      if (chipEl) chipEl.remove();

      this._syncHidden();
    }

    _renderChip(word) {
      const chip = document.createElement("span");
      chip.className = "chip";
      chip.dataset.id = word;
      chip.textContent = word;

      chip.addEventListener("click", () => {
        if (this.root.getAttribute("data-locked") === "1") {
          return;
        }
        this.remove(word);
      });

      if (this.entryWrap && this.entryWrap.parentNode === this.root) {
        this.root.insertBefore(chip, this.entryWrap);
      } else if (this.entry && this.entry.parentNode === this.root) {
        this.root.insertBefore(chip, this.entry);
      } else {
        this.root.appendChild(chip);
      }
    }

    _syncHidden() {
      this.hidden.value = this.values.join(",");
      this.hidden.dispatchEvent(new Event("input", { bubbles: true }));
    }

    set(list) {
      this.values = [];
      for (const el of Array.from(this.root.querySelectorAll(".chip"))) {
        el.remove();
      }

      const cleaned = Array.isArray(list)
        ? list.map((x) => String(x).trim()).filter(Boolean)
        : [];

      const changed = this.addMany(cleaned);

      if (this.entry) {
        this.entry.value = "";
      }

      if (!changed) {
        this._syncHidden();
      }
    }

    get() {
      return [...this.values];
    }
  }

  function parseIdList(str) {
    return String(str || "")
      .split(/[^\d]+/)
      .map((s) => s.trim())
      .filter(Boolean);
  }

  const BASELINES = { cmd_users_csv: "", cfg: "", filters: "" };
  let CHIPS = Object.create(null);

  function initChips() {
    CHIPS = Object.create(null);

    const defs = [
      ["wl_categories", "wl_categories", "ids"],
      ["wl_channels", "wl_channels", "ids"],
      ["ex_categories", "ex_categories", "ids"],
      ["ex_channels", "ex_channels", "ids"],
      ["blocked_words", "blocked_words", "words"],
      ["cmd_users", "COMMAND_USERS", "ids"],
      ["blocked_roles", "blocked_role_ids", "ids"],
      ["wl_users", "wl_users", "ids"],
      ["bl_users", "bl_users", "ids"],
    ];
    for (const [dataKey, hiddenId, mode] of defs) {
      const root = document.querySelector(`.chips[data-chips="${dataKey}"]`);
      if (!root) continue;

      const hidden = document.getElementById(hiddenId);
      if (!hidden) continue;

      const ci =
        mode === "words"
          ? new WordChipsInput(root, hidden)
          : new ChipsInput(root, hidden);

      if (mode === "words") {
        const seedWords = String(hidden.value || "")
          .split(",")
          .map((s) => s.trim())
          .filter(Boolean);
        ci.set(seedWords);
      } else {
        ci.set(parseIdList(hidden.value || ""));
      }

      CHIPS[dataKey] = ci;

      if (dataKey === "cmd_users") {
        BASELINES.cmd_users_csv = hidden.value || "";
      }

      const form = hidden.closest("form");
      if (form) {
        form.addEventListener("reset", () => {
          window.setTimeout(() => {
            if (mode === "words") {
              const resetWords = String(hidden.value || "")
                .split(",")
                .map((s) => s.trim())
                .filter(Boolean);
              ci.set(resetWords);
            } else {
              ci.set(parseIdList(hidden.value || ""));
            }
          }, 0);
        });
      }
    }
  }

  function initSlideMenu() {
    const menu = document.getElementById("side-menu");
    const backdrop = document.getElementById("menu-backdrop");
    const toggleBtn = document.getElementById("menu-toggle");
    if (!menu || !backdrop || !toggleBtn) return;

    let lastFocus = null;

    const bodyLock = (on) => {
      document.documentElement.classList.toggle("no-scroll", on);
    };
    const setAria = (open) => {
      menu.setAttribute("aria-hidden", open ? "false" : "true");
      toggleBtn.setAttribute("aria-expanded", open ? "true" : "false");
    };

    const openMenu = () => {
      if (menu.classList.contains("is-open")) return;
      lastFocus = document.activeElement;

      backdrop.hidden = false;

      menu.classList.add("is-open");
      backdrop.classList.add("show");
      toggleBtn.classList.add("is-open");
      setAria(true);
      bodyLock(true);

      const focusable = menu.querySelector(
        'button, [href], input, select, textarea, [tabindex]:not([tabindex="-1"])'
      );
      (focusable || menu).focus();
    };

    const closeMenu = () => {
      if (!menu.classList.contains("is-open")) return;

      menu.classList.remove("is-open");
      backdrop.classList.remove("show");
      toggleBtn.classList.remove("is-open");
      setAria(false);
      bodyLock(false);

      requestAnimationFrame(() => {
        backdrop.hidden = true;
      });

      if (lastFocus && typeof lastFocus.focus === "function") lastFocus.focus();
    };

    toggleBtn.addEventListener("click", () => {
      if (menu.classList.contains("is-open")) closeMenu();
      else openMenu();
    });

    backdrop.addEventListener("click", closeMenu);
    document.addEventListener("keydown", (e) => {
      if (e.key === "Escape" && menu.classList.contains("is-open")) {
        e.preventDefault();
        closeMenu();
      }
    });

    setAria(false);

    backdrop.hidden = true;
  }

  async function safeText(res) {
    try {
      return await res.text();
    } catch {
      return "";
    }
  }
  function snapshotForm(form) {
    const fd = new FormData(form);
    return new URLSearchParams(fd).toString();
  }

  const REQUIRED_KEYS = ["SERVER_TOKEN", "CLIENT_TOKEN"];
  let cfgValidated = false;

  function setCfgButtonsVisible(show) {
    const saveBtn = document.getElementById("cfg-save-btn");
    const cancelBtn = document.getElementById("cfg-cancel-btn");
    if (!saveBtn || !cancelBtn) return;

    const vis = show ? "visible" : "hidden";

    saveBtn.style.visibility = vis;
    cancelBtn.style.visibility = vis;

    saveBtn.disabled = !show;
    cancelBtn.disabled = !show;
  }

  function configState() {
    const get = (id) => (document.getElementById(id)?.value || "").trim();
    const vals = Object.fromEntries(REQUIRED_KEYS.map((k) => [k, get(k)]));

    const hasTokens = !!(vals.SERVER_TOKEN && vals.CLIENT_TOKEN);
    const hasAtLeastOneMapping =
      Array.isArray(GUILD_MAPPINGS) && GUILD_MAPPINGS.length > 0;

    const ok = hasTokens && hasAtLeastOneMapping;

    const missing = [];
    if (!vals.SERVER_TOKEN) missing.push("SERVER_TOKEN");
    if (!vals.CLIENT_TOKEN) missing.push("CLIENT_TOKEN");
    if (!hasAtLeastOneMapping) missing.push("GUILD_MAPPINGS");

    return { ok, missing };
  }

  function markInvalid(missing) {
    REQUIRED_KEYS.forEach((k) => {
      const el = document.getElementById(k);
      if (!el) return;
      el.classList.toggle("is-invalid", missing.includes(k));
    });
  }

  function clearInvalid() {
    REQUIRED_KEYS.forEach((k) => {
      const el = document.getElementById(k);
      if (el) el.classList.remove("is-invalid");
    });
  }

  function validateConfigAndToggle({ decorate = false } = {}) {
    const btn = document.getElementById("toggle-btn");
    const form = document.getElementById("toggle-form");

    const saveBtn = document.getElementById("cfg-save-btn");
    const cancelBtn = document.getElementById("cfg-cancel-btn");

    const runningNow = !!(RUNTIME_CACHE.server || RUNTIME_CACHE.client);

    if (runningNow) {
      if (saveBtn) {
        saveBtn.disabled = true;
        saveBtn.classList.add("disabled-btn");
        saveBtn.title = "Stop the bot to edit global configuration";
      }
      if (cancelBtn) {
        cancelBtn.disabled = true;
        cancelBtn.classList.add("disabled-btn");
        cancelBtn.title = "Stop the bot to edit global configuration";
      }
      return;
    }

    if (!btn || !form) return;

    const { ok, missing } = configState();

    if (decorate === true) {
      markInvalid(missing);
    } else if (decorate === "clear") {
      clearInvalid();
    }

    const running =
      form.action.endsWith("/stop") ||
      btn.textContent.trim().toLowerCase() === "stop";

    const blockStart = !ok && !running;
    btn.dataset.invalid = blockStart ? "1" : "0";
    btn.title = blockStart
      ? "Provide SERVER_TOKEN, CLIENT_TOKEN, and at least one Guild Mapping to start."
      : "";
    btn.disabled = !!toggleLocked || blockStart;

    if (saveBtn) {
      saveBtn.disabled = false;
      saveBtn.classList.remove("disabled-btn");
      saveBtn.title = "";
    }
    if (cancelBtn) {
      cancelBtn.disabled = false;
      cancelBtn.classList.remove("disabled-btn");
      cancelBtn.title = "";
    }
  }

  function collectMappingForm() {
    const id = document.getElementById("map_mapping_id").value.trim() || null;
    const mapping_name = document
      .getElementById("map_mapping_name")
      .value.trim();

    const original_guild_id = document
      .getElementById("map_original_guild_id")
      .value.trim();

    const cloned_guild_id = document
      .getElementById("map_cloned_guild_id")
      .value.trim();

    const settings = {};
    document
      .querySelectorAll("#mapping-form select[id^='map_']")
      .forEach((sel) => {
        const key = sel.id.replace(/^map_/, "");
        const val = sel.value;
        settings[key] = String(val).toLowerCase() === "true";
      });

    return {
      mapping_id: id,
      mapping_name,
      original_guild_id,
      cloned_guild_id,
      settings,
    };
  }

  document.querySelectorAll(".reveal-btn").forEach((btn) => {
    btn.removeAttribute("title");

    btn.addEventListener("click", () => {
      const cfgForm = document.getElementById("cfg-form");
      if (cfgForm && cfgForm.classList.contains("cfg-locked")) {
        return;
      }

      const targetId = btn.getAttribute("data-target");
      const input = document.getElementById(targetId);
      if (!input) return;

      const eyeOn = btn.querySelector(".icon-eye");
      const eyeOff = btn.querySelector(".icon-eye-off");

      if (input.type === "password") {
        input.type = "text";
        btn.setAttribute("aria-pressed", "true");
        btn.setAttribute("aria-label", "Hide " + targetId);

        if (eyeOn) eyeOn.style.display = "none";
        if (eyeOff) eyeOff.style.display = "";
      } else {
        input.type = "password";
        btn.setAttribute("aria-pressed", "false");
        btn.setAttribute("aria-label", "Show " + targetId);

        if (eyeOn) eyeOn.style.display = "";
        if (eyeOff) eyeOff.style.display = "none";
      }
    });
  });

  function closeMappingModal() {
    const modal = document.getElementById("mapping-modal");
    if (!modal) return;

    if (modal._outsideClickHandler) {
      modal.removeEventListener("mousedown", modal._outsideClickHandler);
      modal._outsideClickHandler = null;
    }

    const active = document.activeElement;
    if (active && modal.contains(active)) {
      try {
        active.blur();
      } catch {}
    }

    setInert(modal, true);
    modal.classList.remove("show");
    modal.setAttribute("aria-hidden", "true");

    document.body.classList.remove("body-lock-scroll");

    if (lastFocusMapping && typeof lastFocusMapping.focus === "function") {
      try {
        lastFocusMapping.focus();
      } catch {}
    }
    lastFocusMapping = null;

    MAPPING_BASELINE = "";
    const cancelBtn = document.getElementById("mapping-cancel-btn");
    if (cancelBtn) {
      cancelBtn.hidden = true;
    }

    const settingsSearch = document.getElementById("mappingSettingsSearch");
    if (settingsSearch) {
      settingsSearch.value = "";
      settingsSearch.dispatchEvent(new Event("input", { bubbles: true }));
    }
  }

  function openMappingModal(mapping) {
    const modal = document.getElementById("mapping-modal");
    if (!modal) return;

    lastFocusMapping = document.activeElement;

    const idInput = document.getElementById("map_mapping_id");
    const nameInput = document.getElementById("map_mapping_name");
    const hostInput = document.getElementById("map_original_guild_id");
    const cloneInput = document.getElementById("map_cloned_guild_id");

    const subtleEl = document.getElementById("mapping-id-subtle");

    const searchInput = document.getElementById("mappingSettingsSearch");
    if (searchInput) {
      searchInput.value = "";
    }
    setupMappingSettingsSearch();

    mapValidated = false;
    bindMappingFieldListeners();
    [nameInput, hostInput, cloneInput].forEach((el) => {
      if (!el) return;
      el.classList.remove("is-invalid", "flash");
    });

    const isEdit = !!mapping;

    if (idInput) idInput.value = (isEdit && mapping.mapping_id) || "";
    if (nameInput) nameInput.value = (isEdit && mapping.mapping_name) || "";
    if (hostInput)
      hostInput.value = (isEdit && mapping.original_guild_id) || "";
    if (cloneInput)
      cloneInput.value = (isEdit && mapping.cloned_guild_id) || "";

    if (subtleEl) {
      if (isEdit && mapping.mapping_id) {
        subtleEl.hidden = false;
        subtleEl.textContent = `${mapping.mapping_id}`;
      } else {
        subtleEl.hidden = true;
        subtleEl.textContent = "";
      }
    }

    document
      .querySelectorAll("#mapping-form select[id^='map_']")
      .forEach((sel) => {
        const key = sel.id.replace(/^map_/, "");

        let rawVal;
        if (isEdit && mapping.settings && key in mapping.settings) {
          rawVal = mapping.settings[key];
        } else {
          rawVal = DEFAULT_MAPPING_SETTINGS[key];
        }

        let normalized;
        if (typeof rawVal === "boolean") {
          normalized = rawVal;
        } else if (typeof rawVal === "string") {
          const lower = rawVal.toLowerCase();
          if (lower === "true") {
            normalized = true;
          } else if (lower === "false") {
            normalized = false;
          } else {
            normalized = !!rawVal;
          }
        } else {
          normalized = !!rawVal;
        }

        sel.value = normalized ? "True" : "False";
        sel.dispatchEvent(new Event("change", { bubbles: true }));
      });

    const mappingFormEl = document.getElementById("mapping-form");
    if (mappingFormEl) {
      MAPPING_BASELINE = snapshotForm(mappingFormEl);

      if (!mappingFormEl._mapDirtyBound) {
        mappingFormEl._mapDirtyBound = true;
        const handler = () => updateMappingCancelVisibility();
        mappingFormEl.addEventListener("input", handler);
        mappingFormEl.addEventListener("change", handler);
      }
    } else {
      MAPPING_BASELINE = "";
    }

    const cancelBtn = document.getElementById("mapping-cancel-btn");
    if (cancelBtn) {
      cancelBtn.hidden = true;
      cancelBtn.onclick = (e) => {
        e.preventDefault();
        resetMappingFormToBaseline();
      };
    }

    const headerCloseBtn = document.getElementById("mapping-close");
    if (headerCloseBtn) {
      headerCloseBtn.onclick = maybeCloseMappingModal;
    }

    updateMappingCancelVisibility();

    setInert(modal, false);
    modal.classList.add("show");
    modal.setAttribute("aria-hidden", "false");
    document.body.classList.add("body-lock-scroll");

    const firstField =
      nameInput || document.getElementById("mapping-save-btn") || modal;
    setTimeout(() => {
      if (firstField && typeof firstField.focus === "function") {
        try {
          firstField.focus();
        } catch {}
      }
    }, 0);

    modal._outsideClickHandler = function (evt) {
      const contentEl = modal.querySelector(".modal-content");
      if (contentEl && !contentEl.contains(evt.target)) {
        maybeCloseMappingModal();
      }
    };
    modal.addEventListener("mousedown", modal._outsideClickHandler);
  }

  window.openMappingModal = openMappingModal;
  window.closeMappingModal = closeMappingModal;

  function setMappingSaveBusy(isBusy) {
    const btn = document.getElementById("mapping-save-btn");
    if (!btn) return;

    if (isBusy) {
      if (!btn.dataset.origLabel) {
        btn.dataset.origLabel = btn.textContent.trim() || "Save Mapping";
      }
      btn.disabled = true;
      btn.textContent = "Saving…";
    } else {
      btn.disabled = false;
      btn.textContent = btn.dataset.origLabel || "Save Mapping";
    }
  }

  async function saveMappingFromModal() {
    if (saveMappingFromModal._busy) return;
    saveMappingFromModal._busy = true;

    mapValidated = true;
    const { ok, firstBad } = validateMappingFields({ decorate: true });
    if (!ok) {
      const active = document.activeElement;
      if (
        firstBad &&
        typeof firstBad.focus === "function" &&
        firstBad !== active
      ) {
        firstBad.focus();
      }

      saveMappingFromModal._busy = false;
      return;
    }

    setMappingSaveBusy(true);

    const data = collectMappingForm();
    const isEdit = !!data.mapping_id;

    const url = isEdit
      ? `/api/guild-mappings/${encodeURIComponent(data.mapping_id)}`
      : "/api/guild-mappings";
    const method = isEdit ? "PATCH" : "POST";

    const payload = {
      mapping_name: data.mapping_name,
      original_guild_id: data.original_guild_id,
      cloned_guild_id: data.cloned_guild_id,

      original_guild_name: "",
      cloned_guild_name: "",
      settings: data.settings,
    };

    let res;
    try {
      res = await fetch(url, {
        method,
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
    } catch (netErr) {
      showToast("Network error while saving mapping.", {
        type: "error",
        timeout: 7000,
      });

      setMappingSaveBusy(false);
      saveMappingFromModal._busy = false;
      return;
    }

    if (!res.ok) {
      let msg = "Failed to save mapping.";
      try {
        const errJson = await res.json();
        if (errJson && errJson.error) {
          msg = errJson.error;
        } else if (errJson && errJson.message) {
          msg = errJson.message;
        }
      } catch {}

      showToast(msg, {
        type: "error",
        timeout: 7000,
      });

      setMappingSaveBusy(false);
      saveMappingFromModal._busy = false;
      return;
    }

    await refreshGuildMappings();
    closeMappingModal();
    validateConfigAndToggle({ decorate: false });

    showToast(isEdit ? "Mapping updated." : "Mapping created.", {
      type: "success",
    });

    setMappingSaveBusy(false);
    saveMappingFromModal._busy = false;
  }

  function closeConfirm() {
    if (!cModal) return;

    const active = document.activeElement;
    if (active && cModal.contains(active)) {
      try {
        active.blur();
      } catch {}
    }

    setInert(cModal, true);
    cModal.classList.remove("show");
    cModal.setAttribute("aria-hidden", "true");

    const anyOtherOpen = document.querySelector(
      "#filters-modal.show, " +
        "#mapping-modal.show, " +
        "#log-modal.show, " +
        "#roleBlocksModal.show, " +
        "#filterObjectsModal.show, " +
        "#confirm-modal.show"
    );
    if (!anyOtherOpen) {
      document.body.classList.remove("body-lock-scroll");
    }

    if (lastFocusConfirm && typeof lastFocusConfirm.blur === "function") {
      try {
        lastFocusConfirm.blur();
      } catch {}
    }
    lastFocusConfirm = null;

    confirmResolve = null;
    confirmReject = null;
  }

  function closeFiltersModal() {
    const modal = document.getElementById("filters-modal");
    if (!modal) return;

    const active = document.activeElement;
    if (active && modal.contains(active)) {
      try {
        active.blur();
      } catch {}
    }

    setInert(modal, true);
    modal.classList.remove("show");
    modal.setAttribute("aria-hidden", "true");

    document.body.classList.remove("body-lock-scroll");

    if (lastFocusFilters && typeof lastFocusFilters.focus === "function") {
      try {
        lastFocusFilters.focus();
      } catch {}
    }
    lastFocusFilters = null;
    currentFilterMapping = null;
  }

  function filtersFormIsDirty() {
    const form = document.getElementById("form-filters");
    if (!form) return false;
    return snapshotForm(form) !== FILTERS_BASELINE;
  }

  function mappingFormIsDirty() {
    const form = document.getElementById("mapping-form");
    if (!form) return false;
    if (!MAPPING_BASELINE) return false;
    return snapshotForm(form) !== MAPPING_BASELINE;
  }

  function updateMappingCancelVisibility() {
    const cancelBtn = document.getElementById("mapping-cancel-btn");
    if (!cancelBtn) return;
    cancelBtn.hidden = !mappingFormIsDirty();
  }

  function maybeCloseMappingModal() {
    const modal = document.getElementById("mapping-modal");
    if (!modal) return;

    // If it's not showing or not dirty, just close
    if (!modal.classList.contains("show") || !mappingFormIsDirty()) {
      closeMappingModal();
      return;
    }

    // Don't stack confirm dialogs
    if (cModal && cModal.classList.contains("show")) {
      return;
    }

    openConfirm({
      title: "Discard unsaved mapping changes?",
      body: "You have unsaved changes to this mapping. If you close now, your changes will be lost.",
      confirmText: "Discard changes",
      confirmClass: "btn-ghost-red",
      showCancel: true,
      onConfirm: () => {
        closeMappingModal();
      },
    });
  }

  function resetMappingFormToBaseline() {
    const form = document.getElementById("mapping-form");
    if (!form || !MAPPING_BASELINE) return;

    const params = new URLSearchParams(MAPPING_BASELINE);

    Array.from(form.elements).forEach((el) => {
      const name = el.name;
      if (!name) return;

      if (el.type === "submit" || el.type === "button") return;

      // Checkboxes / radios (you probably don't have any here, but safe anyway)
      if (el.type === "checkbox" || el.type === "radio") {
        const values = params.getAll(name);
        el.checked = values.includes(el.value);
        return;
      }

      const val = params.get(name);
      el.value = val != null ? val : "";

      if (el.tagName === "SELECT") {
        el.dispatchEvent(new Event("change", { bubbles: true }));
      } else {
        el.dispatchEvent(new Event("input", { bubbles: true }));
      }
    });

    mapValidated = false;
    const { name, host, clone } = getMappingInputs();
    [name, host, clone].forEach((el) => {
      if (!el) return;
      el.classList.remove("is-invalid", "flash");
    });

    updateMappingCancelVisibility();
  }

  document.addEventListener("keydown", (e) => {
    if (e.key !== "Escape") return;

    const mappingModal = document.getElementById("mapping-modal");
    if (mappingModal && mappingModal.classList.contains("show")) {
      e.preventDefault();
      maybeCloseMappingModal();
    }
  });

  function maybeCloseFiltersModal() {
    // If the confirm modal is already open, don't stack another one
    if (cModal && cModal.classList.contains("show")) {
      return;
    }

    const modal = document.getElementById("filters-modal");
    if (!modal || !modal.classList.contains("show")) {
      closeFiltersModal();
      return;
    }

    if (!filtersFormIsDirty()) {
      closeFiltersModal();
      return;
    }

    openConfirm({
      title: "Discard unsaved filter changes?",
      body: "You have unsaved changes to these filters. If you close now, your changes will be lost.",
      confirmText: "Discard changes",
      confirmClass: "btn-ghost-red",
      showCancel: true,
      onConfirm: () => {
        closeFiltersModal();
      },
    });
  }

  async function openFiltersModal(mapping) {
    const modal = document.getElementById("filters-modal");
    if (!modal) return;

    lastFocusFilters = document.activeElement;
    currentFilterMapping = mapping || null;

    const form = document.getElementById("form-filters");
    const titleEl = document.getElementById("filters-title");
    const mapIdInput = document.getElementById("filters_mapping_id");
    const cancelBtn = document.getElementById("btn-cancel-filters");

    const mid = mapping?.mapping_id || "";
    if (mapIdInput) mapIdInput.value = mid;
    if (titleEl) {
      const niceName = mapping?.mapping_name || mid || "Filters";
      titleEl.textContent = `Filters – ${niceName}`;
    }

    if (form) {
      form.action = `/filters/${encodeURIComponent(mid)}/save`;
    }

    await loadFiltersIntoFormForMapping(mid);

    if (form) {
      FILTERS_BASELINE = snapshotForm(form);
    }
    if (cancelBtn) {
      cancelBtn.hidden = true;
    }

    setInert(modal, false);
    modal.classList.add("show");
    modal.setAttribute("aria-hidden", "false");
    document.body.classList.add("body-lock-scroll");

    const xBtn = document.getElementById("filters-close");
    if (xBtn) xBtn.onclick = maybeCloseFiltersModal;
    if (cancelBtn) {
      cancelBtn.onclick = async () => {
        await loadFiltersIntoFormForMapping(mid);
        const f2 = document.getElementById("form-filters");
        if (f2) {
          FILTERS_BASELINE = snapshotForm(f2);
        }
        cancelBtn.hidden = true;
      };
    }

    if (modal._outsideClickHandler) {
      modal.removeEventListener("mousedown", modal._outsideClickHandler);
    }
    modal._outsideClickHandler = function (evt) {
      const contentEl = modal.querySelector(".modal-content");
      if (contentEl && !contentEl.contains(evt.target)) {
        maybeCloseFiltersModal();
      }
    };
    modal.addEventListener("mousedown", modal._outsideClickHandler);

    const firstField =
      document.getElementById("wl_categories_input") ||
      document.getElementById("filters-save-btn") ||
      modal;
    setTimeout(() => {
      if (firstField && typeof firstField.focus === "function") {
        try {
          firstField.focus();
        } catch {}
      }
    }, 0);
  }

  function setupRoleBlocksSearch() {
    const input = document.getElementById("roleBlocksSearch");
    if (!input) return;

    if (input._boundForModal) return;
    input._boundForModal = true;

    input.addEventListener("input", () => {
      const q = input.value.trim().toLowerCase();

      if (!Array.isArray(ROLE_BLOCKS_ALL_ROLES)) return;

      const filtered = !q
        ? ROLE_BLOCKS_ALL_ROLES
        : ROLE_BLOCKS_ALL_ROLES.filter((role) => {
            const name = String(role.name || "").toLowerCase();
            const idStr = String(role.id || "");
            return name.includes(q) || idStr.includes(q);
          });

      renderRoleBlocksTable(filtered);
    });
  }

  async function openRoleBlocksModal(mappingId) {
    const modal = document.getElementById("roleBlocksModal");
    const tbody = document.getElementById("roleBlocksTableBody");
    const status = document.getElementById("filters-role-status");

    if (!modal || !tbody) return;

    const mid =
      mappingId ||
      (currentFilterMapping && currentFilterMapping.mapping_id) ||
      document.getElementById("filters_mapping_id")?.value ||
      "";

    if (!mid) {
      if (status) status.textContent = "No mapping selected.";
      return;
    }

    if (status) status.textContent = "";

    tbody.innerHTML =
      '<tr><td colspan="3" class="text-center small">Loading…</td></tr>';

    setInert(modal, false);
    modal.classList.add("show");
    modal.setAttribute("aria-hidden", "false");
    document.body.classList.add("body-lock-scroll");

    const closeBtn = document.getElementById("roleBlocksCloseBtn");
    setTimeout(() => {
      try {
        (closeBtn || modal).focus();
      } catch {}
    }, 0);

    if (modal._outsideClickHandler) {
      modal.removeEventListener("mousedown", modal._outsideClickHandler);
    }
    modal._outsideClickHandler = function (evt) {
      const contentEl = modal.querySelector(".modal-content");
      if (contentEl && !contentEl.contains(evt.target)) {
        closeRoleBlocksModal();
      }
    };
    modal.addEventListener("mousedown", modal._outsideClickHandler);

    if (CHIPS.blocked_roles) {
      CURRENT_BLOCKED_ROLE_IDS = new Set(CHIPS.blocked_roles.get());
    } else {
      const hidden = document.getElementById("blocked_role_ids");
      CURRENT_BLOCKED_ROLE_IDS = hidden
        ? parseBlockedRolesCsv(hidden.value)
        : new Set();
    }

    try {
      const res = await fetch(
        `/api/mappings/${encodeURIComponent(mid)}/roles`,
        {
          method: "GET",
          headers: { "X-Requested-With": "XMLHttpRequest" },
        }
      );

      if (!res.ok) {
        throw new Error(`Failed: ${res.status}`);
      }

      const payload = await res.json();
      const roles = Array.isArray(payload.roles) ? payload.roles : [];

      ROLE_BLOCKS_ALL_ROLES = roles;
      renderRoleBlocksTable(roles);
      setupRoleBlocksSearch();

      if (status) status.textContent = "";
    } catch (err) {
      console.error("Error fetching roles:", err);
      tbody.innerHTML =
        '<tr><td colspan="3" class="text-center small">Failed to load roles.</td></tr>';
      if (status) status.textContent = "Failed to load roles.";
    }
  }

  async function fetchGuildTreeForMapping(mid, opts = {}) {
    const { noCache = false } = opts;
    const cacheKey = `filters_tree:${mid}`;

    if (!noCache && RUNTIME_CACHE[cacheKey]) {
      return RUNTIME_CACHE[cacheKey];
    }

    const res = await fetch(
      `/api/mappings/${encodeURIComponent(mid)}/channels`,
      {
        method: "GET",
        headers: { "X-Requested-With": "XMLHttpRequest" },
      }
    );

    if (!res.ok) {
      throw new Error(`Failed to fetch channels: ${res.status}`);
    }

    const payload = await res.json();
    const categories = Array.isArray(payload.categories)
      ? payload.categories
      : [];
    const channels = Array.isArray(payload.channels) ? payload.channels : [];

    const tree = { categories, channels };

    if (!noCache) {
      RUNTIME_CACHE[cacheKey] = tree;
    }

    return tree;
  }

  function closeFilterObjectsModal() {
    const modal = document.getElementById("filterObjectsModal");
    const tbody = document.getElementById("filterObjectsTableBody");
    const selectAll = document.getElementById("filterObjectsSelectAll");
    const searchInput = document.getElementById("filterObjectsSearch");

    if (!modal) return;

    setInert(modal, true);
    modal.classList.remove("show");
    modal.setAttribute("aria-hidden", "true");

    if (tbody) {
      tbody.innerHTML = "";
    }
    if (selectAll) {
      selectAll.checked = false;
      selectAll.indeterminate = false;
    }

    if (searchInput) {
      searchInput.value = "";
    }

    if (window.CURRENT_FILTER_PICKER && CURRENT_FILTER_PICKER.mappingId) {
      const cacheKey = `filters_tree:${CURRENT_FILTER_PICKER.mappingId}`;
      delete RUNTIME_CACHE[cacheKey];
    }
    CURRENT_FILTER_PICKER = null;

    if (modal._outsideClickHandler) {
      modal.removeEventListener("mousedown", modal._outsideClickHandler);
      modal._outsideClickHandler = null;
    }
    if (modal._keyHandler) {
      modal.removeEventListener("keydown", modal._keyHandler);
      modal._keyHandler = null;
    }

    const anyOtherOpen = document.querySelector(
      "#filters-modal.show, " +
        "#mapping-modal.show, " +
        "#log-modal.show, " +
        "#roleBlocksModal.show, " +
        "#confirm-modal.show"
    );

    if (!anyOtherOpen) {
      document.body.classList.remove("body-lock-scroll");
    }
  }

  function buildFilterObjectsRows(items, kind, catMap) {
    return items
      .map((obj) => {
        const id = String(obj.id);
        const safeName = escapeHtml(obj.name || `ID ${id}`);
        const checked = CURRENT_FILTER_PICKER.selected.has(id)
          ? " checked"
          : "";

        let catCell = "";
        if (kind === "channel") {
          const pid = obj.parent_id ? String(obj.parent_id) : "";
          const parentName = pid && catMap.get(pid);
          catCell = `<td class="filter-obj-cat">
          <span class="filter-obj-cat-text">${escapeHtml(
            parentName || ""
          )}</span>
        </td>`;
        } else {
          catCell = `<td class="filter-obj-cat"></td>`;
        }

        return `
        <tr data-obj-id="${id}">
          <td class="filter-obj-select-col">
            <label class="checkbox-inline">
              <input
                type="checkbox"
                class="role-block-toggle filter-obj-toggle"
                data-obj-id="${id}"${checked}
              />
            </label>
          </td>
          <td class="filter-obj-name-col">
            <span class="filter-obj-name">${safeName}</span>
          </td>
          ${catCell}
          <td class="filter-obj-id-col"><code>${id}</code></td>
        </tr>
      `;
      })
      .join("");
  }

  function wireFilterObjectsTableInteractions() {
    const tbody = document.getElementById("filterObjectsTableBody");
    const selectAll = document.getElementById("filterObjectsSelectAll");
    if (!tbody) return;

    function getCheckboxes() {
      return Array.from(tbody.querySelectorAll(".filter-obj-toggle"));
    }

    function updateSelectAllState() {
      if (!selectAll) return;
      const cbs = getCheckboxes();
      const total = cbs.length;
      const checkedCount = cbs.filter((cb) => cb.checked).length;

      if (!total) {
        selectAll.checked = false;
        selectAll.indeterminate = false;
      } else if (checkedCount === 0) {
        selectAll.checked = false;
        selectAll.indeterminate = false;
      } else if (checkedCount === total) {
        selectAll.checked = true;
        selectAll.indeterminate = false;
      } else {
        selectAll.checked = false;
        selectAll.indeterminate = true;
      }
    }

    getCheckboxes().forEach((cb) => {
      cb.addEventListener("change", (evt) => {
        const id = evt.target.getAttribute("data-obj-id");
        if (!id) return;

        if (evt.target.checked) {
          CURRENT_FILTER_PICKER.selected.add(id);
        } else {
          CURRENT_FILTER_PICKER.selected.delete(id);
        }

        updateSelectAllState();
      });
    });

    if (selectAll) {
      selectAll.onchange = () => {
        const shouldCheck = !!selectAll.checked;
        const cbs = getCheckboxes();

        cbs.forEach((cb) => {
          cb.checked = shouldCheck;
          const id = cb.getAttribute("data-obj-id");
          if (!id) return;
          if (shouldCheck) {
            CURRENT_FILTER_PICKER.selected.add(id);
          } else {
            CURRENT_FILTER_PICKER.selected.delete(id);
          }
        });

        updateSelectAllState();
      };
    }

    tbody.querySelectorAll("tr[data-obj-id]").forEach((row) => {
      row.addEventListener("click", (evt) => {
        if (evt.target.closest("input[type='checkbox']")) return;

        const cb = row.querySelector(".filter-obj-toggle");
        if (!cb) return;

        cb.checked = !cb.checked;
        cb.dispatchEvent(new Event("change", { bubbles: true }));
      });
    });

    updateSelectAllState();
  }

  function setupFilterObjectsSearch() {
    const input = document.getElementById("filterObjectsSearch");
    if (!input) return;

    if (input._boundForModal) return;
    input._boundForModal = true;

    input.addEventListener("input", () => {
      const q = input.value.trim().toLowerCase();
      const tbody = document.getElementById("filterObjectsTableBody");
      if (!tbody) return;

      const items = Array.isArray(FILTER_OBJECTS_ALL_ITEMS)
        ? FILTER_OBJECTS_ALL_ITEMS
        : [];
      if (!items.length) return;

      const filtered = !q
        ? items
        : items.filter((obj) => {
            const name = String(obj.name || "").toLowerCase();
            const idStr = String(obj.id || "");

            let catName = "";
            if (FILTER_OBJECTS_KIND === "channel" && FILTER_OBJECTS_CATMAP) {
              const pid = obj.parent_id ? String(obj.parent_id) : "";
              catName = String(
                FILTER_OBJECTS_CATMAP.get(pid) || ""
              ).toLowerCase();
            }

            return (
              name.includes(q) ||
              idStr.includes(q) ||
              (catName && catName.includes(q))
            );
          });

      tbody.innerHTML = buildFilterObjectsRows(
        filtered,
        FILTER_OBJECTS_KIND,
        FILTER_OBJECTS_CATMAP || new Map()
      );

      wireFilterObjectsTableInteractions();
    });
  }

  async function openFilterObjectsModal(mappingId, options) {
    const modal = document.getElementById("filterObjectsModal");
    const tbody = document.getElementById("filterObjectsTableBody");
    const titleEl = document.getElementById("filterObjectsTitle");
    const helpEl = document.getElementById("filterObjectsHelp");
    const nameHeader = document.getElementById("filterObjectsNameHeader");
    const catHeader = document.getElementById("filterObjectsCategoryHeader");
    const selectAll = document.getElementById("filterObjectsSelectAll");
    const table = document.getElementById("filterObjectsTable");

    if (!modal || !tbody) return;

    if (table) {
      table.classList.add("role-blocks-table", "filter-objects-table");
    }

    const mid =
      mappingId ||
      (currentFilterMapping && currentFilterMapping.mapping_id) ||
      document.getElementById("filters_mapping_id")?.value ||
      "";

    tbody.innerHTML =
      '<tr><td colspan="4" class="text-center small">Loading…</td></tr>';

    if (!mid) {
      tbody.innerHTML =
        '<tr><td colspan="4" class="text-center small">No mapping selected.</td></tr>';
      return;
    }

    const listKey = options.listKey;
    const kind = options.kind;

    const chip = CHIPS[listKey];
    const baseIds = chip
      ? chip.get()
      : parseIdList(document.getElementById(listKey)?.value || "");

    CURRENT_FILTER_PICKER = {
      mappingId: mid,
      listKey,
      kind,
      selected: new Set((baseIds || []).map(String)),
    };

    if (titleEl) {
      const prefix = listKey.startsWith("wl_") ? "Allow" : "Block";
      const what = kind === "category" ? "Categories" : "Channels";
      titleEl.textContent = `${prefix} ${what}`;
    }
    if (helpEl) {
      if (kind === "category") {
        helpEl.textContent = listKey.startsWith("wl_")
          ? "Only the selected categories will be cloned (including their channels)."
          : "The selected categories (and their channels) will not be cloned.";
      } else {
        helpEl.textContent = listKey.startsWith("wl_")
          ? "Only the selected channels will be cloned."
          : "The selected channels will not be cloned.";
      }
    }
    if (catHeader) {
      catHeader.style.display = kind === "channel" ? "" : "none";
    }

    setInert(modal, false);
    modal.classList.add("show");
    modal.setAttribute("aria-hidden", "false");
    document.body.classList.add("body-lock-scroll");

    const closeBtn = document.getElementById("filterObjectsCloseBtn");
    setTimeout(() => {
      try {
        (closeBtn || modal).focus();
      } catch {}
    }, 0);

    if (modal._outsideClickHandler) {
      modal.removeEventListener("mousedown", modal._outsideClickHandler);
    }
    modal._outsideClickHandler = function (evt) {
      const contentEl = modal.querySelector(".modal-content");
      if (contentEl && !contentEl.contains(evt.target)) {
        closeFilterObjectsModal();
      }
    };
    modal.addEventListener("mousedown", modal._outsideClickHandler);

    if (modal._keyHandler) {
      modal.removeEventListener("keydown", modal._keyHandler);
    }
    modal._keyHandler = function (evt) {
      if (evt.key === "Escape" || evt.key === "Esc") {
        evt.preventDefault();
        evt.stopPropagation();
        closeFilterObjectsModal();
      }
    };
    modal.addEventListener("keydown", modal._keyHandler);

    try {
      await new Promise((resolve) => setTimeout(resolve, 0));

      const tree = await fetchGuildTreeForMapping(mid, { noCache: true });
      const categories = Array.isArray(tree.categories) ? tree.categories : [];
      const channels = Array.isArray(tree.channels) ? tree.channels : [];

      const catMap = new Map();
      for (const c of categories) {
        const id = String(c.id);
        catMap.set(id, c.name || `Category ${id}`);
      }

      const items = kind === "category" ? categories : channels;
      if (!items.length) {
        tbody.innerHTML = `<tr><td colspan="4" class="text-center small">No ${
          kind === "category" ? "categories" : "channels"
        } found for this guild.</td></tr>`;
        return;
      }

      FILTER_OBJECTS_ALL_ITEMS = items;
      FILTER_OBJECTS_KIND = kind;
      FILTER_OBJECTS_CATMAP = catMap;

      tbody.innerHTML = buildFilterObjectsRows(items, kind, catMap);

      wireFilterObjectsTableInteractions();
      setupFilterObjectsSearch();
    } catch (err) {
      console.error("Error fetching guild channels/categories:", err);
      tbody.innerHTML =
        '<tr><td colspan="4" class="text-center small text-danger">Failed to fetch channels/categories from Discord.</td></tr>';
    }
  }

  function renderRoleBlocksTable(roles) {
    const tbody = document.getElementById("roleBlocksTableBody");
    const selectAll = document.getElementById("roleBlocksSelectAll");
    if (!tbody) return;

    if (selectAll) {
      selectAll.checked = false;
      selectAll.indeterminate = false;
    }

    if (!roles.length) {
      tbody.innerHTML =
        '<tr><td colspan="3" class="text-center small">No roles found for this guild.</td></tr>';
      return;
    }

    const rows = roles
      .map((r) => {
        const id = String(r.id);
        const isBlocked = CURRENT_BLOCKED_ROLE_IDS.has(id);
        const safeName = (r.name || "")
          .replace(/&/g, "&amp;")
          .replace(/</g, "&lt;")
          .replace(/>/g, "&gt;");

        const hex = (r.color_hex || "").trim() || "#99AAB5";

        return `
        <tr data-role-id="${id}">
          <td>
            <label class="checkbox-inline">
              <input
                type="checkbox"
                class="role-block-toggle"
                data-role-id="${id}"
                ${isBlocked ? "checked" : ""}
              />
            </label>
          </td>
          <td>
            <span class="role-pill" style="--role-color: ${hex}">
              <span class="role-pill-text">${safeName}</span>
            </span>
          </td>
          <td><code>${id}</code></td>
        </tr>
      `;
      })
      .join("");

    tbody.innerHTML = rows;

    tbody.querySelectorAll(".role-block-toggle").forEach((chk) => {
      chk.addEventListener("change", (evt) => {
        const rid = evt.target.getAttribute("data-role-id");
        if (!rid) return;

        if (evt.target.checked) {
          CURRENT_BLOCKED_ROLE_IDS.add(rid);
        } else {
          CURRENT_BLOCKED_ROLE_IDS.delete(rid);
        }

        updateSelectAllState();
      });
    });

    if (selectAll && !selectAll._bound) {
      selectAll._bound = true;
      selectAll.addEventListener("change", () => {
        const checked = selectAll.checked;
        const boxes = tbody.querySelectorAll(".role-block-toggle");
        CURRENT_BLOCKED_ROLE_IDS.clear();

        boxes.forEach((cb) => {
          cb.checked = checked;
          const rid = cb.getAttribute("data-role-id");
          if (checked && rid) CURRENT_BLOCKED_ROLE_IDS.add(rid);
        });

        selectAll.indeterminate = false;
      });
    }

    tbody.querySelectorAll("tr[data-role-id]").forEach((row) => {
      row.addEventListener("click", (evt) => {
        if (evt.target.closest("input[type='checkbox']")) return;

        const cb = row.querySelector(".role-block-toggle");
        if (!cb) return;

        cb.checked = !cb.checked;
        cb.dispatchEvent(new Event("change", { bubbles: true }));
      });
    });

    updateSelectAllState();
  }

  function updateSelectAllState() {
    const tbody = document.getElementById("roleBlocksTableBody");
    const selectAll = document.getElementById("roleBlocksSelectAll");
    if (!tbody || !selectAll) return;

    const boxes = Array.from(tbody.querySelectorAll(".role-block-toggle"));
    if (!boxes.length) {
      selectAll.checked = false;
      selectAll.indeterminate = false;
      return;
    }

    const checkedCount = boxes.filter((b) => b.checked).length;

    if (checkedCount === 0) {
      selectAll.checked = false;
      selectAll.indeterminate = false;
    } else if (checkedCount === boxes.length) {
      selectAll.checked = true;
      selectAll.indeterminate = false;
    } else {
      selectAll.checked = false;
      selectAll.indeterminate = true;
    }
  }

  function closeRoleBlocksModal() {
    const modal = document.getElementById("roleBlocksModal");
    if (!modal) return;

    const active = document.activeElement;
    if (active && modal.contains(active)) {
      try {
        active.blur();
      } catch {}
    }

    const searchInput = document.getElementById("roleBlocksSearch");
    if (searchInput) {
      searchInput.value = "";
    }

    setInert(modal, true);
    modal.classList.remove("show");
    modal.setAttribute("aria-hidden", "true");

    const anyOtherOpen = document.querySelector(
      "#filters-modal.show, #mapping-modal.show, #log-modal.show, #confirm-modal.show"
    );
    if (!anyOtherOpen) {
      document.body.classList.remove("body-lock-scroll");
    }
  }

  window.openFiltersModal = openFiltersModal;
  window.closeFiltersModal = closeFiltersModal;

  function openConfirm({
    title,
    body,
    confirmText = "OK",
    confirmClass = "btn-ghost",
    onConfirm,
    showCancel = true,
  }) {
    if (!cModal) return;

    if (cModal.parentNode) {
      cModal.parentNode.appendChild(cModal);
    }

    confirmResolve = () => {
      try {
        onConfirm && onConfirm();
      } finally {
        closeConfirm();
      }
    };

    confirmReject = () => closeConfirm();
    lastFocusConfirm = document.activeElement;

    cTitle.textContent = title || "Confirm";
    cBody.textContent = body || "Are you sure?";
    cBtnOk.textContent = confirmText || "OK";

    cBtnOk.classList.remove(
      "btn-primary",
      "btn-outline",
      "btn-ghost",
      "btn-danger"
    );
    cBtnOk.classList.add(confirmClass || "btn-primary");

    if (cBtnCa) {
      if (showCancel) {
        cBtnCa.removeAttribute("hidden");
      } else {
        cBtnCa.setAttribute("hidden", "");
      }
    }

    cModal.classList.add("show");
    setInert(cModal, false);
    cModal.setAttribute("aria-hidden", "false");

    document.body.classList.add("body-lock-scroll");

    setTimeout(() => (cBtnOk || cModal).focus(), 0);
  }

  function confirmDeleteMapping(mapping) {
    if (!mapping || !mapping.mapping_id) return;

    const name = (mapping.mapping_name || "").trim();
    const label = name ? `“${name}”` : `ID ${mapping.mapping_id}`;

    openConfirm({
      title: "Delete guild mapping?",
      body: `This will remove the mapping ${label}. This cannot be undone.`,
      confirmText: "Delete mapping",
      confirmClass: "btn-ghost-red",
      showCancel: true,
      onConfirm: async () => {
        try {
          const res = await fetch(
            `/api/guild-mappings/${encodeURIComponent(mapping.mapping_id)}`,
            { method: "DELETE", credentials: "same-origin" }
          );

          if (!res.ok) {
            const txt = await (async () => {
              try {
                return await res.text();
              } catch {
                return "";
              }
            })();
            showToast(txt || `Delete failed (${res.status})`, {
              type: "error",
              timeout: 7000,
            });
            return;
          }

          showToast("Mapping deleted.", { type: "success" });
          await refreshGuildMappings();

          validateConfigAndToggle({ decorate: false });
        } catch {
          showToast("Network error", { type: "error" });
        }
      },
    });
  }

  window.confirmDeleteMapping = confirmDeleteMapping;

  function findMappingById(id) {
    if (!id) return null;

    const sid = String(id);
    return (
      GUILD_MAPPINGS.find((m) => m && String(m.mapping_id) === sid) || null
    );
  }

  function renderGuildMappings() {
    const listEl = document.getElementById("guild-mapping-list");
    if (!listEl) return;

    const searchInput = document.getElementById("mappingSearchInput");
    const query = searchInput ? searchInput.value.trim().toLowerCase() : "";

    let mappings = Array.isArray(GUILD_MAPPINGS) ? GUILD_MAPPINGS.slice() : [];

    if (query) {
      mappings = mappings.filter((m) => {
        if (!m) return false;
        const parts = [];

        if (m.mapping_name) parts.push(m.mapping_name);
        if (m.original_guild_name) parts.push(m.original_guild_name);
        if (m.cloned_guild_name) parts.push(m.cloned_guild_name);
        if (m.original_guild_id) parts.push(String(m.original_guild_id));
        if (m.cloned_guild_id) parts.push(String(m.cloned_guild_id));

        const haystack = parts.join(" ").toLowerCase();
        return haystack.includes(query);
      });
    }

    const mappingCardsHtml = mappings
      .map((m) => {
        const iconSrc = m.original_guild_icon_url || "/static/logo.png";
        return `
        <div class="guild-card" data-id="${m.mapping_id}">
          <div class="guild-card-logo">
            <img src="${iconSrc}" alt="" class="guild-card-logo-img">
          </div>

          <div class="guild-card-inner">
            <div class="guild-card-main">
              <div class="guild-card-name">
                <div class="guild-card-name-title" title="${escapeHtml(
                  m.mapping_name || ""
                )}">
                  ${escapeHtml(m.mapping_name || "")}
                </div>
                <div class="guild-card-name-meta"></div>
              </div>

              <div class="guild-card-actions">
                <button class="btn-icon edit-mapping-btn"
                        data-id="${m.mapping_id}"
                        aria-label="Edit mapping"
                        title="Settings">
                  ${ICONS.settings}
                </button>

                <button class="btn-icon mapping-filters-btn"
                        data-id="${m.mapping_id}"
                        aria-label="Filters for this mapping"
                        title="Filters">
                  ${ICONS.filters}
                </button>

                <button
                  class="btn-icon delete-mapping-btn"
                  type="button"
                  data-action="delete"
                  data-id="${m.mapping_id}"
                  aria-label="Delete mapping"
                  title="Delete mapping"
                >
                  ${ICONS.trash}
                </button>
              </div>
            </div>
          </div>
        </div>
      `;
      })
      .join("");

    const newCardHtml = `
      <button
        class="guild-card guild-card--new"
        id="new-mapping-card"
        type="button"
        aria-label="Add new mapping"
        title="Add new mapping"
      >
        <div class="new-card-inner">
          <div class="new-card-plus">+</div>
          <div class="new-card-label">Add Mapping</div>
        </div>
      </button>
    `;

    listEl.innerHTML = mappingCardsHtml + newCardHtml;

    listEl.querySelectorAll(".edit-mapping-btn").forEach((btn) => {
      btn.addEventListener("click", (ev) => {
        const mapId = ev.currentTarget.getAttribute("data-id");
        const mapping = findMappingById(mapId);
        openMappingModal(mapping);
      });
    });

    listEl.querySelectorAll(".mapping-filters-btn").forEach((btn) => {
      btn.addEventListener("click", async (ev) => {
        const mapId = ev.currentTarget.getAttribute("data-id");
        const mapping = findMappingById(mapId);
        await openFiltersModal(mapping);
      });
    });

    listEl.querySelectorAll(".delete-mapping-btn").forEach((btn) => {
      btn.addEventListener("click", (ev) => {
        const mapId = ev.currentTarget.getAttribute("data-id");
        const mapping = findMappingById(mapId);
        confirmDeleteMapping(mapping);
      });
    });

    const newCardBtn = document.getElementById("new-mapping-card");
    if (newCardBtn) {
      newCardBtn.addEventListener("click", () => {
        openMappingModal();
      });
    }

    if (lastRunning === true) {
      setGuildCardsLocked(true);
    }
  }

  async function refreshGuildMappings() {
    const res = await fetch("/api/guild-mappings", {
      cache: "no-store",
      credentials: "same-origin",
    });
    if (!res.ok) return;
    const data = await res.json();
    GUILD_MAPPINGS = data.mappings || [];
    renderGuildMappings();
    setGuildCardsLocked(lastRunning === true);
    updateStartButtonOnly();
  }

  async function loadFiltersIntoFormForMapping(mid) {
    const ff = document.getElementById("form-filters");
    if (!ff) return;

    const wlCats = document.getElementById("wl_categories");
    const wlCh = document.getElementById("wl_channels");
    const exCats = document.getElementById("ex_categories");
    const exCh = document.getElementById("ex_channels");
    const bw = document.getElementById("blocked_words");
    const hiddenBlockedRoles = document.getElementById("blocked_role_ids");

    const wlUsers = document.getElementById("wl_users");
    const blUsers = document.getElementById("bl_users");

    if (wlCats) wlCats.value = "";
    if (wlCh) wlCh.value = "";
    if (exCats) exCats.value = "";
    if (exCh) exCh.value = "";
    if (bw) bw.value = "";
    if (hiddenBlockedRoles) hiddenBlockedRoles.value = "";
    if (wlUsers) wlUsers.value = "";
    if (blUsers) blUsers.value = "";

    if (CHIPS.wl_categories) CHIPS.wl_categories.set([]);
    if (CHIPS.wl_channels) CHIPS.wl_channels.set([]);
    if (CHIPS.ex_categories) CHIPS.ex_categories.set([]);
    if (CHIPS.ex_channels) CHIPS.ex_channels.set([]);
    if (CHIPS.blocked_words) CHIPS.blocked_words.set([]);
    if (CHIPS.blocked_roles) CHIPS.blocked_roles.set([]);
    if (CHIPS.wl_users) CHIPS.wl_users.set([]);
    if (CHIPS.bl_users) CHIPS.bl_users.set([]);

    if (!mid) return;

    try {
      const res = await fetch(`/filters/${encodeURIComponent(mid)}`, {
        method: "GET",
        credentials: "same-origin",
      });
      if (!res.ok) throw new Error("bad");

      const data = await res.json();

      if (data && typeof data === "object") {
        if (Array.isArray(data.wl_categories) && CHIPS.wl_categories) {
          CHIPS.wl_categories.set(data.wl_categories);
        }
        if (Array.isArray(data.wl_channels) && CHIPS.wl_channels) {
          CHIPS.wl_channels.set(data.wl_channels);
        }
        if (Array.isArray(data.ex_categories) && CHIPS.ex_categories) {
          CHIPS.ex_categories.set(data.ex_categories);
        }
        if (Array.isArray(data.ex_channels) && CHIPS.ex_channels) {
          CHIPS.ex_channels.set(data.ex_channels);
        }
        if (Array.isArray(data.blocked_words) && CHIPS.blocked_words) {
          CHIPS.blocked_words.set(data.blocked_words);
        }

        let roleIds = [];
        if (Array.isArray(data.blocked_role_ids)) {
          roleIds = data.blocked_role_ids.map(String);
        } else if (typeof data.blocked_role_ids_csv === "string") {
          roleIds = parseIdList(data.blocked_role_ids_csv);
        } else if (Array.isArray(data.blocked_roles)) {
          roleIds = data.blocked_roles.map(String);
        }

        if (hiddenBlockedRoles) {
          hiddenBlockedRoles.value = roleIds.join(",");
          hiddenBlockedRoles.dispatchEvent(
            new Event("input", { bubbles: true })
          );
        }
        if (CHIPS.blocked_roles) {
          CHIPS.blocked_roles.set(roleIds);
        }

        if (Array.isArray(data.wl_users)) {
          if (wlUsers) {
            wlUsers.value = data.wl_users.join(",");
            wlUsers.dispatchEvent(new Event("input", { bubbles: true }));
          }
          if (CHIPS.wl_users) {
            CHIPS.wl_users.set(data.wl_users);
          }
        }

        if (Array.isArray(data.bl_users)) {
          if (blUsers) {
            blUsers.value = data.bl_users.join(",");
            blUsers.dispatchEvent(new Event("input", { bubbles: true }));
          }
          if (CHIPS.bl_users) {
            CHIPS.bl_users.set(data.bl_users);
          }
        }
      }
    } catch (err) {
      console.warn("Failed to load filters", err);
    }
  }

  function setupMappingSettingsSearch() {
    const input = document.getElementById("mappingSettingsSearch");
    if (!input) return;
    if (input._bound) return;
    input._bound = true;

    function applyFilter() {
      const q = input.value.trim().toLowerCase();

      const fields = document.querySelectorAll(
        "#mapping-form .mapping-setting-field"
      );

      fields.forEach((field) => {
        const key = (field.dataset.settingKey || "").toLowerCase();
        const label = (
          field.querySelector(".label-text")?.textContent || ""
        ).toLowerCase();

        const match = !q || key.includes(q) || label.includes(q);
        field.style.display = match ? "" : "none";
      });
    }

    input.addEventListener("input", applyFilter);

    applyFilter();
  }

  document.addEventListener("DOMContentLoaded", () => {
    cModal = document.getElementById("confirm-modal");
    cTitle = document.getElementById("confirm-title");
    cBody = document.getElementById("confirm-body");
    cBtnOk = document.getElementById("confirm-okay");
    cBtnX = document.getElementById("confirm-close");
    cBtnCa = document.getElementById("confirm-cancel");
    cBack = cModal ? cModal.querySelector(".modal-backdrop") : null;
    refreshFooterVersion();
    setInterval(refreshFooterVersion, 600_000);

    validateConfigAndToggle({ decorate: false });

    REQUIRED_KEYS.forEach((k) => {
      const el = document.getElementById(k);
      if (!el) return;

      el.addEventListener("input", () => {
        updateStartButtonOnly();

        if (cfgValidated) validateField(k);
      });
    });

    setupMappingSettingsSearch();

    const fetchBtn = document.getElementById("filters-role-refresh");
    const applyBtn = document.getElementById("roleBlocksApply");
    const cancelBtn = document.getElementById("roleBlocksCancel");
    const xBtn = document.getElementById("roleBlocksCloseBtn");
    const modal = document.getElementById("roleBlocksModal");

    const filterBrowseButtons = document.querySelectorAll(".filter-fetch-btn");
    filterBrowseButtons.forEach((btn) => {
      btn.addEventListener("click", async () => {
        const listKey = btn.getAttribute("data-filter-target");
        const kind = btn.getAttribute("data-filter-kind");
        if (!listKey || !kind) return;

        const mid =
          (currentFilterMapping && currentFilterMapping.mapping_id) ||
          document.getElementById("filters_mapping_id")?.value ||
          "";
        if (!mid) return;

        await openFilterObjectsModal(mid, { listKey, kind });
      });
    });

    const filterApply = document.getElementById("filterObjectsApply");
    const filterCancel = document.getElementById("filterObjectsCancel");
    const filterClose = document.getElementById("filterObjectsCloseBtn");

    if (filterApply) {
      filterApply.addEventListener("click", () => {
        if (!CURRENT_FILTER_PICKER) {
          closeFilterObjectsModal();
          return;
        }

        const { listKey, selected } = CURRENT_FILTER_PICKER;
        const list = Array.from(selected || []);

        if (CHIPS[listKey]) {
          CHIPS[listKey].set(list);
        } else {
          const hidden = document.getElementById(listKey);
          if (hidden) {
            hidden.value = list.join(",");
            hidden.dispatchEvent(new Event("input", { bubbles: true }));
          }
        }

        closeFilterObjectsModal();

        // before checking if it's dirty
        setTimeout(() => {
          updateFiltersDirtyForModal();
        }, 0);
      });
    }

    [filterCancel, filterClose].forEach((btn) => {
      if (btn) {
        btn.addEventListener("click", () => {
          closeFilterObjectsModal();
        });
      }
    });

    if (fetchBtn) {
      fetchBtn.addEventListener("click", async () => {
        const mid =
          (currentFilterMapping && currentFilterMapping.mapping_id) ||
          document.getElementById("filters_mapping_id")?.value ||
          "";
        await openRoleBlocksModal(mid);
      });
    }

    if (applyBtn) {
      applyBtn.addEventListener("click", () => {
        const hidden = document.getElementById("blocked_role_ids");
        const list = Array.from(CURRENT_BLOCKED_ROLE_IDS);

        if (hidden) {
          hidden.value = list.join(",");
          hidden.dispatchEvent(new Event("input", { bubbles: true }));
        }
        if (CHIPS.blocked_roles) {
          CHIPS.blocked_roles.set(list);
        }

        closeRoleBlocksModal();
      });
    }

    if (cancelBtn) {
      cancelBtn.addEventListener("click", () => {
        const hidden = document.getElementById("blocked_role_ids");

        // Reset CURRENT_BLOCKED_ROLE_IDS to match what's *actually* blocked
        if (CHIPS.blocked_roles) {
          CURRENT_BLOCKED_ROLE_IDS = new Set(CHIPS.blocked_roles.get());
        } else {
          CURRENT_BLOCKED_ROLE_IDS = hidden
            ? parseBlockedRolesCsv(hidden.value)
            : new Set();
        }

        closeRoleBlocksModal();
      });
    }

    if (xBtn) {
      xBtn.addEventListener("click", () => {
        closeRoleBlocksModal();
      });
    }

    if (modal) {
      modal.addEventListener("keydown", (evt) => {
        if (evt.key === "Escape") {
          evt.stopPropagation();
          closeRoleBlocksModal();
        }
      });
    }
    const cfgForm = document.getElementById("cfg-form");
    const cfgSaveBtn = document.getElementById("cfg-save-btn");
    const cfgCancelBtn = document.getElementById("cfg-cancel-btn");

    if (cfgForm && cfgSaveBtn && cfgCancelBtn) {
      BASELINES.cfg = snapshotForm(cfgForm);

      setCfgButtonsVisible(false);

      const updateCfgDirty = () => {
        const dirty = snapshotForm(cfgForm) !== BASELINES.cfg;
        setCfgButtonsVisible(dirty);
      };

      cfgForm.addEventListener("input", updateCfgDirty);
      cfgForm.addEventListener("change", updateCfgDirty);

      cfgForm.addEventListener("reset", () => {
        cfgValidated = false;
        setTimeout(() => {
          BASELINES.cfg = snapshotForm(cfgForm);
          validateConfigAndToggle({ decorate: "clear" });
          setCfgButtonsVisible(false);
        }, 0);
      });

      cfgCancelBtn.addEventListener("click", () => {
        cfgForm.reset();

        const cmdHidden = document.getElementById("COMMAND_USERS");
        if (cmdHidden && typeof BASELINES.cmd_users_csv === "string") {
          cmdHidden.value = BASELINES.cmd_users_csv;

          if (CHIPS.cmd_users) {
            CHIPS.cmd_users.set(parseIdList(BASELINES.cmd_users_csv));
          }
        }

        cfgValidated = false;
        validateConfigAndToggle({ decorate: "clear" });
        setCfgButtonsVisible(false);
      });
    }
    updateToggleButton({ server: {}, client: {} });

    startStatusPoll(4000);
    fetchAndRenderStatus();
    attachAdminBus();
    initSlideMenu();

    if (cBtnOk)
      cBtnOk.addEventListener("click", () => {
        if (confirmResolve) confirmResolve();
      });

    if (cBtnX)
      cBtnX.addEventListener("click", () => {
        if (confirmReject) confirmReject();
      });

    if (cBtnCa)
      cBtnCa.addEventListener("click", () => {
        if (confirmReject) confirmReject();
      });

    if (cBack)
      cBack.addEventListener("click", () => {
        if (confirmReject) confirmReject();
      });

    document.addEventListener("keydown", (e) => {
      const open = cModal && cModal.classList.contains("show");
      if (!open) return;
      if (e.key === "Escape") {
        e.preventDefault();
        if (confirmReject) confirmReject();
      }
      if (e.key === "Enter") {
        e.preventDefault();
        if (confirmResolve) confirmResolve();
      }
    });

    function bindNewMappingCard() {
      const newCard = document.getElementById("new-mapping-card");
      if (!newCard) return;
      newCard.addEventListener("click", () => {
        openMappingModal(null);
      });
      newCard.addEventListener("keydown", (e) => {
        if (e.key === "Enter" || e.key === " ") {
          e.preventDefault();
          openMappingModal(null);
        }
      });
    }

    bindNewMappingCard();

    const clearForm = document.querySelector('form[action="/logs/clear"]');
    if (clearForm) {
      clearForm.addEventListener(
        "submit",
        (e) => {
          if (clearForm.dataset.skipConfirm === "1") {
            delete clearForm.dataset.skipConfirm;
            return;
          }
          e.preventDefault();
          e.stopImmediatePropagation();

          openConfirm({
            title: "Clear all logs?",
            body: "This will permanently delete server and client logs. This cannot be undone.",
            confirmText: "Clear logs",
            confirmClass: "btn-ghost-red",
            showCancel: false,
            onConfirm: () => {
              clearForm.dataset.skipConfirm = "1";
              clearForm.requestSubmit();
            },
          });
        },
        { passive: false }
      );
    }

    const messages = {
      "/save": "Configuration saved.",
      "/start": "Start command sent.",
      "/stop": "Stop command sent.",
      "/logs/clear": "Logs cleared.",
      "/filters/save": "Filters saved.",
    };

    document.querySelectorAll('form[method="post"]').forEach((f) => {
      f.addEventListener("submit", async (e) => {
        e.preventDefault();

        const actionPath = new URL(f.action, location.origin).pathname;

        if (actionPath === "/save") {
          cfgValidated = true;
          const { missing } = configState();
          markInvalid(missing);
        }

        const btn = f.querySelector('button[type="submit"],button:not([type])');

        if (f.id === "toggle-form" && actionPath === "/start") {
          const { ok, missing } = configState();
          if (!ok) {
            showToast(`Missing required config: ${missing.join(", ")}`, {
              type: "error",
              timeout: 6000,
            });
            document.getElementById(missing[0])?.focus();
            f.classList.remove("is-loading");
            const btn2 = f.querySelector(
              'button[type="submit"],button:not([type])'
            );
            if (btn2) btn2.disabled = false;
            return;
          }
        }

        if (btn) btn.disabled = true;
        f.classList.add("is-loading");

        try {
          const body = new FormData(f);
          const res = await fetch(f.action, {
            method: "POST",
            body,
            credentials: "same-origin",
          });
          const isSuccess = res.ok || (res.status >= 300 && res.status < 400);

          if (isSuccess) {
            const isFilterSave =
              /^\/filters\/[^/]+\/save$/.test(actionPath) ||
              actionPath === "/filters/save";

            showToast(
              isFilterSave ? "Filters saved." : messages[actionPath] || "Done.",
              { type: "success" }
            );

            if (actionPath === "/stop") {
              try {
                localStorage.setItem("bf:__wipe", String(Date.now()));
                [
                  "bf:running",
                  "bf:launching",
                  "bf:pulling",
                  "bf:queued",
                  "bf:cleaning",
                  "bf:done_tasks",
                ].forEach((k) => localStorage.removeItem(k));
                sessionStorage.removeItem("bf:taskmap");
              } catch {}
            }

            if (actionPath === "/start" || actionPath === "/stop") {
              burstStatusPoll(800, 15000, 4000);
            } else if (actionPath === "/save") {
              const cmdHidden = document.getElementById("COMMAND_USERS");
              if (cmdHidden) {
                BASELINES.cmd_users_csv = cmdHidden.value;
                cmdHidden.defaultValue = cmdHidden.value;
              }

              if (cfgForm) {
                BASELINES.cfg = snapshotForm(cfgForm);

                Array.from(cfgForm.elements).forEach((el) => {
                  if (!el || !el.tagName) return;
                  const tag = el.tagName.toUpperCase();

                  if (tag === "INPUT" || tag === "TEXTAREA") {
                    if (el.type === "checkbox" || el.type === "radio") {
                      el.defaultChecked = el.checked;
                    } else {
                      el.defaultValue = el.value;
                    }
                  } else if (tag === "SELECT") {
                    Array.from(el.options).forEach((opt) => {
                      opt.defaultSelected = opt.selected;
                    });
                  }
                });

                setCfgButtonsVisible(false);
              }

              fetchAndRenderStatus();
            } else if (isFilterSave) {
              const ff = document.getElementById("form-filters");
              if (ff) {
                FILTERS_BASELINE = snapshotForm(ff);
              }
              const cancelBtn = document.getElementById("btn-cancel-filters");
              if (cancelBtn) cancelBtn.setAttribute("hidden", "");
              closeFiltersModal();
            }
          } else {
            const text = await safeText(res);
            showToast(text || `Request failed (${res.status})`, {
              type: "error",
              timeout: 7000,
            });
          }
        } catch {
          showToast("Network error", { type: "error" });
        } finally {
          f.classList.remove("is-loading");
          if (btn) btn.disabled = false;
        }
      });
    });

    enhanceAllSelects();
    initCollapsibleCards();
    initChips();

    const modalFiltersForm = document.getElementById("form-filters");
    if (modalFiltersForm) {
      modalFiltersForm.addEventListener("input", updateFiltersDirtyForModal);
      modalFiltersForm.addEventListener("change", updateFiltersDirtyForModal);
      modalFiltersForm.addEventListener("reset", () => {
        setTimeout(updateFiltersDirtyForModal, 0);
      });
    }

    document.addEventListener("keydown", (e) => {
      if (e.key !== "Escape") return;

      const rb = document.getElementById("roleBlocksModal");
      const fm = document.getElementById("filters-modal");
      const om = document.getElementById("filterObjectsModal");

      if (rb && rb.classList.contains("show")) {
        e.preventDefault();
        closeRoleBlocksModal();
        return;
      }

      if (om && om.classList.contains("show")) {
        e.preventDefault();
        closeFilterObjectsModal();
        return;
      }

      if (fm && fm.classList.contains("show")) {
        e.preventDefault();
        maybeCloseFiltersModal();
      }
    });

    function updateFiltersDirtyForModal() {
      const form = document.getElementById("form-filters");
      const cancelBtn = document.getElementById("btn-cancel-filters");
      if (!form || !cancelBtn) return;
      cancelBtn.hidden = snapshotForm(form) === FILTERS_BASELINE;
    }

    (function initAdminRealtime() {
      const url =
        (location.protocol === "https:" ? "wss://" : "ws://") +
        location.host +
        "/ws/out";
      const ws = new WebSocket(url);

      ws.onmessage = (ev) => {
        try {
          const msg = JSON.parse(ev.data);
          if (msg.kind === "agent" && msg.type === "status") {
            const prefix = msg.role === "server" ? "server" : "client";
            renderStatusRow(prefix, msg.data || {});
          }
          if (msg.type === "info")
            showToast(msg.data?.msg || "Info", { type: "success" });
          if (msg.type === "error")
            showToast(msg.data?.msg || "Error", { type: "error" });
        } catch {}
      };

      ws.onclose = () => {
        setTimeout(initAdminRealtime, 2000);
      };
    })();

    function connectUiBus() {
      try {
        if (uiSock) uiSock.close();
      } catch (_) {}
      const proto = location.protocol === "https:" ? "wss" : "ws";
      uiSock = new WebSocket(`${proto}://${location.host}/ws/ui`);

      uiSock.onopen = () => {
        startStatusPoll(12000);
      };
      uiSock.onclose = () => {
        startStatusPoll(4000);
        uiSockTimer = setTimeout(connectUiBus, 1500);
      };
      uiSock.onerror = () => {
        try {
          uiSock.close();
        } catch (_) {}
      };

      uiSock.onmessage = (ev) => {
        let msg;
        try {
          msg = JSON.parse(ev.data);
        } catch {
          return;
        }
        const { type, source, data } = msg;
        if (type === "status") {
          const common = {
            running: !!data.running,
            uptime_sec: data.uptime_sec,
            status: data.note || "",
          };
          if (source === "server") renderStatusRow("server", common);
          if (source === "client") renderStatusRow("client", common);
        }
        if (msg.kind === "agent" && msg.type === "status") {
          const prefix = msg.role === "server" ? "server" : "client";
          renderStatusRow(prefix, msg.data || {});
        }
      };
    }
    connectUiBus();

    function attachAdminBus() {
      try {
        const es = new EventSource("/bus/stream");
        es.onmessage = (ev) => {
          try {
            const evt = JSON.parse(ev.data);
            if (evt.kind === "filters") {
              if (currentFilterMapping && currentFilterMapping.mapping_id) {
                loadFiltersIntoFormForMapping(currentFilterMapping.mapping_id);
              }
            }
            if (evt.kind === "status") {
              const prefix = evt.role === "server" ? "server" : "client";
              const p = evt.payload || {};
              renderStatusRow(prefix, {
                running: !!p.running,
                uptime_sec: p.uptime_sec,
                error: p.error ?? "",
                status: typeof p.status === "string" ? p.status : "",
              });
            }
          } catch {}
        };
      } catch (e) {
        console.warn("Failed to attach admin bus", e);
      }
    }

    (function InfoDots() {
      const SEL = ".info-dot";

      function ensureBubbleFor(btn) {
        if (!btn.getAttribute("aria-describedby") && btn.dataset.tip) {
          const id = `tip-${Math.random().toString(36).slice(2, 8)}`;
          const bubble = document.createElement("div");
          bubble.className = "tip-bubble";
          bubble.id = id;
          bubble.setAttribute("role", "tooltip");
          bubble.setAttribute("aria-hidden", "true");
          bubble.textContent = btn.dataset.tip;
          btn.after(bubble);
          btn.setAttribute("aria-describedby", id);
        }
      }

      function prime(btn) {
        if (!btn.hasAttribute("type")) btn.setAttribute("type", "button");
        if (!btn.hasAttribute("aria-expanded"))
          btn.setAttribute("aria-expanded", "false");
        ensureBubbleFor(btn);

        const id = btn.getAttribute("aria-describedby");
        const bubble = id
          ? document.getElementById(id)
          : btn.nextElementSibling;
        if (bubble && !bubble.hasAttribute("aria-hidden"))
          bubble.setAttribute("aria-hidden", "true");
      }

      function init(root = document) {
        root.querySelectorAll(SEL).forEach(prime);
      }
      if (document.readyState !== "loading") init();
      else document.addEventListener("DOMContentLoaded", init);

      function closeAll(exceptBtn = null) {
        document
          .querySelectorAll(`${SEL}[aria-expanded="true"]`)
          .forEach((openBtn) => {
            if (openBtn === exceptBtn) return;
            openBtn.setAttribute("aria-expanded", "false");
            const id = openBtn.getAttribute("aria-describedby");
            const b = id
              ? document.getElementById(id)
              : openBtn.nextElementSibling;
            if (b) b.setAttribute("aria-hidden", "true");
          });
      }

      document.addEventListener("click", (e) => {
        const btn = e.target.closest(SEL);
        const inBubble = e.target.closest(".tip-bubble");
        if (btn) {
          prime(btn);
          const id = btn.getAttribute("aria-describedby");
          const bubble = id
            ? document.getElementById(id)
            : btn.nextElementSibling;
          const open = btn.getAttribute("aria-expanded") === "true";
          closeAll(btn);
          btn.setAttribute("aria-expanded", open ? "false" : "true");
          if (bubble)
            bubble.setAttribute("aria-hidden", open ? "true" : "false");
        } else if (!inBubble) {
          closeAll();
        }
      });

      document.addEventListener("keydown", (e) => {
        if (e.key === "Escape") closeAll();
      });

      const mo = new MutationObserver((muts) => {
        for (const m of muts) {
          m.addedNodes.forEach((n) => {
            if (!(n instanceof Element)) return;
            if (n.matches?.(SEL)) prime(n);
            n.querySelectorAll?.(SEL).forEach(prime);
          });
        }
      });
      mo.observe(document.documentElement, { childList: true, subtree: true });

      window.InfoDots = { init, prime };
    })();

    const mapModal = document.getElementById("mapping-modal");
    const mapClose = document.getElementById("mapping-close");
    const mapCancel = document.getElementById("mapping-cancel-btn");
    const mapForm = document.getElementById("mapping-form");
    const addBtn = document.getElementById("add-mapping-btn");

    if (addBtn) {
      addBtn.addEventListener("click", () => {
        openMappingModal(null);
      });
    }

    if (mapClose) {
      mapClose.addEventListener("click", (e) => {
        e.preventDefault();
        maybeCloseMappingModal();
      });
    }

    if (mapCancel) {
      mapCancel.addEventListener("click", (e) => {
        e.preventDefault();
        resetMappingFormToBaseline();
      });
    }

    if (mapForm) {
      mapForm.addEventListener("submit", async (e) => {
        e.preventDefault();
        await saveMappingFromModal();
      });
    }

    const mappingSearchInput = document.getElementById("mappingSearchInput");
    if (mappingSearchInput) {
      let searchDebounce;
      mappingSearchInput.addEventListener("input", () => {
        clearTimeout(searchDebounce);
        searchDebounce = setTimeout(() => {
          renderGuildMappings();
        }, 80);
      });

      mappingSearchInput.addEventListener("keydown", (e) => {
        if (e.key === "Escape") {
          e.preventDefault();
          mappingSearchInput.value = "";
          renderGuildMappings();
          mappingSearchInput.blur();
        }
      });
    }

    refreshGuildMappings();
  });

  ["server", "client"].forEach((role) => {
    const cached = loadUptime(role);
    if (!cached) return;
    const elapsed = Math.floor((Date.now() - cached.t) / 1000);
    const startSec = Math.max(0, cached.sec + elapsed);
    const elUp = document.getElementById(`${role}-uptime`);
    if (elUp) elUp.textContent = formatUptime(startSec);

    RUNTIME_CACHE[role] = { baseSec: startSec, lastUpdateMs: Date.now() };
  });

  function setToggleDisabled(on) {
    const btn = document.getElementById("toggle-btn");
    const form = document.getElementById("toggle-form");
    if (form) form.dataset.locked = on ? "1" : "0";
    if (btn) {
      btn.disabled = on;
      btn.classList.toggle("is-loading", on);
    }
  }

  function isFieldValid(key, raw) {
    const v = String(raw || "").trim();
    return v.length > 0;
  }

  function validateField(key) {
    const el = document.getElementById(key);
    if (!el) return;
    const valid = isFieldValid(key, el.value);
    el.classList.toggle("is-invalid", cfgValidated && !valid);
  }

  function updateStartButtonOnly() {
    const btn = document.getElementById("toggle-btn");
    const form = document.getElementById("toggle-form");
    if (!btn || !form) return;

    const { ok } = configState();
    const running =
      form.action.endsWith("/stop") ||
      btn.textContent.trim().toLowerCase() === "stop";

    const blockStart = !ok && !running;

    btn.dataset.invalid = blockStart ? "1" : "0";
    btn.title = blockStart
      ? "Provide SERVER_TOKEN, CLIENT_TOKEN, and at least one Guild Mapping to start."
      : "";
    btn.disabled = !!toggleLocked || blockStart;
  }
})();
