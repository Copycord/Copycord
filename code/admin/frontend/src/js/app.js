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
  let cModal, cTitle, cBody, cBtnOk, cBtnX, cBtnCa, cBack;
  let confirmResolve = null;
  let confirmReject = null;
  const UPTIME_KEY = (role) => `cpc:uptime:${role}`;
  const ICONS = {
    trash: `
      <svg viewBox="0 0 24 24" width="20" height="20" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">
        <path d="M10 11v6"/><path d="M14 11v6"/><path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6"/><path d="M3 6h18"/><path d="M8 6V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"/>
      </svg>
    `,
    settings: `
      <svg viewBox="0 0 24 24" width="20" height="20" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">
        <path d="M9.671 4.136a2.34 2.34 0 0 1 4.659 0 2.34 2.34 0 0 0 3.319 1.915 2.34 2.34 0 0 1 2.33 4.033 2.34 2.34 0 0 0 0 3.831 2.34 2.34 0 0 1-2.33 4.033 2.34 2.34 0 0 0-3.319 1.915 2.34 2.34 0 0 1-4.659 0 2.34 2.34 0 0 0-3.32-1.915 2.34 2.34 0 0 1-2.33-4.033 2.34 2.34 0 0 0 0-3.831A2.34 2.34 0 0 1 6.35 6.051a2.34 2.34 0 0 0 3.319-1.915"/><circle cx="12" cy="12" r="3"/>
      </svg>
    `,
    filters: `
      <svg viewBox="0 0 24 24" width="20" height="20" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">
        <path d="M10 20a1 1 0 0 0 .553.895l2 1A1 1 0 0 0 14 21v-7a2 2 0 0 1 .517-1.341L21.74 4.67A1 1 0 0 0 21 3H3a1 1 0 0 0-.742 1.67l7.225 7.989A2 2 0 0 1 10 14z"/>
      </svg>
    `,
    pause: `
      <svg viewBox="0 0 24 24" width="20" height="20" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">
        <rect x="14" y="3" width="5" height="18" rx="1"/><rect x="5" y="3" width="5" height="18" rx="1"/>
      </svg>
    `,
    play: `
      <svg viewBox="0 0 24 24" width="20" height="20" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">
        <path d="M5 5a2 2 0 0 1 3.008-1.728l11.997 6.998a2 2 0 0 1 .003 3.458l-12 7A2 2 0 0 1 5 19z"/>
      </svg>
    `,
    clone: `
      <svg viewBox="0 0 24 24" width="20" height="20" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">
        <rect width="14" height="14" x="8" y="8" rx="2" ry="2"/><path d="M4 16c-1.1 0-2-.9-2-2V4c0-1.1.9-2 2-2h10c1.1 0 2 .9 2 2"/>
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

  function triggerConfetti() {
    const duration = 3000;
    const animationEnd = Date.now() + duration;
    const defaults = {
      startVelocity: 30,
      spread: 360,
      ticks: 60,
      zIndex: 99999,
      colors: [
        "#8b5cf6",
        "#a78bfa",
        "#c4b5fd",
        "#ef4444",
        "#f87171",
        "#fca5a5",
      ],
    };

    function randomInRange(min, max) {
      return Math.random() * (max - min) + min;
    }

    const interval = setInterval(function () {
      const timeLeft = animationEnd - Date.now();

      if (timeLeft <= 0) {
        return clearInterval(interval);
      }

      const particleCount = 50 * (timeLeft / duration);

      confetti({
        ...defaults,
        particleCount,
        origin: { x: randomInRange(0.1, 0.3), y: Math.random() - 0.2 },
      });
      confetti({
        ...defaults,
        particleCount,
        origin: { x: randomInRange(0.7, 0.9), y: Math.random() - 0.2 },
      });
    }, 250);
  }

  window.triggerConfetti = triggerConfetti;

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
    RESEND_EDITED_MESSAGES: true,
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
    SYNC_CHANNEL_NSFW: false,
    SYNC_CHANNEL_TOPIC: false,
    SYNC_CHANNEL_SLOWMODE: false,
    REARRANGE_ROLES: false,
    CLONE_ROLE_ICONS: false,
    CLONE_VOICE: true,
    CLONE_VOICE_PROPERTIES: false,
    CLONE_STAGE: true,
    CLONE_STAGE_PROPERTIES: false,
    CLONE_GUILD_ICON: false,
    CLONE_GUILD_BANNER: false,
    CLONE_GUILD_SPLASH: false,
    CLONE_GUILD_DISCOVERY_SPLASH: false,
    SYNC_GUILD_DESCRIPTION: false,
    SYNC_FORUM_PROPERTIES: false,
    ANONYMIZE_USERS: false,
    DISABLE_EVERYONE_MENTIONS: false,
    DISABLE_ROLE_MENTIONS: false,
    TAG_REPLY_MSG: false,
    APPEND_TIMESTAMP: false,
    APPEND_AUTHOR: false,
    DB_CLEANUP_MSG: true,
    ON_DEMAND_WEBHOOKS: true,
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
        const isTokenUser = el.classList.contains("token-user-btn");
        const isReveal = el.classList.contains("reveal-btn") && !isTokenUser;

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

        if (isTokenUser) {
          el.disabled = running;
          el.classList.toggle("disabled-btn", running);
          el.title = running ? "Stop the bot to manage backup tokens" : "";
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

    // Wire auto-start toggle to hidden select
    const gcAutoToggle = document.getElementById("gc-autostart-toggle");
    const gcAutoSelect = document.getElementById("COPYCORD_AUTOSTART");
    if (gcAutoToggle && gcAutoSelect) {
      gcAutoToggle.addEventListener("change", () => {
        gcAutoSelect.value = gcAutoToggle.checked ? "true" : "false";
      });
    }
  }

  function setGuildCardsLocked(running) {
    const cards = document.querySelectorAll("#guild-mapping-list .guild-card");
    cards.forEach((card) => {
      if (running) {
        card.classList.add("locked");
        card.title = "Stop the bots to edit mappings";
      } else {
        card.classList.remove("locked");
        card.title = "";
      }
    });
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

  let currentLogType = null;

  function openLogs(which) {
    if (!modal || !logBody) return;
    currentLogType = which;
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

  const logClearBtn = document.getElementById("log-clear-btn");
  if (logClearBtn) {
    logClearBtn.addEventListener("click", () => {
      if (!currentLogType) return;
      const label = currentLogType === "server" ? "Server" : "Client";
      openConfirm({
        title: "Clear logs?",
        body: `This will permanently delete all ${label.toLowerCase()} logs. This cannot be undone.`,
        confirmText: "Clear logs",
        confirmClass: "btn-ghost-red",
        onConfirm: async () => {
          try {
            const resp = await fetch(`/logs/clear/${currentLogType}`, { method: "POST" });
            if (resp.ok) {
              LOG_LINES = [];
              renderLogView();
              showToast(`${label} logs cleared`, { type: "success", timeout: 2000 });
            } else {
              showToast("Failed to clear logs", { type: "error" });
            }
          } catch {
            showToast("Failed to clear logs", { type: "error" });
          }
        },
      });
    });
  }

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
      ["channel_name_blacklist", "channel_name_blacklist", "words"],
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
  let blockStartForBadTokens = false;

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

    const ok = hasTokens;

    const missing = [];
    if (!vals.SERVER_TOKEN) missing.push("SERVER_TOKEN");
    if (!vals.CLIENT_TOKEN) missing.push("CLIENT_TOKEN");

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

  function highlightTokenInputsFromErrors(errs) {
    const bad = new Set();

    if (Array.isArray(errs)) {
      for (const msg of errs) {
        const m = String(msg || "");
        if (m.includes("CLIENT_TOKEN")) bad.add("CLIENT_TOKEN");
        if (m.includes("SERVER_TOKEN")) bad.add("SERVER_TOKEN");
      }
    }

    if (!bad.size && blockStartForBadTokens) {
      bad.add("CLIENT_TOKEN");
      bad.add("SERVER_TOKEN");
    }

    ["CLIENT_TOKEN", "SERVER_TOKEN"].forEach((id) => {
      const el = document.getElementById(id);
      if (!el) return;
      el.classList.toggle("is-invalid", bad.has(id));
    });
  }

  async function checkSavedTokensOnLoad() {
    try {
      const res = await fetch("/api/validate-tokens", {
        method: "GET",
        headers: { Accept: "application/json" },
        credentials: "same-origin",
      });

      if (!res.ok) {
        console.warn("Token validation failed with status", res.status);
        return;
      }

      const data = await res.json();
      const hasTokens = !!data.has_tokens;
      const ok = !!data.ok;
      const errs = Array.isArray(data.errors) ? data.errors : [];

      blockStartForBadTokens = hasTokens && !ok;

      if (blockStartForBadTokens) {
        highlightTokenInputsFromErrors(errs);

        const message =
          errs.join(" ") ||
          "Saved tokens are no longer valid. Please update SERVER_TOKEN and CLIENT_TOKEN.";
        showToast(message, { type: "error", timeout: 8000 });
      } else {
        highlightTokenInputsFromErrors([]);
      }

      // Enable invite button if bot client ID is available
      const inviteBtn = document.getElementById("bot-invite-btn");
      if (inviteBtn && data.bot_client_id) {
        inviteBtn.href = `https://discord.com/oauth2/authorize?client_id=${data.bot_client_id}&permissions=8&integration_type=0&scope=bot`;
        inviteBtn.classList.remove("bot-invite-disabled");
      }
    } catch (err) {
      console.error("Token validation on load failed:", err);
    } finally {
      updateStartButtonOnly();
    }
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

    const blockStart = (!ok || blockStartForBadTokens) && !running;
    btn.dataset.invalid = blockStart ? "1" : "0";

    if (blockStartForBadTokens) {
      btn.title =
        "Saved tokens are no longer valid. Please update SERVER_TOKEN and CLIENT_TOKEN to start.";
    } else if (!ok) {
      btn.title =
        "Provide SERVER_TOKEN and CLIENT_TOKEN to start.";
    } else {
      btn.title = "";
    }

    btn.disabled = !!toggleLocked || blockStart;

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
    // Hidden selects (message features from sub-modal)
    document
      .querySelectorAll("#mapping-form select[id^='map_']")
      .forEach((sel) => {
        const key = sel.id.replace(/^map_/, "");
        settings[key] = String(sel.value).toLowerCase() === "true";
      });
    // Toggle checkboxes (main settings)
    document
      .querySelectorAll("#mapping-form input[data-map-toggle]")
      .forEach((cb) => {
        const key = cb.name;
        if (key) settings[key] = cb.checked;
      });

    return {
      mapping_id: id,
      mapping_name,
      original_guild_id,
      cloned_guild_id,
      settings,
    };
  }

  document;
  document
    .querySelectorAll(".reveal-btn:not(.token-user-btn)")
    .forEach((btn) => {
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

  function syncBodyScrollLock() {
    const anyOpen = document.querySelector(".modal.show");
    document.body.classList.toggle("body-lock-scroll", !!anyOpen);
  }

  let lastFocusBackupTokens = null;

  function fmtUnix(ts) {
    const n = Number(ts);
    if (!n || n <= 0) return "";
    try {
      return new Date(n * 1000).toLocaleString();
    } catch {
      return String(n);
    }
  }

  async function fetchBackupTokens() {
    const res = await fetch("/api/backup-tokens", {
      headers: { "X-Requested-With": "XMLHttpRequest" },
    });
    if (!res.ok) {
      throw new Error(`Failed to fetch backup tokens: ${res.status}`);
    }
    return await res.json();
  }

  function renderBackupTokensList(tokens) {
    const list = document.getElementById("backupTokensList");
    if (!list) return;

    const rows = Array.isArray(tokens) ? tokens : [];
    if (!rows.length) {
      list.innerHTML =
        '<div class="text-subtle" style="padding:10px 0;">No backup tokens saved yet.</div>';
      return;
    }

    list.innerHTML = rows
      .map((t) => {
        const id = escapeHtml(String(t.token_id || ""));
        const masked = escapeHtml(String(t.masked || ""));
        const note = escapeHtml(String(t.note || ""));
        const added = fmtUnix(t.added_at) || "";
        const used = fmtUnix(t.last_used) || "Never";

        return `
        <div class="card" data-token-id="${id}" style="padding:10px 12px; margin-bottom:10px;">
          <div style="display:flex; align-items:flex-start; justify-content:space-between; gap:12px;">
            <div style="min-width:0;">
              <div style="font-weight:700; letter-spacing:.2px;">${
                masked || "(hidden)"
              }</div>
              ${
                note
                  ? `<div class="text-subtle" style="margin-top:4px;">${note}</div>`
                  : ""
              }
              <div class="text-subtle" style="margin-top:6px; font-size:.85rem;">
                <span>Added: ${escapeHtml(added || "—")}</span>
                <span style="margin:0 8px; opacity:.6;">•</span>
                <span>Last used: ${escapeHtml(used || "Never")}</span>
              </div>
            </div>

            <button type="button"
              class="btn-icon delete-token-btn"
              data-token-id="${id}"
              aria-label="Remove backup token"
              title="Remove">
              ${ICONS.trash}
            </button>
          </div>
        </div>
      `;
      })
      .join("");
  }

  function _getBackupListAnchor(listEl) {
    if (!listEl) return null;

    const cards = listEl.querySelectorAll(".card[data-token-id]");
    const y = listEl.scrollTop;

    for (const c of cards) {
      if (c.offsetTop + c.offsetHeight > y + 4) {
        return c.getAttribute("data-token-id");
      }
    }
    return null;
  }

  async function refreshBackupTokensList(opts = {}) {
    const { keepScroll = true } = opts;

    const list = document.getElementById("backupTokensList");
    const prevScrollTop = list ? list.scrollTop : 0;
    const anchorId = keepScroll ? _getBackupListAnchor(list) : null;

    const payload = await fetchBackupTokens();
    if (!payload || !payload.ok)
      throw new Error("Backup tokens request failed");

    renderBackupTokensList(payload.tokens || []);

    if (!keepScroll || !list) return;

    requestAnimationFrame(() => {
      if (anchorId) {
        const safe = window.CSS && CSS.escape ? CSS.escape(anchorId) : anchorId;
        const anchorCard = list.querySelector(`.card[data-token-id="${safe}"]`);
        if (anchorCard) {
          list.scrollTop = Math.max(0, anchorCard.offsetTop - 6);
          return;
        }
      }

      const maxTop = Math.max(0, list.scrollHeight - list.clientHeight);
      list.scrollTop = Math.min(prevScrollTop, maxTop);
    });
  }

  function openBackupTokensModal() {
    const modal = document.getElementById("backup-token-modal");
    if (!modal) return;

    lastFocusBackupTokens = document.activeElement;

    setInert(modal, false);
    modal.classList.add("show");
    modal.setAttribute("aria-hidden", "false");
    document.body.classList.add("body-lock-scroll");

    refreshBackupTokensList().catch((e) => {
      console.warn(e);
      showToast("Failed to load backup tokens.", { type: "error" });
    });

    const tokenInput = document.getElementById("backup_token_value");
    if (tokenInput && typeof tokenInput.focus === "function") {
      setTimeout(() => {
        try {
          tokenInput.focus();
        } catch {}
      }, 0);
    }
  }

  function closeBackupTokensModal() {
    const modal = document.getElementById("backup-token-modal");
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

    const anyOtherOpen = document.querySelector(
      "#filters-modal.show, " +
        "#mapping-modal.show, " +
        "#log-modal.show, " +
        "#backup-token-modal.show, " +
        "#roleBlocksModal.show, " +
        "#filterObjectsModal.show, " +
        "#confirm-modal.show"
    );
    if (!anyOtherOpen) {
      document.body.classList.remove("body-lock-scroll");
    }

    if (
      lastFocusBackupTokens &&
      typeof lastFocusBackupTokens.focus === "function"
    ) {
      try {
        lastFocusBackupTokens.focus();
      } catch {}
    }
    lastFocusBackupTokens = null;
  }

  const backupBtn = document.getElementById("backupTokensBtn");
  if (backupBtn) {
    backupBtn.addEventListener("click", (e) => {
      e.preventDefault();
      const cfgForm = document.getElementById("cfg-form");
      if (cfgForm && cfgForm.classList.contains("cfg-locked")) {
        return;
      }
      openBackupTokensModal();
    });
  }

  const backupClose = document.getElementById("backup-token-close");
  if (backupClose)
    backupClose.addEventListener("click", closeBackupTokensModal);

  const backupAdd = document.getElementById("backup-token-add");
  if (backupAdd) {
    backupAdd.addEventListener("click", async () => {
      const tokenEl = document.getElementById("backup_token_value");
      const noteEl = document.getElementById("backup_token_note");

      const tokenValue = (tokenEl?.value || "").trim();
      const noteValue = (noteEl?.value || "").trim();

      if (!tokenValue) {
        showToast("Paste a token first.", { type: "error" });
        return;
      }

      backupAdd.disabled = true;
      backupAdd.classList.add("disabled-btn");

      try {
        const fd = new FormData();
        fd.append("token_value", tokenValue);
        fd.append("note", noteValue);

        const res = await fetch("/api/backup-tokens/add", {
          method: "POST",
          body: fd,
          headers: { "X-Requested-With": "XMLHttpRequest" },
        });

        const payload = await res.json().catch(() => ({}));

        if (!res.ok || !payload.ok) {
          const msg = payload?.detail || "Failed to add backup token.";
          throw new Error(msg);
        }

        if (tokenEl) tokenEl.value = "";
        if (noteEl) noteEl.value = "";

        showToast("Backup token added.", { type: "success" });
        await refreshBackupTokensList();
      } catch (e) {
        console.warn(e);

        const msg =
          e &&
          typeof e === "object" &&
          "message" in e &&
          String(e.message).trim()
            ? String(e.message).trim()
            : "Failed to add backup token.";

        showToast(msg, { type: "error" });
      } finally {
        backupAdd.disabled = false;
        backupAdd.classList.remove("disabled-btn");
      }
    });
  }

  document.addEventListener("click", async (e) => {
    const btn = e.target && e.target.closest(".delete-token-btn");
    if (!btn) return;

    const tokenId = (btn.getAttribute("data-token-id") || "").trim();
    if (!tokenId) return;

    if (typeof openConfirm === "function") {
      openConfirm({
        title: "Remove backup token?",
        body: "This will delete the backup token from the database.",
        confirmText: "Remove",
        confirmClass: "btn-ghost-red",
        showCancel: true,
        onConfirm: async () => {
          try {
            const fd = new FormData();
            fd.append("token_id", tokenId);

            const res = await fetch("/api/backup-tokens/delete", {
              method: "POST",
              body: fd,
              headers: { "X-Requested-With": "XMLHttpRequest" },
            });
            const payload = await res.json().catch(() => ({}));
            if (!res.ok || !payload.ok) {
              throw new Error("delete failed");
            }

            showToast("Backup token removed.", { type: "success" });
            await refreshBackupTokensList({ keepScroll: true });
          } catch (err) {
            console.warn(err);
            showToast("Failed to remove backup token.", { type: "error" });
          }
        },
      });
    }
  });

  const backupModal = document.getElementById("backup-token-modal");
  if (backupModal) {
    const backdrop = backupModal.querySelector(".modal-backdrop");
    if (backdrop) {
      backdrop.addEventListener("click", closeBackupTokensModal);
    }

    document.addEventListener("keydown", (e) => {
      if (e.key !== "Escape") return;
      if (!backupModal.classList.contains("show")) return;
      if (cModal && cModal.classList.contains("show")) return;
      closeBackupTokensModal();
    });
  }

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

    syncBodyScrollLock();

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

  function openMappingModal(mapping, opts = {}) {
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

    const mode = opts.mode || (mapping ? "edit" : "create");
    const isEdit = mode === "edit";
    const cloneFrom = mode === "clone" && mapping ? mapping : null;

    if (idInput) {
      idInput.value = isEdit && mapping?.mapping_id ? mapping.mapping_id : "";
    }

    if (nameInput) {
      nameInput.value =
        isEdit && mapping?.mapping_name ? mapping.mapping_name : "";
    }
    if (hostInput) {
      hostInput.value =
        isEdit && mapping?.original_guild_id ? mapping.original_guild_id : "";
    }
    if (cloneInput) {
      cloneInput.value =
        isEdit && mapping?.cloned_guild_id ? mapping.cloned_guild_id : "";
    }

    if (!isEdit) {
      if (nameInput) nameInput.value = "";
      if (hostInput) hostInput.value = "";
      if (cloneInput) cloneInput.value = "";
    }

    if (subtleEl) {
      if (isEdit && mapping?.mapping_id) {
        subtleEl.hidden = false;
        subtleEl.textContent = `${mapping.mapping_id}`;
      } else {
        subtleEl.hidden = true;
        subtleEl.textContent = "";
      }
    }

    if (hostInput) {
      if (isEdit) {
        hostInput.readOnly = true;
        hostInput.classList.add("field-readonly");
        hostInput.setAttribute("aria-readonly", "true");
        hostInput.title =
          "HOST_GUILD_ID cannot be changed after the mapping is created.";
      } else {
        hostInput.readOnly = false;
        hostInput.classList.remove("field-readonly");
        hostInput.removeAttribute("aria-readonly");
        hostInput.removeAttribute("title");
      }
    }

    if (cloneInput) {
      if (isEdit) {
        cloneInput.readOnly = true;
        cloneInput.classList.add("field-readonly");
        cloneInput.setAttribute("aria-readonly", "true");
        cloneInput.title =
          "CLONE_GUILD_ID cannot be changed after the mapping is created.";
      } else {
        cloneInput.readOnly = false;
        cloneInput.classList.remove("field-readonly");
        cloneInput.removeAttribute("aria-readonly");
        cloneInput.removeAttribute("title");
      }
    }

    document
      .querySelectorAll("#mapping-form select[id^='map_']")
      .forEach((sel) => {
        const key = sel.id.replace(/^map_/, "");

        let rawVal;
        if (cloneFrom && cloneFrom.settings && key in cloneFrom.settings) {
          rawVal = cloneFrom.settings[key];
        } else if (isEdit && mapping?.settings && key in mapping.settings) {
          rawVal = mapping.settings[key];
        } else {
          rawVal = DEFAULT_MAPPING_SETTINGS[key];
        }

        let normalized;
        if (typeof rawVal === "boolean") {
          normalized = rawVal;
        } else if (typeof rawVal === "string") {
          const lower = rawVal.toLowerCase();
          if (lower === "true") normalized = true;
          else if (lower === "false") normalized = false;
          else normalized = !!rawVal;
        } else {
          normalized = !!rawVal;
        }

        sel.value = normalized ? "True" : "False";
        sel.dispatchEvent(new Event("change", { bubbles: true }));
      });

    // Populate toggle checkboxes
    document
      .querySelectorAll("#mapping-form input[data-map-toggle]")
      .forEach((cb) => {
        const key = cb.name;
        let rawVal;
        if (cloneFrom && cloneFrom.settings && key in cloneFrom.settings) {
          rawVal = cloneFrom.settings[key];
        } else if (isEdit && mapping?.settings && key in mapping.settings) {
          rawVal = mapping.settings[key];
        } else {
          rawVal = DEFAULT_MAPPING_SETTINGS[key];
        }
        if (typeof rawVal === "string") rawVal = rawVal.toLowerCase() === "true";
        cb.checked = !!rawVal;
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

    if (modal._outsideClickHandler) {
      modal.removeEventListener("mousedown", modal._outsideClickHandler);
    }
    modal._outsideClickHandler = function (evt) {
      const contentEl = modal.querySelector(".modal-content");
      if (contentEl && !contentEl.contains(evt.target)) {
        maybeCloseMappingModal();
      }
    };
    modal.addEventListener("mousedown", modal._outsideClickHandler);
  }

  window.openMappingModal = openMappingModal;

  // ─── Message Features Modal ──────────────────────────────────────────
  const msgFeaturesModal = document.getElementById("msg-features-modal");
  const msgFeaturesOpen  = document.getElementById("msg-features-open");
  const msgFeaturesClose = document.getElementById("msg-features-close");
  const msgFeaturesPreview = document.getElementById("msg-features-preview-footer");
  const msgFeatureToggles = msgFeaturesModal
    ? msgFeaturesModal.querySelectorAll(".msg-feature-toggle[data-key]")
    : [];

  function openMsgFeatures() {
    if (!msgFeaturesModal) return;
    // Sync toggle state from mapping form hidden values
    for (const toggle of msgFeatureToggles) {
      const key = toggle.dataset.key;
      const formEl = document.getElementById(`map_${key}`);
      const checked = formEl ? formEl.value === "True" : false;
      toggle.querySelector("input").checked = checked;
    }
    updateMsgFeaturesPreview();
    msgFeaturesModal.classList.add("show");
    msgFeaturesModal.setAttribute("aria-hidden", "false");
  }

  function closeMsgFeatures() {
    if (!msgFeaturesModal) return;
    msgFeaturesModal.classList.remove("show");
    msgFeaturesModal.setAttribute("aria-hidden", "true");
  }

  function getMsgFeatureState(key) {
    const toggle = msgFeaturesModal?.querySelector(`.msg-feature-toggle[data-key="${key}"] input`);
    return toggle?.checked || false;
  }

  function updateMsgFeaturesPreview() {
    if (!msgFeaturesPreview) return;
    const box = msgFeaturesPreview.closest(".msg-features-preview-box");
    const replyEl = document.getElementById("msg-features-preview-reply");
    const contentEl = document.getElementById("msg-features-preview-content");
    const usernameEl = box?.querySelector(".msg-preview-username");
    const avatarEl = box?.querySelector(".msg-preview-avatar");
    const isAnon = getMsgFeatureState("ANONYMIZE_USERS");
    const noEveryone = getMsgFeatureState("DISABLE_EVERYONE_MENTIONS");
    const noRoles = getMsgFeatureState("DISABLE_ROLE_MENTIONS");

    // Reply
    if (replyEl) {
      if (getMsgFeatureState("TAG_REPLY_MSG")) {
        replyEl.innerHTML = `<span class="msg-reply-icon">↩</span> In reply to: <a class="msg-reply-link">Copycord is the best!</a>`;
      } else {
        replyEl.innerHTML = "";
      }
    }

    // Anonymize
    if (usernameEl) usernameEl.textContent = isAnon ? "SwiftFox123" : "Copycord";
    if (avatarEl) {
      if (isAnon) {
        avatarEl.src = "https://picsum.photos/seed/preview42/200";
        avatarEl.onerror = () => { avatarEl.src = "/static/logo.png"; avatarEl.onerror = null; };
      } else {
        avatarEl.src = "/static/logo.png";
        avatarEl.onerror = null;
      }
    }

    // Message content with mention previews
    if (contentEl) {
      const everyone = noEveryone
        ? ` <span class="msg-mention-plain">@everyone</span>`
        : ` <span class="msg-mention">@everyone</span>`;
      const role = noRoles
        ? ` <span class="msg-mention-plain">@Moderator</span>`
        : ` <span class="msg-mention msg-mention--role">@Moderator</span>`;
      contentEl.innerHTML = `Hey${everyone} check this out${role}`;
    }

    // Footer
    const parts = [];
    if (getMsgFeatureState("APPEND_TIMESTAMP")) parts.push("Oct 15, 2025 3:42 PM");
    if (getMsgFeatureState("APPEND_AUTHOR")) {
      const name = isAnon ? "SwiftFox123" : "Copycord";
      parts.push(`<strong>${name}</strong>`);
    }
    msgFeaturesPreview.innerHTML = parts.length ? parts.join(" · ") : "";
  }

  // Toggle change handler — sync to hidden form field
  for (const toggle of msgFeatureToggles) {
    const input = toggle.querySelector("input");
    if (!input) continue;
    input.addEventListener("change", () => {
      const key = toggle.dataset.key;
      const formEl = document.getElementById(`map_${key}`);
      if (formEl) formEl.value = input.checked ? "True" : "False";
      updateMsgFeaturesPreview();
    });
  }

  if (msgFeaturesOpen)  msgFeaturesOpen.addEventListener("click", openMsgFeatures);
  if (msgFeaturesClose) msgFeaturesClose.addEventListener("click", closeMsgFeatures);
  if (msgFeaturesModal) {
    msgFeaturesModal.querySelector(".modal-backdrop")?.addEventListener("click", closeMsgFeatures);
  }

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

    if (!isEdit) {
      triggerConfetti();
    }

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

    syncBodyScrollLock();

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

    syncBodyScrollLock();

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

    if (!modal.classList.contains("show") || !mappingFormIsDirty()) {
      closeMappingModal();
      return;
    }

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
        "#backup-token-modal.show, " +
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
        '<tr><td colspan="4" class="text-center small text-danger">Failed to fetch channels/categories from Discord. Try again later.</td></tr>';
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
      "#filters-modal.show, #mapping-modal.show, #log-modal.show, #backup-token-modal.show, #confirm-modal.show"
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
    cBody.innerHTML = body || "Are you sure?";
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

        const statusRaw = (m.status || "active").toLowerCase();
        const isPaused = statusRaw === "paused";

        const statusIcon = isPaused ? ICONS.play : ICONS.pause;
        const statusLabel = isPaused
          ? "Resume cloning for this mapping"
          : "Pause cloning for this mapping";

        const statusBadgeHtml = isPaused
          ? `<span class="guild-card-paused-icon" title="Paused"><svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M10.268 21a2 2 0 0 0 3.464 0"/><path d="M17 17H4a1 1 0 0 1-.74-1.673C4.59 13.956 6 12.499 6 8a6 6 0 0 1 .258-1.742"/><path d="m2 2 20 20"/><path d="M8.668 3.01A6 6 0 0 1 18 8c0 2.687.77 4.653 1.707 6.05"/></svg></span>`
          : "";

        const hostName = escapeHtml(m.original_guild_name || `ID: ${m.original_guild_id}`);

        return `
      <div class="guild-card ${isPaused ? "is-paused" : "is-active"}"
           data-id="${m.mapping_id}"
           data-status="${statusRaw}">

        <div class="guild-card-inner">
          <div class="guild-card-header">
            <img src="${iconSrc}" alt="" class="guild-card-avatar">
            <div class="guild-card-info">
              <div class="guild-card-name-title" title="${escapeHtml(m.mapping_name || "")}">
                ${escapeHtml(m.mapping_name || "")}
              </div>
              <div class="guild-card-route">${hostName}</div>
            </div>
            ${statusBadgeHtml}
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

            <button class="btn-icon mapping-status-btn ${isPaused ? "is-paused" : "is-active"}"
                    data-id="${m.mapping_id}"
                    type="button"
                    aria-pressed="${isPaused ? "true" : "false"}"
                    aria-label="${statusLabel}"
                    title="${statusLabel}">
              ${statusIcon}
            </button>

            <button class="btn-icon clone-mapping-btn"
                    data-id="${m.mapping_id}"
                    aria-label="Clone mapping"
                    title="Clone mapping">
              ${ICONS.clone}
            </button>

            <span class="guild-card-action-spacer"></span>

            <button class="btn-icon delete-mapping-btn"
                    type="button"
                    data-action="delete"
                    data-id="${m.mapping_id}"
                    aria-label="Delete mapping"
                    title="Delete mapping">
              ${ICONS.trash}
            </button>
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
          <div class="new-card-plus"><svg xmlns="http://www.w3.org/2000/svg" width="40" height="40" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"><path d="m11 19-1.106-.552a2 2 0 0 0-1.788 0l-3.659 1.83A1 1 0 0 1 3 19.381V6.618a1 1 0 0 1 .553-.894l4.553-2.277a2 2 0 0 1 1.788 0l4.212 2.106a2 2 0 0 0 1.788 0l3.659-1.83A1 1 0 0 1 21 4.619V12"/><path d="M15 5.764V12"/><path d="M18 15v6"/><path d="M21 18h-6"/><path d="M9 3.236v15"/></svg></div>
        </div>
      </button>
    `;

    listEl.innerHTML = newCardHtml + mappingCardsHtml;

    listEl.querySelectorAll(".edit-mapping-btn").forEach((btn) => {
      btn.addEventListener("click", (ev) => {
        const mapId = ev.currentTarget.getAttribute("data-id");
        const mapping = findMappingById(mapId);
        openMappingModal(mapping);
      });
    });

    listEl.querySelectorAll(".clone-mapping-btn").forEach((btn) => {
      btn.addEventListener("click", (ev) => {
        const mapId = ev.currentTarget.getAttribute("data-id");
        const mapping = findMappingById(mapId);
        if (!mapping) return;

        openMappingModal(mapping, { mode: "clone" });
      });
    });

    listEl.querySelectorAll(".mapping-filters-btn").forEach((btn) => {
      btn.addEventListener("click", async (ev) => {
        const mapId = ev.currentTarget.getAttribute("data-id");
        const mapping = findMappingById(mapId);
        await openFiltersModal(mapping);
      });
    });

    listEl.querySelectorAll(".mapping-status-btn").forEach((btn) => {
      btn.addEventListener("click", async (ev) => {
        const mapId = ev.currentTarget.getAttribute("data-id");
        if (!mapId) return;

        const thisBtn = ev.currentTarget;
        thisBtn.disabled = true;

        try {
          const res = await fetch(
            `/api/guild-mappings/${encodeURIComponent(mapId)}/toggle-status`,
            {
              method: "POST",
              credentials: "same-origin",
            }
          );

          if (!res.ok) {
            let msg = `Failed to update status (${res.status})`;
            try {
              const errJson = await res.json();
              if (errJson && errJson.error) msg = errJson.error;
            } catch {}
            showToast(msg, { type: "error", timeout: 7000 });
            return;
          }

          const data = await res.json();
          const newStatus = (data.status || "").toLowerCase();

          const mapping = findMappingById(mapId);
          if (mapping) {
            mapping.status = newStatus;
          }

          renderGuildMappings();

          showToast(
            newStatus === "paused"
              ? "Mapping paused. It will no longer clone until resumed."
              : "Mapping resumed. Cloning is active again.",
            { type: "success" }
          );

          validateConfigAndToggle?.({ decorate: false });
        } catch {
          showToast("Network error while updating mapping status.", {
            type: "error",
          });
        } finally {
          thisBtn.disabled = false;
        }
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
    const chanBl = document.getElementById("channel_name_blacklist");

    if (wlCats) wlCats.value = "";
    if (wlCh) wlCh.value = "";
    if (exCats) exCats.value = "";
    if (exCh) exCh.value = "";
    if (bw) bw.value = "";
    if (hiddenBlockedRoles) hiddenBlockedRoles.value = "";
    if (wlUsers) wlUsers.value = "";
    if (blUsers) blUsers.value = "";
    if (chanBl) chanBl.value = "";

    if (CHIPS.wl_categories) CHIPS.wl_categories.set([]);
    if (CHIPS.wl_channels) CHIPS.wl_channels.set([]);
    if (CHIPS.ex_categories) CHIPS.ex_categories.set([]);
    if (CHIPS.ex_channels) CHIPS.ex_channels.set([]);
    if (CHIPS.blocked_words) CHIPS.blocked_words.set([]);
    if (CHIPS.channel_name_blacklist) CHIPS.channel_name_blacklist.set([]);
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
        if (Array.isArray(data.channel_name_blacklist) && CHIPS.channel_name_blacklist) {
          CHIPS.channel_name_blacklist.set(data.channel_name_blacklist);
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
        const name = (
          field.querySelector(".mapping-toggle-name")?.textContent || ""
        ).toLowerCase();
        const desc = (
          field.querySelector(".mapping-toggle-desc")?.textContent || ""
        ).toLowerCase();

        const match = !q || key.includes(q) || name.includes(q) || desc.includes(q);
        field.style.display = match ? "" : "none";
      });

      // Hide sections with no matches, auto-expand when searching
      let anyMatchTotal = false;
      document.querySelectorAll("#mapping-form .mapping-settings-section").forEach((section) => {
        const rows = section.querySelectorAll(".mapping-setting-field");
        const anyVisible = Array.from(rows).some(r => r.style.display !== "none");
        section.style.display = anyVisible ? "" : "none";
        if (anyVisible) anyMatchTotal = true;
        if (q && anyVisible) section.open = true;
        if (!q) section.open = false;
      });

      // Show/hide no results message
      let noResults = document.getElementById("mapping-no-results");
      if (!noResults) {
        noResults = document.createElement("div");
        noResults.id = "mapping-no-results";
        noResults.className = "mapping-no-results";
        noResults.textContent = "No settings match your search";
        const toolbar = document.querySelector(".mapping-toolbar");
        if (toolbar) toolbar.after(noResults);
      }
      noResults.style.display = (q && !anyMatchTotal) ? "" : "none";
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

    checkSavedTokensOnLoad();
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
              blockStartForBadTokens = false;
              ["CLIENT_TOKEN", "SERVER_TOKEN"].forEach((id) => {
                const el = document.getElementById(id);
                if (el) el.classList.remove("is-invalid");
              });
              updateStartButtonOnly();
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
      let wsReady = false;

      // Ignore replayed history — only process live events after a short delay
      setTimeout(() => { wsReady = true; }, 1500);

      ws.onmessage = (ev) => {
        try {
          const msg = JSON.parse(ev.data);
          if (msg.kind === "agent" && msg.type === "status") {
            const prefix = msg.role === "server" ? "server" : "client";
            renderStatusRow(prefix, msg.data || {});
          }
          if (msg.kind === "proxy_test" && msg.payload && wsReady) {
            handleProxyTestEvent(msg.payload);
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
          fetchAndRenderStatus();
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

    // ─── Client Proxy Rotation ──────────────────────────────────────────────
    const srvProxyCard    = document.getElementById("srv-proxy-card");
    const srvProxyToggle  = document.getElementById("srv-proxy-toggle");
    const srvProxyChips   = document.getElementById("srv-proxy-chips");
    const srvProxyInput   = document.getElementById("srv-proxy-chips-input");
    const srvProxyStatus  = document.getElementById("srv-proxy-status");
    const srvProxyClear   = document.getElementById("srv-proxy-clear");
    const srvProxyTest    = document.getElementById("srv-proxy-test");

    const srvRotationToggle   = document.getElementById("srv-proxy-rotation-toggle");
    const srvRotationControls = document.getElementById("srv-proxy-rotation-controls");
    const srvIntervalInput    = document.getElementById("srv-proxy-interval");
    const srvIntervalUnit     = document.getElementById("srv-proxy-interval-unit");

    // ── Proxy chip helpers ──

    function getProxyChipValues() {
      if (!srvProxyChips) return [];
      return Array.from(srvProxyChips.querySelectorAll(".srv-proxy-chip-item"))
        .map(el => el.dataset.proxy)
        .filter(Boolean);
    }

    function createProxyChip(proxy) {
      const el = document.createElement("span");
      el.className = "srv-proxy-chip-item";
      el.dataset.proxy = proxy;
      el.innerHTML = `<span class="srv-proxy-chip-text">${escapeHtml(proxy)}</span><span class="srv-proxy-chip-x">×</span>`;
      el.querySelector(".srv-proxy-chip-x").addEventListener("click", () => {
        el.remove();
        refreshStatusChip();
        scheduleProxySave();
      });
      return el;
    }

    function escapeHtml(s) {
      const d = document.createElement("div");
      d.textContent = s;
      return d.innerHTML;
    }

    function addProxies(text) {
      if (!srvProxyChips || !text) return;
      const existing = new Set(getProxyChipValues());
      const lines = text.split(/[\n\r,]+/).map(l => l.trim()).filter(Boolean);
      let added = false;
      for (const line of lines) {
        if (!existing.has(line)) {
          existing.add(line);
          srvProxyChips.insertBefore(createProxyChip(line), srvProxyInput);
          added = true;
        }
      }
      if (added) {
        refreshStatusChip();
        scheduleProxySave();
      }
    }

    function setProxyChips(proxies) {
      if (!srvProxyChips) return;
      srvProxyChips.querySelectorAll(".srv-proxy-chip-item").forEach(el => el.remove());
      for (const p of proxies) {
        srvProxyChips.insertBefore(createProxyChip(p), srvProxyInput);
      }
      refreshStatusChip();
    }

    function pluralProxy(n) {
      return n === 1 ? "1 proxy" : `${n} proxies`;
    }

    function refreshStatusChip() {
      updateSrvProxyChip(srvProxyToggle?.checked, getProxyChipValues().length);
    }

    function updateSrvProxyChip(enabled, count) {
      if (!srvProxyStatus) return;
      if (enabled && count > 0) {
        srvProxyStatus.textContent = `Enabled · ${pluralProxy(count)}`;
        srvProxyStatus.className = "srv-proxy-chip is-on";
      } else if (enabled) {
        srvProxyStatus.textContent = "Enabled · no proxies";
        srvProxyStatus.className = "srv-proxy-chip is-warn";
      } else {
        srvProxyStatus.textContent = "Disabled";
        srvProxyStatus.className = "srv-proxy-chip";
      }
    }

    function updateRotationControls(intervalSec) {
      if (!srvRotationControls || !srvRotationToggle) return;
      const on = intervalSec > 0;
      srvRotationToggle.checked = on;
      srvRotationControls.classList.toggle("is-hidden", !on);
      if (on && srvIntervalInput && srvIntervalUnit) {
        if (intervalSec >= 3600 && intervalSec % 3600 === 0) {
          srvIntervalInput.value = intervalSec / 3600;
          srvIntervalUnit.value = "3600";
        } else {
          srvIntervalInput.value = Math.max(1, Math.round(intervalSec / 60));
          srvIntervalUnit.value = "60";
        }
      }
    }

    function getRotationIntervalSec() {
      if (!srvRotationToggle || !srvRotationToggle.checked) return 0;
      if (!srvIntervalInput || !srvIntervalUnit) return 0;
      const val = Math.max(1, parseInt(srvIntervalInput.value, 10) || 1);
      const multiplier = parseInt(srvIntervalUnit.value, 10) || 60;
      return val * multiplier;
    }

    // ── Input handling ──

    if (srvProxyInput) {
      srvProxyInput.addEventListener("keydown", (e) => {
        if (e.key === "Enter") {
          e.preventDefault();
          const val = srvProxyInput.value.trim();
          if (val) {
            addProxies(val);
            srvProxyInput.value = "";
          }
        }
        // Backspace on empty input removes last chip
        if (e.key === "Backspace" && !srvProxyInput.value) {
          const chips = srvProxyChips.querySelectorAll(".srv-proxy-chip-item");
          if (chips.length) {
            chips[chips.length - 1].remove();
            refreshStatusChip();
            scheduleProxySave();
          }
        }
      });
      srvProxyInput.addEventListener("paste", (e) => {
        e.preventDefault();
        const text = (e.clipboardData || window.clipboardData).getData("text");
        if (text) {
          addProxies(text);
          srvProxyInput.value = "";
        }
      });
    }

    // Click on container focuses input
    if (srvProxyChips && srvProxyInput) {
      srvProxyChips.addEventListener("click", (e) => {
        if (e.target === srvProxyChips) srvProxyInput.focus();
      });
    }

    // ── Auto-save (debounced) ──

    let _proxySaveTimer = null;
    function scheduleProxySave() {
      clearTimeout(_proxySaveTimer);
      _proxySaveTimer = setTimeout(saveProxySettings, 400);
    }

    async function saveProxySettings() {
      const lines = getProxyChipValues();
      const intervalSec = getRotationIntervalSec();
      try {
        const [rProxies, rInterval] = await Promise.all([
          fetch("/api/server/proxies", {
            method: "PUT",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ proxies: lines }),
          }),
          fetch("/api/server/proxies/rotation-interval", {
            method: "PUT",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ interval: intervalSec }),
          }),
        ]);
        const jProxies = await rProxies.json();
        const jInterval = await rInterval.json();
        if (!jProxies.ok) showToast(jProxies.error || "Failed to save proxies", { type: "error" });
        if (!jInterval.ok) showToast(jInterval.error || "Failed to save rotation", { type: "error" });
        updateSrvProxyChip(srvProxyToggle?.checked, lines.length);
      } catch (e) {
        console.error(e);
        showToast("Failed to save proxy settings", { type: "error" });
      }
    }

    // ── API calls ──

    async function loadSrvProxies() {
      if (!srvProxyCard) return;
      try {
        const r = await fetch("/api/server/proxies");
        const j = await r.json();
        if (j.ok) {
          setProxyChips(j.proxies || []);
          if (srvProxyToggle) srvProxyToggle.checked = !!j.enabled;
          updateSrvProxyChip(!!j.enabled, (j.proxies || []).length);
          updateRotationControls(j.rotation_interval || 0);
        }
      } catch (e) {
        console.error("Failed to load client proxies:", e);
      }
    }

    async function toggleSrvProxies(enabled) {
      try {
        const r = await fetch("/api/server/proxies/toggle", {
          method: "PUT",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ enabled }),
        });
        const j = await r.json();
        if (j.ok) {
          const count = getProxyChipValues().length;
          showToast(
            enabled
              ? `Client proxies enabled (${pluralProxy(count)})`
              : "Client proxies disabled",
            { type: "success" }
          );
          updateSrvProxyChip(enabled, count);
        } else {
          showToast(j.error || "Failed to toggle", { type: "error" });
        }
      } catch (e) {
        console.error(e);
        showToast("Failed to toggle client proxies", { type: "error" });
      }
    }

    function toggleRotation(enabled) {
      if (srvRotationControls) {
        srvRotationControls.classList.toggle("is-hidden", !enabled);
      }
      scheduleProxySave();
    }

    let SLOW_THRESHOLD_MS = 3000;
    const proxyProgress     = document.getElementById("srv-proxy-progress");
    const proxyProgressFill = document.getElementById("srv-proxy-progress-fill");
    const proxyProgressText = document.getElementById("srv-proxy-progress-text");

    function maskProxy(p) {
      return p.includes("@") ? p.replace(/\/\/[^@]+@/, "//***@") : p;
    }

    function buildProxyResultRow(r, category) {
      const masked = escapeHtml(maskProxy(r.proxy));
      const color = category === "failed" ? "#ff6b6b" : category === "slow" ? "#ff9800" : "#4caf50";
      const icon = category === "failed" ? "✗" : category === "slow" ? "⚠" : "✓";
      const detail = r.ok ? `${r.ms}ms` : (r.error || "failed");
      return `<div style="display:flex;align-items:center;gap:8px;padding:4px 0;font-size:12px;font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace">`
        + `<span style="color:${color};flex-shrink:0">${icon}</span>`
        + `<span style="color:var(--text);min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;flex:1">${masked}</span>`
        + `<span style="color:${color};flex-shrink:0;font-size:11px">${escapeHtml(detail)}</span>`
        + `</div>`;
    }

    function showProxyProgress(current, total) {
      if (!proxyProgress) return;
      proxyProgress.style.display = "flex";
      const pct = total > 0 ? Math.round((current / total) * 100) : 0;
      if (proxyProgressFill) proxyProgressFill.style.width = pct + "%";
      if (proxyProgressText) proxyProgressText.textContent = `${current} / ${total}`;
    }

    function hideProxyProgress() {
      if (proxyProgress) proxyProgress.style.display = "none";
      if (proxyProgressFill) proxyProgressFill.style.width = "0%";
    }

    function applyBatchResultsToChips(results) {
      const chipEls = srvProxyChips.querySelectorAll(".srv-proxy-chip-item");
      for (const result of results) {
        const chip = Array.from(chipEls).find(el => el.dataset.proxy === result.proxy);
        if (!chip) continue;
        chip.classList.remove("test-loading");
        const msEl = chip.querySelector(".srv-proxy-chip-ms");
        if (!result.ok) {
          chip.classList.add("test-fail");
          if (msEl) msEl.textContent = result.error || "failed";
        } else if (result.ms >= SLOW_THRESHOLD_MS) {
          chip.classList.add("test-slow");
          if (msEl) msEl.textContent = `${result.ms}ms`;
        } else {
          chip.classList.add("test-pass");
          if (msEl) msEl.textContent = `${result.ms}ms`;
        }
      }
    }

    function showTestResultsModal(allResults) {
      const good = [], slow = [], failed = [];
      for (const r of allResults) {
        if (!r.ok) failed.push(r);
        else if (r.ms >= SLOW_THRESHOLD_MS) slow.push(r);
        else good.push(r);
      }

      const hasIssues = failed.length > 0 || slow.length > 0;
      if (!hasIssues) {
        showToast(`All ${good.length} proxies passed`, { type: "success" });
        return;
      }

      const stats = `
        <div style="display:flex;gap:10px;margin-bottom:16px;flex-wrap:wrap">
          <div style="flex:1;min-width:70px;text-align:center;padding:10px 8px;border-radius:10px;background:rgba(76,175,80,0.1)">
            <div style="font-size:22px;font-weight:700;color:#4caf50">${good.length}</div>
            <div style="font-size:11px;color:var(--muted)">healthy</div>
          </div>
          ${slow.length ? `<div style="flex:1;min-width:70px;text-align:center;padding:10px 8px;border-radius:10px;background:rgba(255,152,0,0.1)">
            <div style="font-size:22px;font-weight:700;color:#ff9800">${slow.length}</div>
            <div style="font-size:11px;color:var(--muted)">slow</div>
          </div>` : ""}
          ${failed.length ? `<div style="flex:1;min-width:70px;text-align:center;padding:10px 8px;border-radius:10px;background:rgba(255,80,80,0.1)">
            <div style="font-size:22px;font-weight:700;color:#ff6b6b">${failed.length}</div>
            <div style="font-size:11px;color:var(--muted)">failed</div>
          </div>` : ""}
        </div>`;

      let listHtml = "";
      if (failed.length) {
        listHtml += `<div style="font-size:11px;font-weight:600;color:#ff6b6b;text-transform:uppercase;letter-spacing:0.5px;margin-bottom:4px">Failed</div>`;
        listHtml += failed.map(r => buildProxyResultRow(r, "failed")).join("");
      }
      if (slow.length) {
        listHtml += `<div style="font-size:11px;font-weight:600;color:#ff9800;text-transform:uppercase;letter-spacing:0.5px;margin:${failed.length ? "10px" : "0"} 0 4px">Slow (&gt;${SLOW_THRESHOLD_MS / 1000}s)</div>`;
        listHtml += slow.map(r => buildProxyResultRow(r, "slow")).join("");
      }

      const resultsBox = `
        <div class="srv-proxy-results-scroll">
          ${listHtml}
        </div>`;

      const checkboxes = `
        <div style="display:flex;flex-direction:column;gap:8px">
          ${failed.length ? `<label class="srv-proxy-check" style="cursor:pointer">
            <input type="checkbox" id="ptr-remove-failed" checked />
            <span>Remove <strong>${failed.length}</strong> failed proxy${failed.length !== 1 ? "es" : ""}</span>
          </label>` : ""}
          ${slow.length ? `<label class="srv-proxy-check" style="cursor:pointer">
            <input type="checkbox" id="ptr-remove-slow" />
            <span>Remove <strong>${slow.length}</strong> slow proxy${slow.length !== 1 ? "es" : ""}</span>
          </label>` : ""}
        </div>`;

      openConfirm({
        title: "Proxy Test Results",
        body: stats + resultsBox + checkboxes,
        confirmText: "Remove Selected",
        confirmClass: "btn-ghost-red",
        showCancel: true,
        onConfirm: () => {
          const removeFailed = document.getElementById("ptr-remove-failed")?.checked;
          const removeSlow = document.getElementById("ptr-remove-slow")?.checked;
          const toRemove = new Set();
          if (removeFailed) failed.forEach(r => toRemove.add(r.proxy));
          if (removeSlow) slow.forEach(r => toRemove.add(r.proxy));
          if (toRemove.size) {
            const chipEls = srvProxyChips.querySelectorAll(".srv-proxy-chip-item");
            chipEls.forEach(el => { if (toRemove.has(el.dataset.proxy)) el.remove(); });
            refreshStatusChip();
            saveProxySettings();
            showToast(`Removed ${toRemove.size} proxy${toRemove.size !== 1 ? "es" : ""}`, { type: "success" });
          }
        },
      });
    }

    // ── Proxy test streaming state ──
    let _proxyTestRunning = false;
    let _proxyTestResults = [];

    function setTestButton(mode) {
      if (!srvProxyTest) return;
      if (mode === "running") {
        _proxyTestRunning = true;
        srvProxyTest.disabled = false;
        srvProxyTest.textContent = "Stop";
        srvProxyTest.classList.add("btn-ghost-red");
        srvProxyTest.classList.remove("btn-outline");
      } else {
        _proxyTestRunning = false;
        srvProxyTest.disabled = false;
        srvProxyTest.textContent = "Test All";
        srvProxyTest.classList.remove("btn-ghost-red");
        srvProxyTest.classList.add("btn-outline");
      }
    }

    function handleProxyTestEvent(payload) {
      if (payload.type === "started") {
        _proxyTestResults = [];
        setTestButton("running");
        showProxyProgress(0, payload.total);
        const chipEls = srvProxyChips.querySelectorAll(".srv-proxy-chip-item");
        chipEls.forEach(el => {
          el.classList.remove("test-pass", "test-fail", "test-slow");
          el.classList.add("test-loading");
          const ms = el.querySelector(".srv-proxy-chip-ms");
          if (ms) ms.textContent = "testing…";
          else {
            const badge = document.createElement("span");
            badge.className = "srv-proxy-chip-ms";
            badge.textContent = "testing…";
            el.insertBefore(badge, el.querySelector(".srv-proxy-chip-x"));
          }
        });
      }

      if (payload.type === "progress") {
        showProxyProgress(payload.current, payload.total);
        if (payload.results) {
          _proxyTestResults.push(...payload.results);
          applyBatchResultsToChips(payload.results);
        }
      }

      if (payload.type === "complete") {
        setTestButton("idle");
        hideProxyProgress();
        const allResults = payload.results || _proxyTestResults;
        showTestResultsModal(allResults);
      }

      if (payload.type === "stopped") {
        setTestButton("idle");
        hideProxyProgress();
        // Clear loading state from untested chips
        srvProxyChips.querySelectorAll(".srv-proxy-chip-item.test-loading").forEach(el => {
          el.classList.remove("test-loading");
          const ms = el.querySelector(".srv-proxy-chip-ms");
          if (ms) ms.remove();
        });
        if (_proxyTestResults.length) {
          showTestResultsModal(_proxyTestResults);
        } else {
          showToast("Proxy test stopped", { type: "success" });
        }
      }
    }

    async function testAllProxies() {
      const proxies = getProxyChipValues();
      if (!proxies.length) { showToast("No proxies to test", { type: "error" }); return; }

      srvProxyTest.disabled = true;
      try {
        const r = await fetch("/api/server/proxies/test", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ proxies }),
        });
        const j = await r.json();
        if (!j.ok) {
          showToast(j.error || "Failed to start test", { type: "error" });
          setTestButton("idle");
        }
      } catch (e) {
        console.error(e);
        showToast("Failed to start proxy test", { type: "error" });
        setTestButton("idle");
      }
    }

    async function stopProxyTest() {
      srvProxyTest.disabled = true;
      try {
        await fetch("/api/server/proxies/test/stop", { method: "POST" });
      } catch (e) {
        console.error(e);
        setTestButton("idle");
        hideProxyProgress();
      }
    }

    // Check if a test is already running on page load
    async function checkProxyTestStatus() {
      try {
        const r = await fetch("/api/server/proxies/test/status");
        const j = await r.json();
        if (j.ok && j.running) {
          setTestButton("running");
          showProxyProgress(0, 0);
          if (proxyProgressText) proxyProgressText.textContent = "resuming…";
        }
      } catch {}
    }

    if (srvProxyTest) {
      srvProxyTest.addEventListener("click", () => {
        if (_proxyTestRunning) stopProxyTest();
        else testAllProxies();
      });
    }
    if (srvProxyClear)      srvProxyClear.addEventListener("click", () => {
      if (!getProxyChipValues().length) return;
      openConfirm({
        title: "Clear all proxies?",
        body: "This will remove all proxies from the list.",
        confirmText: "Clear",
        confirmClass: "btn-ghost-red",
        showCancel: true,
        onConfirm: () => { setProxyChips([]); saveProxySettings(); },
      });
    });
    if (srvProxyToggle)     srvProxyToggle.addEventListener("change", () => toggleSrvProxies(srvProxyToggle.checked));
    if (srvRotationToggle)  srvRotationToggle.addEventListener("change", () => toggleRotation(srvRotationToggle.checked));
    if (srvIntervalInput) {
      srvIntervalInput.addEventListener("input", () => {
        const v = parseInt(srvIntervalInput.value, 10);
        if (v < 1 && srvIntervalInput.value !== "") srvIntervalInput.value = 1;
      });
      srvIntervalInput.addEventListener("change", () => {
        if (!srvIntervalInput.value || parseInt(srvIntervalInput.value, 10) < 1) srvIntervalInput.value = 1;
        scheduleProxySave();
      });
    }
    if (srvIntervalUnit)    srvIntervalUnit.addEventListener("change", () => scheduleProxySave());

    // Persist open/closed state
    if (srvProxyCard) {
      if (localStorage.getItem("cpc.proxy-card-open") === "1") srvProxyCard.open = true;
      srvProxyCard.addEventListener("toggle", () => {
        localStorage.setItem("cpc.proxy-card-open", srvProxyCard.open ? "1" : "0");
      });
    }

    // ─── Proxy Settings Modal ──────────────────────────────────────────────
    const psModal     = document.getElementById("proxy-settings-modal");
    const psClose     = document.getElementById("proxy-settings-close");
    const psBtn       = document.getElementById("srv-proxy-settings-btn");
    const psFields    = {
      PROXY_SUSPEND_DURATION: document.getElementById("ps-suspend-duration"),
      PROXY_TEST_BATCH_SIZE:  document.getElementById("ps-test-batch"),
      PROXY_SLOW_THRESHOLD:   document.getElementById("ps-slow-threshold"),
    };

    let _psLoaded = false;

    function openProxySettings() {
      if (!psModal) return;
      psModal.classList.add("show");
      psModal.setAttribute("aria-hidden", "false");
      document.body.classList.add("body-lock-scroll");
      if (!_psLoaded) loadProxySettings();
    }

    function closeProxySettings() {
      if (!psModal) return;
      psModal.classList.remove("show");
      psModal.setAttribute("aria-hidden", "true");
      const anyOther = document.querySelector(
        "#confirm-modal.show, #log-modal.show, #mapping-modal.show, #filters-modal.show"
      );
      if (!anyOther) document.body.classList.remove("body-lock-scroll");
    }

    async function loadProxySettings() {
      try {
        const r = await fetch("/api/server/proxies/settings");
        const j = await r.json();
        if (j.ok && j.settings) {
          for (const [key, el] of Object.entries(psFields)) {
            if (el && j.settings[key] !== undefined) el.value = j.settings[key];
          }
          // Sync slow threshold to test UI
          if (j.settings.PROXY_SLOW_THRESHOLD) {
            SLOW_THRESHOLD_MS = j.settings.PROXY_SLOW_THRESHOLD * 1000;
          }
          _psLoaded = true;
        }
      } catch (e) {
        console.error("Failed to load proxy settings:", e);
      }
    }

    let _psSaveTimer = null;
    function scheduleProxySettingsSave() {
      clearTimeout(_psSaveTimer);
      _psSaveTimer = setTimeout(saveProxySettings_modal, 600);
    }

    async function saveProxySettings_modal() {
      const settings = {};
      for (const [key, el] of Object.entries(psFields)) {
        if (el) settings[key] = parseInt(el.value, 10) || 1;
      }
      // Sync slow threshold immediately
      if (settings.PROXY_SLOW_THRESHOLD) {
        SLOW_THRESHOLD_MS = settings.PROXY_SLOW_THRESHOLD * 1000;
      }
      try {
        const r = await fetch("/api/server/proxies/settings", {
          method: "PUT",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ settings }),
        });
        const j = await r.json();
        if (!j.ok) showToast(j.error || "Failed to save settings", { type: "error" });
      } catch (e) {
        console.error(e);
        showToast("Failed to save proxy settings", { type: "error" });
      }
    }

    if (psBtn) psBtn.addEventListener("click", openProxySettings);
    if (psClose) psClose.addEventListener("click", closeProxySettings);
    if (psModal) {
      psModal.querySelector(".modal-backdrop")?.addEventListener("click", closeProxySettings);
    }
    for (const el of Object.values(psFields)) {
      if (el) el.addEventListener("change", scheduleProxySettingsSave);
    }

    const PS_DEFAULTS = {
      PROXY_SUSPEND_DURATION: 300,
      PROXY_TEST_BATCH_SIZE: 50,
      PROXY_SLOW_THRESHOLD: 3,
    };
    const psResetBtn = document.getElementById("ps-reset-defaults");
    if (psResetBtn) {
      psResetBtn.addEventListener("click", () => {
        for (const [key, el] of Object.entries(psFields)) {
          if (el && PS_DEFAULTS[key] !== undefined) el.value = PS_DEFAULTS[key];
        }
        SLOW_THRESHOLD_MS = PS_DEFAULTS.PROXY_SLOW_THRESHOLD * 1000;
        saveProxySettings_modal();
        showToast("Proxy settings reset to defaults", { type: "success" });
      });
    }

    loadSrvProxies();
    checkProxyTestStatus();
    loadProxySettings();

    // ─── Sync Timing Settings ─────────────────────────────────────────────
    const syncStartupDelay = document.getElementById("sync-startup-delay");
    const syncGuildDelay   = document.getElementById("sync-guild-delay");
    const syncRandomize    = document.getElementById("sync-randomize");
    const syncCard         = document.getElementById("sync-settings-card");

    async function loadSyncSettings() {
      try {
        const r = await fetch("/api/sync/settings");
        const j = await r.json();
        if (j.ok && j.settings) {
          if (syncStartupDelay) syncStartupDelay.value = j.settings.SYNC_STARTUP_DELAY || 15;
          if (syncGuildDelay) syncGuildDelay.value = j.settings.SYNC_INTER_GUILD_DELAY || 3;
          if (syncRandomize) syncRandomize.checked = !!(j.settings.SYNC_RANDOMIZE_ORDER ?? 1);
        }
      } catch (e) { console.error("Failed to load sync settings:", e); }
    }

    let _syncSaveTimer = null;
    function scheduleSyncSave() {
      clearTimeout(_syncSaveTimer);
      _syncSaveTimer = setTimeout(async () => {
        const settings = {};
        if (syncStartupDelay) settings.SYNC_STARTUP_DELAY = parseInt(syncStartupDelay.value, 10) || 15;
        if (syncGuildDelay) settings.SYNC_INTER_GUILD_DELAY = parseInt(syncGuildDelay.value, 10) || 3;
        if (syncRandomize) settings.SYNC_RANDOMIZE_ORDER = syncRandomize.checked ? 1 : 0;
        try {
          await fetch("/api/sync/settings", {
            method: "PUT",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ settings }),
          });
        } catch (e) { console.error(e); }
      }, 600);
    }

    if (syncStartupDelay) syncStartupDelay.addEventListener("change", scheduleSyncSave);
    if (syncGuildDelay) syncGuildDelay.addEventListener("change", scheduleSyncSave);
    if (syncRandomize) syncRandomize.addEventListener("change", scheduleSyncSave);

    if (syncCard) {
      if (localStorage.getItem("cpc.sync-card-open") === "1") syncCard.open = true;
      syncCard.addEventListener("toggle", () => {
        localStorage.setItem("cpc.sync-card-open", syncCard.open ? "1" : "0");
      });
    }

    // Persist open/closed state for Global Config and Cloning Config
    ["global-config-card", "guild-config-card"].forEach(id => {
      const card = document.getElementById(id);
      if (!card) return;
      const key = `cpc.${id}-open`;
      const saved = localStorage.getItem(key);
      if (saved !== null) card.open = saved === "1";
      card.addEventListener("toggle", () => {
        localStorage.setItem(key, card.open ? "1" : "0");
      });
    });

    // ─── Notification Settings ──────────────────────────────────────────
    const notifCard       = document.getElementById("notifications-card");
    const notifWebhookUrl = document.getElementById("notif-webhook-url");
    const notifTestBtn    = document.getElementById("notif-test-btn");
    const notifToggles    = document.querySelectorAll(".notif-toggle input[data-event]");

    async function loadNotifSettings() {
      try {
        const r = await fetch("/api/notifications/settings");
        const j = await r.json();
        if (!j.ok) return;
        if (notifWebhookUrl) notifWebhookUrl.value = j.webhook_url || "";
        notifToggles.forEach(cb => {
          const key = cb.dataset.event;
          if (key in j.events) cb.checked = j.events[key];
        });
      } catch (e) { console.error("Failed to load notification settings:", e); }
    }

    let _notifSaveTimer = null;
    function scheduleNotifSave() {
      clearTimeout(_notifSaveTimer);
      _notifSaveTimer = setTimeout(async () => {
        const events = {};
        notifToggles.forEach(cb => { events[cb.dataset.event] = cb.checked; });
        try {
          await fetch("/api/notifications/settings", {
            method: "PUT",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
              webhook_url: notifWebhookUrl ? notifWebhookUrl.value.trim() : "",
              events,
            }),
          });
        } catch (e) { console.error(e); }
      }, 600);
    }

    if (notifWebhookUrl) notifWebhookUrl.addEventListener("input", scheduleNotifSave);
    notifToggles.forEach(cb => cb.addEventListener("change", scheduleNotifSave));

    if (notifTestBtn) {
      notifTestBtn.addEventListener("click", async () => {
        notifTestBtn.disabled = true;
        notifTestBtn.textContent = "Sending…";
        try {
          const r = await fetch("/api/notifications/test", { method: "POST" });
          const j = await r.json();
          if (j.ok) {
            showToast("Test notification sent!", { type: "success" });
          } else {
            showToast(j.error || "Failed to send test notification", { type: "error" });
          }
        } catch (e) {
          showToast("Network error sending test", { type: "error" });
        } finally {
          notifTestBtn.disabled = false;
          notifTestBtn.textContent = "Test";
        }
      });
    }

    if (notifCard) {
      const nKey = "cpc.notifications-card-open";
      const nSaved = localStorage.getItem(nKey);
      if (nSaved !== null) notifCard.open = nSaved === "1";
      notifCard.addEventListener("toggle", () => {
        localStorage.setItem(nKey, notifCard.open ? "1" : "0");
      });
    }

    loadSyncSettings();
    loadNotifSettings();
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

    const blockStart = (!ok || blockStartForBadTokens) && !running;

    btn.dataset.invalid = blockStart ? "1" : "0";

    if (blockStartForBadTokens) {
      btn.title =
        "Saved tokens are no longer valid. Please update SERVER_TOKEN and CLIENT_TOKEN to start.";
    } else if (!ok) {
      btn.title =
        "Provide SERVER_TOKEN and CLIENT_TOKEN to start.";
    } else {
      btn.title = "";
    }

    btn.disabled = !!toggleLocked || blockStart;
  }
})();
