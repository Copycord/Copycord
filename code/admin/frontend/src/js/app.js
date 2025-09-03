(() => {
  let uiSock = null;
  let toggleLocked = false;
  const RUNTIME_CACHE = {};
  const UPTIME_KEY = (role) => `cpc:uptime:${role}`;

  let lastFocusLog = null;
  let lastFocusConfirm = null;

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
      if (!document.getElementById("toast-root")) {
        const div = document.createElement("div");
        div.id = "toast-root";
        div.style.position = "fixed";
        div.style.top = "16px";
        div.style.right = "16px";
        div.style.zIndex = "10000";
        div.style.display = "flex";
        div.style.flexDirection = "column";
        div.style.gap = "8px";
        document.body.appendChild(div);
      }
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

  async function fetchAndRenderStatus() {
    try {
      const res = await fetch("/status", { credentials: "same-origin" });
      if (!res.ok) return;
      const data = await res.json();

      renderStatusRow("server", data.server || {});
      renderStatusRow("client", data.client || {});
      updateToggleButton(data);

      const running = getCurrentRunning(data);
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
      titleBar.className = "card-title-bar";
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
      btn.setAttribute("aria-expanded", collapsed ? "false" : "true");
      btn.setAttribute(
        "aria-label",
        collapsed ? "Expand panel" : "Collapse panel"
      );

      const ev = new CustomEvent("card-toggled", {
        detail: { collapsed },
        bubbles: true,
      });
      card.dispatchEvent(ev);
    }
  }

  class ChipsInput {
    /**
     * @param {HTMLElement} root .chips element
     * @param {HTMLInputElement} hidden hidden input to carry CSV to server
     */
    constructor(root, hidden) {
      this.root = root;
      this.entry = root.querySelector(".chip-input");
      this.hidden = hidden;
      this.values = [];

      this.root.addEventListener("click", (e) => {
        const chipEl = e.target.closest(".chip");
        if (chipEl) {
          const id = chipEl.dataset.id;
          if (id) this.remove(id);
        } else {
          this.entry.focus();
        }
      });

      this.entry.addEventListener("keydown", (e) => {
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
        const text = (e.clipboardData || navigator.clipboard).getData("text");
        if (text) {
          e.preventDefault();
          this.addFromText(text);
        }
      });
    }

    /** Accept a string; split on anything non-digit; add integers */
    addFromText(text) {
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
      let changed = false;
      for (const id of arr) {
        if (!this.values.includes(id)) {
          this.values.push(id);
          this.renderChip(id);
          changed = true;
        }
      }
      if (changed) this.syncHidden();
    }

    remove(id) {
      const ix = this.values.indexOf(id);
      if (ix >= 0) {
        this.values.splice(ix, 1);
        const el = this.root.querySelector(
          `.chip[data-id="${CSS.escape(id)}"]`
        );
        if (el) el.remove();
        this.syncHidden();
      }
    }

    renderChip(id) {
      const chip = document.createElement("span");
      chip.className = "chip";
      chip.dataset.id = id;
      chip.innerHTML = `<span class="chip-text">${escapeHtml(id)}</span>`;
      this.root.insertBefore(chip, this.entry);
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
      this.addMany((list || []).map(String));
      if (this.entry) this.entry.value = "";
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

  const CHIPS = {};
  const BASELINES = { cmd_users_csv: "", cfg: "", filters: "" };

  function initChips() {
    const defs = [
      ["wl_categories", "wl_categories"],
      ["wl_channels", "wl_channels"],
      ["ex_categories", "ex_categories"],
      ["ex_channels", "ex_channels"],
      ["cmd_users", "COMMAND_USERS"],
    ];

    for (const [dataKey, hiddenId] of defs) {
      const root = document.querySelector(`.chips[data-chips="${dataKey}"]`);
      const hidden = document.getElementById(hiddenId);
      if (!root || !hidden) continue;

      const inst = new ChipsInput(root, hidden);
      CHIPS[dataKey] = inst;

      inst.set(parseIdList(hidden.value));

      if (dataKey === "cmd_users") {
        BASELINES.cmd_users_csv = hidden.value;
      }

      const form = root.closest("form");
      if (form) {
        form.addEventListener("reset", () => {
          setTimeout(() => {
            if (dataKey === "cmd_users") {
              hidden.value = BASELINES.cmd_users_csv;
              inst.set(parseIdList(BASELINES.cmd_users_csv));
            } else {
              inst.set(parseIdList(hidden.value));
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

  const REQUIRED_KEYS = [
    "SERVER_TOKEN",
    "CLIENT_TOKEN",
    "HOST_GUILD_ID",
    "CLONE_GUILD_ID",
  ];
  let cfgValidated = false;

  function configState() {
    const get = (id) => (document.getElementById(id)?.value || "").trim();
    const vals = Object.fromEntries(REQUIRED_KEYS.map((k) => [k, get(k)]));
    const idsOK =
      /^\d+$/.test(vals.HOST_GUILD_ID) && /^\d+$/.test(vals.CLONE_GUILD_ID);
    const ok = !!(vals.SERVER_TOKEN && vals.CLIENT_TOKEN && idsOK);

    const missing = [];
    if (!vals.SERVER_TOKEN) missing.push("SERVER_TOKEN");
    if (!vals.CLIENT_TOKEN) missing.push("CLIENT_TOKEN");
    if (!/^\d+$/.test(vals.HOST_GUILD_ID)) missing.push("HOST_GUILD_ID");
    if (!/^\d+$/.test(vals.CLONE_GUILD_ID)) missing.push("CLONE_GUILD_ID");

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
      ? "Provide SERVER_TOKEN, CLIENT_TOKEN, HOST_GUILD_ID, CLONE_GUILD_ID to start."
      : "";
    btn.disabled = !!toggleLocked || blockStart;
  }

  document.addEventListener("DOMContentLoaded", () => {
    const cModal = document.getElementById("confirm-modal");
    const cTitle = document.getElementById("confirm-title");
    const cBody = document.getElementById("confirm-body");
    const cBtnOk = document.getElementById("confirm-okay");
    const cBtnX = document.getElementById("confirm-close");
    const cBtnCa = document.getElementById("confirm-cancel");
    const cBack = cModal ? cModal.querySelector(".modal-backdrop") : null;

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

    const cfgForm = document.getElementById("cfg-form");
    if (cfgForm)
      cfgForm.addEventListener("reset", () => {
        cfgValidated = false;
        setTimeout(() => validateConfigAndToggle({ decorate: false }), 0);
      });

    updateToggleButton({ server: {}, client: {} });

    startStatusPoll(4000);
    fetchAndRenderStatus();
    attachAdminBus();
    initSlideMenu();

    let confirmResolve = null,
      confirmReject = null,
      lastFocus = null;

    function openConfirm({
      title,
      body,
      confirmText = "OK",
      confirmClass = "btn-ghost",
      onConfirm,
      showCancel = true,
    }) {
      if (!cModal) return;
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
        if (showCancel) cBtnCa.removeAttribute("hidden");
        else cBtnCa.setAttribute("hidden", "");
      }

      cModal.classList.add("show");
      setInert(cModal, false);
      cModal.setAttribute("aria-hidden", "false");

      setTimeout(() => (cBtnOk || cModal).focus(), 0);
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
      if (lastFocusConfirm && typeof lastFocusConfirm.focus === "function") {
        try {
          lastFocusConfirm.focus();
        } catch {}
      }
      lastFocusConfirm = null;
      confirmResolve = null;
      confirmReject = null;
    }

    if (cBtnOk)
      cBtnOk.addEventListener(
        "click",
        () => confirmResolve && confirmResolve()
      );
    if (cBtnX)
      cBtnX.addEventListener("click", () => confirmReject && confirmReject());
    if (cBtnCa)
      cBtnCa.addEventListener("click", () => confirmReject && confirmReject());
    if (cBack)
      cBack.addEventListener("click", () => confirmReject && confirmReject());
    document.addEventListener("keydown", (e) => {
      if (cModal && cModal.classList.contains("show")) {
        if (e.key === "Escape") {
          e.preventDefault();
          confirmReject && confirmReject();
        }
        if (e.key === "Enter") {
          e.preventDefault();
          confirmResolve && confirmResolve();
        }
      }
    });

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
            confirmClass: "btn-ghost",
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

    const cfgCancel = document.getElementById("btn-cancel");
    if (cfgForm && cfgCancel) {
      BASELINES.cfg = snapshotForm(cfgForm);
      cfgCancel.hidden = true;

      const updateCfgDirty = () => {
        cfgCancel.hidden = snapshotForm(cfgForm) === BASELINES.cfg;
      };

      cfgForm.addEventListener("input", updateCfgDirty);
      cfgForm.addEventListener("change", updateCfgDirty);
      cfgForm.addEventListener("reset", () => setTimeout(updateCfgDirty, 0));
      cfgCancel.addEventListener("click", () => {
        cfgForm.reset();

        cfgValidated = false;
        validateConfigAndToggle({ decorate: "clear" });
      });
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
            showToast(messages[actionPath] || "Done.", { type: "success" });
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
                document
                  .getElementById("btn-cancel")
                  ?.setAttribute("hidden", "");
              }
              fetchAndRenderStatus();
            } else if (actionPath === "/filters/save") {
              const ff = document.getElementById("form-filters");
              if (ff) {
                BASELINES.filters = snapshotForm(ff);
                document
                  .getElementById("btn-cancel-filters")
                  ?.setAttribute("hidden", "");
              }
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

    const filtersForm = document.getElementById("form-filters");
    const filtersCancel = document.getElementById("btn-cancel-filters");

    function updateFiltersDirty() {
      if (!filtersForm || !filtersCancel) return;
      filtersCancel.hidden = snapshotForm(filtersForm) === BASELINES.filters;
    }

    async function loadFiltersIntoForm() {
      try {
        const res = await fetch("/filters", {
          cache: "no-store",
          credentials: "same-origin",
        });
        if (!res.ok) return;
        const data = await res.json();
        CHIPS["wl_categories"]?.set(data.whitelist?.category ?? []);
        CHIPS["wl_channels"]?.set(data.whitelist?.channel ?? []);
        CHIPS["ex_categories"]?.set(data.exclude?.category ?? []);
        CHIPS["ex_channels"]?.set(data.exclude?.channel ?? []);
      } catch (e) {
        console.warn("Failed loading filters", e);
      }
    }

    (async () => {
      await loadFiltersIntoForm();
      if (filtersForm) {
        BASELINES.filters = snapshotForm(filtersForm);
        if (filtersCancel) filtersCancel.hidden = true;
      }
    })();

    const filtersCard = document.getElementById("card-filters");
    if (filtersCard) {
      filtersCard.addEventListener("card-toggled", async (e) => {
        if (!e.detail?.collapsed) {
          await loadFiltersIntoForm();
          if (filtersForm) {
            BASELINES.filters = snapshotForm(filtersForm);
            updateFiltersDirty();
          }
        }
      });
    }

    if (filtersForm && filtersCancel) {
      filtersForm.addEventListener("input", updateFiltersDirty);
      filtersForm.addEventListener("change", updateFiltersDirty);
      filtersForm.addEventListener("reset", () =>
        setTimeout(updateFiltersDirty, 0)
      );

      filtersCancel.addEventListener("click", async () => {
        filtersForm.reset();
        await loadFiltersIntoForm();
        BASELINES.filters = snapshotForm(filtersForm);
        filtersCancel.hidden = true;
      });
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
              loadFiltersIntoForm();
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
    if (key === "HOST_GUILD_ID" || key === "CLONE_GUILD_ID")
      return /^\d+$/.test(v);
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
      ? "Provide SERVER_TOKEN, CLIENT_TOKEN, HOST_GUILD_ID, CLONE_GUILD_ID to start."
      : "";
    btn.disabled = !!toggleLocked || blockStart;
  }
})();
