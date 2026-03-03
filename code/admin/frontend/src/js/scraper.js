(() => {
  const ROOT = document.getElementById("scraper-root");
  if (!ROOT) return;

  const $ = (s, p) => (p || ROOT).querySelector(s);
  const $$ = (s, p) => [...(p || ROOT).querySelectorAll(s)];

  const statusPill = $("#sc-status-pill");
  const statusLabel = $("#sc-status-label");
  const tokenCount = $("#sc-token-count");
  const tokenList = $("#sc-token-list");
  const tokenEmpty = $("#sc-token-empty");
  const addForm = $("#sc-add-form");
  const addSubmit = $("#sc-add-submit");
  const tokenValueIn = $("#sc-token-value");
  const tokenLabelIn = $("#sc-token-label");
  const guildIdIn = $("#sc-guild-id");
  const validateBtn = $("#sc-validate-btn");
  const startBtn = $("#sc-start-btn");
  const cancelBtn = $("#sc-cancel-btn");
  const validResult = $("#sc-validation-result");
  const progressCard = $("#sc-progress-card");
  const progressFill = $("#sc-progress-fill");
  const progressText = $("#sc-progress-text");
  const progressPct = $("#sc-progress-pct");
  const elapsedEl = $("#sc-elapsed");
  const etaEl = $("#sc-eta");
  const memberCountEl = $("#sc-member-count");
  const logCard = $("#sc-log-card");
  const logBody = $("#sc-log-body");
  const logSearch = $("#sc-log-search");
  const logClear = $("#sc-log-clear");
  const logAutoClear = $("#sc-log-autoclear");
  const scrapesCard = $("#sc-scrapes-card");
  const scrapesBody = $("#sc-scrapes-body");
  const scrapesList = $("#sc-scrapes-list");
  const scrapesEmpty = $("#sc-scrapes-empty");
  const scrapesRefresh = $("#sc-scrapes-refresh");
  const proxyCard = $("#sc-proxy-card");
  const proxyTextarea = $("#sc-proxy-textarea");
  const proxyCount = $("#sc-proxy-count");
  const proxySaveBtn = $("#sc-proxy-save");
  const proxyClearBtn = $("#sc-proxy-clear");
  const proxyPrependHttp = $("#sc-proxy-prepend-http");
  const proxyPrependSocks5 = $("#sc-proxy-prepend-socks5");
  const queueCard = $("#sc-queue-card");
  const queueBody = $("#sc-queue-body");
  const queueList = $("#sc-queue-list");
  const queueEmpty = $("#sc-queue-empty");
  const queueCount = $("#sc-queue-count");
  const queueClearBtn = $("#sc-queue-clear");
  const cancelAllBtn = $("#sc-cancel-all-btn");
  const queueBtn = $("#sc-queue-btn");

  let tokens = [];
  let running = false;
  let es = null;
  let ws = null;
  let LOG_LINES = [];
  let LOG_QUERY = "";
  const MAX_LINES = 10000;
  let autoFollow = true;
  const THRESH = 24;
  let elapsedTimer = null;
  let startTime = null;
  let lastProgress = { current: 0, total: 0, time: 0 };
  let queueItems = [];

  const loader = document.getElementById("app-loader");

  function hideLoader() {
    if (loader) {
      loader.classList.add("is-hiding");
      setTimeout(() => {
        loader.classList.add("is-hidden");
      }, 350);
    }
    document.body.classList.remove("is-loading");
  }

  const toast = (msg, opts) => window.showToast?.(msg, opts);

  async function api(url, opts = {}) {
    try {
      const r = await fetch(url, opts);
      const j = await r.json();
      return j;
    } catch (e) {
      return { ok: false, error: String(e.message || e) };
    }
  }

  function setStatus(status, label) {
    statusPill.dataset.status = status;
    statusLabel.textContent = label || status;
  }

  async function loadTokens() {
    const j = await api("/api/scraper/tokens");
    if (j.ok) {
      tokens = j.tokens || [];
      renderTokens();
    }
  }

  function renderTokens() {
    tokenCount.textContent = `${tokens.length} token${
      tokens.length !== 1 ? "s" : ""
    }`;

    $$(".sc-token-item", tokenList).forEach((el) => el.remove());

    if (tokens.length === 0) {
      tokenEmpty.hidden = false;
      return;
    }
    tokenEmpty.hidden = true;

    for (const t of tokens) {
      const el = document.createElement("div");
      el.className = "sc-token-item";
      el.dataset.id = t.token_id;

      const validClass = t.is_valid ? "valid" : "invalid";
      const validIcon = t.is_valid
        ? `<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path stroke-linecap="round" stroke-linejoin="round" d="m4.5 12.75 6 6 9-13.5"/></svg>`
        : `<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path stroke-linecap="round" stroke-linejoin="round" d="M6 18 18 6M6 6l12 12"/></svg>`;

      el.innerHTML = `
        <div class="sc-token-info">
          <span class="sc-token-badge ${validClass}">${validIcon}</span>
          <div class="sc-token-meta">
            <span class="sc-token-name">${esc(
              t.username || t.label || "Unlabeled"
            )}</span>
            <span class="sc-token-masked">${esc(t.masked)}</span>
          </div>
        </div>
        <div class="sc-token-actions">
          <button class="btn btn-ghost btn-xs sc-token-revalidate" data-id="${
            t.token_id
          }" title="Re-validate">
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path stroke-linecap="round" stroke-linejoin="round" d="M16.023 9.348h4.992v-.001M2.985 19.644v-4.992m0 0h4.992m-4.993 0 3.181 3.183a8.25 8.25 0 0 0 13.803-3.7M4.031 9.865a8.25 8.25 0 0 1 13.803-3.7l3.181 3.182"/></svg>
          </button>
          <button class="btn btn-ghost btn-xs sc-token-delete" data-id="${
            t.token_id
          }" title="Delete">
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path stroke-linecap="round" stroke-linejoin="round" d="m14.74 9-.346 9m-4.788 0L9.26 9m9.968-3.21c.342.052.682.107 1.022.166m-1.022-.165L18.16 19.673a2.25 2.25 0 0 1-2.244 2.077H8.084a2.25 2.25 0 0 1-2.244-2.077L4.772 5.79m14.456 0a48.108 48.108 0 0 0-3.478-.397m-12 .562c.34-.059.68-.114 1.022-.165m0 0a48.11 48.11 0 0 1 3.478-.397m7.5 0v-.916c0-1.18-.91-2.164-2.09-2.201a51.964 51.964 0 0 0-3.32 0c-1.18.037-2.09 1.022-2.09 2.201v.916m7.5 0a48.667 48.667 0 0 0-7.5 0"/></svg>
          </button>
        </div>`;

      tokenList.appendChild(el);
    }
  }

  function esc(s) {
    const d = document.createElement("div");
    d.textContent = s ?? "";
    return d.innerHTML;
  }

  addSubmit.addEventListener("click", async () => {
    const val = tokenValueIn.value.trim();
    if (!val) {
      toast("Enter a token", { type: "warning" });
      return;
    }
    addSubmit.disabled = true;
    addSubmit.innerHTML = `<svg class="spin" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path stroke-linecap="round" stroke-linejoin="round" d="M16.023 9.348h4.992v-.001M2.985 19.644v-4.992m0 0h4.992m-4.993 0 3.181 3.183a8.25 8.25 0 0 0 13.803-3.7M4.031 9.865a8.25 8.25 0 0 1 13.803-3.7l3.181 3.182"/></svg>`;

    const fd = new FormData();
    fd.append("token_value", val);
    fd.append("label", tokenLabelIn.value.trim());

    const j = await api("/api/scraper/tokens/add", {
      method: "POST",
      body: fd,
    });
    addSubmit.disabled = false;
    addSubmit.textContent = "Add";

    if (j.ok) {
      toast(`Token added${j.is_valid ? " ✓ valid" : " — invalid"}`, {
        type: j.is_valid ? "success" : "warning",
      });
      tokenValueIn.value = "";
      tokenLabelIn.value = "";
      tokenValueIn.focus();
      await loadTokens();
    } else {
      toast(j.error || j.detail || "Failed to add token", { type: "error" });
    }
  });

  tokenList.addEventListener("click", async (e) => {
    const revalBtn = e.target.closest(".sc-token-revalidate");
    const delBtn = e.target.closest(".sc-token-delete");

    if (revalBtn) {
      const id = revalBtn.dataset.id;
      revalBtn.disabled = true;
      const j = await api(`/api/scraper/tokens/${id}/validate`, {
        method: "POST",
      });
      revalBtn.disabled = false;
      if (j.ok) {
        toast(`Token ${j.is_valid ? "valid ✓" : "invalid ✗"}`, {
          type: j.is_valid ? "success" : "warning",
        });
        await loadTokens();
      } else {
        toast(j.error || "Validation failed", { type: "error" });
      }
    }

    if (delBtn) {
      const id = delBtn.dataset.id;
      if (!confirm("Delete this token?")) return;
      const j = await api(`/api/scraper/tokens/${id}`, { method: "DELETE" });
      if (j.ok) {
        toast("Token deleted", { type: "success" });
        await loadTokens();
      } else {
        toast(j.error || "Delete failed", { type: "error" });
      }
    }
  });

  validateBtn.addEventListener("click", async () => {
    const gid = guildIdIn.value.trim();
    if (!gid) {
      toast("Enter a Guild ID", { type: "warning" });
      return;
    }

    validateBtn.disabled = true;
    validateBtn.innerHTML = `<svg class="spin" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path stroke-linecap="round" stroke-linejoin="round" d="M16.023 9.348h4.992v-.001M2.985 19.644v-4.992m0 0h4.992m-4.993 0 3.181 3.183a8.25 8.25 0 0 0 13.803-3.7M4.031 9.865a8.25 8.25 0 0 1 13.803-3.7l3.181 3.182"/></svg> Validating…`;

    const fd = new FormData();
    fd.append("guild_id", gid);
    const j = await api("/api/scraper/validate-setup", {
      method: "POST",
      body: fd,
    });

    validateBtn.disabled = false;
    validateBtn.textContent = "Validate";

    validResult.hidden = false;

    if (j.ok) {
      validResult.className = "sc-validation-result sc-val-ok";
      validResult.innerHTML =
        `<span class="sc-val-icon">✓</span> ${j.tokens_in_guild} token${
          j.tokens_in_guild !== 1 ? "s" : ""
        } have access` +
        (j.tokens_missing
          ? ` · <span class="sc-val-warn">${j.tokens_missing} missing access</span>`
          : "");
    } else {
      validResult.className = "sc-validation-result sc-val-err";
      validResult.innerHTML = `<span class="sc-val-icon">✗</span> ${esc(
        j.error || "Validation failed"
      )}`;
    }

    setTimeout(() => {
      validResult.hidden = true;
    }, 5000);
  });

  function renderLogView({ preserveScroll = false } = {}) {
    if (!logBody) return;
    const shouldStick =
      logBody.scrollHeight - logBody.scrollTop - logBody.clientHeight <= THRESH;
    const q = LOG_QUERY.trim().toLowerCase();
    let view = LOG_LINES;
    if (q) view = LOG_LINES.filter((l) => l.toLowerCase().includes(q));
    logBody.textContent = view.length ? view.join("\n") + "\n" : "";
    if (shouldStick || !preserveScroll)
      logBody.scrollTop = logBody.scrollHeight;
  }

  function onLogScroll() {
    autoFollow =
      logBody.scrollHeight - logBody.scrollTop - logBody.clientHeight <= THRESH;
  }

  function appendLines(lines) {
    if (!Array.isArray(lines) || !lines.length) return;
    for (const l of lines) LOG_LINES.push(String(l ?? ""));
    if (LOG_LINES.length > MAX_LINES)
      LOG_LINES.splice(0, LOG_LINES.length - MAX_LINES);
    renderLogView({ preserveScroll: true });
  }

  function appendLine(line) {
    LOG_LINES.push(String(line ?? ""));
    if (LOG_LINES.length > MAX_LINES)
      LOG_LINES.splice(0, LOG_LINES.length - MAX_LINES);
    renderLogView({ preserveScroll: true });
  }

  logBody.addEventListener("scroll", onLogScroll, { passive: true });

  let logSearchTimer;
  logSearch.addEventListener("input", () => {
    clearTimeout(logSearchTimer);
    logSearchTimer = setTimeout(() => {
      LOG_QUERY = logSearch.value;
      renderLogView();
    }, 80);
  });
  logSearch.addEventListener("keydown", (e) => {
    if (e.key === "Escape") {
      logSearch.value = "";
      LOG_QUERY = "";
      renderLogView();
      e.preventDefault();
    }
  });
  logClear.addEventListener("click", () => {
    LOG_LINES = [];
    LOG_QUERY = "";
    logSearch.value = "";
    renderLogView();
    // Also clear the log file on disk so refresh doesn't bring them back
    api("/api/scraper/logs/clear", { method: "POST" });
  });

  const AC_KEY = "scraper_log_autoclear";
  logAutoClear.checked = localStorage.getItem(AC_KEY) !== "false";
  logAutoClear.addEventListener("change", () => {
    localStorage.setItem(AC_KEY, logAutoClear.checked ? "true" : "false");
  });

  let retryTimer = null;

  function startSSE() {
    clearTimeout(retryTimer);
    if (es) {
      try {
        es.close();
      } catch {}
    }

    es = new EventSource("/logs/stream/scraper");

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
      try {
        es.close();
      } catch {}
      retryTimer = setTimeout(startSSE, 2000);
    };
  }

  function stopSSE() {
    clearTimeout(retryTimer);
    if (es) {
      try {
        es.close();
      } catch {}
      es = null;
    }
  }

  function connectWS() {
    if (ws && ws.readyState <= 1) return;
    const proto = location.protocol === "https:" ? "wss" : "ws";
    ws = new WebSocket(`${proto}://${location.host}/ws/out`);
    ws.onmessage = (ev) => {
      try {
        const msg = JSON.parse(ev.data);
        if (msg.kind !== "scraper") return;
        const p = msg.payload || {};

        if (p.type === "progress") {
          onProgress(p.current, p.total, p.message);
        } else if (p.type === "complete") {
          onComplete(p);
        } else if (p.type === "queue_update") {
          onQueueUpdate(p);
        }
      } catch {}
    };
    ws.onclose = () => {
      setTimeout(connectWS, 3000);
    };
    ws.onerror = () => {
      try {
        ws.close();
      } catch {}
    };
  }

  function getProxyLines() {
    return proxyTextarea.value
      .split("\n")
      .map((l) => l.trim())
      .filter((l) => l.length > 0);
  }

  function updateProxyCount() {
    const n = getProxyLines().length;
    proxyCount.textContent = n > 0 ? `${n} proxy${n !== 1 ? "ies" : ""}` : "";
  }

  async function loadProxies() {
    const j = await api("/api/scraper/proxies");
    if (j.ok) {
      proxyTextarea.value = (j.proxies || []).join("\n");
      updateProxyCount();
    }
  }

  async function saveProxies() {
    const lines = getProxyLines();
    const j = await api("/api/scraper/proxies", {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ proxies: lines }),
    });
    if (j.ok) {
      toast(`Saved ${lines.length} prox${lines.length !== 1 ? "ies" : "y"}`, {
        type: "success",
      });
      updateProxyCount();
    } else {
      toast(j.error || "Failed to save proxies", { type: "error" });
    }
  }

  proxySaveBtn.addEventListener("click", saveProxies);

  proxyClearBtn.addEventListener("click", async () => {
    proxyTextarea.value = "";
    await saveProxies();
  });

  proxyTextarea.addEventListener("input", updateProxyCount);

  /** Prepend a scheme (http:// or socks5://) to every line that doesn't already have one. */
  function prependScheme(scheme) {
    const lines = proxyTextarea.value.split("\n");
    proxyTextarea.value = lines
      .map((line) => {
        const l = line.trim();
        if (!l) return l;
        if (l.includes("://")) return l;
        return `${scheme}${l}`;
      })
      .join("\n");
    updateProxyCount();
  }

  proxyPrependHttp.addEventListener("click", () => prependScheme("http://"));
  proxyPrependSocks5.addEventListener("click", () =>
    prependScheme("socks5://")
  );

  /* ── Queue helpers ── */

  function getScraperPayload() {
    const proxies = getProxyLines();
    return {
      guild_id: guildIdIn.value.trim(),
      include_username: $("#sc-opt-username").checked,
      include_avatar_url: $("#sc-opt-avatar").checked,
      include_bio: $("#sc-opt-bio").checked,
      include_roles: $("#sc-opt-roles").checked,
      proxies: proxies.length > 0 ? proxies : undefined,
    };
  }

  function renderQueue() {
    if (!queueList || !queueCard) return;

    if (queueItems.length === 0) {
      queueCard.hidden = true;
      return;
    }

    queueCard.hidden = false;
    queueCount.textContent = `${queueItems.length} item${queueItems.length !== 1 ? "s" : ""}`;
    queueList.innerHTML = "";
    queueEmpty.hidden = queueItems.length > 0;

    for (const item of queueItems) {
      const el = document.createElement("div");
      el.className = "sc-queue-item";
      el.dataset.id = item.id;

      const statusClass = item.status === "running" ? "running" : "pending";
      const statusIcon = item.status === "running"
        ? `<svg class="spin" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path stroke-linecap="round" stroke-linejoin="round" d="M16.023 9.348h4.992v-.001M2.985 19.644v-4.992m0 0h4.992m-4.993 0 3.181 3.183a8.25 8.25 0 0 0 13.803-3.7M4.031 9.865a8.25 8.25 0 0 1 13.803-3.7l3.181 3.182"/></svg>`
        : `<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path stroke-linecap="round" stroke-linejoin="round" d="M12 6v6h4.5m4.5 0a9 9 0 1 1-18 0 9 9 0 0 1 18 0Z"/></svg>`;

      el.innerHTML = `
        <div class="sc-queue-info">
          <span class="sc-queue-badge ${statusClass}">${statusIcon}</span>
          <div class="sc-queue-meta">
            <span class="sc-queue-guild">${esc(String(item.guild_id))}</span>
            <span class="sc-queue-status">${item.status === "running" ? "Scraping…" : "Pending"}</span>
          </div>
        </div>
        <div class="sc-queue-actions">
          ${item.status !== "running" ? `<button class="btn btn-ghost btn-xs sc-queue-remove" data-id="${item.id}" title="Remove from queue">
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path stroke-linecap="round" stroke-linejoin="round" d="M6 18 18 6M6 6l12 12"/></svg>
          </button>` : ""}
        </div>`;

      queueList.appendChild(el);
    }
  }

  queueList.addEventListener("click", async (e) => {
    const removeBtn = e.target.closest(".sc-queue-remove");
    if (!removeBtn) return;
    const id = removeBtn.dataset.id;
    removeBtn.disabled = true;
    const j = await api(`/api/scraper/queue/${id}`, { method: "DELETE" });
    if (j.ok) {
      toast("Removed from queue", { type: "success" });
    } else {
      toast(j.error || "Failed to remove", { type: "error" });
      removeBtn.disabled = false;
    }
  });

  queueClearBtn.addEventListener("click", async () => {
    const j = await api("/api/scraper/queue/clear", { method: "POST" });
    if (j.ok) {
      toast("Queue cleared", { type: "success" });
    } else {
      toast(j.error || "Failed to clear queue", { type: "error" });
    }
  });

  cancelAllBtn.addEventListener("click", async () => {
    if (!confirm("Cancel the current scrape and clear the entire queue?")) return;
    cancelAllBtn.disabled = true;
    const j = await api("/api/scraper/cancel-all", { method: "POST" });
    cancelAllBtn.disabled = false;
    if (j.ok) {
      toast("All cancelled", { type: "warning" });
    } else {
      toast(j.error || "Cancel failed", { type: "error" });
    }
  });

  queueBtn.addEventListener("click", async () => {
    const gid = guildIdIn.value.trim();
    if (!gid) {
      toast("Enter a Guild ID", { type: "warning" });
      return;
    }
    if (!tokens.length) {
      toast("Add tokens first", { type: "warning" });
      return;
    }

    queueBtn.disabled = true;
    const payload = getScraperPayload();

    const j = await api("/api/scraper/queue/add", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });

    queueBtn.disabled = false;

    if (j.ok) {
      toast(`Guild ${gid} added to queue (position ${j.queue_position})`, { type: "success" });
      guildIdIn.value = "";
    } else {
      toast(j.error || "Failed to add to queue", { type: "error" });
    }
  });

  startBtn.addEventListener("click", async () => {
    const gid = guildIdIn.value.trim();

    // If no guild ID but there are queued items, just start the queue
    if (!gid && queueItems.length > 0) {
      startBtn.disabled = true;
      const j = await api("/api/scraper/queue/start", { method: "POST" });
      startBtn.disabled = false;
      if (j.ok) {
        toast("Queue started", { type: "success" });
        enterRunningState();
      } else {
        toast(j.error || "Failed to start queue", { type: "error" });
      }
      return;
    }

    if (!gid) {
      toast("Enter a Guild ID or add guilds to the queue", { type: "warning" });
      return;
    }
    if (!tokens.length) {
      toast("Add tokens first", { type: "warning" });
      return;
    }

    startBtn.disabled = true;
    const payload = getScraperPayload();

    const j = await api("/api/scraper/start", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });

    startBtn.disabled = false;

    if (j.ok) {
      const msg = j.queue_position > 1
        ? `Queued — position ${j.queue_position} (${j.tokens_used} token(s))`
        : `Scraper started — ${j.tokens_used} token(s)`;
      toast(msg, { type: "success" });
      guildIdIn.value = "";
      enterRunningState();
    } else {
      toast(j.error || "Failed to start", { type: "error" });
    }
  });

  cancelBtn.addEventListener("click", async () => {
    cancelBtn.disabled = true;
    const j = await api("/api/scraper/cancel", { method: "POST" });
    cancelBtn.disabled = false;
    if (j.ok) {
      toast("Cancel requested", { type: "warning" });
    } else {
      toast(j.error || "Cancel failed", { type: "error" });
    }
  });

  function enterRunningState(restoredStartTime) {
    running = true;
    setStatus("running", "Running");
    startBtn.hidden = true;
    cancelBtn.hidden = false;
    progressCard.hidden = false;
    progressFill.style.width = "0%";
    progressText.textContent = "Initializing…";
    progressPct.textContent = "0%";
    memberCountEl.textContent = "";
    etaEl.textContent = "";
    lastProgress = { current: 0, total: 0, time: 0 };

    if (!restoredStartTime && logAutoClear.checked) {
      LOG_LINES = [];
      renderLogView();
    }

    startSSE();
    startTime = restoredStartTime || Date.now();
    clearInterval(elapsedTimer);
    elapsedTimer = setInterval(updateElapsed, 1000);
    updateElapsed();
  }

  function exitRunningState() {
    running = false;
    clearInterval(elapsedTimer);
    startBtn.hidden = false;
    startBtn.disabled = false;
    cancelBtn.hidden = true;
  }

  function updateElapsed() {
    if (!startTime) return;
    const s = Math.round((Date.now() - startTime) / 1000);
    const m = Math.floor(s / 60);
    elapsedEl.textContent = m > 0 ? `${m}m ${s % 60}s` : `${s}s`;
  }

  function onProgress(current, total, message) {
    if (total && total > 0) {
      const pct = Math.min(100, Math.round((current / total) * 100));
      progressFill.style.width = pct + "%";
      progressPct.textContent = pct + "%";

      const now = Date.now();
      if (current > 0 && startTime) {
        const elapsedMs = now - startTime;
        const rate = current / (elapsedMs / 1000);
        const remaining = total - current;
        if (rate > 0 && remaining > 0) {
          const etaSec = Math.round(remaining / rate);
          etaEl.textContent = `~${fmtSec(etaSec)} left`;
        } else {
          etaEl.textContent = "";
        }
      }
      lastProgress = { current, total, time: now };
    }
    progressText.textContent = message || `${current} found`;
    // Only show the target (goal) count — don't update with current
    if (total && total > 0) {
      memberCountEl.textContent = `${total.toLocaleString()} members`;
    }
  }

  function onComplete(data) {
    if (!running) return;

    if (data.success) {
      toast(
        `Scrape complete${data.guild_id ? ` (${data.guild_id})` : ""} — ${data.total_count} members in ${fmtSec(
          data.elapsed_seconds
        )}`,
        { type: "success" }
      );
    } else {
      toast(data.error || "Scrape failed", { type: "error" });
    }

    // If there are more items in the queue, keep running state
    const hasPending = queueItems.some(q => q.status === "pending");
    if (hasPending) {
      // Reset progress for the next item
      progressFill.style.width = "0%";
      progressText.textContent = "Starting next in queue…";
      progressPct.textContent = "0%";
      memberCountEl.textContent = "";
      etaEl.textContent = "";
      startTime = Date.now();
      setStatus("running", "Running Queue");

      if (data.success && data.total_count > 0) {
        loadScrapes();
      }
      return;
    }

    // No more items — fully exit running state
    exitRunningState();

    if (data.success) {
      setStatus("done", "Complete");
    } else {
      setStatus("error", "Error");
    }

    progressText.textContent = data.success
      ? `Done — ${data.total_count} members`
      : `Failed: ${data.error || "unknown"}`;
    progressPct.textContent = data.success ? "100%" : "—";
    if (data.success) progressFill.style.width = "100%";
    etaEl.textContent = "";

    updateElapsed();

    if (data.success && data.total_count > 0) {
      loadScrapes();
    }

    setTimeout(() => {
      if (logAutoClear.checked) {
        LOG_LINES = [];
        LOG_QUERY = "";
        logSearch.value = "";
        renderLogView();
        api("/api/scraper/logs/clear", { method: "POST" });
      }
      progressCard.hidden = true;
    }, 3000);
  }

  function onQueueUpdate(data) {
    queueItems = data.queue || [];
    renderQueue();

    // If the queue is now empty and nothing is running, ensure we exit running state
    if (!data.running && running) {
      exitRunningState();
      setStatus("idle", "Idle");
    }
    // If something is running and we weren't in running state, enter it
    if (data.running && !running) {
      enterRunningState();
    }
  }

  function fmtSec(s) {
    if (s == null) return "—";
    s = Math.round(s);
    if (s < 60) return s + "s";
    return Math.floor(s / 60) + "m " + (s % 60) + "s";
  }

  async function loadScrapes() {
    const j = await api("/api/scraper/scrapes");
    if (!j.ok) return;
    const list = j.scrapes || [];
    scrapesList.innerHTML = "";

    if (list.length === 0) {
      scrapesEmpty.hidden = false;
      return;
    }
    scrapesEmpty.hidden = true;

    for (const s of list) {
      if (s.error) continue;
      const el = document.createElement("div");
      el.className = "sc-scrape-item";

      const when = s.scraped_at ? fmtDate(s.scraped_at) : "Unknown date";
      const size = fmtBytes(s.file_size || 0);
      const method = s.metadata?.method || "gateway";
      const displayName = s.guild_name || s.guild_id || "Unknown";
      const scrapePath = s.path || s.filename;

      el.innerHTML = `
        <div class="sc-scrape-info">
          <span class="sc-scrape-guild" title="Guild ID: ${esc(
            s.guild_id || "?"
          )}">
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5"><path stroke-linecap="round" stroke-linejoin="round" d="M15 19.128a9.38 9.38 0 0 0 2.625.372 9.337 9.337 0 0 0 4.121-.952 4.125 4.125 0 0 0-7.533-2.493M15 19.128v-.003c0-1.113-.285-2.16-.786-3.07M15 19.128v.106A12.318 12.318 0 0 1 8.624 21c-2.331 0-4.512-.645-6.374-1.766l-.001-.109a6.375 6.375 0 0 1 11.964-3.07M12 6.375a3.375 3.375 0 1 1-6.75 0 3.375 3.375 0 0 1 6.75 0Zm8.25 2.25a2.625 2.625 0 1 1-5.25 0 2.625 2.625 0 0 1 5.25 0Z"/></svg>
            ${esc(displayName)}
          </span>
          <span class="sc-scrape-meta">
            ${esc(String(s.total_count ?? 0))} members · ${esc(method)} · ${esc(
        size
      )}
          </span>
          <span class="sc-scrape-date">${esc(when)}</span>
        </div>
        <div class="sc-scrape-actions">
          <a class="btn btn-primary btn-xs" href="/api/scraper/scrapes/${encodeURIComponent(
            scrapePath
          )}" download title="Download">
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path stroke-linecap="round" stroke-linejoin="round" d="M3 16.5v2.25A2.25 2.25 0 0 0 5.25 21h13.5A2.25 2.25 0 0 0 21 18.75V16.5M16.5 12 12 16.5m0 0L7.5 12m4.5 4.5V3"/></svg>
          </a>
          <button class="btn btn-ghost btn-xs sc-scrape-delete" data-path="${esc(
            scrapePath
          )}" title="Delete">
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path stroke-linecap="round" stroke-linejoin="round" d="m14.74 9-.346 9m-4.788 0L9.26 9m9.968-3.21c.342.052.682.107 1.022.166m-1.022-.165L18.16 19.673a2.25 2.25 0 0 1-2.244 2.077H8.084a2.25 2.25 0 0 1-2.244-2.077L4.772 5.79m14.456 0a48.108 48.108 0 0 0-3.478-.397m-12 .562c.34-.059.68-.114 1.022-.165m0 0a48.11 48.11 0 0 1 3.478-.397m7.5 0v-.916c0-1.18-.91-2.164-2.09-2.201a51.964 51.964 0 0 0-3.32 0c-1.18.037-2.09 1.022-2.09 2.201v.916m7.5 0a48.667 48.667 0 0 0-7.5 0"/></svg>
          </button>
        </div>`;

      scrapesList.appendChild(el);
    }
  }

  function fmtDate(iso) {
    try {
      const d = new Date(iso);
      return (
        d.toLocaleDateString(undefined, {
          year: "numeric",
          month: "short",
          day: "numeric",
        }) +
        " " +
        d.toLocaleTimeString(undefined, { hour: "2-digit", minute: "2-digit" })
      );
    } catch {
      return iso;
    }
  }

  function fmtBytes(b) {
    if (b < 1024) return b + " B";
    if (b < 1024 * 1024) return (b / 1024).toFixed(1) + " KB";
    return (b / (1024 * 1024)).toFixed(1) + " MB";
  }

  scrapesBody.addEventListener("click", async (e) => {
    const delBtn = e.target.closest(".sc-scrape-delete");
    if (!delBtn) return;
    const scrapePath = delBtn.dataset.path;
    if (!confirm(`Delete scrape file "${scrapePath}"?`)) return;
    delBtn.disabled = true;
    const j = await api(
      `/api/scraper/scrapes/${encodeURIComponent(scrapePath)}`,
      { method: "DELETE" }
    );
    if (j.ok) {
      toast("Scrape deleted", { type: "success" });
      await loadScrapes();
    } else {
      toast(j.error || "Delete failed", { type: "error" });
      delBtn.disabled = false;
    }
  });

  scrapesRefresh.addEventListener("click", () => loadScrapes());

  async function checkStatus() {
    const j = await api("/api/scraper/status");

    // Restore queue
    if (j.queue && j.queue.length > 0) {
      queueItems = j.queue;
      renderQueue();
    }

    if (j.running) {
      guildIdIn.value = j.guild_id || "";

      const restoredStart = j.started_at ? j.started_at * 1000 : null;
      enterRunningState(restoredStart);
    }
  }

  async function init() {
    try {
      await loadTokens();
      await loadProxies();
      await loadScrapes();
      await checkStatus();
    } catch (e) {
      console.error("[scraper] init error:", e);
    }
    connectWS();
    startSSE();
    hideLoader();
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
