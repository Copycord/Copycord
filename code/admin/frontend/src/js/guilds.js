(() => {
  const root = document.getElementById("guilds-root");
  const empty = document.getElementById("guilds-empty");
  const search = document.getElementById("g-search");
  const dirBtn = document.getElementById("g-sortdir");
  if (!root) return;

  let data = [];
  let filtered = [];
  let sortBy = "name";
  let sortDir = "asc";

  let scrapeRunning = false;
  let activeScrapeGuildId = null;

  function ensurePopoverLayer() {
    let layer = document.getElementById("popover-layer");
    if (!layer) {
      layer = document.createElement("div");
      layer.id = "popover-layer";
      document.body.appendChild(layer);
    }
    return layer;
  }
  function closeAllMenus() {
    root.querySelectorAll(".guild-actions .action-menu").forEach((m) => {
      m.hidden = true;
      m.classList.remove("popover");
    });
    document
      .querySelectorAll("#popover-layer .action-menu")
      .forEach((m) => m.remove());
  }

  function openMenuAsPopover(menuEl, clickEvent, cardEl) {
    const layer = ensurePopoverLayer();
  
    menuEl.dataset.gid = cardEl.dataset.gid;
    menuEl.hidden = false;
    menuEl.classList.add("popover");
    layer.appendChild(menuEl);

    const pad = 8;
    const vw = window.innerWidth;
    const vh = window.innerHeight;

    let x = clickEvent.clientX + pad;
    let y = clickEvent.clientY + pad;

    menuEl.style.left = x + "px";
    menuEl.style.top = y + "px";
    menuEl.style.right = "";

    const r = menuEl.getBoundingClientRect();
    const maxW = Math.min(window.innerWidth - pad * 2, 360);
    menuEl.style.maxWidth = maxW + "px";
    if (r.right > vw - pad) x -= r.right - (vw - pad);
    if (r.bottom > vh - pad) y -= r.bottom - (vh - pad);
    if (x < pad) x = pad;

    const headerH =
      parseInt(
        getComputedStyle(document.documentElement).getPropertyValue(
          "--header-h"
        )
      ) || 60;
    const minTop = headerH + pad;
    if (y < minTop) y = minTop;

    menuEl.style.left = Math.round(x) + "px";
    menuEl.style.top = Math.round(y) + "px";
    function teardown() {
      document.removeEventListener("click", onDocClick, true);
      document.removeEventListener("keydown", onKey, true);
      window.removeEventListener("scroll", onWin, true);
      window.removeEventListener("resize", onWin, true);
      const actions = cardEl.querySelector(".guild-actions");
      actions?.appendChild(menuEl);
      menuEl.hidden = true;
      menuEl.classList.remove("popover");
      menuEl.style.left = menuEl.style.top = menuEl.style.right = "";
    }
  
    const onDocClick = (e) => {
      if (!menuEl.contains(e.target)) teardown();
    };
    const onKey = (e) => {
      if (e.key === "Escape") {
        e.preventDefault();
        teardown();
      }
    };
    const onWin = () => teardown();
  
    setTimeout(() => {
      document.addEventListener("click", onDocClick, true);
    }, 0);
    document.addEventListener("keydown", onKey, true);
    window.addEventListener("scroll", onWin, true);
    window.addEventListener("resize", onWin, true);
  }

  let wsOut = null;
  function ensureOut() {
    if (
      wsOut &&
      (wsOut.readyState === WebSocket.OPEN ||
        wsOut.readyState === WebSocket.CONNECTING)
    )
      return;
    const url = location.origin.replace(/^http/, "ws") + "/ws/out";
    wsOut = new WebSocket(url);

    wsOut.onmessage = (ev) => {
      let raw;
      try {
        raw = JSON.parse(ev.data);
      } catch {
        return;
      }
      const p = raw?.payload ?? raw;
      const kind = raw?.kind ?? p?.kind ?? "client";
      if (kind !== "client") return;

      const t = p?.type;
      const gid = String(
        p?.data?.guild_id || p?.guild_id || activeScrapeGuildId || ""
      );

      const key = `scrape:${gid}`;

      if (t === "scrape_started" || t === "scrape_busy") {
        setScrapeState(true, gid);
        render();
        window.toast.wsGate({
          key,
          msg:
            t === "scrape_busy"
              ? "A scrape is already running."
              : "Scrape started.",
          type: t === "scrape_busy" ? "warning" : "info",
        });
      }

      if (t === "scrape_done") {
        setScrapeState(false);
        render();
        window.toast.wsGate({ key, msg: "Scrape completed.", type: "success" });
        window.toast.clearLaunch(key);
      }

      if (t === "scrape_cancelled") {
        setScrapeState(false, gid);
        render();
        window.toast.wsGate({ key, msg: "Scrape cancelled.", type: "warning" });
        window.toast.clearLaunch(key);
      }

      if (t === "scrape_failed") {
        const err = p?.data?.error || "Unknown error";
        setScrapeState(false, gid);
        render();
        window.toast.wsGate({
          key,
          msg: `Scrape failed: ${err}`,
          type: "error",
        });
        window.toast.clearLaunch(key);
      }
    };
  }

  async function cancelScrape(g) {
    try {
      const res = await fetch("/api/scrape/cancel", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ guild_id: g?.id || activeScrapeGuildId }),
      });
      if (!res.ok) {
        const err = await safeErr(res);
        window.showToast(`Could not cancel: ${err}`, { type: "error" });
        return;
      }
      setScrapeState(true, g?.id || activeScrapeGuildId);
      render();
    } catch (e) {
      console.error("cancel scrape failed", e);
      window.showToast("Failed to cancel scrape.", { type: "error" });
    }
  }

  function setScrapeState(running, gid = activeScrapeGuildId) {
    scrapeRunning = !!running;
    activeScrapeGuildId = scrapeRunning
      ? String(gid || activeScrapeGuildId || "")
      : null;

    const toggleButtonsForMenu = (menuEl, onThis) => {
      if (!menuEl) return;
      const btnScrape = menuEl.querySelector('button[data-act="scrape"]');
      const btnCancel = menuEl.querySelector('button[data-act="cancel"]');
      if (btnScrape) btnScrape.hidden = !!onThis;
      if (btnCancel) btnCancel.hidden = !onThis;
    };

    root.querySelectorAll(".guild-card").forEach((c) => {
      const onThis = scrapeRunning && activeScrapeGuildId === c.dataset.gid;
      c.classList.toggle("scraping", onThis);
      const badge = c.querySelector(".scrape-badge");
      if (badge) {
        if (onThis) badge.removeAttribute("hidden");
        else badge.setAttribute("hidden", "hidden");
      }
      toggleButtonsForMenu(
        c.querySelector(".guild-actions .action-menu"),
        onThis
      );
    });

    document.querySelectorAll("#popover-layer .action-menu").forEach((m) => {
      const onThis = scrapeRunning && activeScrapeGuildId === m.dataset.gid;
      toggleButtonsForMenu(m, onThis);
    });

    const badgeExists = !!document.querySelector(".scrape-badge-global");
    if (running && !badgeExists) {
      const b = document.createElement("div");
      b.className = "scrape-badge-global";
      b.textContent = "Scraping…";
      document.body.appendChild(b);
    } else if (!running && badgeExists) {
      document.querySelector(".scrape-badge-global")?.remove();
    }
  }

  const norm = (s) => String(s || "").toLowerCase();

  function sortData() {
    const dir = sortDir === "asc" ? 1 : -1;
    filtered.sort((a, b) => {
      if (sortBy === "joined_at") {
        const aa = a.joined_at || "";
        const bb = b.joined_at || "";
        return aa === bb ? 0 : (aa > bb ? 1 : -1) * dir;
      }
      const aa = norm(a.name);
      const bb = norm(b.name);
      return aa === bb ? 0 : (aa > bb ? 1 : -1) * dir;
    });
  }

  let menuHandlerBound = false;

  function onMenuClick(e) {
    const item = e.target.closest('.action-menu button[role="menuitem"]');
    if (!item) return;

    e.stopPropagation();

    const menu = item.closest(".action-menu");
    const gid = menu?.dataset.gid;
    const g =
      gid && Array.isArray(data)
        ? data.find((x) => String(x.id) === String(gid))
        : null;

    closeAllMenus();

    switch (item.dataset.act) {
      case "scrape":
        openScraperDialog(g);
        break;
      case "cancel":
        cancelScrape(g);
        break;
      case "view":
        openGuildDetails(g);
        break;
    }
  }

  function bindMenuDelegationOnce() {
    if (menuHandlerBound) return;
    menuHandlerBound = true;
    document.addEventListener("click", onMenuClick);
  }

  function render() {
    sortData();
    root.innerHTML = "";
    if (!filtered.length) {
      empty.hidden = false;
      return;
    }
    empty.hidden = true;
  
    const frag = document.createDocumentFragment();
  
    for (const g of filtered) {
      const card = document.createElement("article");
      card.className = "guild-card";
      card.dataset.gid = String(g.id);
  
      const icon = g.icon_url
        ? `<img class="guild-icon" src="${encodeURI(g.icon_url)}" alt="">`
        : `<img class="guild-icon" src="/static/logo.png" alt="">`;
  
      const actions = `
        <div class="guild-actions">
          <div class="action-menu" role="menu" hidden>
            <button role="menuitem" data-act="view">View details</button>
            <button role="menuitem" data-act="scrape">Scrape members</button>
            <button role="menuitem" data-act="cancel" hidden>Cancel scrape</button>
          </div>
        </div>
      `;
  
      card.innerHTML = `
        ${actions}
        <div class="guild-card-body">
          <div class="guild-icon-wrap">${icon}</div>
          <div class="guild-name">${escapeHtml(g.name)}</div>
          <div class="scrape-badge" hidden>Scraping…</div>
        </div>
      `;
  
      frag.appendChild(card);
    }
  
    root.appendChild(frag);
  
    requestAnimationFrame(() => {
      root.querySelectorAll(".guild-card").forEach((cardEl) => {
        const gid = cardEl.dataset.gid;
        const g = data.find((x) => String(x.id) === String(gid));
        const nameEl = cardEl.querySelector(".guild-name");
        if (nameEl) setEllipsisTitle(nameEl, g?.name ?? nameEl.textContent);
      });
    });
  
    const hideAllCardMenus = () => {
      root.querySelectorAll(".guild-actions .action-menu").forEach((m) => {
        m.hidden = true;
        m.classList.remove("popover");
        m.style.left = m.style.top = m.style.right = "";
      });
  
      document.querySelectorAll("#popover-layer .action-menu").forEach((m) => {
        const gid = m.dataset.gid;
        const card = root.querySelector(`.guild-card[data-gid="${CSS.escape(gid)}"]`);
        if (card) card.querySelector(".guild-actions")?.appendChild(m);
        m.hidden = true;
        m.classList.remove("popover");
        m.style.left = m.style.top = m.style.right = "";
      });
    };
  
    // 🔑 Restore card click to open its menu
    root.querySelectorAll(".guild-card").forEach((cardEl) => {
      cardEl.addEventListener("click", (e) => {
        if (e.target.closest(".action-menu")) return;
  
        const menu = cardEl.querySelector(".guild-actions .action-menu");
        const isOpen =
          menu && !menu.hidden && menu.parentElement?.id === "popover-layer";
  
        hideAllCardMenus();
        if (!menu || isOpen) return;
  
        openMenuAsPopover(menu, e, cardEl);
      });
    });
  
    // Handle clicks on menu items
    root.addEventListener("click", (e) => {
      const item = e.target.closest(
        ".guild-actions .action-menu button[role='menuitem'], #popover-layer .action-menu button[role='menuitem']"
      );
      if (!item) return;
  
      e.stopPropagation();
      const menu = item.closest(".action-menu");
      const gid = menu?.dataset.gid;
      const g = data.find((x) => x.id === gid);
  
      hideAllCardMenus();
  
      switch (item.dataset.act) {
        case "scrape":
          openScraperDialog(g);
          break;
        case "cancel":
          cancelScrape(g);
          break;
        case "view":
          openGuildDetails(g);
          break;
        case "sync":
          window.showToast("Sync settings (soon)", { type: "info" });
          break;
      }
    });
  
    document.addEventListener("click", (e) => {
      if (!root.contains(e.target)) hideAllCardMenus();
    });
    document.addEventListener("keydown", (e) => {
      if (e.key === "Escape") hideAllCardMenus();
    });
  
    setScrapeState(scrapeRunning);
  }

  function ensureDetailsModal() {
    document
      .querySelectorAll(".guild-details-modal")
      .forEach((el) => el.remove());

    const wrap = document.createElement("div");
    wrap.className = "scraper-modal guild-details-modal";
    wrap.innerHTML = `
      <div class="modal-backdrop" aria-hidden="true"></div>
      <div class="modal-content scraper-card" role="dialog" aria-modal="true" aria-label="Guild details">
        <header class="scraper-head">
          <div class="scraper-title-wrap">
            <img id="gd-icon" class="scraper-icon" alt="" src="">
            <h3 id="gd-title" class="scraper-title">Guild details</h3>
          </div>
          <button class="scraper-close" aria-label="Close"><span aria-hidden="true">✕</span></button>
        </header>
  
        <div class="scraper-body">
          <div class="gd-top">
            <div class="guild-name" id="gd-name"></div>
            <div class="muted small" id="gd-sub"></div>
          </div>
          <div class="grid" id="gd-extra" style="grid-template-columns:1fr 1fr; gap:12px;"></div>
          <div id="gd-desc" class="mt-2"></div>
        </div>
  
        <footer class="scraper-actions">
          <button class="btn btn-primary" id="gd-close-btn">Close</button>
        </footer>
      </div>
    `;
    document.body.appendChild(wrap);

    const close = () => wrap.remove();
    wrap.querySelector(".scraper-close")?.addEventListener("click", close);
    wrap.querySelector("#gd-close-btn")?.addEventListener("click", close);
    wrap.addEventListener("click", (e) => {
      if (e.target.classList.contains("modal-backdrop")) close();
    });
    document.addEventListener(
      "keydown",
      function escOnce(e) {
        if (e.key === "Escape") {
          close();
          document.removeEventListener("keydown", escOnce);
        }
      },
      { once: true }
    );

    return wrap;
  }

  function setEllipsisTitleNow(el, fullText) {
    if (!el) return;
    if (fullText != null) el.textContent = fullText;

    el.title = el.textContent;

    const overflowX = el.scrollWidth > el.clientWidth + 1;
    const overflowY = el.scrollHeight > el.clientHeight + 1;

    if (!(overflowX || overflowY)) el.removeAttribute("title");
  }

  function setEllipsisTitle(el, fullText) {
    if (!el) return;
    if (fullText != null) el.textContent = fullText;

    // set a temporary title so users still get something if RAF doesn't run
    el.title = el.textContent;

    requestAnimationFrame(() => {
      setEllipsisTitleNow(el);
    });
  }

  async function openGuildDetails(guild) {
    const modal = ensureDetailsModal();
    const iconEl = modal.querySelector("#gd-icon");
    const titleEl = modal.querySelector("#gd-title");
    const nameEl = modal.querySelector("#gd-name");
    const subEl = modal.querySelector("#gd-sub");
    const extraEl = modal.querySelector("#gd-extra");
    const descEl = modal.querySelector("#gd-desc");

    iconEl.src = guild?.icon_url || "/static/logo.png";
    titleEl.textContent = "Guild details";
    nameEl.textContent = "Loading…";
    subEl.textContent = "";
    extraEl.innerHTML = "";
    descEl.textContent = "";

    setEllipsisTitle(titleEl, "Guild details");
    setEllipsisTitle(nameEl, name);

    try {
      const res = await fetch(`/api/guilds/${encodeURIComponent(guild.id)}`, {
        cache: "no-store",
      });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const json = await res.json();
      if (!json?.ok) throw new Error(json?.error || "Failed");

      const g = json.item || {};
      const iconUrl = g.icon_url || "/static/logo.png";
      const name = g.name || "Unknown guild";
      const members =
        g.member_count != null
          ? `${formatNumber(g.member_count)} member${
              g.member_count === 1 ? "" : "s"
            }`
          : "—";

      iconEl.src = iconUrl;
      iconEl.alt = `${name} icon`;
      nameEl.textContent = name;
      subEl.textContent = members;

      requestAnimationFrame(() => {
        setEllipsisTitle(nameEl, name);
      });

      const rows = [];
      if (g.owner_id)
        rows.push(
          `<div><span class="muted small">Owner ID</span><div>${escapeHtml(
            g.owner_id
          )}</div></div>`
        );
      if (g.created_at)
        rows.push(
          `<div><span class="muted small">Created</span><div>${escapeHtml(
            String(g.created_at)
          )}</div></div>`
        );
      extraEl.innerHTML =
        rows.join("") || `<div class="muted small">No extra fields.</div>`;

      if (g.description) {
        descEl.innerHTML = `
          <div class="muted small" style="margin-bottom:4px">Description</div>
          <div>${escapeHtml(g.description)}</div>
        `;
      }
    } catch (e) {
      nameEl.textContent = "Error loading details";
      subEl.textContent = "";
      extraEl.innerHTML = `<div class="muted small">${escapeHtml(
        String(e)
      )}</div>`;
    }
  }

  function openScraperDialog(guild) {
    if (!guild) return;
    if (scrapeRunning) {
      window.showToast(
        "A scrape is already running. Please wait for it to finish.",
        {
          type: "warning",
        }
      );
      return;
    }

    document.querySelectorAll(".scraper-modal").forEach((el) => el.remove());

    const modal = document.createElement("div");
    modal.className = "scraper-modal";
    modal.innerHTML = `
      <div class="modal-backdrop" aria-hidden="true"></div>
      <div class="modal-content scraper-card" role="dialog" aria-modal="true" aria-label="Scrape members">
        <header class="scraper-head">
          <div class="scraper-title-wrap">
            <img class="scraper-icon" alt="" src="${
              guild.icon_url ? encodeURI(guild.icon_url) : "/static/logo.png"
            }">
            <h3 class="scraper-title">Scraper — ${escapeHtml(guild.name)}</h3>
          </div>
          <button class="scraper-close" aria-label="Close"><span aria-hidden="true">✕</span></button>
        </header>

        <div class="scraper-body">
          <fieldset class="scrape-opts">
            <label class="check">
              <input type="checkbox" id="scr-inc-username">
              <span>Username</span>
            </label>

            <label class="check">
              <input type="checkbox" id="scr-inc-avatar">
              <span>Avatar</span>
            </label>

          <label class="check has-tip">
            <input type="checkbox" id="scr-inc-bio">
            <span class="label-text">Bio</span>
            <button type="button" class="info-dot" aria-expanded="false" aria-controls="scr-bio-tip"></button>
            <span id="scr-bio-tip" class="tip-bubble" hidden>
              Fetching bios is slow — Discord rate-limits these requests heavily, so fetching all user bios can take a long time.
            </span>
          </label>

            <p class="hint">If none are selected, we’ll scrape <b>IDs only</b>.</p>
          </fieldset>

          <div class="field compact">
            <label for="scr-sessions">Sessions</label>
            <input id="scr-sessions" class="input" type="number" min="1" max="5" value="1">
          </div>

          <p class="note">Output is saved to <code>/data/scrapes/</code></p>
        </div>

        <footer class="scraper-actions">
          <button class="btn btn-primary" data-act="start">Start scrape</button>
        </footer>
      </div>
    `;
    document.body.appendChild(modal);

    (() => {
      const dots = modal.querySelectorAll(".check.has-tip .info-dot");

      dots.forEach((dot) => {
        const bubbleId = dot.getAttribute("aria-controls");
        const bubble =
          (bubbleId && modal.querySelector(`#${CSS.escape(bubbleId)}`)) ||
          dot.nextElementSibling;

        if (!bubble) return;

        const closeOutside = (ev) => {
          if (ev.target === dot || bubble.contains(ev.target)) return;
          dot.setAttribute("aria-expanded", "false");
          bubble.hidden = true;
          document.removeEventListener("click", closeOutside, true);
        };

        dot.addEventListener("click", (ev) => {
          ev.stopPropagation();
          const expanded = dot.getAttribute("aria-expanded") === "true";
          const nextState = !expanded;

          dot.setAttribute("aria-expanded", String(nextState));
          bubble.hidden = !nextState;

          document.removeEventListener("click", closeOutside, true);
          if (nextState)
            setTimeout(
              () => document.addEventListener("click", closeOutside, true),
              0
            );
        });


        dot.addEventListener("keydown", (ev) => {
          if (ev.key === "Escape") {
            dot.setAttribute("aria-expanded", "false");
            bubble.hidden = true;
            dot.blur();
          }
        });
      });
    })();

    const close = () => {
      modal.remove();
      document.removeEventListener("keydown", onEsc);
    };

    const onEsc = (e) => {
      if (e.key === "Escape") {
        e.preventDefault();
        close();
      }
    };

    modal.querySelector(".scraper-close")?.addEventListener("click", close);
    modal
      .querySelector('[data-act="cancel"]')
      ?.addEventListener("click", close);
    modal.addEventListener("click", (e) => {
      if (e.target.classList.contains("modal-backdrop")) close();
    });
    document.addEventListener("keydown", onEsc);

    modal
      .querySelector('[data-act="start"]')
      .addEventListener("click", async () => {
        const ns = clampInt(
          modal.querySelector("#scr-sessions")?.value,
          2,
          1,
          5
        );
        const include_username =
          !!modal.querySelector("#scr-inc-username")?.checked;
        const include_avatar_url =
          !!modal.querySelector("#scr-inc-avatar")?.checked;
        const include_bio = !!modal.querySelector("#scr-inc-bio")?.checked;

        try {
          modal.remove();
          window.toast.markLaunched(`scrape:${guild.id}`);
          const res = await fetch("/api/scrape", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
              guild_id: guild.id,
              num_sessions: ns,
              include_username,
              include_avatar_url,
              include_bio,
            }),
          });

          if (res.status === 409) {
            window.showToast("A scrape is already running.", {
              type: "warning",
            });
            setScrapeState(true, guild.id);
            render();
            return;
          }
          if (!res.ok) {
            const err = await safeErr(res);
            console.error("SCRAPE ERROR", res.status);
            window.showToast(`Could not start scrape: ${err}`, {
              type: "error",
            });
            window.toast?.clearLaunch?.(`scrape:${guild.id}`);
            setScrapeState(false, guild.id);
            render();
            return;
          }
        } catch (e) {
          console.error("scrape start failed", e);
          window.toast?.clearLaunch?.(`scrape:${guild.id}`);
          window.showToast("Failed to start scrape.", { type: "error" });
          setScrapeState(false, guild.id);
          render();
        }
      });
  }

  function formatNumber(n) {
    if (n == null || isNaN(n)) return "0";
    return new Intl.NumberFormat("en-US").format(n);
  }

  function clampInt(v, def, lo, hi) {
    const n = parseInt(String(v ?? ""), 10);
    if (Number.isFinite(n)) return Math.max(lo, Math.min(hi, n));
    return def;
  }

  async function safeErr(res) {
    try {
      const j = await res.json();
      if (j?.error) return j.error;
    } catch {}
    try {
      const t = await res.text();
      return t || res.statusText || String(res.status);
    } catch {
      return res.statusText || String(res.status);
    }
  }

  function applySearch() {
    const q = norm(search.value);
    filtered = !q ? [...data] : data.filter((g) => norm(g.name).includes(q));
    render();
  }

  function updateSortUI() {
    const az = sortDir === "asc";
    dirBtn.textContent = az ? "A → Z" : "Z → A";
    dirBtn.setAttribute("aria-pressed", String(!az));
  }

  function toggleDir() {
    sortDir = sortDir === "asc" ? "desc" : "asc";
    updateSortUI();
    render();
  }

  async function load() {
    try {
      const res = await fetch("/api/guilds");
      const json = await res.json();
      data = json.items || [];
      filtered = [...data];
      render();
    } catch (e) {
      console.error("Failed to load guilds", e);
    }
  }

  function escapeAttr(s) {
    return escapeHtml(s).replaceAll('"', "&quot;");
  }
  function escapeHtml(s) {
    return String(s ?? "").replace(
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

  search?.addEventListener("input", applySearch);
  dirBtn?.addEventListener("click", toggleDir);

  document.addEventListener("DOMContentLoaded", () => {
    try {
      window.initSlideMenu?.();
    } catch {}
    try {
      window.enhanceAllSelects?.();
    } catch {}
    bindMenuDelegationOnce();

    const gate = createStatusGate({
      hideSelectors: [
        "#guilds-root",
        "#guilds-empty",
        "#g-search",
        "#g-sortdir",
      ],
    });

    if (!gate.lastUpIsFresh()) gate.showGateSoon();

    // Poll status; when ready we'll finish boot in afterGateReady()
    gate.checkAndGate(() => afterGateReady());
  });

  let bootedAfterGate = false;
  async function afterGateReady() {
    if (bootedAfterGate) return;
    bootedAfterGate = true;

    ensureOut();
    await load();

    try {
      const r = await fetch("/api/scrape/state", { cache: "no-store" });
      if (!r.ok) return;
      const s = await r.json();
      if (s?.running && s?.guild_id) {
        setScrapeState(true, String(s.guild_id));
        render();
      } else {
        setScrapeState(false);
        render();
      }
    } catch {}
  }
})();
