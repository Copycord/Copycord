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
          type: t === "scrape_busy" ? "warning" : "success",
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
      case "export":
        openExportDialog(g);
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
            <button role="menuitem" data-act="export">Export messages</button>
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
        const card = root.querySelector(
          `.guild-card[data-gid="${CSS.escape(gid)}"]`
        );
        if (card) card.querySelector(".guild-actions")?.appendChild(m);
        m.hidden = true;
        m.classList.remove("popover");
        m.style.left = m.style.top = m.style.right = "";
      });
    };

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
          <button class="btn btn-ghost" id="gd-close-btn">Close</button>
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

  function ensureExportModal() {
    console.debug("[Export] ensureExportModal()");

    document.querySelectorAll(".export-modal").forEach((el) => el.remove());

    const wrap = document.createElement("div");
    wrap.className = "modal export-modal show";

    const prevDocOverflow = document.documentElement.style.overflow;
    document.documentElement.style.overflow = "hidden";

    wrap.innerHTML = `
      <div class="modal-backdrop" aria-hidden="true"></div>
      <div class="modal-content scraper-card" role="dialog" aria-modal="true" aria-label="Export messages">
        <header class="scraper-head export-head">
          <div class="scraper-title-wrap">
            <h3 class="scraper-title">Export messages</h3>
            <p class="muted small">Export messages to a JSON file and optionally forward via webhook.</p>
          </div>
          <button type="button" class="scraper-close js-x" aria-label="Close">
            <span aria-hidden="true">✕</span>
          </button>
        </header>
  
        <div class="scraper-body">
          <div class="export-body-wrap">
            <div class="two-col">
              <label class="form-field">
                <span class="label">Channel ID (optional)</span>
                <input class="input" type="text" inputmode="numeric" placeholder="e.g. 123456789012345678" id="ex-channel">
              </label>
  
              <label class="form-field">
                <span class="label">Filter by User ID (optional)</span>
                <input class="input" type="text" inputmode="numeric" placeholder="e.g. 123456789012345678" id="ex-user">
              </label>
            </div>
  
            <!-- Filters -->
            <details class="export-advanced" id="ex-filters">
              <summary class="label">Filters</summary>
              <div class="mt-2 grid gap-2">
                <label class="check">
                  <input type="checkbox" id="ex-f-hascontent" checked>
                  <span>Include text content</span>
                </label>
                <label class="check">
                  <input type="checkbox" id="ex-f-embeds" checked>
                  <span>Include embeds</span>
                </label>
  
                <div class="check">
                  <input type="checkbox" id="ex-f-attachments" checked>
                  <span>Include attachments</span>
                  <button
                    type="button"
                    class="btn-tip"
                    id="ex-f-att-tip"
                    aria-label="Attachment types"
                    data-tooltip="Choose attachment types (images, videos, audio, other)">
                    i
                  </button>
                  <span class="muted small" id="ex-f-att-summary" aria-live="polite"></span>
                </div>
  
                <!-- Hidden mirrors keep the same IDs used in payload -->
                <div class="visually-hidden" aria-hidden="true">
                  <input type="checkbox" id="ex-f-att-images" checked>
                  <input type="checkbox" id="ex-f-att-videos" checked>
                  <input type="checkbox" id="ex-f-att-audio" checked>
                  <input type="checkbox" id="ex-f-att-other" checked>
                </div>
  
                <label class="check">
                  <input type="checkbox" id="ex-f-links" checked>
                  <span>Include links</span>
                </label>
  
                <label class="check">
                  <input type="checkbox" id="ex-f-emojis" checked>
                  <span>Include emojis</span>
                </label>
  
                <div class="two-col items-center ex-word-row">
                  <label class="check" for="ex-f-word">
                    <input type="checkbox" id="ex-f-word-on">
                    <span>Includes word</span>
                  </label>
                  <input class="input" id="ex-f-word" type="text" placeholder="must have this word..." value="" disabled>
                </div>
  
                <label class="check">
                  <input type="checkbox" id="ex-f-replies" checked>
                  <span>Include replies</span>
                </label>
                <label class="check">
                  <input type="checkbox" id="ex-f-bots" checked>
                  <span>Include bot messages</span>
                </label>
                <label class="check">
                  <input type="checkbox" id="ex-f-system">
                  <span>Include system messages</span>
                </label>
                <label class="check">
                  <input type="checkbox" id="ex-f-pinned" checked>
                  <span>Include pinned messages</span>
                </label>
                <label class="check">
                  <input type="checkbox" id="ex-f-stickers" checked>
                  <span>Include stickers</span>
                </label>
                <label class="check">
                  <input type="checkbox" id="ex-f-mentions" checked>
                  <span>Include mentions</span>
                </label>
                <label class="check">
                  <input type="checkbox" id="ex-f-threads" checked>
                  <span>Include threads</span>
                </label>
  
                <div class="ex-metrics two-col">
                  <label class="form-field">
                    <span class="label small">Min message length</span>
                    <input class="input" id="ex-f-minlen" type="number" min="0" step="1" value="0">
                  </label>
                  <label class="form-field">
                    <span class="label small">Min total reactions</span>
                    <input class="input" id="ex-f-minreacts" type="number" min="0" step="1" value="0">
                  </label>
                </div>
              </div>
            </details>
  
            <!-- Webhook is optional & collapsible -->
            <details class="export-advanced" id="ex-whwrap">
              <summary class="label">Forward to a webhook (optional)</summary>
              <div class="mt-2">
                <label class="form-field">
                  <span class="label">Webhook URL</span>
                  <input class="input" type="url" placeholder="https://discord.com/api/webhooks/..." id="ex-webhook">
                  <span class="hint">Messages will be forwarded to this Discord webhook.</span>
                </label>
              </div>
            </details>
  
            <details class="export-advanced" id="ex-range">
              <summary class="label">Date Range (optional)</summary>
  
              <div class="two-col mt-2 ex-range-grid">
                <label class="form-field">
                  <span class="label">After</span>
                  <input class="input" id="ex-after" type="datetime-local" step="1">
                </label>
  
                <label class="form-field">
                  <span class="label">Before</span>
                  <input class="input" id="ex-before" type="datetime-local" step="1">
                </label>
              </div>
  
              <p class="hint small ex-range-hint">
                Times use your local timezone; they’re converted to UTC.
              </p>
            </details>

          <details class="export-advanced" id="ex-dlwrap">
            <summary class="label">Download media (optional)</summary>
            <div class="mt-2 grid gap-2">
              <label class="check"><input type="checkbox" id="ex-dl-images"><span>Images</span></label>
              <label class="check"><input type="checkbox" id="ex-dl-videos"><span>Videos</span></label>
              <label class="check"><input type="checkbox" id="ex-dl-audio"><span>Audio</span></label>
              <label class="check"><input type="checkbox" id="ex-dl-other"><span>Other files</span></label>
              <p class="hint small">Files are saved to <code>/data/exports/&lt;guild&gt;/&lt;timestamp&gt;/media/&lt;type&gt;/</code>.</p>
            </div>
          </details>
          </div>
        </div>
  
        <footer class="scraper-actions">
          <button type="button" class="btn btn-ghost">Cancel</button>
          <button type="button" class="btn btn-ghost" ">Start export</button>
        </footer>
      </div>
    `;

    document.body.appendChild(wrap);
    console.debug("[Export] modal appended to DOM");

    wrap.querySelector("#ex-filters")?.removeAttribute("open");

    const close = () => {
      console.debug("[Export] closing modal");
      wrap.remove();
      document.documentElement.style.overflow = prevDocOverflow || "";
    };
    wrap
      .querySelectorAll(".js-x")
      .forEach((b) => b.addEventListener("click", close));
    wrap.querySelector(".modal-backdrop")?.addEventListener("click", close);
    document.addEventListener(
      "keydown",
      function escOnce(e) {
        if (e.key === "Escape") {
          console.debug("[Export] ESC pressed, closing modal");
          close();
          document.removeEventListener("keydown", escOnce, true);
        }
      },
      true
    );

    const wordToggle = wrap.querySelector("#ex-f-word-on");
    const wordInput = wrap.querySelector("#ex-f-word");
    function syncWordFilter() {
      const on = !!wordToggle?.checked;
      if (wordInput) {
        wordInput.disabled = !on;
        wordInput.classList.toggle("is-disabled", !on);
      }
      console.debug("[Export] word filter toggle:", {
        enabled: on,
        value: wordInput?.value ?? "",
      });
    }
    syncWordFilter();
    wordToggle?.addEventListener("change", syncWordFilter);

    const whWrap = wrap.querySelector("#ex-whwrap");
    whWrap?.addEventListener("toggle", () => {
      console.debug("[Export] webhook details toggled:", { open: whWrap.open });
      if (whWrap.open) {
        const urlInput = wrap.querySelector("#ex-webhook");
        setTimeout(() => urlInput?.focus(), 0);
      }
    });

    function ensureAttachmentTypesModal(parentModal) {
      console.debug("[Export] open Attachment Types sub-modal");
      parentModal
        .querySelectorAll(".att-types-modal")
        .forEach((el) => el.remove());

      const sub = document.createElement("div");
      sub.className = "att-types-modal";
      sub.setAttribute("role", "dialog");
      sub.setAttribute("aria-modal", "true");
      sub.setAttribute("id", "att-types-modal");

      sub.innerHTML = `
        <div class="att-backdrop" tabindex="-1" aria-hidden="true"></div>
        <div class="att-panel" role="document" aria-label="Attachment types">
          <header class="att-head">
            <h4 class="att-title">Attachment types</h4>
            <button type="button" class="att-x" aria-label="Close">✕</button>
          </header>
  
          <div class="att-body">
            <label class="check"><input type="checkbox" id="att-ui-images"><span>Images</span></label>
            <label class="check"><input type="checkbox" id="att-ui-videos"><span>Videos</span></label>
            <label class="check"><input type="checkbox" id="att-ui-audio"><span>Audio</span></label>
            <label class="check"><input type="checkbox" id="att-ui-other"><span>Other files</span></label>
          </div>
  
          <footer class="att-actions">
            <button type="button" class="btn btn-ghost" id="att-cancel">Cancel</button>
            <button type="button" class="btn btn-ghost" id="att-apply">Apply</button>
          </footer>
        </div>
      `;

      parentModal.appendChild(sub);

      const mirror = (sel) => parentModal.querySelector(sel);
      const ui = (sel) => sub.querySelector(sel);

      ui("#att-ui-images").checked = !!mirror("#ex-f-att-images")?.checked;
      ui("#att-ui-videos").checked = !!mirror("#ex-f-att-videos")?.checked;
      ui("#att-ui-audio").checked = !!mirror("#ex-f-att-audio")?.checked;
      ui("#att-ui-other").checked = !!mirror("#ex-f-att-other")?.checked;
      console.debug("[Export] att types state", {
        images: ui("#att-ui-images").checked,
        videos: ui("#att-ui-videos").checked,
        audio: ui("#att-ui-audio").checked,
        other: ui("#att-ui-other").checked,
      });

      const kill = () => {
        sub.remove();
      };
      sub.querySelector(".att-backdrop")?.addEventListener("click", () => {
        console.debug("[Export] sub-modal backdrop click");
        kill();
      });
      sub.querySelector(".att-x")?.addEventListener("click", () => {
        console.debug("[Export] sub-modal X click");
        kill();
      });
      sub.querySelector("#att-cancel")?.addEventListener("click", () => {
        console.debug("[Export] sub-modal Cancel");
        kill();
      });

      document.addEventListener(
        "keydown",
        function escOnce(e) {
          if (e.key === "Escape") {
            console.debug("[Export] sub-modal ESC close");
            kill();
            document.removeEventListener("keydown", escOnce, true);
          }
        },
        true
      );

      sub.querySelector("#att-apply")?.addEventListener("click", () => {
        const imgs = !!ui("#att-ui-images")?.checked;
        const vids = !!ui("#att-ui-videos")?.checked;
        const aud = !!ui("#att-ui-audio")?.checked;
        const oth = !!ui("#att-ui-other")?.checked;

        const mImgs = mirror("#ex-f-att-images");
        const mVids = mirror("#ex-f-att-videos");
        const mAud = mirror("#ex-f-att-audio");
        const mOth = mirror("#ex-f-att-other");
        if (mImgs) mImgs.checked = imgs;
        if (mVids) mVids.checked = vids;
        if (mAud) mAud.checked = aud;
        if (mOth) mOth.checked = oth;

        console.debug("[Export] att types applied", {
          images: imgs,
          videos: vids,
          audio: aud,
          other: oth,
        });
        updateAttachmentSummary();
        kill();
      });

      setTimeout(() => ui("#att-ui-images")?.focus(), 0);
    }

    function updateAttachmentSummary() {
      const s = wrap.querySelector("#ex-f-att-summary");
      if (s) s.textContent = "";
    }
    updateAttachmentSummary();

    const tipBtn = wrap.querySelector("#ex-f-att-tip");
    const attToggle = wrap.querySelector("#ex-f-attachments");

    function syncAttTipState() {
      const disabled = !attToggle?.checked;
      if (tipBtn) {
        tipBtn.disabled = !!disabled;
        tipBtn.classList.toggle("is-disabled", !!disabled);
      }
      console.debug("[Export] attachments toggle:", {
        includeAttachments: !disabled,
      });
    }

    syncAttTipState();
    attToggle?.addEventListener("change", syncAttTipState);
    tipBtn?.addEventListener("click", (e) => {
      e.preventDefault();
      if (!tipBtn.disabled) ensureAttachmentTypesModal(wrap);
    });

    return wrap;
  }

  function openExportDialog(guild) {
    const modal = ensureExportModal();
    const $ = (sel) => modal.querySelector(sel);

    function readLocalDT(sel) {
      const el = $(sel);
      if (!el) return null;
      const v = (el.value || "").trim();
      if (!v) return null;
      const d = new Date(v);
      return isNaN(d.getTime()) ? null : d.toISOString();
    }

    const POST_LABEL = "[Export] POST /api/export/messages";
    function redact(val) {
      if (!val) return val;
      try {
        const u = new URL(val);
        const tail = (u.pathname + u.search).replace(
          /.{8}.*$/,
          (m) => m.slice(0, 8) + "…"
        );
        return `${u.origin}${tail}`;
      } catch {
        return val.slice(0, 8) + "…";
      }
    }
    function payloadPreview(p) {
      return {
        ...p,
        webhook_url: p.webhook_url ? redact(p.webhook_url) : null,
      };
    }

    $("#ex-go").addEventListener("click", async () => {
      const includeAttachments = $("#ex-f-attachments")?.checked ?? true;
      const attTypes = includeAttachments
        ? {
            images: $("#ex-f-att-images")?.checked ?? true,
            videos: $("#ex-f-att-videos")?.checked ?? true,
            audio: $("#ex-f-att-audio")?.checked ?? true,
            other: $("#ex-f-att-other")?.checked ?? true,
          }
        : { images: false, videos: false, audio: false, other: false };

      const downloadMedia = {
        images: $("#ex-dl-images")?.checked ?? false,
        videos: $("#ex-dl-videos")?.checked ?? false,
        audio: $("#ex-dl-audio")?.checked ?? false,
        other: $("#ex-dl-other")?.checked ?? false,
      };

      const filters = {
        has_content: $("#ex-f-hascontent")?.checked ?? true,
        embeds: $("#ex-f-embeds")?.checked ?? true,

        attachments: includeAttachments,
        att_types: attTypes,

        links: $("#ex-f-links")?.checked ?? true,
        emojis: $("#ex-f-emojis")?.checked ?? true,

        word_on: $("#ex-f-word-on")?.checked ?? false,
        word: ($("#ex-f-word")?.value || "").trim(),

        replies: $("#ex-f-replies")?.checked ?? true,
        bots: $("#ex-f-bots")?.checked ?? true,
        system: $("#ex-f-system")?.checked ?? false,
        min_length: Math.max(0, parseInt($("#ex-f-minlen")?.value || "0", 10)),
        min_reactions: Math.max(
          0,
          parseInt($("#ex-f-minreacts")?.value || "0", 10)
        ),
        pinned: $("#ex-f-pinned")?.checked ?? true,
        stickers: $("#ex-f-stickers")?.checked ?? true,
        mentions: $("#ex-f-mentions")?.checked ?? true,

        threads: $("#ex-f-threads")?.checked ?? true,
        forum_threads: $("#ex-f-threads")?.checked ?? true,
        private_threads: $("#ex-f-threads")?.checked ?? true,
        download_media: downloadMedia,
      };

      const afterISO = readLocalDT("#ex-after");
      const beforeISO = readLocalDT("#ex-before");

      console.debug("[Export] guild:", { passedGuildId: guild?.id ?? null });
      console.debug("[Export] filters:", filters);
      console.debug("[Export] range (ISO UTC):", { afterISO, beforeISO });

      const payload = {
        guild_id: String(guild?.id || ""),
        channel_id: ($("#ex-channel")?.value || "").trim() || null,
        user_id: ($("#ex-user")?.value || "").trim() || null,
        webhook_url: ($("#ex-webhook")?.value || "").trim() || null,
        has_attachments: $("#ex-hasatt")?.checked || false,
        after_iso: afterISO,
        before_iso: beforeISO,
        filters,
      };

      console.log(`${POST_LABEL} payload:`, payloadPreview(payload));

      try {
        const body = JSON.stringify(payload);
        console.debug("[Export] fetch options:", {
          method: "POST",
          headers: { "content-type": "application/json" },
          bodyBytes: body.length,
        });

        const res = await fetch("/api/export/messages", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body,
        });

        console.debug("[Export] response status:", res.status, res.statusText);
        let j = null;
        try {
          j = await res.json();
        } catch (parseErr) {
          console.debug("[Export] response JSON parse failed:", parseErr);
        }
        console.debug("[Export] response JSON:", j);

        if (!res.ok || j?.ok === false) {
          const errMsg = j?.error || `HTTP ${res.status}`;
          console.error("[Export] request failed:", {
            errMsg,
            status: res.status,
            json: j,
          });
          throw new Error(errMsg);
        }

        window.showToast("Export started. You’ll see progress in logs.", {
          type: "success",
        });
        console.info("[Export] started successfully");
        modal.querySelector(".js-x")?.click();
      } catch (e) {
        console.error("[Export] request error:", e);
        window.showToast(String(e?.message || e), { type: "error" });
      }
    });
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
    setEllipsisTitle(nameEl, guild?.name ?? "Loading…");

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
          <button class="btn btn-ghost" data-act="start">Start scrape</button>
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
