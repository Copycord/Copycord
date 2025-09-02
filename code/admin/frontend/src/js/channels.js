(() => {
  const root = document.getElementById("channels-root");
  const empty = document.getElementById("channels-empty");
  const search = document.getElementById("ch-search");
  const sortSel = document.getElementById("ch-sort");
  const menu = document.getElementById("ch-menu");
  const UNGROUPED_LABEL = "Uncategorized";
  const filterSel = document.getElementById("ch-filter");
  const dirBtn = document.getElementById("ch-sortdir");
  const vBtn = document.getElementById("verify-btn");
  const vDlg = document.getElementById("verify-dialog");
  const vBack = document.getElementById("verify-backdrop");
  const vClose = document.getElementById("verify-close");
  const vFetch = document.getElementById("verify-fetch");
  const vDelAll = document.getElementById("verify-delall");
  const vCats = document.getElementById("orph-cats");
  const vChs = document.getElementById("orph-chs");
  const vStatus = document.getElementById("verify-status");
  const delAllBtn = document.getElementById("orph-delall");
  const pendingDeletes = new Set();
  const LAST_DELETED_SIG_KEY = "verify:last_deleted_sig";
  const RECENT_DELETE_WINDOW_MS = 8000;
  const cancelledThisSession = new Set();
  document.documentElement.classList.remove("boot");

  function setInert(el, on) {
    if (!el) return;
    try {
      on ? el.setAttribute("inert", "") : el.removeAttribute("inert");
    } catch {}
  }
  function blurIfInside(container) {
    const active = document.activeElement;
    if (active && container && container.contains(active)) {
      try {
        active.blur();
      } catch {}
    }
  }
  let lastFocusConfirm = null;
  let lastFocusVerify = null;
  let custChannel = null;
  let menuContext = null;
  let catPinByOrig = new Map();
  let catOrigByEither = new Map();

  (function () {
    if (window.__toastInit) return;
    window.__toastInit = true;
    function ensureToastRoot() {
      if (document.getElementById("toast-root")) return;
      const div = document.createElement("div");
      div.id = "toast-root";
      document.body.appendChild(div);
    }
    if (document.readyState !== "loading") {
      ensureToastRoot();
    } else {
      document.addEventListener("DOMContentLoaded", ensureToastRoot);
    }
  })();

  if (!root) return;

  const gate = createStatusGate({
    hideSelectors: [
      "#channels-root",
      "#channels-empty",
      "#verify-btn",
      "#ch-search",
      "#ch-sort",
      "#ch-sortdir",
      "#ch-filter",
    ],
  });
  if (!gate.lastUpIsFresh()) gate.showGateSoon();

  let data = [];
  let filtered = [];
  let pinsByOrig = new Map();
  let menuForId = null;
  let wsIn;
  let wsOut;
  let orph = { categories: [], channels: [] };
  let sortBy = "name";
  let sortDir = "asc";
  let lastDeleteAt = 0;
  let menuAnchorBtn = null;
  let bfCleanup = null;

  function rebuildCategoryPinMaps(rows) {
    catPinByOrig = new Map();
    catOrigByEither = new Map();
    for (const ch of rows || []) {
      const orig = String(
        ch.original_category_name ??
          ch.category_original_name ??
          ch.category_upstream_name ??
          ch.category_name ??
          ""
      ).trim();

      const pin = String(ch.cloned_category_name ?? "").trim();

      if (orig) {
        const oKey = orig.toLowerCase();
        if (!catOrigByEither.has(oKey)) catOrigByEither.set(oKey, orig);
        if (pin) catOrigByEither.set(pin.toLowerCase(), orig);
      }
      if (orig && pin && pin !== orig) {
        catPinByOrig.set(orig, pin);
      }
    }

    for (const [orig, pin] of pinsByOrig) {
      if (!orig || !pin || pin === orig) continue;
      catPinByOrig.set(orig, pin);
      if (!catOrigByEither.has(orig.toLowerCase())) {
        catOrigByEither.set(orig.toLowerCase(), orig);
      }
      catOrigByEither.set(pin.toLowerCase(), orig);
    }
  }

  function clearBackfillBootResidue() {
    for (const id of [...runningClones]) setCardLoading(id, false);
    for (const id of [...launchingClones]) setCardLoading(id, false);
    runningClones.clear();
    launchingClones.clear();
    try {
      sessionStorage.setItem("bf:running", "[]");
      sessionStorage.setItem("bf:launching", "[]");
    } catch {}

    try {
      const rm = [];
      for (let i = 0; i < sessionStorage.length; i++) {
        const k = sessionStorage.key(i);
        if (k && k.startsWith("toast:persist:bf:")) rm.push(k);
      }
      rm.forEach((k) => sessionStorage.removeItem(k));
    } catch {}
  }

  function upsertStatusPill(card, text = "Cloning…") {
    if (!card) return;
    const slot = card.querySelector(".ch-top-right");
    if (!slot) return;
    let pill = slot.querySelector(".ch-status");
    if (!pill) {
      pill = document.createElement("span");
      pill.className = "ch-status";
      slot.prepend(pill);
    }
    pill.textContent = text;
  }

  function setCardLoading(channelId, on, text = "Cloning…") {
    const k = String(channelId);
    const card = document.querySelector(`.ch-card[data-cid="${k}"]`);
    if (!card) return;
    if (on) {
      card.classList.add("is-cloning");
      card.setAttribute("aria-busy", "true");
      upsertStatusPill(card, text);
    } else {
      card.classList.remove("is-cloning");
      card.removeAttribute("aria-busy");
      const pill = card.querySelector(".ch-status");
      if (pill) pill.remove();
    }
  }

  function toastOncePersist(key, message, opts = {}, ttlMs = 8000) {
    const now = Date.now();
    const k = `toast:persist:${key}`;
    try {
      const prev = JSON.parse(sessionStorage.getItem(k) || "null");
      if (prev && now < prev.expiresAt) return;
      sessionStorage.setItem(k, JSON.stringify({ expiresAt: now + ttlMs }));
    } catch {}
    window.showToast(message, opts);
  }

  const BOOT_TS = Date.now();
  const SUPPRESS_BOOT_MS = 1200;
  function shouldAnnounceNow() {
    return Date.now() - BOOT_TS > SUPPRESS_BOOT_MS;
  }

  function markPending(id) {
    const nid = String(id);
    pendingDeletes.add(nid);
    lastDeleteAt = Date.now();
  }

  function escapeAttr(s) {
    return escapeHtml(s).replaceAll('"', "&quot;");
  }

  function makeDeletedSig(results) {
    try {
      const arr = results.map((r) => [
        String(r?.id ?? r?.channel_id ?? r?.category_id ?? r?.target_id ?? ""),
        String(r?.reason ?? r?.status ?? ""),
        r?.deleted === true ||
        r?.ok === true ||
        r?.success === true ||
        String(r?.status || "").toLowerCase() === "deleted"
          ? 1
          : 0,
      ]);
      arr.sort((a, b) => a[0].localeCompare(b[0]));
      return JSON.stringify(arr);
    } catch {
      return null;
    }
  }

  const taskMap = new Map(
    (() => {
      try {
        return Object.entries(
          JSON.parse(sessionStorage.getItem("bf:taskmap") || "{}")
        );
      } catch {
        return [];
      }
    })()
  );
  function saveTaskMap() {
    try {
      sessionStorage.setItem(
        "bf:taskmap",
        JSON.stringify(Object.fromEntries(taskMap))
      );
    } catch {}
  }
  function rememberTask(taskId, channelId) {
    if (!taskId || !channelId) return;
    taskMap.set(String(taskId), String(channelId));
    saveTaskMap();
  }
  function forgetTask(taskId) {
    if (!taskId) return;
    taskMap.delete(String(taskId));
    saveTaskMap();
  }

  function openCustomizeDialog(ch) {
    const modal = document.getElementById("customize-modal");
    const back = modal.querySelector('[data-role="backdrop"]');
    const dlg = modal.querySelector(".modal-content");
    const name = document.getElementById("customize-name");
    const btnSave = document.getElementById("customize-save");
    const btnClose = document.getElementById("customize-close");

    custChannel = ch;

    const initial =
      ch.clone_channel_name && ch.clone_channel_name.trim()
        ? ch.clone_channel_name
        : ch.original_channel_name || "";
    name.value = initial;

    function close() {
      blurIfInside(modal);
      setInert(modal, true);
      modal.setAttribute("aria-hidden", "true");
      modal.classList.remove("show");
      custChannel = null;
    }

    [btnClose].forEach((b) => {
      if (b)
        b.onclick = (e) => {
          e?.preventDefault?.();
          close();
        };
    });
    back.onclick = (e) => {
      if (e.target === back) close();
    };
    document.addEventListener(
      "keydown",
      function onEsc(e) {
        if (e.key === "Escape") {
          close();
          document.removeEventListener("keydown", onEsc);
        }
      },
      { once: true }
    );

    btnSave.onclick = async (e) => {
      e.preventDefault();
      const body = {
        original_channel_id: custChannel.original_channel_id,
        clone_channel_name: String(name.value || ""),
      };
      try {
        const res = await fetch("/api/channels/customize", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(body),
          credentials: "same-origin",
          cache: "no-store",
        });
        const json = await res.json().catch(() => ({}));
        if (!res.ok || json?.ok === false) {
          window.showToast(json?.error || "Failed to save.", { type: "error" });
          return;
        }
        window.showToast("Saved channel customization.", { type: "success" });
        close();
        try {
          await load();
        } catch {}
      } catch {
        window.showToast("Network error saving customization.", {
          type: "error",
        });
      }
    };

    hideMenu({ restoreFocus: false });
    setInert(modal, false);
    modal.removeAttribute("aria-hidden");
    modal.classList.add("show");
    modal.querySelector(".modal-content")?.focus?.({ preventScroll: true });
  }

  function openCustomizeCategoryDialog(
    categoryName,
    originalCategoryId = null
  ) {
    injectCustomizeCategoryModal();

    const modal = document.getElementById("customize-cat-modal");
    const back = modal.querySelector('[data-role="backdrop"]');
    const dlg = modal.querySelector(".modal-content");
    const nameInp = document.getElementById("customize-cat-name");
    const btnSave = document.getElementById("customize-cat-save");
    const btnClose = document.getElementById("customize-cat-close");
    const titleEl = document.getElementById("customize-cat-title");

    titleEl.textContent = `Customize category`;
    const resolvedOrig =
      catOrigByEither.get(String(categoryName).toLowerCase()) || categoryName;
    const pinned = catPinByOrig.get(resolvedOrig);
    const initial = pinned && pinned.trim() ? pinned : resolvedOrig;
    nameInp.value = initial;

    function close() {
      blurIfInside(modal);
      setInert(modal, true);
      modal.setAttribute("aria-hidden", "true");
      modal.classList.remove("show");
    }

    btnClose.onclick = (e) => {
      e?.preventDefault?.();
      close();
    };
    back.onclick = (e) => {
      if (e.target === back) close();
    };

    document.addEventListener(
      "keydown",
      function onEsc(e) {
        if (e.key === "Escape") {
          close();
          document.removeEventListener("keydown", onEsc);
        }
      },
      { once: true }
    );

    btnSave.onclick = async (e) => {
      e.preventDefault();

      const raw = String(nameInp.value || "").trim();
      const body = originalCategoryId
        ? {
            original_category_id: Number(originalCategoryId),
            custom_category_name: raw,
          }
        : {
            category_name: String(categoryName),
            custom_category_name: raw,
          };

      try {
        const res = await fetch("/api/categories/customize", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(body),
          credentials: "same-origin",
          cache: "no-store",
        });
        const json = await res.json().catch(() => ({}));
        if (!res.ok || json?.ok === false) {
          window.showToast(json?.error || "Failed to save.", { type: "error" });
          return;
        }
        window.showToast("Saved category customization.", { type: "success" });
        close();
        try {
          await load();
        } catch {}
      } catch {
        window.showToast("Network error saving customization.", {
          type: "error",
        });
      }
    };

    setInert(modal, false);
    modal.removeAttribute("aria-hidden");
    modal.classList.add("show");
    dlg?.focus?.({ preventScroll: true });
  }

  function tooltipForChannel(orig, custom) {
    if (!custom || !custom.trim() || custom === orig) return "";
    return `Cloned channel = #${orig}\nCustomized channel = #${custom}`;
  }

  function tooltipForCategory(orig, pin) {
    if (!pin || pin.trim() === "" || pin === orig) return "";
    return `Cloned category = ${orig}\nCustomized category = ${pin}`;
  }

  function updateCategoryChipUI(originalName, pinnedName) {
    const sel = `.cat-chip[data-cat-name="${
      window.CSS && CSS.escape
        ? CSS.escape(originalName)
        : String(originalName).replace(/"/g, '\\"')
    }"]`;

    const chip = document.querySelector(sel);
    if (!chip) return;

    const isOrphan = chip.classList.contains("badge-orphan");

    const display = pinnedName && pinnedName.trim() ? pinnedName : originalName;

    chip.innerHTML = `
      ${escapeHtml(display)}
      <button class="cat-menu-trigger" aria-haspopup="menu" aria-controls="ch-menu" aria-label="Category menu" type="button">⋯</button>
    `;

    chip.classList.toggle(
      "badge-custom",
      !!(pinnedName && pinnedName.trim() && pinnedName !== originalName)
    );

    const tip = tooltipForCategory(originalName, pinnedName);
    if (tip) chip.setAttribute("title", tip);
    else chip.removeAttribute("title");

    chip.setAttribute("data-cat-name", originalName);
  }

  function injectCustomizeCategoryModal() {
    if (document.getElementById("customize-cat-modal")) return;

    const wrap = document.createElement("div");
    wrap.id = "customize-cat-modal";
    wrap.className = "modal";
    wrap.setAttribute("aria-hidden", "true");

    wrap.innerHTML = `
      <div class="modal-backdrop" data-role="backdrop"></div>
      <div class="modal-content" role="dialog" aria-modal="true" aria-labelledby="customize-cat-title" tabindex="-1">
        <div class="modal-header">
          <h3 id="customize-cat-title">Customize category</h3>
          <button id="customize-cat-close" class="icon-btn verify-close" aria-label="Close">×</button>
        </div>
        <div class="modal-body">
          <label for="customize-cat-name" class="label has-tip">
            Custom category name
            <button class="info-dot" aria-describedby="tip-custom-cat" type="button"></button>
            <div id="tip-custom-cat" class="tip-bubble" aria-hidden="true" role="tooltip">
              Set a custom category name. Leave empty to use the original.
            </div>
          </label>
          <input id="customize-cat-name" class="input" type="text" placeholder="Leave empty to use original name" />
        </div>
        <div class="btns">
          <button id="customize-cat-save" class="btn btn-primary" type="button">Save</button>
        </div>
      </div>
    `;
    document.body.appendChild(wrap);
  }

  (function injectCustomizeModal() {
    if (document.getElementById("customize-modal")) return;

    const wrap = document.createElement("div");
    wrap.id = "customize-modal";
    wrap.className = "modal";
    wrap.setAttribute("aria-hidden", "true");

    wrap.innerHTML = `
  <div class="modal-backdrop" data-role="backdrop"></div>
  <div class="modal-content" role="dialog" aria-modal="true" aria-labelledby="customize-title" tabindex="-1">
    <div class="modal-header">
      <h3 id="customize-title">Customize channel</h3>
      <button id="customize-close" class="icon-btn verify-close" aria-label="Close">×</button>
    </div>
    <div class="modal-body">
    <label for="customize-name" class="label has-tip">
      Custom channel name
      <button class="info-dot" aria-describedby="tip-custom-name" type="button"></button>
      <div id="tip-custom-name" class="tip-bubble" aria-hidden="true" role="tooltip">
        Set a custom channel name. Leave empty to use the original.
      </div>
    </label>
      <input id="customize-name" class="input" type="text" placeholder="Leave empty to use original name" />
    </div>
    <div class="btns">
      <button id="customize-save" class="btn primary" type="button">Save</button>
    </div>
  </div>
`;
    document.body.appendChild(wrap);

    if (!document.getElementById("customize-compact-styles")) {
      const css = document.createElement("style");
      css.id = "customize-compact-styles";
      document.head.appendChild(css);
    }
  })();

  let bulkDeleteInFlight = false;

  function ensureBusyOverlay() {
    if (document.getElementById("page-busy")) return;
    const wrap = document.createElement("div");
    wrap.id = "page-busy";
    wrap.innerHTML = `
      <div class="busy-box" role="alert" aria-live="assertive">
        <div class="busy-spinner" aria-hidden="true"></div>
        <div class="busy-msg">Working…</div>
      </div>
    `;
    document.body.appendChild(wrap);
  }

  function showBusyOverlay(msg = "Deleting orphans…") {
    ensureBusyOverlay();
    const el = document.getElementById("page-busy");
    el.querySelector(".busy-msg").textContent = msg;
    el.style.display = "flex";
    document.body.style.overflow = "hidden";
  }

  function hideBusyOverlay() {
    const el = document.getElementById("page-busy");
    if (el) el.style.display = "none";
    document.body.style.overflow = "";
  }

  function setHeaderHeightVar() {
    const h = document.querySelector(".site-header");
    if (h)
      document.documentElement.style.setProperty(
        "--header-h",
        `${h.offsetHeight}px`
      );
  }
  setHeaderHeightVar();
  window.addEventListener("resize", setHeaderHeightVar, { passive: true });

  const runningClones = new Set(
    (() => {
      try {
        return JSON.parse(sessionStorage.getItem("bf:running") || "[]");
      } catch {
        return [];
      }
    })()
  );

  const launchingClones = new Set(
    (() => {
      try {
        return JSON.parse(sessionStorage.getItem("bf:launching") || "[]");
      } catch {
        return [];
      }
    })()
  );

  (function resetCloneStateImmediately() {
    runningClones.clear();
    launchingClones.clear();
    try {
      sessionStorage.setItem("bf:running", "[]");
      sessionStorage.setItem("bf:launching", "[]");
    } catch {}
    requestAnimationFrame(() => {
      document.querySelectorAll(".ch-card.is-cloning").forEach((card) => {
        card.classList.remove("is-cloning");
        card.removeAttribute("aria-busy");
        card.querySelector(".ch-status")?.remove();
      });
    });
  })();

  function setCloneLaunching(id, on) {
    const k = String(id);
    if (on) launchingClones.add(k);
    else launchingClones.delete(k);
    try {
      sessionStorage.setItem(
        "bf:launching",
        JSON.stringify([...launchingClones])
      );
    } catch {}
  }
  function cloneIsLocked(id) {
    const k = String(id);
    return launchingClones.has(k) || runningClones.has(k);
  }
  function cloneIsRunning(id) {
    return runningClones.has(String(id));
  }
  function setCloneRunning(id, on) {
    const k = String(id);
    if (on) runningClones.add(k);
    else runningClones.delete(k);
    try {
      sessionStorage.setItem("bf:running", JSON.stringify([...runningClones]));
    } catch {}
    const card = document.querySelector(`.ch-card[data-cid="${k}"]`);
    if (card) {
      card.classList.toggle("is-cloning", on);
      card.setAttribute("aria-busy", on ? "true" : "false");
    }
  }
  function unlockBackfill(id) {
    if (id == null) return;
    setCloneLaunching(id, false);
    setCloneRunning(id, false);
  }

  function attachPicker(input) {
    if (!input || input.closest(".bf-input-wrap")) return;
    const wrap = document.createElement("div");
    wrap.className = "bf-input-wrap";
    input.parentNode.insertBefore(wrap, input);
    wrap.appendChild(input);

    const btn = document.createElement("button");
    btn.type = "button";
    btn.className = "bf-cal-btn";
    btn.setAttribute("aria-label", "Open calendar");
    btn.innerHTML = `<svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor" aria-hidden="true">
      <path d="M7 2a1 1 0 0 1 1 1v1h8V3a1 1 0 1 1 2 0v1h1a2 2 0 0 1 2 2v12a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V6a2 2 0 0 1 2-2h1V3a1 1 0 0 1 1-1Zm12 8H5v8h14v-8ZM5 8h14V6H5v2Z"/>
    </svg>`;
    wrap.appendChild(btn);

    btn.addEventListener("click", (e) => {
      e.preventDefault();
      if (typeof input.showPicker === "function") {
        try {
          input.showPicker();
          return;
        } catch {}
      }
      input.focus();
    });
  }

  function resetAllCloningUI() {
    for (const id of [...runningClones]) setCardLoading(id, false);
    for (const id of [...launchingClones]) setCardLoading(id, false);
    runningClones.clear();
    launchingClones.clear();
    try {
      sessionStorage.setItem("bf:running", "[]");
      sessionStorage.setItem("bf:launching", "[]");
    } catch {}
    try {
      const key = `toast:persist:bf:stopped`;
      sessionStorage.setItem(
        key,
        JSON.stringify({ expiresAt: Date.now() + 10_000 })
      );
    } catch {}
  }

  async function load() {
    try {
      const chRes = await fetch("/api/channels");
      const chJson = await chRes.json();
      data = chJson.items || [];
      pinsByOrig = new Map();
      data = chJson.items || [];
      filtered = [...data];
      rebuildCategoryPinMaps(data);
      render();
    } catch (e) {
      console.error("Failed to load channels", e);
    }
  }

  function chTypeLabel(t) {
    const map = { 0: "Text", 2: "Voice", 5: "Announcements", 15: "Forum" };
    return map[t] || `Type ${t ?? "-"}`;
  }

  function normId(x) {
    return String(x);
  }

  function clearPendingByIds(ids) {
    const set = new Set((ids || []).map(normId));
    document.querySelectorAll(".ch-card.is-pending").forEach((card) => {
      if (set.has(String(card.dataset.cid))) {
        card.classList.remove("is-pending");
        card.removeAttribute("aria-busy");
      }
    });
  }

  function removeCardsByIds(ids) {
    const set = new Set((ids || []).map(normId));
    document.querySelectorAll(".ch-card").forEach((card) => {
      if (set.has(normId(card.dataset.cid))) card.remove();
    });
    document.querySelectorAll(".ch-section").forEach((sec) => {
      if (!sec.querySelector(".ch-card")) sec.remove();
    });
    const anyCardsLeft = !!document.querySelector(".ch-card");
    const anyOrphansLeft =
      (orph.categories?.length || 0) + (orph.channels?.length || 0) > 0;
    empty.hidden = anyCardsLeft || anyOrphansLeft;
  }

  function toggleDir() {
    sortDir = sortDir === "asc" ? "desc" : "asc";
    updateSortUI();
    render();
  }

  function updateSortUI() {
    if (!dirBtn) return;
    const az = sortDir === "asc";
    dirBtn.textContent = az ? "A–Z" : "Z–A";
    dirBtn.setAttribute("aria-pressed", (!az).toString());
    const nameOpt = sortSel?.querySelector('option[value="name"]');
    const catOpt = sortSel?.querySelector('option[value="category"]');
    const typeOpt = sortSel?.querySelector('option[value="type"]');
    if (nameOpt) nameOpt.textContent = `Name (${az ? "A–Z" : "Z–A"})`;
    if (catOpt) catOpt.textContent = `Category (${az ? "A–Z" : "Z–A"})`;
    if (typeOpt) typeOpt.textContent = `Type (${az ? "0–9" : "9–0"})`;
  }

  function groupByCategory(items) {
    const groups = new Map();
    for (const ch of items) {
      const key =
        (ch.category_name && ch.category_name.trim()) || UNGROUPED_LABEL;
      if (!groups.has(key)) groups.set(key, []);
      groups.get(key).push(ch);
    }
    return groups;
  }

  const normalize = (s) => {
    const v = String(s || "")
      .toLowerCase()
      .replace(/^#\s*/, "");
    try {
      return v.normalize("NFKD").replace(/\p{Diacritic}/gu, "");
    } catch {
      return v;
    }
  };

  function getOriginalCategoryFromRow(ch) {
    return String(
      ch.original_category_name ??
        ch.category_original_name ??
        ch.category_upstream_name ??
        ch.category_name ??
        ""
    ).trim();
  }

  function applyFilterAndSort() {
    const q = normalize(search.value);

    filtered = !q
      ? [...data]
      : data.filter((ch) => {
          const origCat = getOriginalCategoryFromRow(ch);
          const pinnedCat = origCat ? catPinByOrig.get(origCat) || "" : "";

          return (
            normalize(ch.original_channel_name).includes(q) ||
            normalize(ch.clone_channel_name).includes(q) ||
            normalize(ch.category_name).includes(q) ||
            normalize(origCat).includes(q) ||
            normalize(pinnedCat).includes(q) ||
            normalize(ch.original_channel_id).includes(q) ||
            normalize(ch.cloned_channel_id).includes(q)
          );
        });
  }

  function matches(str, q) {
    return normalize(str).includes(q);
  }

  function mergeOrphansIntoGroups(groups, q) {
    const orphanCats = Array.isArray(orph.categories) ? orph.categories : [];
    for (const c of orphanCats) {
      if (q && !matches(c.name, q)) continue;
      if (!groups.has(c.name)) groups.set(c.name, []);
      const arr = groups.get(c.name);
      arr.__orphanCategory = true;
      arr.__orphanCategoryId = c.id;
    }
    const catNameById = new Map();
    for (const c of orphanCats) {
      if (c?.id != null) {
        catNameById.set(String(c.id), c.name);
        const num = Number(c.id);
        if (!Number.isNaN(num)) catNameById.set(num, c.name);
      }
    }
    const orphanChs = Array.isArray(orph.channels) ? orph.channels : [];
    for (const ch of orphanChs) {
      const explicitName = (ch.category_name ?? "").trim();
      const catId =
        ch.parent_id ?? ch.category_id ?? ch.parentId ?? ch.categoryId ?? null;

      let catName =
        explicitName ||
        (catId != null ? catNameById.get(String(catId)) : null) ||
        UNGROUPED_LABEL;

      if (q && !(matches(ch.name, q) || matches(catName, q))) continue;

      if (!groups.has(catName)) groups.set(catName, []);
      const arr = groups.get(catName);

      arr.push({
        __orphan: true,
        __kind: "channel",
        original_channel_name: ch.name,
        original_channel_id: ch.id,
        channel_type: ch.type ?? 0,
        category_name: catName,
        cloned_channel_id: null,
      });
    }
    return groups;
  }

  function isUngroupedName(name) {
    return name === "— Ungrouped —";
  }

  function sortedGroups(groups) {
    function rank(name, arr) {
      if (isUngroupedName(name)) return 2;
      if (arr?.__orphanCategory) return 1;
      return 0;
    }
    const out = [...groups.entries()];
    out.sort(([aName, aArr], [bName, bArr]) => {
      const ar = rank(aName, aArr);
      const br = rank(bName, bArr);
      if (ar !== br) return ar - br;
      return aName.localeCompare(bName);
    });
    return out;
  }

  function compareCategoryNames(aName, bName) {
    const UN = UNGROUPED_LABEL;
    const aUng = aName === UN,
      bUng = bName === UN;
    if (aUng !== bUng) return aUng ? 1 : -1;
    return String(aName || "").localeCompare(String(bName || ""));
  }

  function makeChannelCmp(sortBy) {
    if (sortBy === "type") {
      return (a, b) => {
        const t = (a.channel_type || 0) - (b.channel_type || 0);
        if (t) return t;
        return (a.original_channel_name || "").localeCompare(
          b.original_channel_name || ""
        );
      };
    }
    return (a, b) =>
      (a.original_channel_name || "").localeCompare(
        b.original_channel_name || ""
      );
  }

  function normalizeCatName(name) {
    const s = String(name || "").trim();
    return s || UNGROUPED_LABEL;
  }
  function catKey(name) {
    const s = normalizeCatName(name);
    return s === UNGROUPED_LABEL ? `~~${s}` : s.toLowerCase();
  }

  function getRowComparator(mode) {
    if (mode === "type") {
      return (a, b) => {
        const ta = a.channel_type ?? 0,
          tb = b.channel_type ?? 0;
        if (ta !== tb) return ta - tb;
        const na = a.original_channel_name || "",
          nb = b.original_channel_name || "";
        if (na !== nb) return na.localeCompare(nb);
        return catKey(a.category_name).localeCompare(catKey(b.category_name));
      };
    }
    if (mode === "category") {
      return (a, b) => {
        const ca = catKey(a.category_name),
          cb = catKey(b.category_name);
        if (ca !== cb) return ca.localeCompare(cb);
        const na = a.original_channel_name || "",
          nb = b.original_channel_name || "";
        return na.localeCompare(nb);
      };
    }
    return (a, b) => {
      const na = a.original_channel_name || "",
        nb = b.original_channel_name || "";
      if (na !== nb) return na.localeCompare(nb);
      return catKey(a.category_name).localeCompare(catKey(b.category_name));
    };
  }

  function getSortMode() {
    const raw = (sortSel?.value || "name").toString().toLowerCase();
    if (raw.includes("cat")) return "category";
    if (raw.includes("type") || raw.includes("kind")) return "type";
    return "name";
  }

  function render() {
    applyFilterAndSort();
    root.innerHTML = "";

    const hasOrphans =
      (orph.categories?.length || 0) + (orph.channels?.length || 0) > 0;

    if (!filtered.length && !hasOrphans) {
      empty.hidden = false;
      return;
    }
    empty.hidden = true;

    const q = normalize(search.value || "");
    const sortMode = getSortMode();

    const groups = groupByCategory(filtered);
    const merged = mergeOrphansIntoGroups(groups, q);

    document.querySelectorAll(".ch-card.is-cloning").forEach((el) => {
      el.classList.remove("is-cloning");
      el.removeAttribute("aria-busy");
      el.querySelector(".ch-status")?.remove();
    });

    let entries = [...merged.entries()];
    if (sortMode === "category") {
      entries.sort(([aName], [bName]) => compareCategoryNames(aName, bName));
      if (sortDir === "desc") {
        const tail = entries.filter(([n]) => n === UNGROUPED_LABEL);
        const head = entries.filter(([n]) => n !== UNGROUPED_LABEL).reverse();
        entries = [...head, ...tail];
      }
    }

    const baseCmp = makeChannelCmp(sortMode);
    const cmp = (a, b) => (sortDir === "desc" ? -baseCmp(a, b) : baseCmp(a, b));

    const filterMode = (filterSel?.value || "all").toLowerCase();

    for (const [cat, chans] of entries) {
      const items = Array.from(chans)
        .filter((row) => {
          if (filterMode === "orphans") return !!row.__orphan;
          if (filterMode === "nonorphans") return !row.__orphan;
          return true;
        })
        .sort(cmp);

      const isOrphanCategory =
        !!chans.__orphanCategory && cat !== UNGROUPED_LABEL;
      const orphanCatId = isOrphanCategory ? chans.__orphanCategoryId : null;

      if (!items.length && !isOrphanCategory) continue;

      const section = document.createElement("section");
      section.className = "ch-section";

      const resolvedOrig =
        catOrigByEither.get(String(cat).toLowerCase()) || cat;
      const pin = catPinByOrig.get(resolvedOrig);
      const isCustom = !!(pin && pin.trim() && pin !== resolvedOrig);
      const displayCat = isCustom ? pin : resolvedOrig;
      const tooltip = tooltipForCategory(resolvedOrig, pin);

      section.innerHTML = `
      <div class="ch-section-head">
        <h3 class="ch-section-title ${isOrphanCategory ? "orphan-title" : ""}">
          <span
         class="badge cat-chip ${
           isOrphanCategory
             ? "badge-orphan"
             : isCustom
             ? "badge-custom"
             : "good"
         } ${isCustom ? "is-custom" : ""}"
            ${
              isOrphanCategory
                ? `data-orphan-cat-id="${escapeAttr(
                    orphanCatId
                  )}" data-cat-name="${escapeAttr(resolvedOrig)}"`
                : `data-cat-name="${escapeAttr(resolvedOrig)}"`
            }
            ${tooltip ? `title="${escapeAttr(tooltip)}"` : ""}
          >
            ${escapeHtml(displayCat)}
            <button class="cat-menu-trigger" aria-haspopup="menu" aria-controls="ch-menu" aria-label="Category menu" type="button">⋯</button>
          </span>
        </h3>
      </div>
      <div class="ch-cards"></div>
    `;
      const grid = section.querySelector(".ch-cards");

      for (const ch of items) {
        const isOrphanChannel = !!ch.__orphan;

        const card = document.createElement("div");
        card.className = `ch-card${isOrphanChannel ? " orphan" : ""}`;
        card.tabIndex = 0;
        card.dataset.cid = ch.original_channel_id;

        if (isOrphanChannel) {
          card.dataset.orphan = "1";
          card.dataset.kind = "channel";
        }
        const isCustomized = !!(
          ch.clone_channel_name && String(ch.clone_channel_name).trim()
        );

        const displayName = isCustomized
          ? ch.clone_channel_name
          : ch.original_channel_name;
        const tip = tooltipForChannel(
          ch.original_channel_name,
          ch.clone_channel_name
        );

        const type = chTypeLabel(ch.channel_type);

        const cloneChip = ch.cloned_channel_id
          ? `<span class="badge good" title="Part of the host servers structure">Clone</span>${
              isCustomized
                ? ` <span class="badge badge-custom" title="Customized channel">Custom</span>`
                : ""
            }`
          : "";

        card.innerHTML = `
        <div class="ch-head">
          <div class="ch-name">
            <span class="ch-display-name ${isCustomized ? "is-custom" : ""}"
              ${
                tip
                  ? `title="${escapeAttr(tip)}"`
                  : `title="${escapeAttr(ch.original_channel_name)}"`
              }
            >
              # ${escapeHtml(displayName)}
            </span>
          </div>
          <div class="ch-top-right">
            <button class="icon-btn ch-menu-btn" aria-haspopup="menu" aria-controls="ch-menu" aria-label="Channel menu">⋯</button>
          </div>
        </div>
        <div class="ch-meta">
          <span class="badge muted" title="Channel type">${type}</span>
          ${
            isOrphanChannel
              ? `<span class="badge badge-orphan">Orphan</span>`
              : cloneChip
          }
        </div>
        <div class="ch-ids">
          <span title="Original channel ID">${ch.original_channel_id}</span>
          ${
            ch.cloned_channel_id
              ? `<span class="muted" title="Cloned channel ID">→ ${ch.cloned_channel_id}</span>`
              : ""
          }
        </div>
      `;
        if (cloneIsRunning(ch.original_channel_id)) {
          card.classList.add("is-cloning");
          card.setAttribute("aria-busy", "true");
          upsertStatusPill(card, "Cloning…");
        }
        grid.appendChild(card);
      }

      root.appendChild(section);
    }
  }

  function showMenu(btn, ctx) {
    const legacyIsChannel = typeof ctx === "string" || typeof ctx === "number";
    if (legacyIsChannel) ctx = { type: "channel", id: String(ctx) };

    if (menuAnchorBtn && menuAnchorBtn !== btn) {
      menuAnchorBtn.setAttribute("aria-expanded", "false");
    }
    menuAnchorBtn = btn;
    menuAnchorBtn.setAttribute("aria-expanded", "true");

    menuContext = ctx;
    menu.hidden = false;

    menu.classList.add("customize-skin");

    const isChannel = ctx.type === "channel";
    const isCategory = ctx.type === "category";
    const isOrphanCat = ctx.type === "orphan-cat";
    const cloneItem = menu.querySelector('[data-action="clone"]');
    menuForId = isChannel ? String(ctx.id) : null;

    let customizeItem = menu.querySelector('[data-act="customize"]');
    if (!customizeItem) {
      customizeItem = document.createElement("button");
      customizeItem.className = "ctxmenu-item";
      customizeItem.dataset.act = "customize";
      customizeItem.role = "menuitem";
      customizeItem.type = "button";
      customizeItem.textContent = "Customize";
      menu.insertBefore(customizeItem, menu.firstChild);
    }

    let customizeCatItem = menu.querySelector(
      '[data-act="customize-category"]'
    );
    if (!customizeCatItem) {
      customizeCatItem = document.createElement("button");
      customizeCatItem.className = "ctxmenu-item";
      customizeCatItem.dataset.act = "customize-category";
      customizeCatItem.role = "menuitem";
      customizeCatItem.type = "button";
      customizeCatItem.textContent = "Customize category";

      const after = menu.querySelector('[data-act="customize"]');
      if (after?.nextSibling)
        menu.insertBefore(customizeCatItem, after.nextSibling);
      else menu.insertBefore(customizeCatItem, menu.firstChild);
    }

    customizeCatItem.hidden = !(isCategory && !isOrphanCat);

    let delOrphanChItem = menu.querySelector('[data-act="delete-orphan"]');
    if (!delOrphanChItem) {
      delOrphanChItem = document.createElement("button");
      delOrphanChItem.className = "ctxmenu-item";
      delOrphanChItem.dataset.act = "delete-orphan";
      delOrphanChItem.role = "menuitem";
      delOrphanChItem.type = "button";
      delOrphanChItem.textContent = "Delete orphan";
      menu.appendChild(delOrphanChItem);
    }

    let delOrphanCatItem = menu.querySelector('[data-act="delete-orphan-cat"]');
    if (!delOrphanCatItem) {
      delOrphanCatItem = document.createElement("button");
      delOrphanCatItem.className = "ctxmenu-item";
      delOrphanCatItem.dataset.act = "delete-orphan-cat";
      delOrphanCatItem.role = "menuitem";
      delOrphanCatItem.type = "button";
      delOrphanCatItem.textContent = "Delete orphan category";
      menu.appendChild(delOrphanCatItem);
    }

    if (cloneItem) {
      const isLocked = isChannel && cloneIsLocked(ctx.id);
      const hideClone = !isChannel;
      cloneItem.hidden = hideClone;
      cloneItem.setAttribute("aria-hidden", hideClone ? "true" : "false");
      cloneItem.disabled = hideClone || isLocked;
      cloneItem.setAttribute(
        "aria-disabled",
        cloneItem.disabled ? "true" : "false"
      );
      cloneItem.title = hideClone
        ? ""
        : isLocked
        ? "Backfill still in progress"
        : "Clone messages";
      cloneItem.classList.toggle("is-disabled", cloneItem.disabled);
    }

    if (isChannel) {
      const ch = (filtered || data || []).find(
        (c) => String(c.original_channel_id) === String(ctx.id)
      );
      const isClone = !!(ch && ch.cloned_channel_id);
      customizeItem.hidden = !isClone;
    } else {
      customizeItem.hidden = true;
    }

    delOrphanChItem.hidden = true;
    if (isChannel) {
      const card = document.querySelector(`.ch-card[data-cid="${ctx.id}"]`);
      const isOrphanChannel = card?.dataset.orphan === "1";
      delOrphanChItem.hidden = !isOrphanChannel;
      if (!delOrphanChItem.hidden) delOrphanChItem.dataset.kind = "channel";
    }

    delOrphanCatItem.hidden = !isOrphanCat;

    menu.style.position = "fixed";
    const gap = 6,
      pad = 12,
      vw = window.innerWidth,
      vh = window.innerHeight;
    const maxH = Math.max(160, Math.min(360, vh - 2 * pad));
    menu.style.maxHeight = `${maxH}px`;
    menu.style.overflowY = "auto";

    const r = btn.getBoundingClientRect();
    const mw = menu.offsetWidth || 180;
    const mh = menu.offsetHeight;

    let top = r.bottom + gap;
    let left = Math.min(r.left, vw - mw - pad);
    if (vh - r.bottom < mh && r.top > vh - r.bottom) {
      top = r.top - gap - mh;
    }
    top = Math.max(pad, Math.min(top, vh - mh - pad));
    left = Math.max(pad, Math.min(left, vw - mw - pad));

    menu.style.top = `${Math.round(top)}px`;
    menu.style.left = `${Math.round(left)}px`;
    menu.style.transformOrigin = top < r.top ? "bottom left" : "top left";

    menu.setAttribute("tabindex", "-1");
    menu.focus({ preventScroll: true });
  }

  function hideMenu({ restoreFocus = false } = {}) {
    menu.hidden = true;
    menuContext = null;
    menu.classList.remove("customize-skin");
    if (menuAnchorBtn) {
      menuAnchorBtn.setAttribute("aria-expanded", "false");
      if (restoreFocus) menuAnchorBtn.focus();
      menuAnchorBtn = null;
    }
  }

  root.addEventListener("click", (e) => {
    const btn = e.target.closest(".ch-menu-btn");
    if (!btn) return;
    const card = btn.closest(".ch-card");
    const cid = card?.dataset.cid;
    const isOpenForThis =
      !menu.hidden &&
      menuContext &&
      menuContext.type === "channel" &&
      menuContext.id === cid;
    if (isOpenForThis) {
      hideMenu({ restoreFocus: false });
    } else {
      showMenu(btn, { type: "channel", id: cid });
    }
    e.stopPropagation();
  });

  root.addEventListener("click", (e) => {
    const btn = e.target.closest(".cat-menu-trigger");
    if (!btn) return;

    const chip = btn.closest(".cat-chip");
    const orphanCatId = chip?.dataset.orphanCatId || null;
    const catName =
      chip?.dataset.catName || chip?.textContent?.trim() || "Category";

    const ctx = orphanCatId
      ? { type: "orphan-cat", id: String(orphanCatId), name: catName }
      : { type: "category", id: null, name: catName };

    const isOpenForThis =
      !menu.hidden &&
      menuContext &&
      ((ctx.type === "orphan-cat" &&
        menuContext.type === "orphan-cat" &&
        menuContext.id === ctx.id) ||
        (ctx.type === "category" &&
          menuContext.type === "category" &&
          menuContext.name === ctx.name));

    if (isOpenForThis) {
      hideMenu({ restoreFocus: false });
    } else {
      showMenu(btn, ctx);
    }

    e.stopPropagation();
  });

  if (sortSel) {
    sortSel.addEventListener("change", () => {
      const next = (sortSel.value || "name").toLowerCase();
      if (next !== sortBy) {
        sortBy = next;
        sortDir = "asc";
      } else {
        sortDir = sortDir === "asc" ? "desc" : "asc";
      }
      updateSortUI();
      render();
    });
  }
  if (dirBtn) dirBtn.addEventListener("click", toggleDir);
  if (search) search.addEventListener("input", render);
  if (filterSel) {
    filterSel.addEventListener("change", render);
    filterSel.addEventListener("input", render);
  }

  document.addEventListener("click", (e) => {
    if (!menu.hidden && !e.target.closest("#ch-menu")) hideMenu();
  });
  document.addEventListener("keydown", (e) => {
    if (e.key === "Escape") hideMenu();
  });

  const closeMenuOnScroll = (e) => {
    if (menu.hidden) return;
    const path = (e.composedPath && e.composedPath()) || [];
    const insideMenu = path.includes?.(menu) || menu.contains(e.target);
    if (insideMenu) return;
    if (closeMenuOnScroll._raf) cancelAnimationFrame(closeMenuOnScroll._raf);
    closeMenuOnScroll._raf = requestAnimationFrame(() => {
      hideMenu({ restoreFocus: false });
    });
  };
  window.addEventListener("scroll", closeMenuOnScroll, { passive: true });
  window.addEventListener("resize", () => hideMenu({ restoreFocus: false }), {
    passive: true,
  });
  document.addEventListener("wheel", closeMenuOnScroll, { passive: true });
  document.addEventListener("touchmove", closeMenuOnScroll, { passive: true });
  document.addEventListener("click", (e) => {
    if (
      !menu.hidden &&
      !e.target.closest("#ch-menu") &&
      !e.target.closest(".ch-menu-btn")
    ) {
      hideMenu({ restoreFocus: false });
    }
  });

  document.addEventListener("click", (e) => {
    const btn = e.target.closest(".orphan-cat-del");
    if (!btn) return;
    const badge = btn.closest("[data-orphan-cat-id]");
    const catId = badge?.dataset.orphanCatId;
    const catName = badge?.dataset.catName || "Category";
    if (!catId) return;

    openConfirm(
      {
        title: "Delete orphan category?",
        body: `This will delete <b>${escapeHtml(
          catName
        )}</b> <span class="muted">(${escapeHtml(catId)})</span>.`,
        okText: "Delete",
      },
      () => {
        markPending(catId);
        sessionStorage.removeItem(LAST_DELETED_SIG_KEY);
        sendVerify({ action: "delete_one", kind: "category", id: catId });
      }
    );
  });

  menu.addEventListener("click", (e) => {
    const act = e.target.closest(".ctxmenu-item")?.dataset.act;
    if (!act) return;

    if (act === "customize") {
      e.preventDefault();
      const id = menuForId;
      const ch = (filtered || data || []).find(
        (c) => String(c.original_channel_id) === String(id)
      );
      if (!ch || !ch.cloned_channel_id) {
        window.showToast("Customize is only available for cloned channels.", {
          type: "warning",
        });
        return;
      }
      hideMenu({ restoreFocus: false });
      openCustomizeDialog({
        original_channel_id: ch.original_channel_id,
        original_channel_name: ch.original_channel_name,
        clone_channel_name: ch.clone_channel_name || null,
      });
      return;
    }

    if (act === "customize-category") {
      e.preventDefault();
      const ctx = menuContext;
      if (!ctx || ctx.type !== "category" || !ctx.name) {
        hideMenu();
        window.showToast("This item is not a regular category.", {
          type: "warning",
        });
        return;
      }
      hideMenu({ restoreFocus: false });
      openCustomizeCategoryDialog(ctx.name);
      return;
    }

    if (act === "delete-orphan-cat") {
      e.preventDefault();
      const ctx = menuContext;
      if (!ctx || ctx.type !== "orphan-cat" || !ctx.id) {
        hideMenu();
        window.showToast("This item is not an orphan category.", {
          type: "warn",
        });
        return;
      }

      const catId = ctx.id;
      const catName = ctx.name || "Category";

      hideMenu();

      openConfirm(
        {
          title: "Delete orphan category?",
          body: `This will delete <b>${escapeHtml(
            catName
          )}</b> <span class="muted">(${escapeHtml(catId)})</span>.`,
          okText: "Delete",
        },
        () => {
          markPending(catId);
          sessionStorage.removeItem(LAST_DELETED_SIG_KEY);

          sendVerify({ action: "delete_one", kind: "category", id: catId });
        }
      );
      return;
    }
  });

  if (!gate.lastUpIsFresh()) resetAllCloningUI();

  // Kick off the gate polling; when ready we'll finish boot.
  gate.checkAndGate(() => afterGateReady());

  let bootedAfterGate = false;
  async function afterGateReady() {
    if (bootedAfterGate) return;
    bootedAfterGate = true;

    clearBackfillBootResidue();

    ensureIn();
    ensureOut();
    sendVerify({ action: "list" });
    await load();
  }

  document.getElementById("orph-delall")?.addEventListener("click", () => {
    const catCount = orph.categories?.length || 0;
    const chCount = orph.channels?.length || 0;
    const ids = [
      ...(orph.categories || []).map((c) => c.id),
      ...(orph.channels || []).map((c) => c.id),
    ];
    if (!ids.length) return;

    openConfirm(
      {
        title: "Delete all orphans?",
        body: `This will delete <b>${catCount}</b> orphan ${
          catCount === 1 ? "category" : "categories"
        } and <b>${chCount}</b> orphan ${
          chCount === 1 ? "channel" : "channels"
        } that are <em>not part of the original structure</em>.`,
        okText: "Delete all",
      },
      () => {
        ids.forEach((id) => markPending(id));
        sessionStorage.removeItem(LAST_DELETED_SIG_KEY);
        bulkDeleteInFlight = true;
        showBusyOverlay(
          `Deleting ${catCount} categor${
            catCount === 1 ? "y" : "ies"
          } & ${chCount} channel${chCount === 1 ? "" : "s"}…`
        );
        sendVerify({ action: "delete_all", ids });
      }
    );
  });

  vDelAll?.addEventListener("click", () => {
    const catCount = orph.categories?.length || 0;
    const chCount = orph.channels?.length || 0;
    const ids = [
      ...(orph.categories || []).map((c) => c.id),
      ...(orph.channels || []).map((c) => c.id),
    ];
    if (!ids.length) return;

    openConfirm(
      {
        title: "Delete all orphans?",
        body: `This will delete <b>${catCount}</b> orphan ${
          catCount === 1 ? "category" : "categories"
        } and <b>${chCount}</b> orphan ${
          chCount === 1 ? "channel" : "channels"
        } that are <em>not part of the clone</em>.`,
        okText: "Delete all",
      },
      () => {
        ids.forEach((id) => markPending(id));
        sessionStorage.removeItem(LAST_DELETED_SIG_KEY);
        bulkDeleteInFlight = true;
        showBusyOverlay(
          `Deleting ${catCount} categor${
            catCount === 1 ? "y" : "ies"
          } & ${chCount} channel${chCount === 1 ? "" : "s"}…`
        );
        sendVerify({ action: "delete_all", ids });
      }
    );
  });

  function openVerify() {
    lastFocusVerify = document.activeElement;

    vDlg.classList.add("compact");
    vBack.hidden = false;

    setInert(vDlg, false);
    vDlg.removeAttribute("aria-hidden");
    vDlg.hidden = false;
    vDlg.classList.add("show");
    setTimeout(() => vDlg.focus(), 0);

    ensureIn();
    ensureOut();
  }

  function closeVerify() {
    blurIfInside(vDlg);

    vBack.hidden = true;

    setInert(vDlg, true);
    vDlg.setAttribute("aria-hidden", "true");
    vDlg.hidden = true;
    vDlg.classList.remove("show");

    if (lastFocusVerify && typeof lastFocusVerify.focus === "function") {
      try {
        lastFocusVerify.focus();
      } catch {}
    }
  }

  const cModal = document.getElementById("confirm-modal");
  const cTitle = document.getElementById("confirm-title");
  const cBody = document.getElementById("confirm-body");
  const cOk = document.getElementById("confirm-okay");
  const cCancel = document.getElementById("confirm-cancel");
  const cClose = document.getElementById("confirm-close");
  const cBackdrop = cModal?.querySelector(".modal-backdrop");

  function openConfirm(
    { title = "Confirm", body = "Are you sure?", okText = "Delete" },
    onConfirm
  ) {
    if (!cModal) {
      onConfirm?.();
      return;
    }

    cTitle.textContent = title;
    cBody.innerHTML = body;
    cOk.textContent = okText;

    lastFocusConfirm = document.activeElement;

    setInert(cModal, false);
    cModal.removeAttribute("aria-hidden");
    cModal.classList.add("show");
    setTimeout(() => cOk.focus(), 0);

    const close = () => {
      blurIfInside(cModal);

      setInert(cModal, true);
      cModal.setAttribute("aria-hidden", "true");
      cModal.classList.remove("show");

      if (lastFocusConfirm && typeof lastFocusConfirm.focus === "function") {
        try {
          lastFocusConfirm.focus();
        } catch {}
      }
      teardown();
    };

    const onOk = () => {
      onConfirm?.();
      close();
    };
    const onEsc = (e) => {
      if (e.key === "Escape") close();
    };
    const onBackdrop = (e) => {
      if (e.target === cBackdrop) close();
    };

    function teardown() {
      cOk.removeEventListener("click", onOk);
      cCancel?.removeEventListener("click", close);
      cClose?.removeEventListener("click", close);
      cBackdrop?.removeEventListener("click", onBackdrop);
      document.removeEventListener("keydown", onEsc);
    }

    cOk.addEventListener("click", onOk, { once: true });
    cCancel?.addEventListener("click", close, { once: true });
    cClose?.addEventListener("click", close, { once: true });
    cBackdrop?.addEventListener("click", onBackdrop);
    document.addEventListener("keydown", onEsc);
  }

  function sendClient(payload) {
    ensureIn();
    const env = {
      kind: "client",
      role: "ui",
      action: payload?.action || undefined,
      data: payload || undefined,
      payload: payload || undefined,
    };
    const json = JSON.stringify(env);
    const sock = wsIn;

    if (payload?.action === "backfill") {
      const orig = String(
        bfChannelId ||
          payload.clone_channel_id ||
          payload.original_channel_id ||
          payload.channel_id ||
          ""
      );
      if (orig && (launchingClones.has(orig) || runningClones.has(orig))) {
        window.showToast("A clone for this channel is already in progress.", {
          type: "warning",
        });
        closeBackfillDialog();
        return false;
      }
      if (orig) setCloneLaunching(orig, true);
    }

    if (sock?.readyState === WebSocket.OPEN) {
      console.debug("WS → client", env);
      sock.send(json);
      return true;
    } else if (sock) {
      sock.addEventListener(
        "open",
        () => {
          if (sock.readyState === WebSocket.OPEN) sock.send(json);
        },
        { once: true }
      );
      return true;
    } else {
      console.warn("WS IN not ready, cannot send", env);
      window.showToast("Connection is not ready.", { type: "error" });
      return false;
    }
  }

  function ensureIn() {
    if (
      wsIn &&
      (wsIn.readyState === WebSocket.OPEN ||
        wsIn.readyState === WebSocket.CONNECTING)
    )
      return;
    const url = location.origin.replace(/^http/, "ws") + "/ws/in";
    const sock = new WebSocket(url);
    wsIn = sock;
    sock.onopen = () => console.debug("WS IN connected");
    sock.onclose = () => console.debug("WS IN closed");
    sock.onerror = (e) => console.debug("WS IN error", e);
  }

  function ensureOut() {
    if (
      wsOut &&
      (wsOut.readyState === WebSocket.OPEN ||
        wsOut.readyState === WebSocket.CONNECTING)
    )
      return;
    const url = location.origin.replace(/^http/, "ws") + "/ws/out";
    const sock = new WebSocket(url);
    wsOut = sock;
    sock.onopen = () => console.debug("WS OUT connected");
    sock.onclose = () => {
      console.debug("WS OUT closed");
      resetAllCloningUI("ws_out_closed");
    };
    sock.onerror = (e) => {
      console.debug("WS OUT error", e);
      resetAllCloningUI("ws_out_error");
    };

    function getResultId(r) {
      return (
        r?.id ??
        r?.channel_id ??
        r?.category_id ??
        r?.target_id ??
        r?.target?.id ??
        r?.orphan_id ??
        r?.original_id ??
        r?.channel?.id ??
        null
      );
    }
    function isActuallyDeleted(r) {
      const s = String(r?.status || "").toLowerCase();
      return (
        r?.deleted === true ||
        r?.ok === true ||
        r?.success === true ||
        s === "deleted" ||
        s === "ok"
      );
    }
    function asIdString(v) {
      if (v == null) return null;
      if (typeof v === "string" && v.trim()) return v.trim();
      if (typeof v === "number")
        return Number.isSafeInteger(v) ? String(v) : null;
      if (typeof v === "bigint") return v.toString();
      return null;
    }
    function backfillIdFrom(x) {
      if (!x) return null;
      const candidates = [
        x.channel_id,
        x.original_channel_id,
        x.clone_channel_id,
        x.target_id,
        x.channel?.id,
        x.target?.id,
      ];
      for (const v of candidates) {
        const s = asIdString(v);
        if (s) return s;
      }
      return null;
    }

    wsOut.onmessage = (ev) => {
      try {
        const raw = JSON.parse(ev.data);
        const p = raw?.payload ?? raw;
        const kind = raw?.kind ?? p?.kind ?? "client";
        if (!p) return;
        const t = p?.type;

        if (kind === "client") {
          if (
            t === "backfill_started" ||
            t === "backfill_ack" ||
            t === "backfill_busy"
          ) {
            const cid =
              backfillIdFrom(p.data) || backfillIdFrom(p) || bfChannelId;
            if (p.task_id) rememberTask(p.task_id, cid || bfChannelId);
            if (cid) {
              setCloneLaunching(cid, false);
              setCloneRunning(cid, true);
              setCardLoading(cid, true, "Cloning…");
            }
            const weLaunchedThis = cid && launchingClones.has(String(cid));
            if (shouldAnnounceNow() && weLaunchedThis) {
              window.showToast(
                t === "backfill_busy"
                  ? "A clone for this channel is already running."
                  : "Clone started…",
                { type: "info" }
              );
            }
            closeBackfillDialog();
            return;
          }

          if (t === "backfill_progress") {
            const d = p.data || {};
            const delivered = d.delivered ?? d.count ?? 0;
            const total = d.total ?? undefined;
            const cid =
              backfillIdFrom(p.data) || backfillIdFrom(p) || bfChannelId;
            if (cid) setCardLoading(cid, true, "Cloning…");
            console.debug("[backfill] progress", { delivered, total });
            return;
          }

          if (t === "backfill_done") {
            let cid = backfillIdFrom(p.data) || backfillIdFrom(p);
            if (!cid && p.task_id) cid = taskMap.get(String(p.task_id));
            if (!cid && bfChannelId) cid = String(bfChannelId);
            if (p.task_id) forgetTask(p.task_id);

            if (cid) {
              unlockBackfill(cid);
              setCardLoading(cid, false);
            } else {
              console.warn(
                "[backfill_done] Could not resolve channel id; leaving locks as-is.",
                p
              );
            }

            const wasCancelled =
              (cid && cancelledThisSession.has(String(cid))) ||
              !!sessionStorage.getItem(`bf:cancelled:${cid}`);
            if (cid)
              try {
                sessionStorage.removeItem(`bf:cancelled:${cid}`);
              } catch {}

            if (!wasCancelled && shouldAnnounceNow()) {
              toastOncePersist(
                `bf:done:${cid}`,
                "Clone completed.",
                { type: "success" },
                15000
              );
            }

            render();
            return;
          }

          if (t === "backfill_cancelled") {
            let cid = backfillIdFrom(p.data) || backfillIdFrom(p);
            if (!cid && p.task_id) cid = taskMap.get(String(p.task_id));
            if (!cid && bfChannelId) cid = String(bfChannelId);
            if (p.task_id) forgetTask(p.task_id);

            if (cid) {
              unlockBackfill(cid);
              setCardLoading?.(cid, false);
              cancelledThisSession.add(String(cid));
              try {
                sessionStorage.setItem(
                  `bf:cancelled:${cid}`,
                  String(Date.now())
                );
              } catch {}
            } else {
              console.warn(
                "[backfill_cancelled] Could not resolve channel id; leaving locks as-is.",
                p
              );
            }

            const reason = String(p?.data?.reason || p?.reason || "")
              .toLowerCase()
              .trim();
            const msg =
              reason === "server_shutdown"
                ? "Clone cancelled: server is shutting down."
                : reason === "user_cancelled"
                ? "Clone cancelled."
                : reason
                ? `Clone cancelled: ${reason}.`
                : "Clone cancelled.";

            if (shouldAnnounceNow()) {
              toastOncePersist(
                `bf:cancel:${cid || "unknown"}`,
                msg,
                { type: "warning" },
                15000
              );
            }

            render();
            return;
          }
        }

        if (kind === "verify") {
          if (p.type === "orphans") {
            orph.categories = Array.isArray(p.categories) ? p.categories : [];
            orph.channels = Array.isArray(p.channels) ? p.channels : [];
            renderOrphans();
            render();
            delAllBtn?.toggleAttribute(
              "disabled",
              !((orph.categories?.length || 0) + (orph.channels?.length || 0))
            );
            return;
          }

          if (p.type === "deleted") {
            if (Array.isArray(p.results)) {
              const allIds = p.results
                .map((r) => getResultId(r))
                .filter(Boolean);
              const deletedIds = p.results
                .filter(isActuallyDeleted)
                .map((r) => getResultId(r))
                .filter(Boolean);
              const deletedSet = new Set(deletedIds.map(normId));

              const sig = makeDeletedSig(p.results);
              const prevSig = sessionStorage.getItem(LAST_DELETED_SIG_KEY);
              const isReplay = !!sig && sig === prevSig;
              if (sig) sessionStorage.setItem(LAST_DELETED_SIG_KEY, sig);

              const batchToastSeen = new Set();
              for (const r of p.results) {
                const idKey = normId(getResultId(r));
                const name =
                  r?.name ?? r?.channel_name ?? r?.category_name ?? "Item";
                const initiatedHere = pendingDeletes.has(idKey);
                if (initiatedHere) pendingDeletes.delete(idKey);

                const timeOk =
                  !!lastDeleteAt &&
                  Date.now() - lastDeleteAt < RECENT_DELETE_WINDOW_MS;
                if (!(initiatedHere || timeOk)) continue;

                if (isActuallyDeleted(r)) {
                  const k = `ok:${idKey}`;
                  if (!batchToastSeen.has(k)) {
                    window.showToast(`Deleted "${name}"`, { type: "good" });
                    batchToastSeen.add(k);
                  }
                } else {
                  const reason = r?.reason ?? "unknown";
                  const msgTxt =
                    reason === "protected"
                      ? `"${name}" can't be deleted. Manual action required.`
                      : reason === "not_found"
                      ? `"${name}" was not found.`
                      : reason === "not_category" || reason === "not_channel"
                      ? `"${name}" could not be deleted (wrong type).`
                      : `Failed to delete "${name}".`;
                  const variant =
                    reason === "protected" ||
                    reason === "not_found" ||
                    String(reason).startsWith("not_")
                      ? "warn"
                      : "danger";
                  const k = `reason:${idKey}:${reason}`;
                  if (!batchToastSeen.has(k)) {
                    window.showToast(msgTxt, { type: variant });
                    batchToastSeen.add(k);
                  }
                }
              }

              if (deletedIds.length) {
                orph.categories = (orph.categories || []).filter(
                  (x) => !deletedSet.has(normId(x.id))
                );
                orph.channels = (orph.channels || []).filter(
                  (x) => !deletedSet.has(normId(x.id))
                );
                removeCardsByIds(deletedIds);
              }

              if (allIds.length) clearPendingByIds(allIds);

              renderOrphans();
              render();
              delAllBtn?.toggleAttribute(
                "disabled",
                !((orph.categories?.length || 0) + (orph.channels?.length || 0))
              );

              sendVerify({ action: "list" });
              if (bulkDeleteInFlight) {
                bulkDeleteInFlight = false;
                hideBusyOverlay();
              }
              return;
            }

            if (Array.isArray(p.ids)) {
              let initiatedAny = p.ids.some((id) =>
                pendingDeletes.has(normId(id))
              );
              const timeOk =
                !!lastDeleteAt &&
                Date.now() - lastDeleteAt < RECENT_DELETE_WINDOW_MS;
              if (!initiatedAny && timeOk) initiatedAny = true;

              p.ids.forEach((id) => pendingDeletes.delete(normId(id)));

              clearPendingByIds(p.ids);
              removeCardsByIds(p.ids);
              renderOrphans();
              render();
              delAllBtn?.toggleAttribute(
                "disabled",
                !((orph.categories?.length || 0) + (orph.channels?.length || 0))
              );

              if (initiatedAny) {
                window.showToast(`Deleted ${p.ids.length} item(s).`, {
                  type: "good",
                });
              }

              sendVerify({ action: "list" });
              return;
            }
          }
        }
      } catch (e) {
        console.debug("WS parse failed", e);
      }
    };
  }

  function sendVerify(payload) {
    ensureIn();
    const env = { kind: "verify", role: "ui", payload };
    const json = JSON.stringify(env);
    const sock = wsIn;

    if (sock?.readyState === WebSocket.OPEN) {
      console.debug("WS → verify", env);
      sock.send(json);
    } else if (sock) {
      sock.addEventListener(
        "open",
        () => {
          if (sock.readyState === WebSocket.OPEN) {
            console.debug("WS open, sending → verify", env);
            sock.send(json);
          }
        },
        { once: true }
      );
    } else {
      console.warn("WS IN not ready, cannot send", env);
    }
  }

  function renderOrphans() {
    const cats = orph.categories || [];
    const chs = orph.channels || [];
    vCats.innerHTML = "";
    vChs.innerHTML = "";

    if (!cats.length && !chs.length) {
      vStatus.textContent =
        "All channels and categories match the last sitemap.";
      vDelAll.disabled = true;
      return;
    }
    vStatus.textContent = `Found ${cats.length} orphan ${
      cats.length === 1 ? "category" : "categories"
    } and ${chs.length} orphan ${chs.length === 1 ? "channel" : "channels"}.`;
    vDelAll.disabled = false;

    for (const c of cats) {
      const pill = document.createElement("div");
      pill.className = "pill";
      pill.dataset.orphanId = c.id;
      pill.innerHTML = `<span>📂 ${c.name} <span class="muted">(${c.id})</span></span>
                        <button class="kill" aria-label="Delete category ${c.name}">Delete</button>`;
      pill.querySelector(".kill").onclick = () => {
        markPending(c.id);
        sessionStorage.removeItem(LAST_DELETED_SIG_KEY);
        sendVerify({ action: "delete_one", kind: "category", id: c.id });
      };
      vCats.appendChild(pill);
    }

    for (const ch of chs) {
      const pill = document.createElement("div");
      pill.className = "pill";
      pill.dataset.orphanId = ch.id;
      pill.innerHTML = `<span># ${escapeHtml(
        ch.name
      )} <span class="muted">(${escapeHtml(ch.id)})</span></span>
                        <button class="kill" type="button" aria-label="Delete channel ${escapeAttr(
                          ch.name
                        )}">Delete</button>`;
      pill.querySelector(".kill").onclick = () => {
        openConfirm(
          {
            title: "Delete orphan channel?",
            body: `This will delete <b>${escapeHtml(
              ch.name
            )}</b> <span class="muted">(${escapeHtml(ch.id)})</span>.`,
            okText: "Delete",
          },
          () => {
            markPending(ch.id);
            sessionStorage.removeItem(LAST_DELETED_SIG_KEY);
            sendVerify({ action: "delete_one", kind: "channel", id: ch.id });
          }
        );
      };
      vChs.appendChild(pill);
    }
  }

  let bfChannelId = null;

  function fmtYYYYMMDD(d) {
    const y = d.getFullYear();
    const m = String(d.getMonth() + 1).padStart(2, "0");
    const day = String(d.getDate()).padStart(2, "0");
    return `${y}-${m}-${day}`;
  }
  function startOfDayIsoLocal(dateStr) {
    return `${dateStr}T00:00`;
  }
  function nextDayStartIsoLocal(dateStr) {
    const d = new Date(`${dateStr}T00:00`);
    d.setDate(d.getDate() + 1);
    return `${fmtYYYYMMDD(d)}T00:00`;
  }

  function parseLocalDate(dateStr) {
    if (!dateStr) return null;
    const [y, m, d] = dateStr.split("-").map((x) => Number.parseInt(x, 10));
    if (!y || !m || !d) return null;
    const dt = new Date(y, m - 1, d);
    return Number.isNaN(dt.getTime()) ? null : dt;
  }

  function ensureFieldErrorEl(input) {
    const field = input?.closest(".bf-field") || input?.parentElement;
    if (!field) return null;
    let el = field.querySelector(".bf-error");
    if (!el) {
      el = document.createElement("div");
      el.className = "bf-error";
      el.hidden = true;
      field.appendChild(el);
    }
    return el;
  }
  function setFieldError(input, msg) {
    const el = ensureFieldErrorEl(input);
    if (!el) return;
    if (msg) {
      el.textContent = msg;
      el.hidden = false;
    } else {
      el.textContent = "";
      el.hidden = true;
    }
  }

  function setInvalid(el, invalid, msg = "") {
    if (!el) return;
    el.classList.toggle("is-invalid", !!invalid);
    if (invalid) {
      el.setAttribute("aria-invalid", "true");
      try {
        el.setCustomValidity(msg || "Invalid input");
      } catch {}
      setFieldError(el, msg || "Invalid input");
    } else {
      el.removeAttribute("aria-invalid");
      try {
        el.setCustomValidity("");
      } catch {}
      setFieldError(el, "");
    }
  }

  function validateBetween(fromEl, toEl) {
    setInvalid(fromEl, false);
    setInvalid(toEl, false);

    const fromRaw = (fromEl?.value || "").trim();
    const toRaw = (toEl?.value || "").trim();
    if (!fromRaw || !toRaw) return true;

    const fd = parseLocalDate(fromRaw);
    const td = parseLocalDate(toRaw);
    if (!fd || !td) return false;

    if (fd > td) {
      const err = "“From” must be on or before “To”.";
      setInvalid(fromEl, true, err);
      setInvalid(toEl, true, err);
      return false;
    }
    return true;
  }

  function syncMinMax(fromEl, toEl) {
    const f = (fromEl?.value || "").trim();
    const t = (toEl?.value || "").trim();
    if (toEl) toEl.min = f || "";
    if (fromEl) fromEl.max = t || "";
  }

  function hideAllFieldErrors(container) {
    if (!container) return;
    container.querySelectorAll(".bf-error").forEach((el) => {
      el.textContent = "";
      el.hidden = true;
    });
    container.querySelectorAll("input.is-invalid").forEach((inp) => {
      inp.classList.remove("is-invalid");
      inp.removeAttribute("aria-invalid");
      inp.removeAttribute("aria-describedby");
      try {
        inp.setCustomValidity("");
      } catch {}
    });
  }

  function resetBackfillForm(dlg) {
    if (!dlg) return;
    const form = dlg.querySelector("#bf-form");
    if (form) form.reset();
    hideAllFieldErrors(dlg);
  }

  function openBackfillDialog(channelId) {
    const cloneId = String(channelId);
    bfChannelId = cloneId;

    if (cloneIsLocked(cloneId)) {
      window.showToast("A clone for this channel is already in progress.", {
        type: "warning",
      });
      return;
    }

    const dlg = document.getElementById("backfill-dialog");
    const back = document.getElementById("backfill-backdrop");
    if (!dlg) return;

    if (back) back.hidden = false;
    dlg.hidden = false;
    dlg.classList.add("show");

    const card = dlg.querySelector(".modal-card");
    const onEsc = (e) => {
      if (e.key === "Escape") closeBackfillDialog();
    };
    const onOutside = (e) => {
      if (card && !card.contains(e.target)) closeBackfillDialog();
    };
    document.addEventListener("keydown", onEsc);
    document.addEventListener("mousedown", onOutside, true);

    bfCleanup = () => {
      document.removeEventListener("keydown", onEsc);
      document.removeEventListener("mousedown", onOutside, true);
      dlg.removeEventListener("mousedown", clearErrorsOnClickInside);
    };

    const clearErrorsOnClickInside = (e) => {
      if (card && card.contains(e.target)) hideAllFieldErrors(dlg);
    };
    dlg.addEventListener("mousedown", clearErrorsOnClickInside);

    const form = dlg.querySelector("#bf-form");
    if (form) {
      form.setAttribute("novalidate", "");
      form.addEventListener("invalid", (e) => e.preventDefault(), true);
    }
    const btnClose = dlg.querySelector("#bf-close");

    const radios = dlg.querySelectorAll('input[name="mode"]');
    const sinceEl = dlg.querySelector("#bf-since");
    const lastEl = dlg.querySelector("#bf-lastn");
    const fromEl = dlg.querySelector("#bf-from");
    const toEl = dlg.querySelector("#bf-to");

    const rowSince = sinceEl?.closest(".indent");
    const rowLast = lastEl?.closest(".indent");
    const rowBetween = dlg.querySelector(".bf-row-between");

    attachPicker(sinceEl);
    attachPicker(fromEl);
    attachPicker(toEl);

    [sinceEl, lastEl, fromEl, toEl].forEach((el) =>
      el?.addEventListener("input", () => {
        if (!el) return;
        if (el === fromEl || el === toEl) {
          syncMinMax(fromEl, toEl);
          validateBetween(fromEl, toEl);
        } else {
          setInvalid(el, false);
        }
      })
    );

    function refresh() {
      const mode =
        dlg.querySelector('input[name="mode"]:checked')?.value || "all";
      if (sinceEl) sinceEl.disabled = mode !== "since";
      if (lastEl) lastEl.disabled = mode !== "last";
      if (fromEl) fromEl.disabled = mode !== "between";
      if (toEl) toEl.disabled = mode !== "between";

      rowSince?.classList.toggle("is-active", mode === "since");
      rowLast?.classList.toggle("is-active", mode === "last");
      rowBetween?.classList.toggle("is-active", mode === "between");
    }
    radios.forEach((r) => r.addEventListener("change", refresh));
    refresh();

    btnClose?.addEventListener("click", closeBackfillDialog, { once: true });

    const startBtn = dlg.querySelector("#bf-start");

    function ensureAlertBox() {
      let box = dlg.querySelector(".bf-alert");
      if (!box) {
        box = document.createElement("div");
        box.className = "bf-alert";
        box.setAttribute("role", "alert");
        box.setAttribute("aria-live", "assertive");
        const form = dlg.querySelector("#bf-form");
        (form?.parentNode || dlg).insertBefore(box, form);
      }
      return box;
    }
    const alertBox = ensureAlertBox();

    function hideMenuMessage() {
      alertBox?.classList.remove("show");
    }

    [startBtn, dlg].forEach((el) =>
      el?.addEventListener("blur", hideMenuMessage, true)
    );

    form?.addEventListener("submit", (ev) => {
      ev.preventDefault();

      const mode =
        dlg.querySelector('input[name="mode"]:checked')?.value || "all";
      const sinceRaw = (sinceEl?.value || "").trim();
      const lastRaw = (lastEl?.value || "").trim();
      const fromRaw = (fromEl?.value || "").trim();
      const toRaw = (toEl?.value || "").trim();

      const lastVal = Number.parseInt(lastRaw, 10);
      const lastOk = Number.isFinite(lastVal) && lastVal > 0;

      if (mode === "since" && !sinceRaw) {
        setInvalid(sinceEl, true, "Pick a date.");
        sinceEl?.focus();
        return;
      }
      if (mode === "last" && !lastOk) {
        setInvalid(lastEl, true, "Enter a valid number.");
        lastEl?.focus();
        return;
      }
      if (mode === "between") {
        if (!fromRaw || !toRaw) {
          setInvalid(fromEl, !fromRaw, "Pick a date.");
          setInvalid(toEl, !toRaw, "Pick a date.");
          (fromRaw ? toEl : fromEl)?.focus();
          return;
        }
        if (!validateBetween(fromEl, toEl)) {
          fromEl?.focus();
          return;
        }
      }

      [sinceEl, lastEl, fromEl, toEl].forEach((el) => setInvalid(el, false));

      setCloneLaunching(cloneId, true);

      const body = {
        channel_id: cloneId,
        mode,
        ...(mode === "since" ? { since: startOfDayIsoLocal(sinceRaw) } : {}),
        ...(mode === "last" ? { last_n: lastVal } : {}),
        ...(mode === "between"
          ? {
              since: startOfDayIsoLocal(fromRaw),
              before_iso: nextDayStartIsoLocal(toRaw),
            }
          : {}),
      };

      fetch("/api/backfill/start", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
        credentials: "same-origin",
        cache: "no-store",
      })
        .then(async (res) => {
          const json = await res.json().catch(() => ({}));
          if (!res.ok || json?.ok === false) {
            if (res.status === 409) {
              setCloneLaunching(cloneId, false);
              setCloneRunning(cloneId, true);
              toastOncePersist(
                `bf:start:${cloneId}`,
                "Clone started…",
                { type: "info" },
                15000
              );
              closeBackfillDialog();
              return;
            }
            unlockBackfill(cloneId);
            window.showToast(json?.error || "Failed to start clone.", {
              type: "error",
            });
            return;
          }
          toastOncePersist(
            `bf:start:${cloneId}`,
            "Clone started…",
            { type: "info" },
            15000
          );
          closeBackfillDialog();
        })
        .catch(() => {
          unlockBackfill(cloneId);
          window.showToast("Network error starting clone.", { type: "error" });
        });
    });

    setTimeout(() => dlg.querySelector("#bf-start")?.focus(), 0);
  }

  function closeBackfillDialog() {
    const dlg = document.getElementById("backfill-dialog");
    const back = document.getElementById("backfill-backdrop");
    try {
      bfCleanup?.();
    } finally {
      bfCleanup = null;
    }
    if (dlg) {
      dlg.classList.remove("show");
      resetBackfillForm(dlg);
      dlg.hidden = true;
    }
    if (back) back.hidden = true;
    bfChannelId = null;
  }

  document.getElementById("ch-menu")?.addEventListener("click", (ev) => {
    const li = ev.target.closest("[data-action]");
    if (!li) return;
    if (li.dataset.action === "clone") {
      ev.preventDefault();
      ev.stopPropagation();

      const id = menuForId;
      if (!id) {
        window.showToast("No channel selected.", { type: "error" });
        return;
      }
      if (cloneIsLocked(id)) {
        window.showToast("A clone for this channel is already in progress.", {
          type: "warning",
        });
        hideMenu({ restoreFocus: false });
        return;
      }
      hideMenu({ restoreFocus: false });
      openBackfillDialog(id);
    }
  });

  vBtn?.addEventListener("click", openVerify);
  vClose?.addEventListener("click", closeVerify);
  vBack?.addEventListener("click", (e) => {
    if (e.target === vBack) closeVerify();
  });
  (() => {
    const root = document.getElementById("channels-root");
    if (!root) return;
    document.addEventListener("keydown", (e) => {
      if (e.key === "Escape" && vDlg && !vDlg.hidden) closeVerify();
    });
  })();

  vFetch?.addEventListener("click", () => {
    vStatus.textContent = "Scanning…";
    sendVerify({ action: "list" });
  });
})();
