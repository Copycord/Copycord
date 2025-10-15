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
  const PULLING_LABEL = "Pulling all messages";
  const startedHere = new Set();
  const NF_INT = new Intl.NumberFormat();
  const fmtInt = (n) => (Number.isFinite(n) ? NF_INT.format(n) : String(n));

  const DEBUG_BF = true;
  const dbg = (...a) => {
    if (DEBUG_BF) console.debug(...a);
  };
  const group = (label, fn) => {
    if (!DEBUG_BF) return fn();
    console.groupCollapsed(label);
    try {
      fn();
    } finally {
      console.groupEnd();
    }
  };

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
  let inflightReady = false;

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
  
    require: "both",

    onDown() {
      try {
        resetAllCloningUI();
      } catch {}
      inflightReady = false;
      document
        .querySelectorAll(".ch-card .ch-status, .ch-card .ch-progress")
        .forEach((el) => el.remove());
      document
        .querySelectorAll(".ch-card.is-cloning, .ch-card.is-pending")
        .forEach((card) => {
          card.classList.remove("is-cloning", "is-pending");
          card.removeAttribute("aria-busy");
        });
    },

    onUp() {
      try {
        fetchAndApplyInflight().finally(() => {
          inflightReady = true;
        });
      } catch {}
    },
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

  function shouldTrustBackfillPayload(p, cid) {
    if (p?.task_id && taskMap.has(String(p.task_id))) return true;
    if (startedHere.has(String(cid))) return true;

    if (inflightReady && inflightByOrig.has(String(cid))) return true;
    return false;
  }

  function setCardInteractive(card, on) {
    if (!card) return;
    const btn = card.querySelector(".ch-menu-btn");
    if (btn) {
      btn.disabled = !on;
      btn.setAttribute("aria-disabled", (!on).toString());
      btn.title = on ? "Channel menu" : "Disabled while cloning";
    }
  }

  function finalizeBackfillUI(cid, { announce = false } = {}) {
    const k = String(cid);
    setClonePulling(k, false);
    setCloneCleaning(k, false);
    unlockBackfill(k);
    inflightByOrig.delete(k);

    setCardLoading(k, false);
    const card = cardByAnyId(k);
    card?.querySelector(".ch-status")?.remove();
    card?.querySelector(".ch-progress")?.remove();

    if (announce && startedHere.has(k) && shouldAnnounceNow()) {
      announceBackfillDone(k);
    }
    fetchAndApplyInflight().catch(() => {});
    render();
  }

  function findRowByAnyChannelId(id) {
    const s = String(id || "");
    if (!s) return null;
    return (
      (data || []).find(
        (r) =>
          String(r.original_channel_id) === s ||
          String(r.cloned_channel_id) === s
      ) || null
    );
  }

  function toOriginalCid(id) {
    const s = String(id || "");
    const row = findRowByAnyChannelId(s);
    return row ? String(row.original_channel_id) : s;
  }

  function cardByAnyId(id) {
    const orig = toOriginalCid(id);
    return document.querySelector(`.ch-card[data-cid="${orig}"]`);
  }

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
      localStorage.setItem("bf:running", "[]");
      localStorage.setItem("bf:launching", "[]");
    } catch {}

    try {
      localStorage.setItem("bf:pulling", "[]");
    } catch {}
    pullingClones.clear();
    for (const id of [...cleaningClones]) setCardLoading(id, false);
    cleaningClones.clear();
    try {
      localStorage.setItem("bf:cleaning", "[]");
    } catch {}

    try {
      const rm = [];
      for (let i = 0; i < sessionStorage.length; i++) {
        const k = sessionStorage.key(i);
        if (
          k &&
          k.startsWith("toast:persist:bf:") &&
          !k.startsWith("toast:persist:bf:done:")
        ) {
          rm.push(k);
        }
      }
      rm.forEach((k) => sessionStorage.removeItem(k));
    } catch {}
  }

  function upsertStatusPill(card, text = "Cloning") {
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

  function ensureProgressBar(card) {
    if (!card) return null;
    let pr = card.querySelector(".ch-progress");
    if (!pr) {
      pr = document.createElement("div");
      pr.className = "ch-progress";
      pr.setAttribute("role", "progressbar");
      pr.setAttribute("aria-valuemin", "0");
      pr.setAttribute("aria-valuemax", "100");
      pr.setAttribute("aria-valuenow", "0");
      pr.setAttribute("aria-label", "Clone progress");
      const bar = document.createElement("div");
      bar.className = "bar";
      pr.appendChild(bar);

      const after = card.querySelector(".ch-meta") || card.firstElementChild;
      if (after?.nextSibling)
        after.parentNode.insertBefore(pr, after.nextSibling);
      else card.appendChild(pr);
    }
    return pr;
  }

  function updateProgressBar(card, delivered = null, total = null) {
    const pr = ensureProgressBar(card);
    if (!pr) return;

    const bar = pr.querySelector(".bar");
    const indeterminate = !(
      Number.isFinite(delivered) &&
      Number.isFinite(total) &&
      total > 0
    );

    if (indeterminate) {
      pr.classList.add("indeterminate");
      pr.setAttribute("aria-busy", "true");
      pr.setAttribute("aria-valuenow", "0");
      bar.style.width = "30%";
    } else {
      const pct = Math.max(
        0,
        Math.min(100, Math.floor((delivered / total) * 100))
      );
      pr.classList.remove("indeterminate");
      pr.removeAttribute("aria-busy");
      pr.setAttribute("aria-valuenow", String(pct));
      bar.style.width = pct + "%";
    }
  }

  function setProgressCleanupMode(card, on) {
    const pr = ensureProgressBar(card);
    if (!pr) return;
    if (on) {
      pr.classList.add("indeterminate");
      pr.setAttribute("aria-busy", "true");
    } else {
      pr.classList.remove("indeterminate");
      pr.removeAttribute("aria-busy");
    }
  }

  function removeProgressBar(card) {
    const pr = card?.querySelector(".ch-progress");
    if (!pr) return;

    pr.style.opacity = "0";
    pr.style.transform = "translateY(-2px)";
    setTimeout(() => pr.remove(), 180);
  }

  function setCardLoading(channelId, on, text = "Cloning") {
    dbg("[UI] setCardLoading", { channelId: String(channelId), on, text });
    const k = String(channelId);
    const card = document.querySelector(`.ch-card[data-cid="${k}"]`);
    if (!card) return;

    if (on) {
      card.classList.add("is-cloning");
      card.setAttribute("aria-busy", "true");
      upsertStatusPill(card, text);
      updateProgressBar(card, null, null);
      setCardInteractive(card, false);
    } else {
      card.classList.remove("is-cloning");
      card.removeAttribute("aria-busy");
      const pill = card.querySelector(".ch-status");
      if (pill) pill.remove();
      removeProgressBar(card);
      setCardInteractive(card, true);
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
    const orig = toOriginalCid(channelId);
    dbg("[TASKMAP] remember", {
      taskId: String(taskId),
      channelId: String(orig),
    });
    if (!taskId || !orig) return;
    taskMap.set(String(taskId), String(orig));
    saveTaskMap();
  }
  function forgetTask(taskId) {
    dbg("[TASKMAP] forget", { taskId: String(taskId) });
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

    titleEl.textContent = `Customize`;
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
          <button id="customize-cat-close" type="button" class="icon-btn verify-close" aria-label="Close">✕</button>
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
          <button id="customize-cat-save" class="btn btn-ghost" type="button">Save</button>
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
      <button id="customize-close" type="button" class="icon-btn verify-close" aria-label="Close">✕</button>
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
      <button id="customize-save" class="btn btn-ghost" type="button">Save</button>
    </div>
  </div>
`;
    document.body.appendChild(wrap);

    (function wireInfoTips() {
      if (window.__infoTipsWired) return;
      window.__infoTipsWired = true;
    
      function hideAllTips() {
        document.querySelectorAll('.tip-bubble[aria-hidden="false"]')
          .forEach(el => el.setAttribute('aria-hidden', 'true'));
      }
    
      document.addEventListener('click', (e) => {
        const btn = e.target.closest('.info-dot');
        if (btn) {
          e.preventDefault();
          const id = btn.getAttribute('aria-describedby');
          const tip = id ? document.getElementById(id) : null;
          if (!tip) return;
    
          const isOpen = tip.getAttribute('aria-hidden') === 'false';
          // Close other open tips first
          hideAllTips();
          tip.setAttribute('aria-hidden', isOpen ? 'true' : 'false');
          return;
        }
    
        // Clicked outside any labeled tip area? Close.
        if (!e.target.closest('.has-tip')) hideAllTips();
      });
    
      document.addEventListener('keydown', (e) => {
        if (e.key === 'Escape') hideAllTips();
      });
    })();

    if (!document.getElementById("customize-compact-styles")) {
      (function injectProgressStyles() {
        if (document.getElementById("bf-progress-styles")) return;
        const css = document.createElement("style");
        css.id = "bf-progress-styles";
        document.head.appendChild(css);
      })();
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
        return JSON.parse(localStorage.getItem("bf:running") || "[]");
      } catch {
        return [];
      }
    })()
  );
  const launchingClones = new Set(
    (() => {
      try {
        return JSON.parse(localStorage.getItem("bf:launching") || "[]");
      } catch {
        return [];
      }
    })()
  );

  const cleaningClones = new Set(
    (() => {
      try {
        return JSON.parse(localStorage.getItem("bf:cleaning") || "[]");
      } catch {
        return [];
      }
    })()
  );

  const pullingClones = new Set(
    (() => {
      try {
        return JSON.parse(localStorage.getItem("bf:pulling") || "[]");
      } catch {
        return [];
      }
    })()
  );

  function setClonePulling(id, on) {
    const k = String(id);
    if (on) pullingClones.add(k);
    else pullingClones.delete(k);

    try {
      localStorage.setItem("bf:pulling", JSON.stringify([...pullingClones]));
    } catch {}

    const card = document.querySelector(`.ch-card[data-cid="${k}"]`);
    if (on) {
      setCardLoading(k, true, PULLING_LABEL);
      updateProgressBar(card, null, null);
    }
  }

  function setCloneCleaning(id, on) {
    const k = String(id);
    if (on) cleaningClones.add(k);
    else cleaningClones.delete(k);
    try {
      localStorage.setItem("bf:cleaning", JSON.stringify([...cleaningClones]));
    } catch {}
  }

  const inflightByOrig = new Map();
  const inflightMisses = new Map();
  const MAX_MISSES = 3;

  function fmtProgress(v) {
    const d = Number.isFinite(v?.delivered) ? v.delivered : null;
    const t = Number.isFinite(v?.expected_total) ? v.expected_total : null;
    if (d != null && t != null) return `Cloning (${fmtInt(d)}/${fmtInt(t)})`;
    if (d != null && d > 0) return `Cloning (${fmtInt(d)})`;
    return PULLING_LABEL;
  }

  function getChannelDisplayName(cid) {
    const id = String(cid);

    const row = (data || []).find((r) => String(r.original_channel_id) === id);
    if (row) {
      const name =
        (row.clone_channel_name && row.clone_channel_name.trim()) ||
        row.original_channel_name ||
        "";
      return name.replace(/^#\s*/, "").trim();
    }

    try {
      const sel = `.ch-card[data-cid="${
        window.CSS && CSS.escape ? CSS.escape(id) : id.replace(/"/g, '\\"')
      }"] .ch-display-name`;
      const el = document.querySelector(sel);
      if (el) return el.textContent.replace(/^#\s*/, "").trim();
    } catch {}
    return null;
  }

  function announceBackfillDone(cid) {
    const wasCancelled =
      cancelledThisSession.has(String(cid)) ||
      !!sessionStorage.getItem(`bf:cancelled:${cid}`);

    if (!wasCancelled && shouldAnnounceNow()) {
      const chName = getChannelDisplayName(cid);
      const msg = chName
        ? `Clone completed for #${chName}.`
        : `Clone completed (channel ${cid}).`;

      toastOncePersist(`bf:done:${cid}`, msg, { type: "success" }, 15000);
    }

    const card = document.querySelector(`.ch-card[data-cid="${String(cid)}"]`);
    if (card) {
      let pill = card.querySelector(".ch-status");
      if (!pill) {
        pill = document.createElement("span");
        pill.className = "ch-status";
        card.querySelector(".ch-top-right")?.prepend(pill);
      }
      pill.textContent = "Synced ✓";
      setTimeout(() => pill?.remove(), 2000);
    }
  }

  function applyInflightUI(itemsObj) {
    const serverIds = new Set(Object.keys(itemsObj || {}).map(String));

    for (const id of serverIds) inflightMisses.delete(String(id));

    for (const id of [...launchingClones]) {
      if (!serverIds.has(id) && !cleaningClones.has(id)) {
        setCloneLaunching(id, false);
      }
    }

    inflightByOrig.clear();
    for (const [cid, info] of Object.entries(itemsObj || {})) {
      const k = String(cid);
      inflightByOrig.set(k, info || {});
    }

    for (const [cid, info] of inflightByOrig.entries()) {
      setCloneRunning(cid, true);
      const card = document.querySelector(`.ch-card[data-cid="${cid}"]`);

      const d = Number.isFinite(info?.delivered) ? info.delivered : null;
      const t = Number.isFinite(info?.expected_total)
        ? info.expected_total
        : null;
      const haveDelivered = Number.isFinite(d) && d > 0;
      const haveTotal = Number.isFinite(t) && t > 0;
      const isPulling =
        pullingClones.has(String(cid)) || !(haveDelivered || haveTotal);

      if (isPulling) {
        setCardLoading(cid, true, PULLING_LABEL);
        updateProgressBar(card, null, null);
      } else if (haveDelivered && haveTotal) {
        setCardLoading(cid, true, `Cloning (${fmtInt(d)}/${fmtInt(t)})`);
        updateProgressBar(card, d, t);
      } else if (haveDelivered) {
        setCardLoading(cid, true, `Cloning (${fmtInt(d)})`);
        updateProgressBar(card, d, null);
      } else {
        setCardLoading(cid, true, "Cloning");
        updateProgressBar(card, null, null);
      }
    }

    for (const id of cleaningClones) {
      if (!serverIds.has(id)) {
        const card = document.querySelector(`.ch-card[data-cid="${id}"]`);
        setCardLoading(id, true, "Cleaning up…");
        setProgressCleanupMode(card, true);
      }
    }

    for (const id of [...cleaningClones]) {
      const k = String(id);
      if (serverIds.has(k)) {
        inflightMisses.delete(`clean:${k}`);
        continue;
      }
      const misses = (inflightMisses.get(`clean:${k}`) || 0) + 1;
      inflightMisses.set(`clean:${k}`, misses);
      if (misses >= MAX_MISSES) {
        setCloneCleaning(k, false);
        setCardLoading(k, false);
        inflightMisses.delete(`clean:${k}`);
      }
    }

    for (const id of [...runningClones]) {
      const k = String(id);
      if (serverIds.has(k)) continue;
      if (cleaningClones.has(k)) continue;
      if (pullingClones.has(k)) continue;
      const misses = (inflightMisses.get(k) || 0) + 1;
      inflightMisses.set(k, misses);
      if (misses >= MAX_MISSES) {
        setCloneRunning(k, false);
        setCardLoading(k, false);
        inflightMisses.delete(k);
      }
    }

    try {
      localStorage.setItem(
        "bf:running",
        JSON.stringify([...new Set(inflightByOrig.keys())])
      );
    } catch {}
  }

  /** Fetch current in-flight backfills and apply to UI */
  async function fetchAndApplyInflight() {
    try {
      const res = await fetch("/api/backfills/inflight", {
        credentials: "same-origin",
        cache: "no-store",
      });
      const json = await res.json().catch(() => ({}));
      if (res.ok && json?.ok !== false) {
        applyInflightUI(json.items || {});
      }
    } catch {}
  }

  let inflightTimer = null;
  function startInflightPolling() {
    stopInflightPolling();
    inflightTimer = setInterval(fetchAndApplyInflight, 10_000);
  }
  function stopInflightPolling() {
    if (inflightTimer) {
      clearInterval(inflightTimer);
      inflightTimer = null;
    }
  }

  document.addEventListener("visibilitychange", () => {
    if (document.hidden) stopInflightPolling();
    else {
      inflightReady = false;
      fetchAndApplyInflight().finally(() => {
        inflightReady = true;
        startInflightPolling();
      });
    }
  });

  function setCloneLaunching(id, on) {
    dbg("[STATE] launching", { id: String(id), on });
    const k = String(id);
    if (on) launchingClones.add(k);
    else launchingClones.delete(k);
    try {
      localStorage.setItem(
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
    dbg("[STATE] running", {
      id: String(id),
      on,
      runningClones: [...runningClones],
    });
    const k = String(id);
    if (on) runningClones.add(k);
    else runningClones.delete(k);
    try {
      localStorage.setItem("bf:running", JSON.stringify([...runningClones]));
    } catch {}
    const card = document.querySelector(`.ch-card[data-cid="${k}"]`);
    if (card) {
      card.classList.toggle("is-cloning", on);
      if (on) card.setAttribute("aria-busy", "true");
      else card.removeAttribute("aria-busy");
    }
  }
  function unlockBackfill(id) {
    dbg("[STATE] unlockBackfill", { id: String(id) });
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
      localStorage.setItem("bf:running", "[]");
      localStorage.setItem("bf:launching", "[]");
    } catch {}
    try {
      const key = `toast:persist:bf:stopped`;
      sessionStorage.setItem(
        key,
        JSON.stringify({ expiresAt: Date.now() + 10_000 })
      );
    } catch {}
    try {
      localStorage.setItem("bf:pulling", "[]");
    } catch {}
    pullingClones.clear();
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
    const v = String(
      ch.original_category_name ??
        ch.category_original_name ??
        ch.category_upstream_name ??
        ch.category_name ??
        ""
    ).trim();
    return v || UNGROUPED_LABEL;
  }

  function applyFilterAndSort() {
    const q = normalize(search.value);

    filtered = !q
      ? [...data]
      : data.filter((ch) => {
          const origCatRaw = getOriginalCategoryFromRow(ch);
          const origCat = (origCatRaw && origCatRaw.trim()) || "";
          const resolvedOrig = origCat || UNGROUPED_LABEL;
          const pinnedCat = catPinByOrig.get(resolvedOrig) || "";

          const catName =
            (ch.category_name && ch.category_name.trim()) || UNGROUPED_LABEL;

          return (
            normalize(ch.original_channel_name).includes(q) ||
            normalize(ch.clone_channel_name).includes(q) ||
            normalize(catName).includes(q) ||
            normalize(resolvedOrig).includes(q) ||
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
      const id = String(el.dataset.cid || "");
      const stillActive = launchingClones.has(id) || runningClones.has(id);
      if (!stillActive) {
        el.classList.remove("is-cloning");
        el.removeAttribute("aria-busy");
        el.querySelector(".ch-status")?.remove();
      }
    });

    let entries = [...merged.entries()];
    if (sortMode === "category") {
      entries.sort(([aName], [bName]) => compareCategoryNames(aName, bName));
      if (sortDir === "desc") entries.reverse();
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
      const isUncategorized = resolvedOrig === UNGROUPED_LABEL;

      section.innerHTML = `
        <div class="ch-section-head">
          <h3 class="ch-section-title ${
            isOrphanCategory ? "orphan-title" : ""
          }">
            <span class="badge cat-chip ${
              isOrphanCategory
                ? "badge-orphan"
                : isCustom
                ? "badge-custom"
                : "good"
            } ${isCustom ? "is-custom" : ""}"
              ${
                isOrphanCategory
                  ? 'data-orphan-cat-id="' +
                    escapeAttr(orphanCatId) +
                    '" data-cat-name="' +
                    escapeAttr(resolvedOrig) +
                    '"'
                  : 'data-cat-name="' + escapeAttr(resolvedOrig) + '"'
              }
              ${tooltip ? 'title="' + escapeAttr(tooltip) + '"' : ""}
            >
              ${escapeHtml(displayCat)}
              ${
                !isUncategorized
                  ? `<button class="cat-menu-trigger" aria-haspopup="true"
                       aria-controls="ch-menu" aria-label="Category menu" type="button">⋯</button>`
                  : ""
              }
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
        grid.appendChild(card);
        if (
          launchingClones.has(String(ch.original_channel_id)) ||
          runningClones.has(String(ch.original_channel_id)) ||
          pullingClones.has(String(ch.original_channel_id))
        ) {
          setCardInteractive(card, false);
        }
      }

      root.appendChild(section);

      for (const id of launchingClones) setCardLoading(id, true, "Cloning");
      for (const id of runningClones) setCardLoading(id, true, "Cloning");
      for (const id of pullingClones) setCardLoading(id, true, PULLING_LABEL);
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

    const LOCAL_UNGROUPED = UNGROUPED_LABEL;

    const isChannel = ctx.type === "channel";
    const isCategory = ctx.type === "category";
    const isOrphanCat = ctx.type === "orphan-cat";

    let isOrphanChannel = false;
    if (isChannel && ctx.id != null) {
      try {
        const selId = String(ctx.id);
        const card = document.querySelector(
          `.ch-card[data-cid="${
            window.CSS && CSS.escape
              ? CSS.escape(selId)
              : selId.replace(/"/g, '\\"')
          }"]`
        );
        isOrphanChannel =
          card?.dataset?.orphan === "1" || !!card?.dataset?.orphan;
      } catch {}
    }

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
    customizeCatItem.setAttribute(
      "aria-hidden",
      (!!customizeCatItem.hidden).toString()
    );

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
    delOrphanChItem.hidden = !isOrphanChannel;
    if (!delOrphanChItem.hidden) delOrphanChItem.dataset.kind = "channel";

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
    delOrphanCatItem.hidden = !isOrphanCat;

    if (cloneItem) {
      const isLocked = isChannel && cloneIsLocked(ctx.id);
      const hideClone = !isChannel || isOrphanChannel;
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
    customizeItem.setAttribute(
      "aria-hidden",
      (!!customizeItem.hidden).toString()
    );

    function findChannelRowByOrigId(origId) {
      const k = String(origId || "");
      return (
        (data || []).find((r) => String(r.original_channel_id) === k) || null
      );
    }

    let sep = menu.querySelector('[data-act="sep-general"]');
    if (!sep) {
      sep = document.createElement("div");
      sep.className = "ctxmenu-sep";
      sep.dataset.act = "sep-general";
      menu.appendChild(sep);
    }

    let blCh = menu.querySelector('[data-act="bl-channel"]');
    if (!blCh) {
      blCh = document.createElement("button");
      blCh.className = "ctxmenu-item";
      blCh.dataset.act = "bl-channel";
      blCh.role = "menuitem";
      blCh.type = "button";
      blCh.textContent = "Add channel to blacklist";
      blCh.setAttribute("aria-label", "Add channel to blacklist");
      blCh.addEventListener("click", () => {
        const row = findChannelRowByOrigId(menuForId);
        const originalId = row?.original_channel_id;
        const displayName =
          (row?.clone_channel_name && row.clone_channel_name.trim()) ||
          row?.original_channel_name ||
          "Channel";

        if (!originalId) {
          window.showToast("Could not resolve channel ID.", { type: "error" });
          hideMenu({ restoreFocus: false });
          return;
        }

        hideMenu({ restoreFocus: false });
        openConfirm(
          {
            title: "Add channel to blacklist?",
            body: `This will blacklist <b>#${escapeHtml(
              displayName
            )}</b> <span class="muted">(${escapeHtml(
              String(originalId)
            )})</span>.`,
            okText: "Add to blacklist",
            cancelText: "Cancel",

            btnClassOk: "btn btn-ghost-red",
            btnClassCancel: "btn btn-ghost",
          },
          async () => {
            try {
              const res = await fetch("/api/filters/blacklist", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                credentials: "same-origin",
                body: JSON.stringify({
                  scope: "channel",
                  obj_id: String(originalId),
                }),
              });
              const j = await res.json().catch(() => ({}));
              if (!res.ok || j?.ok === false)
                throw new Error(j?.detail || j?.error || "failed");
              window.showToast("Channel added to blacklist.", {
                type: "success",
              });
              await load();
            } catch {
              window.showToast("Failed to add to blacklist.", {
                type: "error",
              });
            }
          }
        );
      });
      menu.appendChild(blCh);
    }

    let copyOrig = menu.querySelector('[data-act="copy-orig-id"]');
    if (!copyOrig) {
      copyOrig = document.createElement("button");
      copyOrig.className = "ctxmenu-item";
      copyOrig.dataset.act = "copy-orig-id";
      copyOrig.role = "menuitem";
      copyOrig.type = "button";
      copyOrig.textContent = "Copy original channel ID";
      copyOrig.setAttribute("aria-label", "Copy original ID");
      copyOrig.addEventListener("click", async () => {
        if (!menuForId) return;
        try {
          await navigator.clipboard.writeText(String(menuForId));
          window.showToast("Copied original channel ID to clipboard.", {
            type: "success",
          });
        } catch {
          window.showToast("Could not copy channel ID.", { type: "error" });
        }
        hideMenu({ restoreFocus: false });
      });
      menu.appendChild(copyOrig);
    }

    let copyClone = menu.querySelector('[data-act="copy-clone-id"]');
    if (!copyClone) {
      copyClone = document.createElement("button");
      copyClone.className = "ctxmenu-item";
      copyClone.dataset.act = "copy-clone-id";
      copyClone.role = "menuitem";
      copyClone.type = "button";
      copyClone.textContent = "Copy clone channel ID";
      copyClone.setAttribute("aria-label", "Copy clone ID");
      copyClone.addEventListener("click", async () => {
        const row = findChannelRowByOrigId(menuForId);
        const cid =
          row && row.cloned_channel_id ? String(row.cloned_channel_id) : "";
        if (!cid) {
          window.showToast("No clone channel ID found.", { type: "error" });
          return;
        }
        try {
          await navigator.clipboard.writeText(cid);
          window.showToast("Copied clone channel ID to clipboard.", {
            type: "success",
          });
        } catch {
          window.showToast("Could not copy channel ID.", { type: "error" });
        }
        hideMenu({ restoreFocus: false });
      });
      menu.appendChild(copyClone);
    }

    let blCat = menu.querySelector('[data-act="bl-category"]');
    if (!blCat) {
      blCat = document.createElement("button");
      blCat.className = "ctxmenu-item";
      blCat.dataset.act = "bl-category";
      blCat.role = "menuitem";
      blCat.type = "button";
      blCat.textContent = "Add category to blacklist";
      blCat.setAttribute("aria-label", "Add category to blacklist");
      blCat.addEventListener("click", () => {
        const name = menuContext?.name ? String(menuContext.name) : "";
        const { originalCatId } = resolveCategoryIdsByName(name);

        if (!originalCatId) {
          window.showToast("Could not resolve category ID.", { type: "error" });
          hideMenu({ restoreFocus: false });
          return;
        }

        hideMenu({ restoreFocus: false });
        openConfirm(
          {
            title: "Add category to blacklist?",
            body: `This will blacklist <b>${escapeHtml(
              name
            )}</b> <span class="muted">(${escapeHtml(
              String(originalCatId)
            )})</span>.`,
            okText: "Add to blacklist",
            btnClassOk: "btn btn-ghost-red",
            btnClassCancel: "btn btn-ghost",
          },
          async () => {
            try {
              const res = await fetch("/api/filters/blacklist", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                credentials: "same-origin",
                body: JSON.stringify({
                  scope: "category",
                  obj_id: String(originalCatId),
                }),
              });
              const j = await res.json().catch(() => ({}));
              if (!res.ok || j?.ok === false)
                throw new Error(j?.detail || j?.error || "failed");
              window.showToast("Category added to blacklist.", {
                type: "success",
              });
            } catch {
              window.showToast("Failed to add to blacklist.", {
                type: "error",
              });
            }
          }
        );
      });
      menu.appendChild(blCat);
    }

    function firstId(row, keys) {
      for (const k of keys) {
        const v = row?.[k];
        if (v != null && String(v).trim() !== "") return String(v).trim();
      }
      return null;
    }

    function heuristicId(rows, testFn) {
      for (const r of rows) {
        for (const [k, v] of Object.entries(r || {})) {
          if (v == null || String(v).trim() === "") continue;
          const key = k.toLowerCase();
          if (testFn(key)) return String(v).trim();
        }
      }
      return null;
    }

    function resolveCategoryIdsByName(name) {
      const raw = String(name || "").trim();
      if (!raw)
        return { originalCatId: null, clonedCatId: null, hasClone: false };

      const resolvedOriginal =
        (catOrigByEither && catOrigByEither.get(raw.toLowerCase())) || raw;

      const norm = (s) =>
        String(s || "")
          .trim()
          .toLowerCase();
      const wantA = norm(raw);
      const wantB = norm(resolvedOriginal);

      const rows = (data || []).filter((r) => {
        const candidates = [
          r?.category_name,
          r?.original_category_name,
          r?.category_original_name,
          r?.category_upstream_name,
          String(r?.category_name || "").trim()
            ? r?.category_name
            : UNGROUPED_LABEL,
        ];
        return candidates.some((c) => {
          const n = norm(c);
          return n && (n === wantA || n === wantB);
        });
      });

      const origKeys = [
        "original_parent_category_id",
        "original_category_id",
        "category_original_id",
        "parent_category_id",
        "category_id",
        "category_upstream_id",
      ];
      const cloneKeys = [
        "cloned_parent_category_id",
        "cloned_category_id",
        "category_cloned_id",
        "parent_cloned_category_id",
      ];

      let originalCatId = null;
      let clonedCatId = null;

      for (const r of rows) {
        if (!originalCatId) originalCatId = firstId(r, origKeys);
        if (!clonedCatId) clonedCatId = firstId(r, cloneKeys);
        if (originalCatId && clonedCatId) break;
      }

      if (!clonedCatId) {
        clonedCatId = heuristicId(
          rows,
          (key) =>
            key.includes("clon") &&
            key.includes("categor") &&
            key.endsWith("id")
        );
      }
      if (!originalCatId) {
        originalCatId = heuristicId(
          rows,
          (key) =>
            !key.includes("clon") &&
            key.includes("categor") &&
            key.endsWith("id")
        );
      }

      console.debug("cat ids for", name, { originalCatId, clonedCatId, rows });

      return { originalCatId, clonedCatId, hasClone: !!clonedCatId };
    }

    let copyCatOrig = menu.querySelector('[data-act="copy-cat-orig-id"]');
    if (!copyCatOrig) {
      copyCatOrig = document.createElement("button");
      copyCatOrig.className = "ctxmenu-item";
      copyCatOrig.dataset.act = "copy-cat-orig-id";
      copyCatOrig.role = "menuitem";
      copyCatOrig.type = "button";
      copyCatOrig.textContent = "Copy original category ID";
      copyCatOrig.setAttribute("aria-label", "Copy original category ID");
      copyCatOrig.addEventListener("click", async () => {
        const name =
          menuContext && menuContext.name ? String(menuContext.name) : "";
        const { originalCatId } = resolveCategoryIdsByName(name);
        if (!originalCatId) {
          window.showToast("No original category ID found.", { type: "error" });
          return hideMenu({ restoreFocus: false });
        }
        try {
          await navigator.clipboard.writeText(String(originalCatId));
          window.showToast("Copied original category ID.", { type: "success" });
        } catch {
          window.showToast("Could not copy ID.", { type: "error" });
        }
        hideMenu({ restoreFocus: false });
      });
      menu.appendChild(copyCatOrig);
    }

    let copyCatClone = menu.querySelector('[data-act="copy-cat-clone-id"]');
    if (!copyCatClone) {
      copyCatClone = document.createElement("button");
      copyCatClone.className = "ctxmenu-item";
      copyCatClone.dataset.act = "copy-cat-clone-id";
      copyCatClone.role = "menuitem";
      copyCatClone.type = "button";
      copyCatClone.textContent = "Copy clone category ID";
      copyCatClone.setAttribute("aria-label", "Copy clone category ID");
      copyCatClone.addEventListener("click", async () => {
        const name =
          menuContext && menuContext.name ? String(menuContext.name) : "";
        const { clonedCatId } = resolveCategoryIdsByName(name);
        if (!clonedCatId) {
          window.showToast("No clone category ID found.", { type: "error" });
          return hideMenu({ restoreFocus: false });
        }
        try {
          await navigator.clipboard.writeText(String(clonedCatId));
          window.showToast("Copied clone category ID.", { type: "success" });
        } catch {
          window.showToast("Could not copy ID.", { type: "error" });
        }
        hideMenu({ restoreFocus: false });
      });
      menu.appendChild(copyCatClone);
    }

    let showCopyOrig = false,
      showCopyClone = false,
      showBlCh = false;
    let showCopyCatOrig = false,
      showCopyCatClone = false,
      showBlCat = false;

    if (isChannel && menuForId != null) {
      const row = findChannelRowByOrigId(menuForId);
      const isCloned = !!row?.cloned_channel_id;
      showCopyOrig = isCloned;
      showCopyClone = isCloned;
      showBlCh = isCloned;
    }

    if (isCategory) {
      if (!isOrphanCat) {
        const name =
          menuContext && menuContext.name ? String(menuContext.name) : "";
        const { originalCatId, clonedCatId } = resolveCategoryIdsByName(name);

        showCopyCatOrig = !!originalCatId;
        showCopyCatClone = !!clonedCatId;

        showBlCat = !!originalCatId;
      } else {
        showCopyCatOrig = false;
        showCopyCatClone = false;
        showBlCat = false;
      }
    }

    copyOrig.hidden = !showCopyOrig;
    copyClone.hidden = !showCopyClone;
    blCh.hidden = !showBlCh;

    copyCatOrig.hidden = !showCopyCatOrig;
    copyCatClone.hidden = !showCopyCatClone;
    blCat.hidden = !showBlCat;

    copyCatOrig.setAttribute("aria-hidden", (!showCopyCatOrig).toString());
    copyCatClone.setAttribute("aria-hidden", (!showCopyCatClone).toString());
    blCh.setAttribute("aria-hidden", (!showBlCh).toString());
    blCat.setAttribute("aria-hidden", (!showBlCat).toString());

    const gap = 6,
      pad = 12,
      vw = window.innerWidth,
      vh = window.innerHeight;
    const maxH = Math.max(160, Math.min(360, vh - 2 * pad));
    menu.style.maxHeight = `${maxH}px`;
    menu.style.overflowY = "auto";
    menu.style.position = "fixed";

    const r = btn.getBoundingClientRect();
    const mw = menu.offsetWidth || 180;
    const mh = menu.offsetHeight || 0;

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
    if (btn.disabled) {
      e.preventDefault();
      e.stopPropagation();
      return;
    }
    const card = btn.closest(".ch-card");
    const cid = card?.dataset?.cid;

    if (cid && cloneIsLocked(cid)) {
      e.preventDefault();
      e.stopPropagation();
      return;
    }

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
        btnClassOk: "btn btn-ghost-red",
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

    if (act === "delete-orphan") {
      e.preventDefault();
      const ctx = menuContext;
      if (!ctx || ctx.type !== "channel" || !ctx.id) {
        hideMenu();
        window.showToast("This item is not an orphan channel.", {
          type: "warning",
        });
        return;
      }

      const selId = String(ctx.id);
      const card = document.querySelector(
        `.ch-card[data-cid="${
          window.CSS && CSS.escape
            ? CSS.escape(selId)
            : selId.replace(/"/g, '\\"')
        }"]`
      );
      const isOrphanChannel = card?.dataset?.orphan === "1";
      const chName =
        card
          ?.querySelector(".ch-display-name")
          ?.textContent?.replace(/^#\s*/, "")
          .trim() || "Channel";

      if (!isOrphanChannel) {
        hideMenu();
        window.showToast("This is not an orphan channel.", { type: "warning" });
        return;
      }

      hideMenu();

      openConfirm(
        {
          title: "Delete orphan channel?",
          body: `This will delete <b>${escapeHtml(
            chName
          )}</b> <span class="muted">(${escapeHtml(selId)})</span>.`,
          okText: "Delete",
          btnClassOk: "btn btn-ghost-red",
        },
        () => {
          markPending(selId);
          sessionStorage.removeItem(LAST_DELETED_SIG_KEY);
          sendVerify({ action: "delete_one", kind: "channel", id: selId });
        }
      );
      return;
    }

    if (act === "delete-orphan-cat") {
      e.preventDefault();
      const ctx = menuContext;
      if (!ctx || ctx.type !== "orphan-cat" || !ctx.id) {
        hideMenu();
        window.showToast("This item is not an orphan category.", {
          type: "warning",
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
          btnClassOk: "btn btn-ghost-red",
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

  gate.checkAndGate(() => afterGateReady());
  gate.startWatch?.();

  let bootedAfterGate = false;
  async function afterGateReady() {
    if (bootedAfterGate) return;
    bootedAfterGate = true;

    clearBackfillBootResidue();

    ensureIn();
    ensureOut();
    sendVerify({ action: "list" });
    await load();
    await fetchAndApplyInflight();
    startInflightPolling();
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
        btnClassOk: "btn btn-ghost-red",
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
        } that are <em>not part of the original structure</em>.`,
        okText: "Delete all",
        btnClassOk: "btn btn-ghost-red",
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

  function sanitizeHtml(html) {
    const tpl = document.createElement("template");
    tpl.innerHTML = String(html);

    tpl.content.querySelectorAll("script,style").forEach((n) => n.remove());

    tpl.content.querySelectorAll("*").forEach((el) => {
      [...el.attributes].forEach((attr) => {
        const n = attr.name.toLowerCase();
        if (n.startsWith("on")) el.removeAttribute(attr.name);
        if ((n === "href" || n === "src") && /^javascript:/i.test(attr.value)) {
          el.removeAttribute(attr.name);
        }
      });
    });

    return tpl.innerHTML;
  }

  /**
   * openConfirm(options, onConfirm)
   *
   * New/optional options:
   * - html: string  → insert as HTML (sanitized by default)
   * - bodyNode: Node → insert DOM node
   * - dangerouslyAllowHtml: boolean → skip sanitizeHtml if true
   * - bodyIsText: boolean → force treat `body` as plain text
   */
  function openConfirm(
    {
      title = "Confirm",
      body = "Are you sure?",
      html = null,
      bodyNode = null,
      dangerouslyAllowHtml = false,
      bodyIsText = false,
      okText = "Delete",
      cancelText = "Cancel",
      onCancel = null,

      btnClassOk = null,
      btnClassCancel = null,
    },
    onConfirm
  ) {
    if (!cModal) {
      onConfirm?.();
      return;
    }

    cTitle.textContent = title;

    if (bodyNode instanceof Node) {
      cBody.replaceChildren(bodyNode);
    } else if (typeof html === "string") {
      cBody.innerHTML = dangerouslyAllowHtml ? html : sanitizeHtml(html);
    } else if (bodyIsText) {
      cBody.textContent = String(body ?? "");
    } else {
      const s = String(body ?? "");
      cBody.innerHTML = dangerouslyAllowHtml ? s : sanitizeHtml(s);
    }

    cOk.textContent = okText;
    if (cCancel) cCancel.textContent = cancelText || "Cancel";

    cOk.className = "btn btn-ghost";
    if (cCancel) cCancel.className = "btn btn-ghost";

    if (btnClassOk) cOk.className = btnClassOk;
    if (btnClassCancel && cCancel) cCancel.className = btnClassCancel;

    const isResume = !!cBody.querySelector(".resume-modal");
    if (isResume && !btnClassOk && !btnClassCancel) {
      cOk.className = "btn btn-ghost";
      if (cCancel) cCancel.className = "btn btn-ghost-red";
    }

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
      try {
        onConfirm?.();
      } finally {
        close();
      }
    };
    const onCancelClick = () => {
      try {
        onCancel?.();
      } finally {
        close();
      }
    };
    const onEsc = (e) => {
      if (e.key === "Escape") close();
    };
    const onBackdrop = (e) => {
      if (e.target === cBackdrop) close();
    };

    function teardown() {
      cOk.removeEventListener("click", onOk);
      cCancel?.removeEventListener("click", onCancelClick);
      cClose?.removeEventListener("click", close);
      cBackdrop?.removeEventListener("click", onBackdrop);
      document.removeEventListener("keydown", onEsc);
    }

    cOk.addEventListener("click", onOk, { once: true });
    cCancel?.addEventListener("click", onCancelClick, { once: true });
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

    group("WS OUT → /ws/in (client)", () => dbg({ env }));

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
      dbg("send → /ws/in", { readyState: sock.readyState, bytes: json.length });
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
      dbg("WS IN not ready, cannot send", { env });
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
    sock.onopen = () => dbg("WS IN connected");
    sock.onclose = () => dbg("WS IN closed");
    sock.onerror = (e) => dbg("WS IN error", e);
  }

  function ensureOut() {
    if (
      wsOut &&
      (wsOut.readyState === WebSocket.OPEN ||
        wsOut.readyState === WebSocket.CONNECTING)
    ) {
      return;
    }

    const url = location.origin.replace(/^http/, "ws") + "/ws/out";
    const sock = new WebSocket(url);
    wsOut = sock;

    sock.onopen = () => {
      dbg("WS OUT connected");
      inflightReady = false;
      fetchAndApplyInflight().finally(() => {
        inflightReady = true;
      });
    };

    sock.onclose = () => {
      dbg("WS OUT closed");
      window.showToast("Connection lost…", {
        type: "warning",
      });
    };

    sock.onerror = (e) => {
      dbg("WS OUT error", e);
      window.showToast("Connection issue — attempting to recover…", {
        type: "warning",
      });
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
        group("WS IN ← /ws/out", () =>
          dbg({ raw: ev.data?.slice?.(0, 2048) || ev.data })
        );
        const raw = JSON.parse(ev.data);
        const p = raw?.payload ?? raw;
        const kind = raw?.kind ?? p?.kind ?? "client";
        if (!p) return;
        const t = p?.type;
        dbg("[/ws/out] parsed", {
          kind,
          type: t,
          task_id: p?.task_id,
          data: p?.data,
        });

        if (kind === "client") {
          if (
            t === "backfill_started" ||
            t === "backfill_ack" ||
            t === "backfill_busy"
          ) {
            let cid = backfillIdFrom(p.data) || backfillIdFrom(p);
            cid = toOriginalCid(cid);
            if (!cid) return;
            if (!shouldTrustBackfillPayload(p, cid)) return;

            if (p.task_id && cid) rememberTask(p.task_id, cid);

            setCloneLaunching(cid, false);
            setCloneRunning(cid, true);
            setClonePulling(cid, true);
            startedHere.add(String(cid));
            setCardLoading(cid, true, PULLING_LABEL);
            if (shouldAnnounceNow() && launchingClones.has(String(cid))) {
              window.showToast(
                t === "backfill_busy"
                  ? "A clone for this channel is already running or finishing up."
                  : "Clone started…",
                { type: "warning" }
              );
            }
            closeBackfillDialog();
            return;
          }

          if (t === "backfill_progress") {
            const d = (p && (p.data ?? p)) || {};

            const delivered = d.delivered ?? d.applied ?? d.count ?? null;
            const total = d.expected_total ?? d.total ?? d.expected ?? null;
            let cid = backfillIdFrom(p.data) || backfillIdFrom(p);
            cid = toOriginalCid(cid);
            if (!cid) return;
            if (!shouldTrustBackfillPayload(p, cid)) return;

            const haveDelivered = Number.isFinite(delivered) && delivered > 0;
            const haveTotal = Number.isFinite(total) && total > 0;

            const pulling = !(haveDelivered || haveTotal);
            setClonePulling(cid, pulling);

            const card = document.querySelector(
              `.ch-card[data-cid="${String(cid)}"]`
            );

            if (pulling) {
              setCardLoading(cid, true, PULLING_LABEL);
              updateProgressBar(card, null, null);
            } else if (haveDelivered && haveTotal) {
              setCardLoading(
                cid,
                true,
                `Cloning (${fmtInt(delivered)}/${fmtInt(total)})`
              );
              updateProgressBar(card, delivered, total);
            } else if (haveDelivered) {
              setCardLoading(cid, true, `Cloning (${fmtInt(delivered)})`);
              updateProgressBar(card, delivered, null);
            } else {
              setCardLoading(cid, true, "Cloning");
              updateProgressBar(card, null, null);
            }
          }

          if (t === "backfill_cleanup") {
            const d = p.data || p;
            let cid = String(d.channel_id || "");
            cid = toOriginalCid(cid);
            if (!cid) return;
            const card = document.querySelector(`.ch-card[data-cid="${cid}"]`);

            if (d.state === "starting") {
              setCloneCleaning(cid, true);

              setCardLoading(cid, true, "Cleaning up");
              setProgressCleanupMode(card, true);
              return;
            }

            if (d.state === "finished") {
              setProgressCleanupMode(card, false);
              finalizeBackfillUI(cid, { announce: false });

              return;
            }
          }

          if (t === "backfill_done") {
            let cid = backfillIdFrom(p.data) || backfillIdFrom(p);
            if (!cid && p.task_id) cid = taskMap.get(String(p.task_id));
            cid = toOriginalCid(cid);
            if (!cid) return;
            if (!shouldTrustBackfillPayload(p, cid)) return;
            if (p.task_id) forgetTask(p.task_id);
            finalizeBackfillUI(cid, { announce: true });
            return;
          }

          if (t === "backfill_cancelled") {
            let cid = backfillIdFrom(p.data) || backfillIdFrom(p);
            if (!cid && p.task_id) cid = taskMap.get(String(p.task_id));
            cid = toOriginalCid(cid);
            dbg("[bf] cancelled", { cid, task_id: p?.task_id, payload: p });
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
          dbg("[verify] event", { type: p?.type, payload: p });
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
                    window.showToast(`Deleted "${name}"`, { type: "success" });
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
                      ? "warning"
                      : "error";
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
                  type: "success",
                });
              }

              sendVerify({ action: "list" });
              return;
            }
          }
        }
      } catch (e) {
        dbg("WS parse failed", e);
      }
    };
  }

  function sendVerify(payload) {
    ensureIn();
    const env = { kind: "verify", role: "ui", payload };
    const json = JSON.stringify(env);
    const sock = wsIn;

    group("WS OUT → /ws/in (verify)", () => dbg({ env }));

    if (sock?.readyState === WebSocket.OPEN) {
      dbg("send → /ws/in (verify)", { bytes: json.length });
      sock.send(json);
    } else if (sock) {
      sock.addEventListener(
        "open",
        () => {
          if (sock.readyState === WebSocket.OPEN) {
            dbg("WS open, sending → verify", { bytes: json.length });
            sock.send(json);
          }
        },
        { once: true }
      );
    } else {
      dbg("WS IN not ready, cannot send (verify)", { env });
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
            title: "Delete all orphans?",
            body: `This will delete <b>${catCount}</b> orphan ${
              catCount === 1 ? "category" : "categories"
            } and <b>${chCount}</b> orphan ${
              chCount === 1 ? "channel" : "channels"
            } that are <em>not part of the clone</em>.`,
            okText: "Delete all",
            btnClassOk: "btn btn-ghost-red",
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

    function onSubmit(ev) {
      ev.preventDefault();

      if (cloneIsLocked(cloneId)) return;

      if (startBtn) startBtn.disabled = true;

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
        if (startBtn) startBtn.disabled = false;
        return;
      }
      if (mode === "last" && !lastOk) {
        setInvalid(lastEl, true, "Enter a valid number.");
        lastEl?.focus();
        if (startBtn) startBtn.disabled = false;
        return;
      }
      if (mode === "between") {
        if (!fromRaw || !toRaw) {
          setInvalid(fromEl, !fromRaw, "Pick a date.");
          setInvalid(toEl, !toRaw, "Pick a date.");
          (fromRaw ? toEl : fromEl)?.focus();
          if (startBtn) startBtn.disabled = false;
          return;
        }
        if (!validateBetween(fromEl, toEl)) {
          fromEl?.focus();
          if (startBtn) startBtn.disabled = false;
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

      dbg("[REST] POST /api/backfill/start →", body);
      fetch("/api/backfill/start", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
        credentials: "same-origin",
        cache: "no-store",
      })
        .then(async (res) => {
          const json = await res.json().catch(() => ({}));
          dbg("[REST] /api/backfill/start ←", { status: res.status, json });

          if (!res.ok || json?.ok === false) {
            if (res.status === 409) {
              const { state } = json || {};
              setCloneLaunching(cloneId, false);
              toastOncePersist(
                `bf:already:${cloneId}`,
                state === "running"
                  ? "A clone for this channel is already running or finishing up."
                  : "A clone launch is already in progress.",
                { type: "warning" },
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
            { type: "success" },
            15000
          );
          startedHere.add(String(cloneId));
          closeBackfillDialog();
        })
        .catch(() => {
          unlockBackfill(cloneId);
          window.showToast("Network error starting clone.", { type: "error" });
        })
        .finally(() => {
          if (startBtn) startBtn.disabled = false;
        });
    }

    if (form) {
      form.setAttribute("novalidate", "");
      form.addEventListener("invalid", (e) => e.preventDefault(), true);

      if (form.__bfSubmit) form.removeEventListener("submit", form.__bfSubmit);
      form.__bfSubmit = onSubmit;
      form.addEventListener("submit", onSubmit);
    }

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

  async function checkResumeAndPrompt(originalId) {
    const cid = String(toOriginalCid(originalId));

    const fmt = (s) => (s ? new Date(s).toLocaleString() : "—");
    const esc = (s) =>
      String(s ?? "").replace(
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

    try {
      const res = await fetch(
        `/api/backfills/resume-info?channel_id=${encodeURIComponent(cid)}`,
        { credentials: "same-origin", cache: "no-store" }
      );
      const json = await res.json().catch(() => ({}));
      const info = json?.resume ?? json?.data ?? null;

      const canResume = !!(info?.available ?? info?.resumable);
      if (!canResume) {
        const row = findRowByAnyChannelId(cid);
        if (row) openBackfillDialog(row.original_channel_id);
        return;
      }

      const runId = info?.run_id || "—";
      const delivered = Number.isFinite(info?.delivered)
        ? info.delivered
        : null;
      const total = Number.isFinite(info?.expected_total)
        ? info.expected_total
        : null;
      const startedAtISO =
        info?.started_at || info?.started_dt || info?.startedAt || null;
      const updatedAtISO =
        info?.updated_at || info?.checkpoint?.last_orig_timestamp || null;

      const sentTxt = delivered != null ? delivered.toLocaleString() : "—";
      const totalTxt = total != null ? total.toLocaleString() : "—";

      const bodyHtml = `
        <div class="resume-modal">
          <p class="mb">A previous backfill for this channel was not finished.</p>
  
          <dl class="kv">
            <dt>Backfill ID:</dt>
            <dd><code class="inline-code" title="${esc(runId)}">${esc(
        runId
      )}</code></dd>
  
            <dt>Started At:</dt>
            <dd><code class="inline-code" title="${esc(
              fmt(startedAtISO)
            )}">${esc(fmt(startedAtISO))}</code></dd>
  
            <dt>Last Updated:</dt>
            <dd><code class="inline-code" title="${esc(
              fmt(updatedAtISO)
            )}">${esc(fmt(updatedAtISO))}</code></dd>
  
            <dt>Messages Sent:</dt>
            <dd>
              <code class="inline-code" title="${esc(sentTxt)}">${esc(
        sentTxt
      )}</code>
              /
              <code class="inline-code" title="${esc(totalTxt)}">${esc(
        totalTxt
      )}</code>
            </dd>
          </dl>
        </div>
      `;

      openConfirm(
        {
          title: "Resume previous backfill?",
          html: bodyHtml,
          okText: "Continue",
          cancelText: "Start Over",
          btnClassOk: "btn btn-ghost",
          btnClassCancel: "btn btn-ghost-red",
          onCancel: () => {
            const row = findRowByAnyChannelId(cid);
            if (row) openBackfillDialog(row.original_channel_id);
          },
        },
        async () => {
          setCloneLaunching(cid, true);
          setCardLoading(cid, true, "Resuming…");
          try {
            const resp = await fetch("/api/backfill/start", {
              method: "POST",
              credentials: "same-origin",
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify({
                channel_id: cid,
                resume: true,
                run_id: info?.run_id ?? undefined,
                checkpoint: info?.checkpoint ?? null,
              }),
            });

            const j = await resp.json().catch(() => ({}));

            if (!resp.ok || j?.ok === false) {
              toastOncePersist(
                `bf:resume:error:${cid}`,
                j?.error || `Couldn't resume (HTTP ${resp.status}).`,
                { type: "error" },
                15000
              );
              throw new Error(j?.error || `HTTP ${resp.status}`);
            }
          } catch (e) {
            console.error("Resume backfill failed:", e);
            setCloneLaunching(cid, false);
            setCardLoading(cid, false);

            toastOncePersist(
              `bf:resume:error:${cid}`,
              "Couldn't resume the clone. You can start a new backfill.",
              { type: "error" },
              15000
            );

            const row = findRowByAnyChannelId(cid);
            if (row) openBackfillDialog(row.original_channel_id);
          }
        }
      );
    } catch (e) {
      console.error("resume-info fetch failed:", e);

      toastOncePersist(
        `bf:resume-info:error:${cid}`,
        "Couldn’t check resume status. You can start a new backfill.",
        { type: "warning" },
        12000
      );

      const row = findRowByAnyChannelId(cid);
      if (row) openBackfillDialog(row.original_channel_id);
    }
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
      checkResumeAndPrompt(id);
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
