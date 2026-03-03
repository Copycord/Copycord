(() => {
  const root = document.getElementById("logs-root");
  if (!root) return;

  let allLogs = [];
  let currentType = "";
  let currentSearch = "";
  let sortColumn = "created_at";
  let sortDir = "desc";
  let debounceTimer = null;

  const tbody = document.getElementById("logs-tbody");
  const emptyEl = document.getElementById("logs-empty");
  const countEl = document.getElementById("logs-count");
  const filterEl = document.getElementById("logs-filter-type");
  const searchEl = document.getElementById("logs-search");
  const clearAllBtn = document.getElementById("logs-clear-all");
  const clearFiltersBtn = document.getElementById("logs-clear-filters");

  function blurActive() {
    const ae = document.activeElement;
    if (ae && typeof ae.blur === "function") ae.blur();
  }

  function ensureConfirmModal() {
    let modal = document.getElementById("confirm-modal");
    if (!modal) {
      modal = document.createElement("div");
      modal.id = "confirm-modal";
      modal.className = "modal";
      modal.setAttribute("aria-hidden", "true");
      modal.innerHTML = `
        <div class="modal-backdrop"></div>
        <div class="modal-content" role="dialog" aria-modal="true" aria-labelledby="confirm-title" tabindex="-1">
          <div class="modal-header">
            <h4 id="confirm-title" class="modal-title">Confirm</h4>
            <button type="button" id="confirm-close" class="icon-btn verify-close" aria-label="Close">✕</button>
          </div>
          <div class="p-4" id="confirm-body" style="padding:12px 16px;"></div>
          <div class="btns" style="padding:0 16px 16px 16px;">
            <button type="button" id="confirm-cancel" class="btn btn-ghost">Cancel</button>
            <button type="button" id="confirm-okay" class="btn btn-ghost">OK</button>
          </div>
        </div>
      `;
      document.body.appendChild(modal);
    }
    let style = document.getElementById("confirm-modal-patch");
    const css = `
      #confirm-modal { display: none; }
      #confirm-modal.show {
        display: flex; opacity: 1; visibility: visible;
        align-items: center; justify-content: center; z-index: 90;
      }
      #confirm-modal .modal-content:focus { outline: none; box-shadow: none; }
      #confirm-modal .btn:focus,
      #confirm-modal .btn:focus-visible { outline: none; box-shadow: none; }
    `;
    if (!style) {
      style = document.createElement("style");
      style.id = "confirm-modal-patch";
      style.textContent = css;
      document.head.appendChild(style);
    } else {
      style.textContent = css;
    }
    return modal;
  }

  function openConfirm({
    title,
    body,
    confirmText = "OK",
    confirmClass = "btn-ghost",
    onConfirm,
    showCancel = true,
  }) {
    const cModal = ensureConfirmModal();
    const cTitle = cModal.querySelector("#confirm-title");
    const cBody = cModal.querySelector("#confirm-body");
    const cBtnOk = cModal.querySelector("#confirm-okay");
    const cBtnCa = cModal.querySelector("#confirm-cancel");
    const cBtnX = cModal.querySelector("#confirm-close");
    const cBack = cModal.querySelector(".modal-backdrop");
    const dialog = cModal.querySelector(".modal-content");

    blurActive();
    if (cTitle) cTitle.textContent = title || "Confirm";
    if (cBody) cBody.textContent = body || "Are you sure?";
    if (cBtnOk) cBtnOk.textContent = confirmText || "OK";
    if (cBtnOk) cBtnOk.className = `btn ${confirmClass || "btn-ghost"}`;
    if (cBtnCa) cBtnCa.hidden = !showCancel;

    const close = () => {
      cModal.classList.remove("show");
      cModal.setAttribute("aria-hidden", "true");
      document.body.classList.remove("body-lock-scroll");
    };
    if (cBtnOk)
      cBtnOk.onclick = () => {
        try {
          if (typeof onConfirm === "function") onConfirm();
        } finally {
          close();
        }
      };
    if (cBtnCa) cBtnCa.onclick = close;
    if (cBtnX) cBtnX.onclick = close;
    if (cBack) cBack.onclick = close;

    const onKey = (e) => {
      if (e.key === "Escape") {
        e.preventDefault();
        close();
        document.removeEventListener("keydown", onKey, { capture: true });
      }
    };
    document.addEventListener("keydown", onKey, { capture: true });

    cModal.classList.add("show");
    cModal.setAttribute("aria-hidden", "false");
    document.body.classList.add("body-lock-scroll");
    requestAnimationFrame(() => {
      if (dialog) dialog.focus({ preventScroll: true });
      else if (cBtnOk) cBtnOk.focus();
    });
  }

  const TYPE_META = {
    channel_created: { label: "Channel Created", cls: "log-created" },
    channel_deleted: { label: "Channel Deleted", cls: "log-deleted" },
    channel_renamed: { label: "Channel Renamed", cls: "log-renamed" },
    channel_moved: { label: "Channel Moved", cls: "log-moved" },
    channel_converted: { label: "Channel Converted", cls: "log-converted" },
    category_created: { label: "Category Created", cls: "log-created" },
    category_deleted: { label: "Category Deleted", cls: "log-deleted" },
    category_renamed: { label: "Category Renamed", cls: "log-renamed" },
    thread_created: { label: "Thread Created", cls: "log-thread" },
    thread_deleted: { label: "Thread Deleted", cls: "log-deleted" },
    thread_renamed: { label: "Thread Renamed", cls: "log-renamed" },
    forum_created: { label: "Forum Created", cls: "log-created" },
    forum_renamed: { label: "Forum Renamed", cls: "log-renamed" },
    forum_moved: { label: "Forum Moved", cls: "log-moved" },
    role_created: { label: "Role Created", cls: "log-role" },
    role_deleted: { label: "Role Deleted", cls: "log-deleted" },
    role_updated: { label: "Role Updated", cls: "log-role" },
    emoji_created: { label: "Emoji Created", cls: "log-emoji" },
    emoji_deleted: { label: "Emoji Deleted", cls: "log-deleted" },
    emoji_renamed: { label: "Emoji Renamed", cls: "log-emoji" },
    emoji_synced: { label: "Emoji Synced", cls: "log-emoji" },
    sticker_created: { label: "Sticker Created", cls: "log-sticker" },
    sticker_deleted: { label: "Sticker Deleted", cls: "log-deleted" },
    sticker_renamed: { label: "Sticker Renamed", cls: "log-sticker" },
    sticker_synced: { label: "Sticker Synced", cls: "log-sticker" },
    guild_metadata: { label: "Guild Metadata", cls: "log-guild" },
    channel_metadata_updated: {
      label: "Channel Metadata",
      cls: "log-metadata",
    },
    voice_metadata_updated: { label: "Voice Metadata", cls: "log-metadata" },
    stage_metadata_updated: { label: "Stage Metadata", cls: "log-metadata" },
    forum_metadata_updated: { label: "Forum Metadata", cls: "log-metadata" },
    permissions_synced: { label: "Permissions Synced", cls: "log-permissions" },
    webhook_created: { label: "Webhook Created", cls: "log-webhook" },
    error: { label: "Error", cls: "log-error" },
  };

  function getMeta(type) {
    return (
      TYPE_META[type] || {
        label: type.replace(/_/g, " ").replace(/\b\w/g, (c) => c.toUpperCase()),
        cls: "log-default",
      }
    );
  }

  function fmtTimestamp(epoch) {
    if (!epoch) return "—";
    try {
      const d = new Date(epoch * 1000);
      const pad = (n) => String(n).padStart(2, "0");
      return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(
        d.getDate()
      )} ${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
    } catch {
      return "—";
    }
  }

  function renderRow(log) {
    const meta = getMeta(log.event_type);
    const tr = document.createElement("tr");
    tr.className = "log-row";
    tr.dataset.logId = log.log_id;

    const guildText =
      log.guild_name || (log.guild_id ? String(log.guild_id) : "SYSTEM");

    tr.innerHTML =
      `<td class="lt-col-time"><span class="log-ts">${fmtTimestamp(
        log.created_at
      )}</span></td>` +
      `<td class="lt-col-type"><span class="log-type-badge ${meta.cls}">${esc(
        meta.label
      )}</span></td>` +
      `<td class="lt-col-guild"><span class="log-guild-label">${esc(
        guildText
      )}</span></td>` +
      `<td class="lt-col-details"><span class="log-detail-text">${esc(
        log.details
      )}</span></td>` +
      `<td class="lt-col-actions"><button class="log-delete-btn" data-log-id="${log.log_id}" title="Delete" aria-label="Delete log">` +
      `<svg viewBox="0 0 24 24" width="14" height="14" fill="none" stroke="currentColor" stroke-width="1.5">` +
      `<path stroke-linecap="round" stroke-linejoin="round" d="M6 18 18 6M6 6l12 12" />` +
      `</svg></button></td>`;

    return tr;
  }

  function esc(s) {
    if (!s) return "";
    return String(s)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function getFilteredSorted() {
    let filtered = allLogs;

    if (currentType) {
      filtered = filtered.filter((l) => l.event_type === currentType);
    }
    if (currentSearch) {
      const q = currentSearch.toLowerCase();
      filtered = filtered.filter(
        (l) =>
          (l.details || "").toLowerCase().includes(q) ||
          (l.guild_name || "").toLowerCase().includes(q) ||
          (l.channel_name || "").toLowerCase().includes(q) ||
          (l.event_type || "").toLowerCase().includes(q)
      );
    }

    filtered.sort((a, b) => {
      let va = a[sortColumn];
      let vb = b[sortColumn];

      if (va == null) va = "";
      if (vb == null) vb = "";

      if (typeof va === "string") va = va.toLowerCase();
      if (typeof vb === "string") vb = vb.toLowerCase();

      let cmp = 0;
      if (va < vb) cmp = -1;
      else if (va > vb) cmp = 1;

      return sortDir === "asc" ? cmp : -cmp;
    });

    return filtered;
  }

  function renderAll() {
    const logs = getFilteredSorted();
    tbody.innerHTML = "";

    if (logs.length === 0) {
      emptyEl.style.display = "";
      countEl.textContent = "";
    } else {
      emptyEl.style.display = "none";
      countEl.textContent = `Showing ${logs.length} of ${allLogs.length} logs`;
      const frag = document.createDocumentFragment();
      logs.forEach((log) => frag.appendChild(renderRow(log)));
      tbody.appendChild(frag);
    }
    updateClearFilters();
  }

  async function loadLogs() {
    try {
      const res = await fetch("/api/event-logs?limit=10000&offset=0", {
        credentials: "same-origin",
        cache: "no-store",
        headers: { "Cache-Control": "no-cache" },
      });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const data = await res.json();

      allLogs = data.logs || [];
      renderAll();
    } catch (err) {
      console.error("Failed to load event logs:", err);
    }
  }

  function populateFilterDropdown() {
    while (filterEl.options.length > 1) filterEl.remove(1);

    const sorted = Object.keys(TYPE_META).sort((a, b) => {
      const la = TYPE_META[a].label.toLowerCase();
      const lb = TYPE_META[b].label.toLowerCase();
      return la < lb ? -1 : la > lb ? 1 : 0;
    });

    sorted.forEach((t) => {
      const opt = document.createElement("option");
      opt.value = t;
      opt.textContent = TYPE_META[t].label;
      filterEl.appendChild(opt);
    });
  }

  function updateClearFilters() {
    clearFiltersBtn.style.display = currentType || currentSearch ? "" : "none";
  }

  document.querySelectorAll(".logs-table th.sortable").forEach((th) => {
    th.addEventListener("click", () => {
      const col = th.dataset.sort;
      if (sortColumn === col) {
        sortDir = sortDir === "asc" ? "desc" : "asc";
      } else {
        sortColumn = col;
        sortDir = col === "created_at" ? "desc" : "asc";
      }

      document.querySelectorAll(".logs-table th.sortable").forEach((h) => {
        const arrow = h.querySelector(".sort-arrow");
        if (h.dataset.sort === sortColumn) {
          h.classList.add("sorted");
          arrow.textContent = sortDir === "asc" ? "▲" : "▼";
        } else {
          h.classList.remove("sorted");
          arrow.textContent = "";
        }
      });
      renderAll();
    });
  });

  filterEl.addEventListener("change", () => {
    currentType = filterEl.value;
    renderAll();
  });

  searchEl.addEventListener("input", () => {
    clearTimeout(debounceTimer);
    debounceTimer = setTimeout(() => {
      currentSearch = searchEl.value.trim();
      renderAll();
    }, 200);
  });

  clearFiltersBtn.addEventListener("click", () => {
    currentType = "";
    currentSearch = "";
    searchEl.value = "";
    filterEl.value = "";
    filterEl.dispatchEvent(new Event("change", { bubbles: true }));
    renderAll();
  });

  tbody.addEventListener("click", async (e) => {
    const btn = e.target.closest(".log-delete-btn");
    if (!btn) return;
    const logId = btn.dataset.logId;
    if (!logId) return;

    btn.disabled = true;
    try {
      const res = await fetch(`/api/event-logs/${logId}`, {
        method: "DELETE",
        credentials: "same-origin",
      });
      if (res.ok) {
        const row = tbody.querySelector(`tr[data-log-id="${logId}"]`);
        if (row) {
          row.classList.add("log-removing");
          row.addEventListener(
            "animationend",
            () => {
              row.remove();
              allLogs = allLogs.filter((l) => l.log_id !== logId);
              if (allLogs.length === 0) {
                emptyEl.style.display = "";
                countEl.textContent = "";
              } else {
                const visible = tbody.querySelectorAll("tr").length;
                countEl.textContent = `Showing ${visible} of ${allLogs.length} logs`;
              }
            },
            { once: true }
          );
        }
      }
    } catch (err) {
      console.error("Delete log failed:", err);
      btn.disabled = false;
    }
  });

  clearAllBtn.addEventListener("click", () => {
    openConfirm({
      title: "Delete All Logs",
      body: "Are you sure you want to delete all event logs? This cannot be undone.",
      confirmText: "Delete All",
      confirmClass: "btn-ghost-red",
      onConfirm: async () => {
        clearAllBtn.disabled = true;
        try {
          const res = await fetch("/api/event-logs", {
            method: "DELETE",
            credentials: "same-origin",
          });
          if (res.ok) {
            allLogs = [];
            renderAll();
            window.showToast?.("All logs cleared", { type: "success" });
          }
        } catch (err) {
          console.error("Clear logs failed:", err);
        } finally {
          clearAllBtn.disabled = false;
        }
      },
    });
  });

  populateFilterDropdown();
  loadLogs();
})();
