(() => {
  const loader = document.getElementById("app-loader");

  function setHeaderOffset() {
    const header = document.querySelector(".site-header");
    let bottom = 0;
    if (header) {
      const rect = header.getBoundingClientRect();
      bottom = Math.max(0, Math.round(rect.bottom));
    }
    document.documentElement.style.setProperty(
      "--header-bottom",
      `${bottom}px`
    );
  }

  function showLoader() {
    if (!loader) return;
    document.body.classList.add("is-loading");
    loader.classList.remove("is-hidden", "is-hiding");
    setHeaderOffset();

    void loader.offsetHeight;
  }

  function waitForTransitions(el, props = [], timeoutMs = 700) {
    return new Promise((resolve) => {
      try {
        if (
          matchMedia &&
          matchMedia("(prefers-reduced-motion: reduce)").matches
        ) {
          resolve();
          return;
        }
      } catch {}

      let remaining = new Set(props);
      let done = false;

      const finish = () => {
        if (done) return;
        done = true;
        el.removeEventListener("transitionend", onEnd);
        resolve();
      };

      const onEnd = (e) => {
        if (!remaining.size) return;

        if (remaining.has(e.propertyName)) {
          remaining.delete(e.propertyName);
        }
        if (remaining.size === 0) finish();
      };

      if (!props.length) remaining = new Set(["__any__"]);

      el.addEventListener("transitionend", onEnd);
      setTimeout(finish, timeoutMs);
    });
  }

  async function hideLoader() {
    if (!loader) return;

    loader.classList.add("is-hiding");

    await waitForTransitions(loader, ["opacity"], 800);

    loader.classList.add("is-hidden");
    document.body.classList.remove("is-loading");
  }

  setHeaderOffset();
  window.addEventListener("resize", setHeaderOffset);
  window.addEventListener("scroll", setHeaderOffset, { passive: true });
  window.addEventListener("load", setHeaderOffset, { once: true });

  const pageLoaded = new Promise((resolve) =>
    window.addEventListener("load", resolve, { once: true })
  );

  window.loaderTest = {
    show: showLoader,
    hide: hideLoader,
    setOffset: setHeaderOffset,
  };

  const advRoot = document.getElementById("adv-root");
  const $ = (sel) => advRoot?.querySelector(sel);
  const SHRUG = String.raw`¯\_(ツ)_/¯`;

  // Generic date-time (kept in case it's used elsewhere)
  const fmtWhen = (iso) => {
    if (!iso) return "—";
    try {
      return new Date(iso).toLocaleString();
    } catch {
      return iso;
    }
  };

  const fmtBytes = (n) => {
    const b = Number(n || 0);
    if (b < 1024) return `${b.toLocaleString()} B`;
    const kb = b / 1024;
    if (kb < 1024) return `${kb.toFixed(kb < 10 ? 1 : 0)} KB`;
    const mb = kb / 1024;
    if (mb < 1024) return `${mb.toFixed(mb < 10 ? 1 : 0)} MB`;
    const gb = mb / 1024;
    return `${gb.toFixed(gb < 10 ? 1 : 0)} GB`;
  };

  const MONTHS = [
    "Jan",
    "Feb",
    "Mar",
    "Apr",
    "May",
    "Jun",
    "Jul",
    "Aug",
    "Sep",
    "Oct",
    "Nov",
    "Dec",
  ];
  function fmtBackupWhen(input) {
    let d;
    try {
      if (typeof input === "number") d = new Date(input * 1000);
      else if (typeof input === "string") d = new Date(input);
      else if (input instanceof Date) d = input;
      else return "—";
      if (isNaN(d.getTime())) return "—";
    } catch {
      return "—";
    }

    const now = new Date();
    const sameDay =
      d.getFullYear() === now.getFullYear() &&
      d.getMonth() === now.getMonth() &&
      d.getDate() === now.getDate();

    if (sameDay) {
      let h = d.getHours();
      const ampm = h >= 12 ? "pm" : "am";
      h = h % 12;
      if (h === 0) h = 12;
      const m = d.getMinutes().toString().padStart(2, "0");
      return `${h}:${m}${ampm}`;
    } else {
      return `${MONTHS[d.getMonth()]} ${d.getDate()} ${d.getFullYear()}`;
    }
  }

  function blurActive() {
    const ae = document.activeElement;
    if (ae && typeof ae.blur === "function") ae.blur();
  }

  function ensureToastUnderHeader() {
    const toastRoot = document.getElementById("toast-root");
    if (!toastRoot) return;

    let style = document.getElementById("toast-pos-patch");
    const css = `
      #toast-root.toast-top-center {
        position: fixed;
        left: 50%;
        transform: translateX(-50%);
        right: auto;
        bottom: auto;
        z-index: 2147483647;
        pointer-events: none;
      }
      #toast-root.toast-top-center > * { pointer-events: auto; }
    `;
    if (!style) {
      style = document.createElement("style");
      style.id = "toast-pos-patch";
      style.textContent = css;
      document.head.appendChild(style);
    } else {
      style.textContent = css;
    }

    const place = () => {
      const header = document.querySelector(".site-header");
      const gap = 12;
      let topPx = gap;
      if (header) {
        const rect = header.getBoundingClientRect();
        topPx = Math.max(rect.bottom, 0) + gap;
      }
      toastRoot.style.top = `${topPx}px`;
    };

    toastRoot.classList.add("toast-top-center");
    place();
    window.addEventListener("resize", place);
    window.addEventListener("scroll", place, { passive: true });
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
    #confirm-modal .modal-content:focus { outline: none; box-shadow: none; }
    #confirm-modal .btn:focus,
    #confirm-modal .btn:focus-visible {
      outline: none;
      box-shadow: none;
    }
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

  function themedConfirm({
    title,
    body,
    confirmText = "OK",
    cancelText = "Cancel",
    btnClassOk = "btn btn-ghost",
    btnClassCancel = "btn btn-ghost",
  } = {}) {
    return new Promise((resolve) => {
      const cModal = ensureConfirmModal();
      const cTitle = cModal.querySelector("#confirm-title");
      const cBody = cModal.querySelector("#confirm-body");
      const cOk = cModal.querySelector("#confirm-okay");
      const cCa = cModal.querySelector("#confirm-cancel");
      const cX = cModal.querySelector("#confirm-close");
      const cBack = cModal.querySelector(".modal-backdrop");
      const dialog = cModal.querySelector(".modal-content");

      const norm = (s) => String(s || "").trim();
      const ensureBtnPrefix = (s) => {
        const k = norm(s);
        return k ? (/\bbtn\b/.test(k) ? k : `btn ${k}`) : "btn btn-ghost";
      };
      const stripBtnVariants = (el) => {
        if (!el) return;
        el.classList.remove(
          "btn-primary",
          "btn-danger",
          "btn-outline",
          "btn-ghost",
          "btn-ghost-red",
          "btn-ghost-purple",
          "btn-ghost-green"
        );
      };

      if (cTitle) cTitle.textContent = title || "Confirm";
      if (cBody) cBody.textContent = body || "Are you sure?";

      if (cOk) {
        cOk.textContent = confirmText;
        stripBtnVariants(cOk);
        cOk.className = ensureBtnPrefix(btnClassOk);
      }
      if (cCa) {
        cCa.textContent = cancelText;
        stripBtnVariants(cCa);
        cCa.className = ensureBtnPrefix(btnClassCancel);
      }

      const close = (result) => {
        cModal.classList.remove("show");
        cModal.setAttribute("aria-hidden", "true");
        document.body.classList.remove("body-lock-scroll");

        cOk && cOk.removeEventListener("click", onOk);
        cCa && cCa.removeEventListener("click", onNo);
        cX && cX.removeEventListener("click", onNo);
        cBack && cBack.removeEventListener("click", onNo);
        document.removeEventListener("keydown", onKey, true);

        setTimeout(blurActive, 0);
        resolve(result);
      };

      const onOk = () => close(true);
      const onNo = () => close(false);
      const onKey = (e) => {
        if (e.key === "Escape") {
          e.preventDefault();
          close(false);
        }
        if (e.key === "Enter") {
          if (dialog && dialog.contains(document.activeElement)) {
            e.preventDefault();
            close(true);
          }
        }
      };

      blurActive();

      cModal.classList.add("show");
      cModal.setAttribute("aria-hidden", "false");
      document.body.classList.add("body-lock-scroll");

      requestAnimationFrame(() => {
        if (dialog && typeof dialog.focus === "function") {
          dialog.focus({ preventScroll: true });
        } else if (cOk && typeof cOk.focus === "function") {
          cOk.focus();
        }
      });

      cOk && cOk.addEventListener("click", onOk);
      cCa && cCa.addEventListener("click", onNo);
      cX && cX.addEventListener("click", onNo);
      cBack && cBack.addEventListener("click", onNo);
      document.addEventListener("keydown", onKey, true);
    });
  }

  ensureToastUnderHeader();

  const lastAtEl = $("#adv-last-backup");
  const lastSizeEl = $("#adv-last-size");
  const dirEl = $("#adv-dir");
  const tableBody = $("#adv-archives tbody");
  const tableEl = $("#adv-archives");
  const tableWrap = tableEl ? tableEl.closest(".table-wrap") : null;
  const dlLatest = $("#adv-download-latest");
  const btnBackup = $("#adv-backup-now");
  const inputFile = $("#adv-restore-file");

  function measureFirstNRowsHeight(tbody, n, fallback = 44) {
    if (!tbody || !tbody.rows || !tbody.rows.length) return fallback * n;
    const limit = Math.min(n, tbody.rows.length);
    let total = 0;
    for (let i = 0; i < limit; i++) {
      const h =
        Math.ceil(tbody.rows[i].getBoundingClientRect().height) || fallback;
      total += h;
    }
    return total || fallback * n;
  }

  function sizeArchivesViewport(maxRows = 5) {
    if (!tableEl || !tableWrap) return;

    const tbody = tableEl.tBodies?.[0];
    const thead = tableEl.tHead || tableEl.querySelector("thead");
    const rowCount = tbody?.rows?.length || 0;

    tableWrap?.classList.toggle("has-scroll", rowCount > maxRows);

    if (rowCount <= maxRows) {
      tableWrap.style.maxHeight = "";
      tableWrap.style.overflowY = "";
      return;
    }

    const measure = () => {
      const headH = thead
        ? Math.ceil(thead.getBoundingClientRect().height) || 36
        : 36;
      const rowsH = measureFirstNRowsHeight(tbody, maxRows, 44);
      tableWrap.style.maxHeight = `${headH + rowsH}px`;
      tableWrap.style.overflowY = "auto";
    };

    requestAnimationFrame(() => requestAnimationFrame(measure));
  }

  function updateLastBackupDisplay(list) {
    if (!lastAtEl) return;
    if (!list || list.length === 0) {
      lastAtEl.textContent = SHRUG;
    } else {
      const first = list[0];
      const when = fmtBackupWhen(first.mtime);
      lastAtEl.textContent = when;
    }
    if (lastSizeEl) lastSizeEl.textContent = "";
  }

  async function loadInfo() {
    const r = await fetch("/api/backup/info");
    const j = await r.json();
    if (!j.ok) throw new Error("failed");

    if (dirEl) dirEl.textContent = j.dir || "";

    if (tableBody) {
      tableBody.innerHTML = "";
      const list = j.archives || [];

      if (dlLatest) {
        dlLatest.setAttribute("aria-disabled", "true");
        dlLatest.classList.add("is-disabled");
        dlLatest.removeAttribute("href");
      }

      list.forEach((a, i) => {
        const fullName = a.name || "";
        const displayName = fullName.replace(/\.tar\.gz$/i, "");
        const when = fmtBackupWhen(a.mtime);

        const tr = document.createElement("tr");
        tr.innerHTML = `
          <td><code title="${fullName}">${displayName}</code></td>
          <td>${fmtBytes(a.size)}</td>
          <td>${when}</td>
          <td>
            <a class="btn btn-ghost" href="/api/backup/download/${encodeURIComponent(
              fullName
            )}">Download</a>
            <button class="btn btn-ghost" data-restore="${fullName}">Restore</button>
            <button class="btn btn-ghost-red" data-delete="${fullName}">Delete</button>
          </td>`;
        tableBody.appendChild(tr);

        if (i === 0 && dlLatest) {
          dlLatest.href = `/api/backup/download/${encodeURIComponent(
            fullName
          )}`;
          dlLatest.removeAttribute("aria-disabled");
          dlLatest.classList.remove("is-disabled");
        }
      });

      updateLastBackupDisplay(list);
      sizeArchivesViewport(5);

      const mo = new MutationObserver(() => sizeArchivesViewport(5));
      mo.observe(tableBody, { childList: true, subtree: false });
    }
  }

  async function backupNow() {
    if (btnBackup) btnBackup.disabled = true;
    try {
      const r = await fetch("/admin/backup-now", { method: "POST" });
      const j = await r.json();
      if (!j.ok) throw new Error("backup failed");
      await loadInfo();
      window.showToast?.("Backup created", { type: "success" });
    } catch (e) {
      console.error(e);
      window.showToast?.("Backup failed", { type: "error" });
    } finally {
      if (btnBackup) btnBackup.disabled = false;
    }
  }

  async function restoreExisting(name) {
    const ok = await themedConfirm({
      title: "Restore database?",
      body: `Restore database from “${name}”? This will stop bots and replace the database.`,
      confirmText: "Restore",
      cancelText: "Cancel",
      btnClassOk: "btn btn-ghost-red",
      btnClassCancel: "btn btn-ghost",
    });
    if (!ok) return;

    const fd = new FormData();
    fd.append("source", "existing");
    fd.append("name", name);

    const r = await fetch("/api/backup/restore", { method: "POST", body: fd });
    const j = await r.json().catch(() => ({}));
    if (!r.ok || !j.ok) throw new Error("restore failed");

    await loadInfo();
    window.showToast?.("Database restored.", { type: "success" });
  }

  async function restoreUpload(file) {
    const ok = await themedConfirm({
      title: "Restore database?",
      body: `Restore database from uploaded file “${file.name}”? This will stop bots and replace the database.`,
      confirmText: "Restore",
      cancelText: "Cancel",
      btnClassOk: "btn btn-ghost-red",
      btnClassCancel: "btn btn-ghost",
    });
    if (!ok) return;

    const fd = new FormData();
    fd.append("source", "upload");
    fd.append("file", file);

    const r = await fetch("/api/backup/restore", { method: "POST", body: fd });
    const j = await r.json().catch(() => ({}));
    if (!r.ok || !j.ok) throw new Error("restore failed");

    await loadInfo();
    window.showToast?.("Database restored from upload.", { type: "success" });
  }

  advRoot?.addEventListener("click", (e) => {
    const t = e.target;
    const restoreName = t?.dataset?.restore;
    const deleteName = t?.dataset?.delete;

    if (restoreName) {
      e.preventDefault();
      blurActive();
      restoreExisting(restoreName).catch(console.error);
      return;
    }

    if (deleteName) {
      e.preventDefault();
      blurActive();

      if (t.disabled) return;
      themedConfirm({
        title: "Delete backup?",
        body: `Permanently delete “${deleteName}”? This cannot be undone.`,
        confirmText: "Delete",
        cancelText: "Cancel",
        btnClassOk: "btn btn-ghost-red",
        btnClassCancel: "btn btn-ghost",
      })
        .then(async (ok) => {
          if (!ok) return;
          try {
            t.disabled = true;
            t.textContent = "Deleting…";
            const fd = new FormData();
            fd.append("name", deleteName);
            const r = await fetch("/api/backup/delete", {
              method: "POST",
              body: fd,
            });
            const j = await r.json().catch(() => ({}));
            if (!r.ok || !j.ok) throw new Error("delete failed");
            await loadInfo();
            window.showToast?.("Backup deleted", { type: "success" });
          } catch (err) {
            console.error(err);
            window.showToast?.("Delete failed", { type: "error" });
          } finally {
            t.disabled = false;
            t.textContent = "Delete";
            setTimeout(blurActive, 0);
          }
        })
        .catch((err) => {
          console.error(err);
          window.showToast?.("Delete failed", { type: "error" });
        });
    }
  });

  btnBackup?.addEventListener("click", () => backupNow().catch(console.error));

  inputFile?.addEventListener("change", (e) => {
    const input = e.target;
    const f = input.files?.[0];
    if (!f) return;

    blurActive();
    setTimeout(blurActive, 0);

    restoreUpload(f)
      .catch((err) => {
        console.error(err);
        window.showToast?.("Restore failed", { type: "error" });
      })
      .finally(() => {
        input.value = "";
        setTimeout(blurActive, 0);
      });
  });

  (function () {
    const card = document.getElementById("adv-data-card");
    const btn = document.getElementById("adv-collapse-btn");
    const panel = document.getElementById("adv-collapse-panel");
    if (!card || !btn || !panel) return;

    const rowsTbody = panel.querySelector("#adv-archives tbody");
    const headThead = panel.querySelector("#adv-archives thead");
    const actions = panel.querySelector(".card-actions");
    const dirNote = panel.querySelector("#adv-dir")?.closest("p");

    const isInteractive = (el) =>
      !!el?.closest?.(
        'a,button,input,select,textarea,label,[role="button"],[contenteditable],.btn'
      );

    const isOpen = () => btn.getAttribute("aria-expanded") === "true";

    const setOpen = (open) => {
      card.classList.toggle("is-collapsed", !open);
      btn.setAttribute("aria-expanded", open ? "true" : "false");

      const chev = btn.querySelector(".chev");
      if (chev) chev.textContent = open ? "▾" : "▸";

      if (rowsTbody) rowsTbody.style.display = open ? "" : "none";
      if (headThead) headThead.style.display = open ? "" : "none";
      if (actions) actions.style.display = open ? "" : "none";
      if (dirNote) dirNote.style.display = open ? "" : "none";

      if (open) sizeArchivesViewport(5);
    };

    try {
      localStorage.removeItem("cpc.panel.database.open");
    } catch {}
    setOpen(false);

    btn.addEventListener("click", (e) => {
      e.stopPropagation();
      setOpen(!isOpen());
    });

    card.addEventListener("click", (e) => {
      if (isInteractive(e.target)) return;
      if (!isOpen()) setOpen(true);
    });

    let openBeforePointer = false;
    card.addEventListener(
      "mousedown",
      () => {
        openBeforePointer = isOpen();
      },
      true
    );

    card.addEventListener("dblclick", (e) => {
      if (isInteractive(e.target)) return;
      if (openBeforePointer && isOpen()) setOpen(false);
    });
  })();

  window.addEventListener("resize", () => sizeArchivesViewport(5));
  window.addEventListener("load", () => sizeArchivesViewport(5));

  const dataReady = advRoot
    ? loadInfo().catch((e) => {
        console.error(e);
      })
    : Promise.resolve();

  Promise.allSettled([pageLoaded, dataReady]).then(() => hideLoader());
})();
