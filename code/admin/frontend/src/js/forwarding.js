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
      display: flex;
      opacity: 1;
      visibility: visible;
      align-items: center;
      justify-content: center;
      z-index: 90;
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

  if (cBtnOk) {
    cBtnOk.onclick = () => {
      try {
        if (typeof onConfirm === "function") onConfirm();
      } finally {
        close();
      }
    };
  }
  if (cBtnCa) cBtnCa.onclick = () => close();
  if (cBtnX) cBtnX.onclick = () => close();
  if (cBack) cBack.onclick = () => close();

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
    if (dialog && typeof dialog.focus === "function")
      dialog.focus({ preventScroll: true });
    else if (cBtnOk && typeof cBtnOk.focus === "function") cBtnOk.focus();
  });
}

export class ForwardingSystem {
  constructor(opts = {}) {
    this.showToast =
      typeof opts.showToast === "function"
        ? opts.showToast
        : (msg, _opts) => alert(msg);

    this.root = null;
    this.guildSelect = null;
    this.listEl = null;
    this.emptyEl = null;
    this.modalEl = null;
    this.formEl = null;

    this.guilds = [];
    this.currentItems = [];
    this.isSaving = false;
    this.guildsPromise = null;
    this.hardFail = false;
    this.guildsLoaded = false;
    this.rulesLoaded = false;
    this.searchQuery = "";

    this.forwardedTotalCount = null;
    this.forwardedByRule = {};
    this._countsPollTimer = null;
    this._countsPollInFlight = false;
  }

  startCountsAutoRefresh(intervalMs = 5000) {
    this.stopCountsAutoRefresh();

    const tick = async () => {
      if (document.hidden) return;
      if (this._countsPollInFlight) return;

      this._countsPollInFlight = true;
      try {
        await Promise.allSettled([
          this.loadForwardingCount(),
          this.loadForwardingCountsByRule(),
        ]);
      } finally {
        this._countsPollInFlight = false;
      }
    };

    tick();
    this._countsPollTimer = setInterval(tick, intervalMs);

    document.addEventListener("visibilitychange", () => {
      if (!document.hidden) tick();
    });
  }

  stopCountsAutoRefresh() {
    if (this._countsPollTimer) {
      clearInterval(this._countsPollTimer);
      this._countsPollTimer = null;
    }
  }

  updateForwardingRuleCountsUI() {
    document.querySelectorAll("[data-rule-count-id]").forEach((el) => {
      const id = el.getAttribute("data-rule-count-id");
      const n = this.getRuleForwardCount(id);
      el.textContent = Number(n).toLocaleString();
    });
  }

  init() {
    const root = document.getElementById("fwd-root");
    if (!root) {
      console.warn("[Forwarding] #fwd-root not found, skipping init.");
      return;
    }
    this.root = root;

    this.listEl = document.getElementById("forwarding-list");
    this.emptyEl = document.getElementById("forwarding-empty");
    this.modalEl = document.getElementById("fwdModal");
    this.formEl = document.getElementById("forwarding-form");

    this.initAdvancedFiltersToggle();

    if (!this.listEl || !this.modalEl || !this.formEl) {
      console.warn(
        "[Forwarding] Required DOM elements missing, aborting init."
      );
      return;
    }

    const createBtn = document.getElementById("fwdCreateBtn");
    const closeBtn = document.getElementById("fwdModalCloseBtn");
    const cancelBtn = document.getElementById("fwdCancelBtn");

    if (createBtn) {
      createBtn.disabled = true;
      createBtn.addEventListener("click", () => {
        this.openCreateModal().catch(console.error);
      });
    }
    if (closeBtn) closeBtn.addEventListener("click", () => this.closeModal());
    if (cancelBtn) cancelBtn.addEventListener("click", () => this.closeModal());

    this.formEl.addEventListener("submit", (ev) => {
      ev.preventDefault();
      this.handleSubmit().catch(console.error);
    });

    const providerInput = document.getElementById("fwd_provider");
    if (providerInput) {
      providerInput.addEventListener("change", () =>
        this.updateProviderFields()
      );
    }

    this.initProviderDropdown();
    this.updateProviderFields();
    this.initChipInputs();
    this.initSelectBounce();

    const loader = window.loaderTest;
    if (loader && typeof loader.show === "function") loader.show();

    if (this.root) this.root.hidden = true;

    this.showSkeletons();

    this.loadForwardingCount().catch(console.error);
    this.loadForwardingCountsByRule().catch(console.error);
    this.startCountsAutoRefresh(5000);

    this.guildsPromise = this.loadGuilds();
    const rulesPromise = this.refreshList();

    Promise.allSettled([this.guildsPromise, rulesPromise]).finally(() => {
      if (this.hardFail) {
        console.warn("[Forwarding] hardFail; keeping loading state.");
        return;
      }

      this.tryRenderList();

      if (this.root) this.root.hidden = false;
      if (createBtn) createBtn.disabled = false;

      document.body.classList.remove("page-loading");
      if (loader && typeof loader.hide === "function") loader.hide();
      const footer = document.getElementById("fixed-footer");
      if (footer) footer.hidden = false;
    });
  }

  initProviderDropdown() {
    const form = this.formEl || document;
    const input = form.querySelector("#fwd_provider");
    const wrap = input?.closest(".provider-select-wrap");
    const btn = form.querySelector("#providerSelectBtn");
    const menu = form.querySelector("#providerSelectMenu");
    if (!input || !wrap || !btn || !menu) return;

    const providers = [
      { value: "pushover", label: "Pushover" },
      { value: "telegram", label: "Telegram" },
      { value: "discord", label: "Discord" },
    ];

    const buildMenu = () => {
      menu.innerHTML = "";
      providers.forEach((p) => {
        const item = document.createElement("div");
        item.className = "provider-option provider-dd-option";
        item.setAttribute("role", "option");
        item.dataset.value = p.value;
        item.innerHTML = `
          <span class="provider-opt-icon" aria-hidden="true">
            ${this.getProviderIconHtml(p.value) || ""}
          </span>
          <span class="provider-opt-label">${this.escapeHtml(p.label)}</span>
        `;
        menu.appendChild(item);
      });
    };

    const syncSelected = () => {
      const val = input.value;
      menu.querySelectorAll(".provider-option").forEach((el) => {
        const isSel = el.dataset.value === val;
        el.setAttribute("aria-selected", isSel ? "true" : "false");
        el.classList.toggle("is-active", isSel);
      });
    };

    const updateButton = () => {
      const val = input.value;
      const selected = providers.find((p) => p.value === val);
      const label = selected ? selected.label : "Choose provider…";

      const labelEl = btn.querySelector(".provider-opt-label");
      if (labelEl) labelEl.textContent = label;

      const iconWrap = btn.querySelector(".provider-opt-icon");
      if (iconWrap) {
        if (selected) {
          iconWrap.innerHTML = this.getProviderIconHtml(selected.value) || "";
          iconWrap.style.visibility = "visible";
        } else {
          iconWrap.innerHTML = "";
          iconWrap.style.visibility = "hidden";
        }
      }

      syncSelected();
    };

    const openMenu = () => {
      buildMenu();
      updateButton();
      wrap.classList.add("is-open");
      menu.hidden = false;
      btn.setAttribute("aria-expanded", "true");
    };

    const closeMenu = () => {
      wrap.classList.remove("is-open");
      menu.hidden = true;
      btn.setAttribute("aria-expanded", "false");
    };

    const isOpen = () => !menu.hidden;

    btn.addEventListener("click", (e) => {
      e.preventDefault();
      e.stopPropagation();
      if (isOpen()) closeMenu();
      else openMenu();
    });

    menu.addEventListener("click", (e) => {
      const item = e.target.closest(".provider-option");
      if (!item) return;

      input.value = item.dataset.value || "";
      input.dispatchEvent(new Event("change", { bubbles: true }));
      closeMenu();
      btn.focus();
    });

    document.addEventListener("mousedown", (e) => {
      if (!isOpen()) return;
      if (wrap.contains(e.target)) return;
      closeMenu();
    });

    btn.addEventListener("keydown", (e) => {
      if (e.key === "Escape" && isOpen()) {
        e.preventDefault();
        closeMenu();
      }
      if ((e.key === "Enter" || e.key === " ") && !isOpen()) {
        e.preventDefault();
        openMenu();
      }
    });

    input.addEventListener("change", updateButton);
    updateButton();
  }

  async loadGuilds() {
    try {
      const res = await fetch("/api/client-guilds", {
        credentials: "same-origin",
        cache: "no-store",
      });

      if (!res.ok) {
        this.hardFail = true;

        if (res.status === 404) {
          this.showToast(
            "We couldn’t load any guilds yet. It looks like your Discord client token isn’t configured—open the Configuration page, add your tokens, then come back here.",
            { type: "warning" }
          );
        } else {
          this.showToast(
            `Failed to load guilds (${res.status}). Please check your configuration and try again.`,
            { type: "error" }
          );
        }
        return;
      }

      const data = await res.json();
      this.guilds = Array.isArray(data.items) ? data.items : [];
      this.populateGuildSelects();

      this.guildsLoaded = true;
      this.tryRenderList();
    } catch (err) {
      console.error("[Forwarding] loadGuilds error:", err);
      this.hardFail = true;
      this.showToast(
        "Failed to load guild list. Check your connection and token configuration, then try reloading.",
        { type: "error" }
      );
    }
  }

  populateGuildSelects() {
    const options = [
      '<option value="">All guilds</option>',
      ...this.guilds.map(
        (g) =>
          `<option value="${this.escapeAttr(g.id)}">${this.escapeHtml(
            g.name || "Unknown guild"
          )} (${this.escapeHtml(g.id)})</option>`
      ),
    ].join("");

    const formSelect = document.getElementById("fwd_guild_id");
    if (formSelect) {
      const current = formSelect.value;
      formSelect.innerHTML = options;
      if (current) formSelect.value = current;
    }
  }

  async refreshList() {
    try {
      const res = await fetch("/api/forwarding", {
        credentials: "same-origin",
        cache: "no-store",
      });

      if (!res.ok) {
        this.showToast(`Failed to load forwarding rules (${res.status})`, {
          type: "error",
        });

        this.currentItems = [];
        this.rulesLoaded = true;
        this.tryRenderList();
        return;
      }

      const data = await res.json();
      const items = Array.isArray(data.items) ? data.items : [];

      this.currentItems = items;

      this.rulesLoaded = true;
      this.tryRenderList();
    } catch (err) {
      console.error("[Forwarding] refreshList error:", err);
      this.showToast("Failed to load forwarding rules (network error)", {
        type: "error",
      });

      this.currentItems = [];
      this.rulesLoaded = true;
      this.tryRenderList();
    }
  }

  showSkeletons(count = 3) {
    if (!this.listEl || !this.emptyEl) return;

    this.emptyEl.hidden = true;

    const skeletons = Array.from(
      { length: count },
      (_, i) => `
      <div class="fwd-card-skeleton" style="animation-delay: ${i * 0.08}s">
        <div class="skeleton-header">
          <div class="skeleton-main">
            <div class="skeleton skeleton-title"></div>
            <div class="skeleton skeleton-meta"></div>
          </div>
          <div class="skeleton-badges">
            <div class="skeleton skeleton-badge"></div>
            <div class="skeleton skeleton-status"></div>
          </div>
        </div>
        <div class="skeleton-chips">
          <div class="skeleton skeleton-chip"></div>
          <div class="skeleton skeleton-chip" style="width: 80px"></div>
          <div class="skeleton skeleton-chip" style="width: 50px"></div>
        </div>
        <div class="skeleton-footer">
          <div class="skeleton skeleton-btn"></div>
          <div class="skeleton skeleton-btn" style="width: 70px"></div>
          <div class="skeleton skeleton-btn" style="width: 55px"></div>
        </div>
      </div>
    `
    ).join("");

    this.listEl.innerHTML = skeletons;
  }

  async loadForwardingCount() {
    try {
      const res = await fetch("/api/forwarding/count", {
        credentials: "same-origin",
        cache: "no-store",
      });
      if (!res.ok) {
        return;
      }
      const data = await res.json();
      const total =
        typeof data.count === "number"
          ? data.count
          : Number(data.count ?? 0) || 0;

      this.forwardedTotalCount = total;
      this.updateForwardingCountUI();
    } catch (err) {
      console.error("[Forwarding] loadForwardingCount error:", err);
    }
  }

  updateForwardingCountUI() {
    const el = document.getElementById("fwdTotalCount");
    if (!el) return;
    const val = this.forwardedTotalCount;
    el.textContent = typeof val === "number" ? val.toLocaleString() : "—";
  }

  async loadForwardingCountsByRule() {
    try {
      const res = await fetch("/api/forwarding/count/by-rule", {
        credentials: "same-origin",
        cache: "no-store",
      });
      if (!res.ok) return;

      const data = await res.json();
      const byRule =
        data && data.by_rule && typeof data.by_rule === "object"
          ? data.by_rule
          : {};

      this.forwardedByRule = byRule;

      this.updateForwardingRuleCountsUI();
    } catch (err) {
      console.error("[Forwarding] loadForwardingCountsByRule error:", err);
    }
  }

  getRuleForwardCount(ruleId) {
    if (!ruleId) return 0;
    const v = this.forwardedByRule?.[String(ruleId)];
    const n = typeof v === "number" ? v : Number(v ?? 0);
    return Number.isFinite(n) ? n : 0;
  }

  renderList(items) {
    if (!this.listEl || !this.emptyEl) return;

    if (!items.length) {
      this.listEl.innerHTML = "";
      this.emptyEl.hidden = false;
      return;
    }

    this.emptyEl.hidden = true;

    const q = (this.searchQuery || "").trim().toLowerCase();

    const filtered = !q
      ? items
      : items.filter((n) => {
          const label = (n.label || "").toLowerCase();
          const provider = (n.provider || "").toLowerCase();

          const f = n.filters || {};
          const any = this.toArray(f.keywords_any).join(" ").toLowerCase();
          const all = this.toArray(f.keywords_all).join(" ").toLowerCase();

          const hay = `${label} ${provider} ${any} ${all}`;
          return hay.includes(q);
        });

    const headerHtml = `
    <div class="fwd-list-header">
      <div class="fwd-list-header-left">
        <div class="fwd-search-wrap">
          <input
            id="fwdSearchInput"
            type="text"
            class="log-search-input"
            placeholder="Search rules…"
            value="${this.escapeAttr(this.searchQuery || "")}"
            autocomplete="off"
            spellcheck="false"
            aria-label="Search forwarding rules"
          />
        </div>

        <div id="forwarding-count" class="fwd-count" role="status" aria-live="polite" style="margin:8px 0 12px;">
          <span class="fwd-count-label">Total Forwarded:</span>
          <span id="fwdTotalCount" class="fwd-count-value">—</span>
        </div>
      </div>

      <div class="fwd-list-header-right">
        <button type="button" class="btn btn-primary fwd-add-btn">New Forward</button>
      </div>
    </div>
  `;

    const listHtml =
      filtered.length > 0
        ? filtered
            .map((n) => {
              const enabled = !!n.enabled;
              const provider = (n.provider || "").toLowerCase();
              const label = this.escapeHtml(n.label || "");
              const providerLabel = this.describeProvider(provider);
              const ruleCount = this.getRuleForwardCount(n.rule_id);
              const countLabel = `${ruleCount.toLocaleString()}`;

              return `
              <article class="fwd-card" data-id="${this.escapeAttr(n.rule_id)}">
                <header class="fwd-card-header">
                  <div class="fwd-card-main">
                    <h3 class="fwd-card-title">${label}</h3>
                  </div>

                  <div class="fwd-card-header-right">
                    <span class="badge-provider badge-provider-${provider}">
                      ${this.getProviderIconHtml(provider)}
                      <span class="badge-provider-label">${this.escapeHtml(
                        providerLabel
                      )}</span>
                    </span>

                    <span class="status-pill ${
                      enabled ? "status-pill-on" : "status-pill-off"
                    }">
                      ${enabled ? "Enabled" : "Disabled"}
                    </span>
                  </div>
                </header>

                <footer class="fwd-card-footer">
                  <button type="button" class="btn btn-ghost fwd-edit-btn">Edit</button>
                  <button type="button" class="btn btn-ghost fwd-toggle-btn">
                    ${enabled ? "Disable" : "Enable"}
                  </button>
                  <button type="button" class="btn btn-ghost-red fwd-delete-btn">Delete</button>

                  <div class="fwd-card-footer-stats" style="margin-left:auto;display:flex;align-items:center;gap:8px">
                    <span class="fwd-card-stat-label" aria-hidden="true">Forwarded:</span>
                    <span
                      class="fwd-card-stat-value"
                      title="Total messages forwarded by this rule"
                      aria-label="Total messages forwarded by this rule"
                      data-rule-count-id="${this.escapeAttr(n.rule_id)}"
                    >${countLabel}</span>
                  </div>
                </footer>
              </article>
            `;
            })
            .join("")
        : `
          <div class="fwd-no-match" role="status" aria-live="polite">
            <div class="fwd-no-match-title">No rules matched</div>
            <div class="fwd-no-match-sub">
              No rules match <strong>${this.escapeHtml(
                this.searchQuery || ""
              )}</strong>
            </div>
          </div>
        `;

    this.listEl.innerHTML = headerHtml + listHtml;

    this.updateForwardingCountUI();

    const searchInput = this.listEl.querySelector("#fwdSearchInput");
    if (searchInput) {
      searchInput.addEventListener("input", (ev) => {
        const el = ev.currentTarget;
        const s = el.selectionStart;
        const e = el.selectionEnd;

        this.searchQuery = el.value || "";
        this.renderList(this.currentItems || []);

        requestAnimationFrame(() => {
          const again = this.listEl.querySelector("#fwdSearchInput");
          if (again) {
            again.focus({ preventScroll: true });
            try {
              again.setSelectionRange(s, e);
            } catch {}
          }
        });
      });

      searchInput.addEventListener("keydown", (e) => {
        if (e.key === "Escape") {
          e.preventDefault();
          this.searchQuery = "";
          this.renderList(this.currentItems || []);
          requestAnimationFrame(() => {
            const again = this.listEl.querySelector("#fwdSearchInput");
            if (again) again.focus({ preventScroll: true });
          });
        }
      });
    }

    const addBtn = this.listEl.querySelector(".fwd-add-btn");
    if (addBtn)
      addBtn.addEventListener("click", () =>
        this.openCreateModal().catch(console.error)
      );

    this.listEl.querySelectorAll(".fwd-edit-btn").forEach((btn) => {
      btn.addEventListener("click", (ev) => {
        const card = ev.currentTarget.closest(".fwd-card");
        const id = card?.getAttribute("data-id");
        const item = this.currentItems.find((x) => x.rule_id === id);
        if (item) this.openEditModal(item);
      });
    });

    this.listEl.querySelectorAll(".fwd-toggle-btn").forEach((btn) => {
      btn.addEventListener("click", (ev) => {
        const card = ev.currentTarget.closest(".fwd-card");
        const id = card?.getAttribute("data-id");
        const item = this.currentItems.find((x) => x.rule_id === id);
        if (!item) return;
        const updated = { ...item, enabled: !item.enabled };
        this.saveForwardingRule(updated, { silent: false }).catch(
          console.error
        );
      });
    });

    this.listEl.querySelectorAll(".fwd-delete-btn").forEach((btn) => {
      btn.addEventListener("click", (ev) => {
        const card = ev.currentTarget.closest(".fwd-card");
        const id = card?.getAttribute("data-id");
        if (!id) return;

        const doDelete = () => {
          this.deleteForwardingRule(id).catch((err) => {
            console.error("[Forwarding] deleteForwardingRule failed:", err);
          });
        };

        try {
          openConfirm({
            title: "Delete forwarding rule",
            body: "Are you sure you want to delete this forwarding rule?",
            confirmText: "Delete",
            confirmClass: "btn-ghost-red",
            onConfirm: () => doDelete(),
            showCancel: true,
          });
        } catch (err) {
          console.error(
            "[Forwarding] openConfirm failed, falling back to confirm():",
            err
          );
          if (window.confirm("Delete this forwarding rule?")) doDelete();
        }
      });
    });
  }

  tryRenderList() {
    if (this.hardFail) return;
    if (!this.guildsLoaded || !this.rulesLoaded) return;

    this.renderList(this.currentItems || []);
  }

  describeProvider(provider) {
    switch (provider) {
      case "pushover":
        return "Pushover";
      case "discord":
        return "Discord";
      case "telegram":
        return "Telegram";
      default:
        return provider || "Custom";
    }
  }

  getProviderIconHtml(provider) {
    const icons = {
      telegram: `
      <svg
        class="badge-provider-icon badge-provider-icon--telegram"
        viewBox="0 0 48 48"
        xmlns="http://www.w3.org/2000/svg"
        aria-hidden="true"
        preserveAspectRatio="xMidYMid meet"
      >
        <path
          d="M0 24c0 13.255 10.745 24 24 24s24-10.745 24-24S37.255 0 24 0 0 10.745 0 24zm19.6 11 .408-6.118 11.129-10.043c.488-.433-.107-.645-.755-.252l-13.735 8.665-5.933-1.851c-1.28-.393-1.29-1.273.288-1.906l23.118-8.914c1.056-.48 2.075.254 1.672 1.87l-3.937 18.553c-.275 1.318-1.072 1.633-2.175 1.024l-5.998-4.43L20.8 34.4l-.027.027c-.323.314-.59.573-1.173.573z"
          fill="currentColor"
          fill-rule="evenodd"
          clip-rule="evenodd"
        />
      </svg>
    `,

      pushover: `
      <svg
        class="badge-provider-icon badge-provider-icon--pushover"
        viewBox="0 0 512 512"
        xmlns="http://www.w3.org/2000/svg"
        aria-hidden="true"
        preserveAspectRatio="xMidYMid meet"
      >
        <path
          d="m280.95 172.51 74.48-9.8-72.52 163.66c12.74-.98 25.233-5.307 37.48-12.98 12.253-7.68 23.527-17.317 33.82-28.91 10.287-11.6 19.187-24.503 26.7-38.71 7.513-14.213 12.903-28.18 16.17-41.9 1.96-8.493 2.86-16.66 2.7-24.5-.167-7.84-2.21-14.7-6.13-20.58s-9.883-10.617-17.89-14.21c-8-3.593-18.86-5.39-32.58-5.39-16.007 0-31.77 2.613-47.29 7.84-15.513 5.227-29.887 12.823-43.12 22.79-13.227 9.96-24.74 22.373-34.54 37.24-9.8 14.86-16.823 31.763-21.07 50.71-1.633 6.207-2.613 11.187-2.94 14.94-.327 3.76-.407 6.863-.24 9.31.16 2.453.483 4.333.97 5.64a35.55 35.55 0 0 1 1.23 3.92c-16.66 0-28.83-3.35-36.51-10.05-7.673-6.693-9.55-18.37-5.63-35.03 3.92-17.313 12.823-33.81 26.71-49.49 13.88-15.68 30.373-29.483 49.48-41.41 19.113-11.92 40.02-21.39 62.72-28.41 22.707-7.027 44.84-10.54 66.4-10.54 18.947 0 34.87 2.693 47.77 8.08 12.907 5.393 22.953 12.5 30.14 21.32s11.677 19.11 13.47 30.87c1.8 11.76 1.23 24.01-1.71 36.75-3.593 15.353-10.373 30.79-20.34 46.31-9.96 15.513-22.453 29.56-37.48 42.14-15.027 12.573-32.26 22.78-51.7 30.62-19.433 7.84-40.093 11.76-61.98 11.76h-2.45l-62.23 139.65h-70.56z"
          fill="currentColor"
        />
      </svg>
    `,

      discord: `
        <svg
          class="badge-provider-icon badge-provider-icon--discord"
          width="14"
          height="14"
          viewBox="-6 -4 76 76"
          xmlns="http://www.w3.org/2000/svg"
          aria-hidden="true"
          preserveAspectRatio="xMidYMid meet"
        >
        <path
          d="M60.104 12.927a58.55 58.55 0 0 0-14.452-4.482.22.22 0 0 0-.232.11 40.783 40.783 0 0 0-1.8 3.696c-5.457-.817-10.886-.817-16.232 0-.484-1.164-1.2-2.586-1.827-3.696a.228.228 0 0 0-.233-.11 58.39 58.39 0 0 0-14.452 4.482.207.207 0 0 0-.095.082C1.577 26.759-.945 40.174.292 53.42a.244.244 0 0 0 .093.166c6.073 4.46 11.956 7.167 17.729 8.962a.23.23 0 0 0 .249-.082 42.08 42.08 0 0 0 3.627-5.9.225.225 0 0 0-.123-.312 38.772 38.772 0 0 1-5.539-2.64.228.228 0 0 1-.022-.377c.372-.28.744-.57 1.1-.862a.22.22 0 0 1 .23-.031c11.62 5.305 24.198 5.305 35.681 0a.219.219 0 0 1 .232.028c.356 .293.728 .586 1.103 .865a.228 .228 0 0 1-.02 .377 36.384 36.384 0 0 1-5.54 2.637 .227 .227 0 0 0-.12 .316 47.249 47.249 0 0 0 3.623 5.897 .225 .225 0 0 0 .25 .084c5.8-1.795 11.683-4.502 17.756-8.962a.228 .228 0 0 0 .093-.163c1.48-15.315-2.48-28.618-10.498-40.412a.18 .18 0 0 0-.093-.085zM23.725 45.355c-3.498 0-6.38-3.212-6.38-7.156s2.826-7.156 6.38-7.156c3.582 0 6.437 3.24 6.38 7.156 0 3.944-2.826 7.156-6.38 7.156zm23.592 0c-3.498 0-6.38-3.212-6.38-7.156s2.826-7.156 6.38-7.156c3.582 0 6.437 3.24 6.38 7.156 0 3.944-2.798 7.156-6.38 7.156z"
          fill="currentColor"
        />
      </svg>
    `,
    };

    return icons[provider] || "";
  }

  renderKeywordChips(filters, maxVisible = 3) {
    const keywords = this.toArray(filters.keywords_any || []).filter(Boolean);
    if (!keywords.length) return "";

    const visible = keywords.slice(0, maxVisible);
    const remaining = keywords.length - maxVisible;

    let html = '<div class="fwd-keyword-chips">';
    visible.forEach((kw) => {
      html += `<span class="fwd-keyword-chip">${this.escapeHtml(kw)}</span>`;
    });
    if (remaining > 0) {
      html += `<span class="fwd-keyword-chip fwd-keyword-chip--more">+${remaining} more</span>`;
    }
    html += "</div>";
    return html;
  }

  describeScope(guildId) {
    if (!guildId) return "Scope: all guilds";
    const gid = String(guildId);
    const g = this.guilds.find((x) => String(x.id) === gid);
    if (!g) return `Scope: guild ${gid}`;
    return `Scope: ${g.name || "Unknown guild"} (${gid})`;
  }

  describeFilters(filters) {
    const any = this.toArray(filters.keywords_any).filter(Boolean);
    const all = this.toArray(filters.keywords_all).filter(Boolean);
    const channels = this.toArray(filters.channel_ids).filter(Boolean);
    const users = this.toArray(filters.user_ids).filter(Boolean);
    const parts = [];

    if (any.length) parts.push(`any of [${any.join(", ")}]`);
    if (all.length) parts.push(`all of [${all.join(", ")}]`);
    if (channels.length)
      parts.push(`channels: ${channels.map((c) => `#${c}`).join(", ")}`);
    if (users.length)
      parts.push(`users: ${users.map((u) => `@${u}`).join(", ")}`);

    if (filters.has_attachments) parts.push("has attachments");

    if (!parts.length) return "No extra filters (all messages)";
    return parts.join(" · ");
  }

  initAdvancedFiltersToggle() {
    this.advDetails = document.getElementById("fwdFiltersMore");
    if (!this.advDetails) return;
    this.advDetails.open = false;
  }

  setAdvancedFiltersOpen(open) {
    if (!this.advDetails) return;
    this.advDetails.open = !!open;
  }

  async openCreateModal() {
    if (this.guildsPromise) {
      try {
        await this.guildsPromise;
      } catch (err) {
        console.error("[Forwarding] guildsPromise failed:", err);
      }
    }

    this.populateGuildSelects();
    this.resetForm();

    const guildSelect = document.getElementById("fwd_guild_id");
    if (guildSelect) guildSelect.value = "";

    this.setModalTitle("New Forwarding Rule");
    this.showModal();
  }

  openEditModal(item) {
    this.resetForm();
    this.setModalTitle("Edit Forwarding Rule");

    const idInput = document.getElementById("fwd_id");
    const labelInput = document.getElementById("fwd_label");
    const guildSelect = document.getElementById("fwd_guild_id");
    const providerInput = document.getElementById("fwd_provider");
    const enabledInput = document.getElementById("fwd_enabled");

    if (idInput) idInput.value = item.rule_id || "";
    if (labelInput) labelInput.value = item.label || "";
    if (guildSelect) guildSelect.value = item.guild_id || "";
    if (providerInput) {
      providerInput.value = item.provider || "";
      providerInput.dispatchEvent(new Event("change", { bubbles: true }));
    }
    if (enabledInput) enabledInput.checked = !!item.enabled;

    const cfg = item.config || {};
    const filters = item.filters || {};

    const pushoverApp = document.getElementById("pushover_app_token");
    const pushoverUser = document.getElementById("pushover_user_key");
    if (pushoverApp) pushoverApp.value = cfg.app_token || "";
    if (pushoverUser) pushoverUser.value = cfg.user_key || "";

    const discordWebhookUrl = document.getElementById("discord_webhook_url");
    if (discordWebhookUrl) discordWebhookUrl.value = cfg.url || "";

    this.ensureDiscordOptionalFields();
    const discordWebhookUsername = document.getElementById(
      "discord_webhook_username"
    );
    const discordWebhookAvatar = document.getElementById(
      "discord_webhook_avatar_url"
    );
    if (discordWebhookUsername)
      discordWebhookUsername.value =
        cfg.username || cfg.bot_username || cfg.webhook_username || "";
    if (discordWebhookAvatar)
      discordWebhookAvatar.value =
        cfg.avatar_url || cfg.bot_avatar_url || cfg.bot_avatar || "";

    const tgToken = document.getElementById("telegram_bot_token");
    const tgChat = document.getElementById("telegram_chat_id");
    if (tgToken) tgToken.value = cfg.bot_token || "";
    if (tgChat) tgChat.value = cfg.chat_id || "";

    const anyInput = document.getElementById("fwd_keywords_any");
    const allInput = document.getElementById("fwd_keywords_all");
    const chInput = document.getElementById("fwd_channels");
    const userInput = document.getElementById("fwd_users");
    const caseCb = document.getElementById("fwd_case_sensitive");
    const embedsCb = document.getElementById("fwd_include_embeds");
    const botsCb = document.getElementById("fwd_bot_messages");
    const mediaCb = document.getElementById("fwd_has_attachments");

    const anyValue = this.toArray(filters.keywords_any).join(", ");
    const allValue = this.toArray(filters.keywords_all).join(", ");
    const chValue = this.toArray(filters.channel_ids).join(", ");
    const userValue = this.toArray(filters.user_ids).join(", ");

    if (anyInput) anyInput.value = anyValue;
    if (allInput) allInput.value = allValue;
    if (chInput) chInput.value = chValue;
    if (userInput) userInput.value = userValue;
    if (caseCb) caseCb.checked = !!filters.case_sensitive;
    if (embedsCb) embedsCb.checked = !!filters.include_embeds;
    if (botsCb) botsCb.checked = !!filters.include_bots;
    if (mediaCb) mediaCb.checked = !!filters.has_attachments;

    const anyWrap = document.querySelector(
      '[data-chip-input="fwd_keywords_any"]'
    );
    const allWrap = document.querySelector(
      '[data-chip-input="fwd_keywords_all"]'
    );
    const chWrap = document.querySelector('[data-chip-input="fwd_channels"]');
    const userWrap = document.querySelector('[data-chip-input="fwd_users"]');
    if (anyWrap) this.setChipsFromValue(anyWrap, anyValue);
    if (allWrap) this.setChipsFromValue(allWrap, allValue);
    if (chWrap) this.setChipsFromValue(chWrap, chValue);
    if (userWrap) this.setChipsFromValue(userWrap, userValue);

    this.updateProviderFields();
    this.showModal();

    const anyAdvancedOn =
      !!filters.case_sensitive ||
      !!filters.include_embeds ||
      !!filters.include_bots ||
      !!filters.has_attachments;

    this.setAdvancedFiltersOpen(anyAdvancedOn);
  }

  setModalTitle(text) {
    const titleEl = document.getElementById("fwdModalTitle");
    if (titleEl) titleEl.textContent = text;
  }

  showModal() {
    this.modalEl.classList.add("show");
    this.modalEl.setAttribute("aria-hidden", "false");
    document.body.classList.add("body-lock-scroll");
  }

  closeModal() {
    this.modalEl.classList.remove("show");
    this.modalEl.setAttribute("aria-hidden", "true");
    document.body.classList.remove("body-lock-scroll");
  }

  resetForm() {
    if (!this.formEl) return;
    this.formEl.reset();

    const idInput = document.getElementById("fwd_id");
    if (idInput) idInput.value = "";

    [
      "pushover_app_token",
      "pushover_user_key",
      "discord_webhook_url",
      "discord_webhook_username",
      "discord_webhook_avatar_url",
      "telegram_bot_token",
      "telegram_chat_id",
    ].forEach((id) => {
      const el = document.getElementById(id);
      if (el) el.value = "";
    });

    document
      .querySelectorAll(".chip-input-wrap .chip")
      .forEach((c) => c.remove());

    const providerInput = document.getElementById("fwd_provider");
    if (providerInput) {
      providerInput.value = "";
      providerInput.dispatchEvent(new Event("change", { bubbles: true }));
    }

    this.updateProviderFields();
    this.setAdvancedFiltersOpen(false);
  }

  updateProviderFields() {
    const providerInput = document.getElementById("fwd_provider");
    const provider = providerInput ? providerInput.value : "";

    const sections = ["pushover", "discord", "telegram"];
    sections.forEach((name) => {
      const el = document.getElementById(`provider_${name}`);
      if (!el) return;
      el.hidden = provider !== name;
    });

    if (provider === "discord") {
      this.ensureDiscordOptionalFields();
    }
  }

  ensureDiscordOptionalFields() {
    const section = document.getElementById("provider_discord");
    if (!section) return;

    const hasUsername = !!document.getElementById("discord_webhook_username");
    const hasAvatar = !!document.getElementById("discord_webhook_avatar_url");
    if (hasUsername && hasAvatar) return;

    const mkField = (id, labelText, placeholder) => {
      const wrap = document.createElement("div");
      wrap.className = "field";

      const label = document.createElement("label");
      label.className = "label";
      label.setAttribute("for", id);
      label.textContent = labelText;

      const input = document.createElement("input");
      input.type = "text";
      input.id = id;
      input.className = "input";
      input.placeholder = placeholder || "";
      input.autocomplete = "off";

      wrap.appendChild(label);
      wrap.appendChild(input);
      return wrap;
    };

    const usernameField = hasUsername
      ? null
      : mkField(
          "discord_webhook_username",
          "Bot Username (optional)",
          "Override webhook username (e.g., Copycord)"
        );

    const avatarField = hasAvatar
      ? null
      : mkField(
          "discord_webhook_avatar_url",
          "Bot Avatar URL (optional)",
          "https://… (image URL)"
        );

    const urlInput = document.getElementById("discord_webhook_url");
    const anchor =
      (urlInput &&
        (urlInput.closest(".field") || urlInput.closest(".form-row"))) ||
      (urlInput ? urlInput.parentElement : null);

    const insertAfter = (node, after) => {
      if (!node) return;
      if (!after || !after.parentNode) {
        section.appendChild(node);
        return;
      }
      after.parentNode.insertBefore(node, after.nextSibling);
    };

    if (usernameField) insertAfter(usernameField, anchor);
    if (avatarField) {
      const uEl = document.getElementById("discord_webhook_username");
      const after =
        (uEl && (uEl.closest(".field") || uEl.closest(".form-row"))) ||
        (uEl ? uEl.parentElement : anchor);
      insertAfter(avatarField, after);
    }
  }

  initChipInputs() {
    const wraps = document.querySelectorAll(".chip-input-wrap");
    wraps.forEach((wrap) => {
      const textInput = wrap.querySelector(".chip-text-input");
      if (!textInput) return;

      textInput.addEventListener("keydown", (e) => {
        if (e.key === "Enter" || e.key === ",") {
          e.preventDefault();
          const val = textInput.value.trim();
          if (val) {
            this.addChip(wrap, val);
            textInput.value = "";
          }
        }
        if (e.key === "Backspace" && textInput.value === "") {
          const chips = wrap.querySelectorAll(".chip");
          if (chips.length) this.removeChip(chips[chips.length - 1]);
        }
      });

      textInput.addEventListener("blur", () => {
        const val = textInput.value.trim();
        if (val) {
          this.addChip(wrap, val);
          textInput.value = "";
        }
      });

      wrap.addEventListener("click", (e) => {
        if (e.target === wrap) textInput.focus();
      });
    });
  }

  initSelectBounce() {
    document.querySelectorAll("select.input").forEach((select) => {
      select.addEventListener("mousedown", () => {
        select.classList.remove("bounce");
        void select.offsetWidth;
        select.classList.add("bounce");
      });
      select.addEventListener("animationend", () =>
        select.classList.remove("bounce")
      );
    });
  }

  addChip(wrap, value) {
    const existing = Array.from(wrap.querySelectorAll(".chip")).map(
      (c) => c.dataset.value
    );
    if (existing.includes(value)) return;

    const chip = document.createElement("span");
    chip.className = "chip";
    chip.dataset.value = value;
    chip.textContent = value;

    chip.tabIndex = 0;
    chip.setAttribute("role", "button");
    chip.setAttribute("aria-label", `Remove ${value}`);

    chip.addEventListener("click", (e) => {
      e.preventDefault();
      e.stopPropagation();
      this.removeChip(chip);
    });

    chip.addEventListener("keydown", (e) => {
      if (
        e.key === "Enter" ||
        e.key === " " ||
        e.key === "Backspace" ||
        e.key === "Delete"
      ) {
        e.preventDefault();
        this.removeChip(chip);
      }
    });

    const textInput = wrap.querySelector(".chip-text-input");
    wrap.insertBefore(chip, textInput);
    this.syncChipsToInput(wrap);
  }

  removeChip(chip) {
    const wrap = chip.closest(".chip-input-wrap");
    chip.remove();
    if (wrap) this.syncChipsToInput(wrap);
  }

  syncChipsToInput(wrap) {
    const inputId = wrap.dataset.chipInput;
    const hiddenInput = document.getElementById(inputId);
    if (!hiddenInput) return;

    const values = Array.from(wrap.querySelectorAll(".chip")).map(
      (c) => c.dataset.value
    );
    hiddenInput.value = values.join(", ");
  }

  setChipsFromValue(wrap, value) {
    wrap.querySelectorAll(".chip").forEach((c) => c.remove());
    this.splitCsv(value).forEach((v) => this.addChip(wrap, v));
  }

  async validateProviderConfig(payload) {
    const provider = (payload?.provider || "").toLowerCase().trim();
    const cfg = payload?.config || {};

    if (provider === "telegram") {
      if (
        !String(cfg.bot_token || "").trim() ||
        !String(cfg.chat_id || "").trim()
      ) {
        this.showToast(
          "Telegram requires both a Bot Token and a Chat/Channel ID.",
          {
            type: "warning",
          }
        );
        return false;
      }
    }
    if (provider === "pushover") {
      if (
        !String(cfg.app_token || "").trim() ||
        !String(cfg.user_key || "").trim()
      ) {
        this.showToast(
          "Pushover requires both an Application Token and a User/Group Key.",
          { type: "warning" }
        );
        return false;
      }
    }

    if (provider !== "telegram" && provider !== "pushover") return true;

    try {
      const res = await fetch("/api/forwarding/validate", {
        method: "POST",
        credentials: "same-origin",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ provider, config: cfg }),
      });

      if (res.ok) return true;

      let txt = "";
      try {
        txt = await res.text();
      } catch {}

      let msg = txt;
      try {
        const j = JSON.parse(txt);
        if (j && Array.isArray(j.errors) && j.errors.length) {
          msg = j.errors.join("\n");
        } else if (j && typeof j.detail === "string") {
          msg = j.detail;
        }
      } catch {}

      this.showToast(msg || "Provider details are invalid.", { type: "error" });
      return false;
    } catch (e) {
      this.showToast(`Validation failed: ${e?.message || "network error"}`, {
        type: "error",
      });
      return false;
    }
  }

  async handleSubmit() {
    if (this.isSaving) return;
    this.isSaving = true;

    try {
      const provider = (
        document.getElementById("fwd_provider")?.value || ""
      ).trim();
      if (!provider) {
        this.showToast("Please choose a provider.", { type: "warning" });
        return;
      }

      const payload = this.buildPayloadFromForm();

      const valid = await this.validateProviderConfig(payload);
      if (!valid) return;

      const saved = await this.saveForwardingRule(payload, { silent: false });
      if (saved) this.closeModal();
    } finally {
      this.isSaving = false;
    }
  }

  buildPayloadFromForm() {
    const idInput = document.getElementById("fwd_id");
    const labelInput = document.getElementById("fwd_label");
    const guildSelect = document.getElementById("fwd_guild_id");
    const providerInput = document.getElementById("fwd_provider");
    const enabledInput = document.getElementById("fwd_enabled");

    const anyInput = document.getElementById("fwd_keywords_any");
    const allInput = document.getElementById("fwd_keywords_all");
    const chInput = document.getElementById("fwd_channels");
    const userInput = document.getElementById("fwd_users");
    const caseCb = document.getElementById("fwd_case_sensitive");
    const embedsCb = document.getElementById("fwd_include_embeds");
    const botsCb = document.getElementById("fwd_bot_messages");
    const mediaCb = document.getElementById("fwd_has_attachments");

    const provider = (providerInput?.value || "").toLowerCase().trim();

    const cfg = {};
    if (provider === "pushover") {
      cfg.app_token =
        document.getElementById("pushover_app_token")?.value.trim() || "";
      cfg.user_key =
        document.getElementById("pushover_user_key")?.value.trim() || "";
    } else if (provider === "discord") {
      cfg.url =
        document.getElementById("discord_webhook_url")?.value.trim() || "";

      const uname =
        document.getElementById("discord_webhook_username")?.value.trim() || "";
      const avatar =
        document.getElementById("discord_webhook_avatar_url")?.value.trim() ||
        "";

      if (uname) cfg.username = uname;
      if (avatar) cfg.avatar_url = avatar;
    } else if (provider === "telegram") {
      cfg.bot_token =
        document.getElementById("telegram_bot_token")?.value.trim() || "";
      cfg.chat_id =
        document.getElementById("telegram_chat_id")?.value.trim() || "";
    }

    const filters = {
      keywords_any: this.splitCsv(anyInput?.value),
      keywords_all: this.splitCsv(allInput?.value),
      channel_ids: this.splitCsv(chInput?.value),
      user_ids: this.splitCsv(userInput?.value),
      case_sensitive: !!(caseCb && caseCb.checked),
      include_embeds: !!(embedsCb && embedsCb.checked),
      include_bots: !!(botsCb && botsCb.checked),
      has_attachments: !!(mediaCb && mediaCb.checked),
    };

    return {
      rule_id: idInput?.value.trim() || null,
      guild_id: guildSelect?.value.trim() || null,
      label: (labelInput?.value || "").trim(),
      provider,
      enabled: !!(enabledInput && enabledInput.checked),
      config: cfg,
      filters,
    };
  }

  splitCsv(value) {
    if (!value) return [];
    return value
      .split(",")
      .map((s) => s.trim())
      .filter(Boolean);
  }

  toArray(v) {
    if (Array.isArray(v)) return v;
    if (typeof v === "string" && v.trim() !== "")
      return v.split(",").map((s) => s.trim());
    return [];
  }

  async saveForwardingRule(payload, { silent = false } = {}) {
    const res = await fetch("/api/forwarding", {
      method: "POST",
      credentials: "same-origin",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });

    if (!res.ok) {
      let txt = "";
      try {
        txt = await res.text();
      } catch {}
      if (!silent) {
        this.showToast(
          txt || `Failed to save forwarding rule (${res.status})`,
          { type: "error" }
        );
      }
      return false;
    }

    const data = await res.json();
    const saved = data.item;
    if (saved) {
      const idx = this.currentItems.findIndex(
        (x) => x.rule_id === saved.rule_id
      );
      if (idx >= 0) this.currentItems[idx] = saved;
      else this.currentItems.unshift(saved);
      this.renderList(this.currentItems);
    } else {
      await this.refreshList();
    }

    if (!silent) this.showToast("Forwarding rule saved.", { type: "success" });
    return true;
  }

  async deleteForwardingRule(id) {
    const res = await fetch(`/api/forwarding/${encodeURIComponent(id)}`, {
      method: "DELETE",
      credentials: "same-origin",
    });

    if (!res.ok) {
      let txt = "";
      try {
        txt = await res.text();
      } catch {}
      this.showToast(
        txt || `Failed to delete forwarding rule (${res.status})`,
        { type: "error" }
      );
      return;
    }

    this.currentItems = this.currentItems.filter((x) => x.rule_id !== id);
    this.renderList(this.currentItems);
    this.showToast("Forwarding rule deleted.", { type: "success" });
  }

  escapeHtml(str) {
    return String(str)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  escapeAttr(str) {
    return this.escapeHtml(str).replace(/"/g, "&quot;");
  }
}

document.addEventListener("DOMContentLoaded", () => {
  const root = document.getElementById("fwd-root");
  if (!root) return;

  const system = new ForwardingSystem({
    showToast: window.showToast,
  });
  system.init();
});
