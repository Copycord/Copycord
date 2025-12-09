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
    #confirm-modal {
      display: none;
    }
    #confirm-modal.show {
      display: flex;
      opacity: 1;
      visibility: visible;
      align-items: center;
      justify-content: center;
      z-index: 90;
    }
    #confirm-modal .modal-content:focus {
      outline: none;
      box-shadow: none;
    }
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

  if (cBtnOk) {
    cBtnOk.className = `btn ${confirmClass || "btn-ghost"}`;
  }
  if (cBtnCa) {
    cBtnCa.hidden = !showCancel;
  }

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
  if (cBtnCa) {
    cBtnCa.onclick = () => close();
  }
  if (cBtnX) {
    cBtnX.onclick = () => close();
  }
  if (cBack) {
    cBack.onclick = () => close();
  }

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
    if (dialog && typeof dialog.focus === "function") {
      dialog.focus({ preventScroll: true });
    } else if (cBtnOk && typeof cBtnOk.focus === "function") {
      cBtnOk.focus();
    }
  });
}

function closeConfirm() {
  const cModal = document.getElementById("confirm-modal");
  if (!cModal) return;
  cModal.classList.remove("show");
  cModal.setAttribute("aria-hidden", "true");
  document.body.classList.remove("body-lock-scroll");
}

export class NotificationSystem {
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
  }

  init() {
    const root = document.getElementById("notif-root");
    if (!root) {
      console.warn("[Notifications] #notif-root not found, skipping init.");
      return;
    }
    this.root = root;

    this.listEl = document.getElementById("notification-list");
    this.emptyEl = document.getElementById("notification-empty");
    this.modalEl = document.getElementById("notifModal");
    this.formEl = document.getElementById("notification-form");

    if (!this.listEl || !this.modalEl || !this.formEl) {
      console.warn(
        "[Notifications] Required DOM elements missing, aborting init."
      );
      return;
    }

    const createBtn = document.getElementById("notifCreateBtn");
    const closeBtn = document.getElementById("notifModalCloseBtn");
    const cancelBtn = document.getElementById("notifCancelBtn");

    if (createBtn) {
      // Disable until guilds are loaded so user can't open an empty select
      createBtn.disabled = true;
      createBtn.addEventListener("click", () => {
        this.openCreateModal().catch(console.error);
      });
    }
    if (closeBtn) {
      closeBtn.addEventListener("click", () => this.closeModal());
    }
    if (cancelBtn) {
      cancelBtn.addEventListener("click", () => this.closeModal());
    }

    this.formEl.addEventListener("submit", (ev) => {
      ev.preventDefault();
      this.handleSubmit().catch(console.error);
    });

    const providerSelect = document.getElementById("notif_provider");
    if (providerSelect) {
      providerSelect.addEventListener("change", () =>
        this.updateProviderFields()
      );
    }
    this.updateProviderFields();
    this.initChipInputs();
    this.initSelectBounce();

    const loader = window.loaderTest;
    if (loader && typeof loader.show === "function") {
      loader.show();
    }

    this.showSkeletons();

    this.guildsPromise = this.loadGuilds();
    const rulesPromise = this.refreshList();

    Promise.allSettled([this.guildsPromise, rulesPromise])
      .catch((err) => {
        console.error("[Notifications] init error:", err);
      })
      .finally(() => {
        const hardFail = this.hardFail === true;

        if (!hardFail) {
          if (createBtn) {
            createBtn.disabled = false;
          }

          if (this.root) {
            this.root.hidden = false;
          }

          document.body.classList.remove("page-loading");

          if (loader && typeof loader.hide === "function") {
            loader.hide();
          }
        } else {
          console.warn(
            "[Notifications] Guild load failed with server error; keeping loading state."
          );
        }
      });
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
    } catch (err) {
      console.error("[Notifications] loadGuilds error:", err);

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

    if (this.guildSelect) {
      const current = this.guildSelect.value;
      this.guildSelect.innerHTML = options;
      if (current) this.guildSelect.value = current;
    }

    const formSelect = document.getElementById("notif_guild_id");
    if (formSelect) {
      const current = formSelect.value;
      formSelect.innerHTML = options;
      if (current) formSelect.value = current;
    }
  }

  async refreshList() {
    const res = await fetch("/api/notifications", {
      credentials: "same-origin",
      cache: "no-store",
    });

    if (!res.ok) {
      this.showToast(`Failed to load notifications (${res.status})`, {
        type: "error",
      });
      return;
    }

    const data = await res.json();
    const items = Array.isArray(data.items) ? data.items : [];
    this.currentItems = items;
    this.renderList(items);
  }

  showSkeletons(count = 3) {
    if (!this.listEl || !this.emptyEl) return;

    this.emptyEl.hidden = true;

    const skeletons = Array.from(
      { length: count },
      (_, i) => `
      <div class="notif-card-skeleton" style="animation-delay: ${i * 0.08}s">
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

  renderList(items) {
    if (!this.listEl || !this.emptyEl) return;

    if (!items.length) {
      this.listEl.innerHTML = "";
      this.emptyEl.hidden = false;
      return;
    }

    this.emptyEl.hidden = true;

    const addBtnHtml = `
      <div class="notif-list-header">
        <button type="button" class="btn btn-ghost notif-add-btn">+ New Notification</button>
      </div>
    `;

    const html =
      addBtnHtml +
      items
        .map((n) => {
          const enabled = !!n.enabled;
          const provider = (n.provider || "").toLowerCase();
          const label = this.escapeHtml(n.label || "");
          const scopeLabel = this.describeScope(n.guild_id);
          const filtersSummary = this.describeFilters(n.filters || {});
          const providerLabel = this.describeProvider(provider);
          const keywordChipsHtml = this.renderKeywordChips(n.filters || {});

          return `
          <article class="notif-card" data-id="${this.escapeAttr(n.notif_id)}">
            <header class="notif-card-header">
              <div class="notif-card-main">
                <h3 class="notif-card-title">${label}</h3>
                <div class="notif-card-meta">
                  <span>${this.escapeHtml(scopeLabel)}</span>
                  <span class="bullet">•</span>
                  <span>${this.escapeHtml(filtersSummary)}</span>
                </div>
              </div>
              <div class="notif-card-header-right">
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
            ${keywordChipsHtml}
            <footer class="notif-card-footer">
              <button type="button" class="btn btn-ghost notif-edit-btn">
                Edit
              </button>
              <button type="button" class="btn btn-ghost notif-toggle-btn">
                ${enabled ? "Disable" : "Enable"}
              </button>
              <button type="button" class="btn btn-ghost-red notif-delete-btn">
                Delete
              </button>
            </footer>
          </article>
        `;
        })
        .join("");

    this.listEl.innerHTML = html;

    const addBtn = this.listEl.querySelector(".notif-add-btn");
    if (addBtn) {
      addBtn.addEventListener("click", () => {
        this.openCreateModal().catch(console.error);
      });
    }

    this.listEl.querySelectorAll(".notif-edit-btn").forEach((btn) => {
      btn.addEventListener("click", (ev) => {
        const card = ev.currentTarget.closest(".notif-card");
        const id = card?.getAttribute("data-id");
        const item = this.currentItems.find((x) => x.notif_id === id);
        if (item) this.openEditModal(item);
      });
    });

    this.listEl.querySelectorAll(".notif-toggle-btn").forEach((btn) => {
      btn.addEventListener("click", (ev) => {
        const card = ev.currentTarget.closest(".notif-card");
        const id = card?.getAttribute("data-id");
        const item = this.currentItems.find((x) => x.notif_id === id);
        if (!item) return;
        const updated = { ...item, enabled: !item.enabled };
        this.saveNotification(updated, { silent: false }).catch(console.error);
      });
    });

    this.listEl.querySelectorAll(".notif-delete-btn").forEach((btn) => {
      btn.addEventListener("click", (ev) => {
        const card = ev.currentTarget.closest(".notif-card");
        const id = card?.getAttribute("data-id");
        if (!id) return;

        const doDelete = () => {
          this.deleteNotification(id).catch((err) => {
            console.error("[Notifications] deleteNotification failed:", err);
          });
        };

        try {
          openConfirm({
            title: "Delete notification rule",
            body: "Are you sure you want to delete this notification rule?",
            confirmText: "Delete",
            confirmClass: "btn-ghost-red",
            onConfirm: () => doDelete(),
            showCancel: true,
          });
        } catch (err) {
          console.error(
            "[Notifications] openConfirm failed, falling back to confirm():",
            err
          );
          if (window.confirm("Delete this notification rule?")) {
            doDelete();
          }
        }
      });
    });
  }

  describeProvider(provider) {
    switch (provider) {
      case "pushover":
        return "Pushover";
      case "webhook":
        return "Webhook";
      case "telegram":
        return "Telegram";
      default:
        return provider || "Custom";
    }
  }

  getProviderIconHtml(provider) {
    const icons = {
      pushover: `<img class="badge-provider-icon" src="https://pushover.net/images/pushover-logo.svg" alt="" />`,
      telegram: `<img class="badge-provider-icon" src="https://upload.wikimedia.org/wikipedia/commons/8/82/Telegram_logo.svg" alt="" />`,
    };
    return icons[provider] || "";
  }

  renderKeywordChips(filters, maxVisible = 3) {
    const keywords = this.toArray(filters.keywords_any || []).filter(Boolean);
    if (!keywords.length) return "";

    const visible = keywords.slice(0, maxVisible);
    const remaining = keywords.length - maxVisible;

    let html = '<div class="notif-keyword-chips">';
    visible.forEach((kw) => {
      html += `<span class="notif-keyword-chip">${this.escapeHtml(kw)}</span>`;
    });
    if (remaining > 0) {
      html += `<span class="notif-keyword-chip notif-keyword-chip--more">+${remaining} more</span>`;
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
    const parts = [];

    if (any.length) parts.push(`any of [${any.join(", ")}]`);
    if (all.length) parts.push(`all of [${all.join(", ")}]`);
    if (channels.length)
      parts.push(`channels: ${channels.map((c) => `#${c}`).join(", ")}`);

    if (!parts.length) return "No extra filters (all messages)";
    return parts.join(" · ");
  }

  async openCreateModal() {
    if (this.guildsPromise) {
      try {
        await this.guildsPromise;
      } catch (err) {
        console.error("[Notifications] guildsPromise failed:", err);
        // We still allow opening, but you'll just see "All guilds" in that case
      }
    }

    this.populateGuildSelects();
    this.resetForm();

    const guildSelect = document.getElementById("notif_guild_id");
    if (guildSelect) guildSelect.value = "";

    this.setModalTitle("New Notification");
    this.showModal();
  }

  openEditModal(item) {
    this.resetForm();
    this.setModalTitle("Edit Notification");

    const idInput = document.getElementById("notif_id");
    const labelInput = document.getElementById("notif_label");
    const guildSelect = document.getElementById("notif_guild_id");
    const providerSelect = document.getElementById("notif_provider");
    const enabledInput = document.getElementById("notif_enabled");

    if (idInput) idInput.value = item.notif_id || "";
    if (labelInput) labelInput.value = item.label || "";
    if (guildSelect) guildSelect.value = item.guild_id || "";
    if (providerSelect) providerSelect.value = item.provider || "";
    if (enabledInput) enabledInput.checked = !!item.enabled;

    const cfg = item.config || {};
    const filters = item.filters || {};

    const pushoverApp = document.getElementById("pushover_app_token");
    const pushoverUser = document.getElementById("pushover_user_key");
    if (pushoverApp) pushoverApp.value = cfg.app_token || "";
    if (pushoverUser) pushoverUser.value = cfg.user_key || "";

    const webhookUrl = document.getElementById("webhook_url");
    if (webhookUrl) webhookUrl.value = cfg.url || "";

    const tgToken = document.getElementById("telegram_bot_token");
    const tgChat = document.getElementById("telegram_chat_id");
    if (tgToken) tgToken.value = cfg.bot_token || "";
    if (tgChat) tgChat.value = cfg.chat_id || "";

    const anyInput = document.getElementById("notif_keywords_any");
    const allInput = document.getElementById("notif_keywords_all");
    const chInput = document.getElementById("notif_channels");
    const caseCb = document.getElementById("notif_case_sensitive");
    const embedsCb = document.getElementById("notif_include_embeds");

    const anyValue = this.toArray(filters.keywords_any).join(", ");
    const allValue = this.toArray(filters.keywords_all).join(", ");

    if (anyInput) anyInput.value = anyValue;
    if (allInput) allInput.value = allValue;
    if (chInput) chInput.value = this.toArray(filters.channel_ids).join(", ");
    if (caseCb) caseCb.checked = !!filters.case_sensitive;
    if (embedsCb) embedsCb.checked = !!filters.include_embeds;

    const anyWrap = document.querySelector(
      '[data-chip-input="notif_keywords_any"]'
    );
    const allWrap = document.querySelector(
      '[data-chip-input="notif_keywords_all"]'
    );
    const chWrap = document.querySelector('[data-chip-input="notif_channels"]');
    if (anyWrap) this.setChipsFromValue(anyWrap, anyValue);
    if (allWrap) this.setChipsFromValue(allWrap, allValue);
    if (chWrap)
      this.setChipsFromValue(
        chWrap,
        this.toArray(filters.channel_ids).join(", ")
      );

    this.updateProviderFields();
    this.showModal();
  }

  setModalTitle(text) {
    const titleEl = document.getElementById("notifModalTitle");
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

    const idInput = document.getElementById("notif_id");
    if (idInput) idInput.value = "";

    [
      "pushover_app_token",
      "pushover_user_key",
      "webhook_url",
      "telegram_bot_token",
      "telegram_chat_id",
    ].forEach((id) => {
      const el = document.getElementById(id);
      if (el) el.value = "";
    });

    document
      .querySelectorAll(".chip-input-wrap .chip")
      .forEach((c) => c.remove());

    const providerSelect = document.getElementById("notif_provider");
    if (providerSelect) {
      const placeholder = providerSelect.querySelector('option[value=""]');
      if (placeholder) placeholder.hidden = false;
    }

    this.updateProviderFields();
  }

  updateProviderFields() {
    const providerSelect = document.getElementById("notif_provider");
    const provider = providerSelect ? providerSelect.value : "";

    const sections = ["pushover", "webhook", "telegram"];
    sections.forEach((name) => {
      const el = document.getElementById(`provider_${name}`);
      if (!el) return;
      el.hidden = provider !== name;
    });

    if (providerSelect && provider) {
      const placeholder = providerSelect.querySelector('option[value=""]');
      if (placeholder) placeholder.hidden = true;
    }

    const iconEl = document.getElementById("provider-icon");
    if (iconEl) {
      const icons = {
        pushover: "https://pushover.net/images/pushover-logo.svg",
        telegram:
          "https://upload.wikimedia.org/wikipedia/commons/8/82/Telegram_logo.svg",
        webhook: null,
      };
      const iconUrl = icons[provider];
      if (iconUrl) {
        iconEl.innerHTML = `<img src="${iconUrl}" alt="${provider}" />`;
        iconEl.hidden = false;
      } else {
        iconEl.innerHTML = "";
        iconEl.hidden = true;
      }
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
          if (chips.length) {
            this.removeChip(chips[chips.length - 1]);
          }
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
        if (e.target === wrap) {
          textInput.focus();
        }
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
      select.addEventListener("animationend", () => {
        select.classList.remove("bounce");
      });
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
    chip.innerHTML = `${this.escapeHtml(
      value
    )}<button type="button" class="chip-remove" aria-label="Remove">×</button>`;

    chip.querySelector(".chip-remove").addEventListener("click", () => {
      this.removeChip(chip);
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
    const chips = wrap.querySelectorAll(".chip");
    chips.forEach((c) => c.remove());

    const values = this.splitCsv(value);
    values.forEach((v) => this.addChip(wrap, v));
  }

  async handleSubmit() {
    if (this.isSaving) return;
    this.isSaving = true;

    try {
      const payload = this.buildPayloadFromForm();
      await this.saveNotification(payload, { silent: false });
      this.closeModal();
    } finally {
      this.isSaving = false;
    }
  }

  buildPayloadFromForm() {
    const idInput = document.getElementById("notif_id");
    const labelInput = document.getElementById("notif_label");
    const guildSelect = document.getElementById("notif_guild_id");
    const providerSelect = document.getElementById("notif_provider");
    const enabledInput = document.getElementById("notif_enabled");

    const anyInput = document.getElementById("notif_keywords_any");
    const allInput = document.getElementById("notif_keywords_all");
    const chInput = document.getElementById("notif_channels");
    const caseCb = document.getElementById("notif_case_sensitive");
    const embedsCb = document.getElementById("notif_include_embeds");

    const provider = (providerSelect?.value || "").toLowerCase().trim();

    const cfg = {};
    if (provider === "pushover") {
      cfg.app_token =
        document.getElementById("pushover_app_token")?.value.trim() || "";
      cfg.user_key =
        document.getElementById("pushover_user_key")?.value.trim() || "";
    } else if (provider === "webhook") {
      cfg.url = document.getElementById("webhook_url")?.value.trim() || "";
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
      case_sensitive: !!(caseCb && caseCb.checked),
      include_embeds: !!(embedsCb && embedsCb.checked),
    };

    return {
      notif_id: idInput?.value.trim() || null,
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

  async saveNotification(payload, { silent = false } = {}) {
    const res = await fetch("/api/notifications", {
      method: "POST",
      credentials: "same-origin",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify(payload),
    });

    if (!res.ok) {
      let txt = "";
      try {
        txt = await res.text();
      } catch {}
      if (!silent) {
        this.showToast(txt || `Failed to save notification (${res.status})`, {
          type: "error",
        });
      }
      return;
    }

    const data = await res.json();
    const saved = data.item;
    if (saved) {
      const idx = this.currentItems.findIndex(
        (x) => x.notif_id === saved.notif_id
      );
      if (idx >= 0) {
        this.currentItems[idx] = saved;
      } else {
        this.currentItems.unshift(saved);
      }
      this.renderList(this.currentItems);
    } else {
      await this.refreshList();
    }

    if (!silent) {
      this.showToast("Notification rule saved.", { type: "success" });
    }
  }

  async deleteNotification(id) {
    const res = await fetch(`/api/notifications/${encodeURIComponent(id)}`, {
      method: "DELETE",
      credentials: "same-origin",
    });

    if (!res.ok) {
      let txt = "";
      try {
        txt = await res.text();
      } catch {}
      this.showToast(txt || `Failed to delete notification (${res.status})`, {
        type: "error",
      });
      return;
    }

    this.currentItems = this.currentItems.filter((x) => x.notif_id !== id);
    this.renderList(this.currentItems);
    this.showToast("Notification rule deleted.", { type: "success" });
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
  const root = document.getElementById("notif-root");
  if (!root) return;

  const system = new NotificationSystem({
    showToast: window.showToast,
  });
  system.init();
});
