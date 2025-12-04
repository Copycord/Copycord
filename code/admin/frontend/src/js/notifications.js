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
      #confirm-modal.is-open { display: block; }
      #confirm-modal .modal-content:focus { outline: none; box-shadow: none; }
      #confirm-modal .btn:focus, #confirm-modal .btn:focus-visible {
        outline: none; box-shadow: none;
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
      if (!k) return "btn btn-ghost";

      const parts = k.split(/\s+/).filter(Boolean);
      if (!parts.includes("btn")) {
        parts.unshift("btn");
      }
      return parts.join(" ");
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
      cModal.classList.remove("is-open");
      cModal.setAttribute("aria-hidden", "true");
      cModal.style.display = "";
      cOk?.removeEventListener("click", onOk);
      cCa?.removeEventListener("click", onNo);
      cX?.removeEventListener("click", onNo);
      cBack?.removeEventListener("click", onNo);
      document.removeEventListener("keydown", onKey, { capture: true });
      setTimeout(blurActive, 0);
      resolve(result);
    };

    const onOk = () => close(true);
    const onNo = () => close(false);
    const onKey = (e) => {
      if (e.key === "Escape") close(false);
      if (e.key === "Enter") close(true);
    };

    blurActive();
    cModal.classList.add("is-open");
    cModal.setAttribute("aria-hidden", "false");
    cModal.style.display = "block";
    setTimeout(() => dialog?.focus({ preventScroll: true }), 0);

    cOk?.addEventListener("click", onOk);
    cCa?.addEventListener("click", onNo);
    cX?.addEventListener("click", onNo);
    cBack?.addEventListener("click", onNo);
    document.addEventListener("keydown", onKey, { capture: true });
  });
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

    const loader = window.loaderTest;
    if (loader && typeof loader.show === "function") {
      loader.show();
    }

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
        if (res.status >= 500) {
          this.hardFail = true;
        }

        this.showToast(`Failed to load guilds (${res.status})`, {
          type: "error",
        });
        return;
      }

      const data = await res.json();
      this.guilds = Array.isArray(data.items) ? data.items : [];
      this.populateGuildSelects();
    } catch (err) {
      console.error("[Notifications] loadGuilds error:", err);

      this.hardFail = true;
      this.showToast(
        "Failed to load guild list. Some features may be limited.",
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

  renderList(items) {
    if (!this.listEl || !this.emptyEl) return;

    if (!items.length) {
      this.listEl.innerHTML = "";
      this.emptyEl.hidden = false;
      return;
    }

    this.emptyEl.hidden = true;

    const html = items
      .map((n) => {
        const enabled = !!n.enabled;
        const provider = (n.provider || "").toLowerCase();
        const label = this.escapeHtml(n.label || "");
        const scopeLabel = this.describeScope(n.guild_id);
        const filtersSummary = this.describeFilters(n.filters || {});
        const providerLabel = this.describeProvider(provider);

        return `
          <article class="notif-card" data-id="${this.escapeAttr(n.notif_id)}">
            <header class="notif-card-header">
              <div>
                <h3 class="notif-card-title">${label}</h3>
                <div class="notif-card-meta small text-muted">
                  <span>${this.escapeHtml(scopeLabel)}</span>
                  <span class="bullet">•</span>
                  <span>${this.escapeHtml(filtersSummary)}</span>
                </div>
              </div>
              <div class="notif-card-header-right">
                <span class="badge badge-provider badge-provider-${provider}">
                  ${this.escapeHtml(providerLabel)}
                </span>
                <span class="status-pill ${
                  enabled ? "status-pill-on" : "status-pill-off"
                }">
                  ${enabled ? "Enabled" : "Disabled"}
                </span>
              </div>
            </header>

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
      btn.addEventListener("click", async (ev) => {
        const card = ev.currentTarget.closest(".notif-card");
        const id = card?.getAttribute("data-id");
        if (!id) return;

        blurActive();

        const confirmed = await themedConfirm({
          title: "Delete notification rule?",
          body: "This will permanently delete this notification rule. This cannot be undone.",
          confirmText: "Delete",
          cancelText: "Cancel",
          btnClassOk: "btn-ghost-red",
          btnClassCancel: "btn-ghost",
        });

        if (!confirmed) return;

        this.deleteNotification(id).catch(console.error);
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

    if (anyInput)
      anyInput.value = this.toArray(filters.keywords_any).join(", ");
    if (allInput)
      allInput.value = this.toArray(filters.keywords_all).join(", ");
    if (chInput) chInput.value = this.toArray(filters.channel_ids).join(", ");
    if (caseCb) caseCb.checked = !!filters.case_sensitive;
    if (embedsCb) embedsCb.checked = !!filters.include_embeds;

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
