// The mapping Filters modal (#filters-modal / #form-filters): loads
// GET /filters/{id}, edits nine chip lists, browses categories, channels and
// roles of the host guild, and saves with POST /filters/{id}/save.

import { useCallback, useEffect, useRef, useState } from "react";
import type { ReactNode } from "react";
import { useToast } from "../../../components/Toast";
import { Button, ChipInput, ConfirmDialog, Tooltip } from "../../../components/ui";
import type { DashboardMapping } from "../../../types/dashboard-mappings";
import type { FilterKey, FiltersState, PickerKey } from "../../../types/dashboard-filters";
import { EMPTY_FILTERS, fetchFilters, isDirty, saveFilters } from "./model";
import { ObjectPicker, RolePicker } from "./Pickers";
import { Shell } from "./Shell";

const LEGEND = "px-1 text-sm font-semibold text-text-1";
const COL = "flex flex-col gap-4 rounded-card border border-border p-4";

function SearchIcon() {
  return (
    <svg viewBox="0 0 16 16" aria-hidden="true" focusable="false" className="size-4 fill-current">
      <path d="M7 2a5 5 0 1 1-3.536 8.536l-1.95 1.95a.75.75 0 1 0 1.06 1.06l1.95-1.95A5 5 0 0 1 7 2zm0 2a3 3 0 1 0 0 6 3 3 0 0 0 0-6z" />
    </svg>
  );
}

function FetchButton({ id, label, onClick, ...data }: { id?: string; label: string; onClick: () => void; [data: `data-${string}`]: string | undefined }) {
  return (
    <button
      type="button"
      id={id}
      aria-label={label}
      title={label}
      onClick={onClick}
      className="role-fetch-btn filter-fetch-btn inline-flex size-7 items-center justify-center rounded-full border border-border text-muted hover:text-text"
      {...data}
    >
      <span className="role-fetch-circle">
        <SearchIcon />
      </span>
    </button>
  );
}

interface ListFieldProps {
  name: FilterKey;
  inputId?: string;
  label: string;
  tip: string;
  placeholder: string;
  mode: "ids" | "words";
  value: string[];
  onChange: (key: FilterKey, v: string[]) => void;
  action?: ReactNode;
}

function ListField({ name, inputId, label, tip, placeholder, mode, value, onChange, action }: ListFieldProps) {
  const id = inputId ?? `${name}_input`;
  return (
    <div className="field flex flex-col gap-1.5">
      <div className="filters-roles-label-row flex items-center gap-2">
        <label htmlFor={id} className="has-tip text-sm font-semibold text-text-1">
          <span className="label-text">{label}</span>
        </label>
        <Tooltip content={tip} side="bottom">
          <button type="button" className="info-dot size-4 rounded-full border border-border text-[10px] leading-none text-muted" aria-label={tip}>
            i
          </button>
        </Tooltip>
        {action}
      </div>
      <ChipInput id={id} mode={mode} value={value} onChange={(v) => onChange(name, v)} placeholder={placeholder} />
      <input type="hidden" id={name} name={name} value={value.join(",")} readOnly />
    </div>
  );
}

type Load = "loading" | "ready" | "failed";

export interface FiltersModalProps {
  mapping: DashboardMapping;
  onClosed: () => void;
}

export function FiltersModal({ mapping, onClosed }: FiltersModalProps) {
  const mid = mapping.mapping_id || "";
  const toast = useToast();
  const [form, setForm] = useState<FiltersState>(EMPTY_FILTERS);
  const [base, setBase] = useState<FiltersState>(EMPTY_FILTERS);
  const [load, setLoad] = useState<Load>("loading");
  const [saving, setSaving] = useState(false);
  const [picker, setPicker] = useState<PickerKey | null>(null);
  const [rolesOpen, setRolesOpen] = useState(false);
  const [roleStatus, setRoleStatus] = useState("");
  const [confirmOpen, setConfirmOpen] = useState(false);

  const reload = useCallback(() => {
    setLoad("loading");
    return fetchFilters(mid)
      .then((s) => {
        setForm(s);
        setBase(s);
        setLoad("ready");
      })
      .catch(() => setLoad("failed"));
  }, [mid]);

  useEffect(() => {
    void reload();
  }, [reload]);

  const dirty = isDirty(form, base);
  const set = (key: FilterKey, v: string[]) => setForm((f) => ({ ...f, [key]: v }));

  const requestClose = () => {
    if (dirty) setConfirmOpen(true);
    else onClosed();
  };

  // Escape closes the top dialog only: picker, then the filters modal.
  const live = useRef({ picker, rolesOpen, confirmOpen, requestClose });
  live.current = { picker, rolesOpen, confirmOpen, requestClose };
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key !== "Escape") return;
      const s = live.current;
      if (s.confirmOpen) return;
      e.preventDefault();
      if (s.rolesOpen) setRolesOpen(false);
      else if (s.picker) setPicker(null);
      else s.requestClose();
    };
    document.addEventListener("keydown", onKey);
    return () => document.removeEventListener("keydown", onKey);
  }, []);

  const onSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (saving || load !== "ready") return;
    setSaving(true);
    try {
      await saveFilters(mid, form);
      toast.show("Filters saved.", { kind: "success" });
      onClosed();
    } catch (err) {
      toast.show(err instanceof Error ? err.message : "Network error", { kind: "error", timeout: 7000 });
      setSaving(false);
    }
  };

  const niceName = mapping.mapping_name || mid || "Filters";
  const browse = (key: PickerKey, kind: "category" | "channel", label: string) => (
    <FetchButton label={label} onClick={() => setPicker(key)} data-filter-target={key} data-filter-kind={kind} />
  );

  return (
    <>
      <Shell id="filters-modal" closeId="filters-close" titleId="filters-title" title={`Filters – ${niceName}`} onClose={requestClose} wide layer={30}>
        <form id="form-filters" className="padded flex flex-col gap-4 p-5" autoComplete="off" onSubmit={onSubmit}>
          <input type="hidden" id="filters_mapping_id" name="mapping_id" value={mid} readOnly />
          {load === "failed" ? (
            <p role="alert" className="text-sm text-danger">
              Failed to load filters. Saving is disabled so the current filters are not overwritten.
            </p>
          ) : null}

          <div className="filters-grid grid gap-4 md:grid-cols-2">
            <fieldset className={`filters-col ${COL}`}>
              <legend className={LEGEND}>Whitelist (Allow-list)</legend>
              <ListField
                name="wl_categories"
                label="Allowed Category IDs"
                tip="Only these categories (and their channels) will sync. Leave empty to allow all categories."
                placeholder="Add category ID(s)…"
                mode="ids"
                value={form.wl_categories}
                onChange={set}
                action={browse("wl_categories", "category", "Browse categories for this host")}
              />
              <ListField
                name="wl_channels"
                label="Allowed Channel / Thread IDs"
                tip="Only these channels will be cloned. Thread and forum-post IDs work here too."
                placeholder="Add channel ID(s)…"
                mode="ids"
                value={form.wl_channels}
                onChange={set}
                action={browse("wl_channels", "channel", "Browse channels for this host")}
              />
            </fieldset>

            <fieldset className={`filters-col ${COL}`}>
              <legend className={LEGEND}>Exclude (Deny-list)</legend>
              <ListField
                name="ex_categories"
                label="Blocked Category IDs"
                tip="Channels inside these categories will be blocked from being cloned."
                placeholder="Add category ID(s)…"
                mode="ids"
                value={form.ex_categories}
                onChange={set}
                action={browse("ex_categories", "category", "Browse categories to block for this host")}
              />
              <ListField
                name="ex_channels"
                label="Blocked Channel / Thread IDs"
                tip="These channels will never sync, even if their category is allowed. Thread and forum-post IDs work here too."
                placeholder="Add channel ID(s)…"
                mode="ids"
                value={form.ex_channels}
                onChange={set}
                action={browse("ex_channels", "channel", "Browse channels to block for this host")}
              />
            </fieldset>
          </div>

          <fieldset className={`filters-col filters-channel-names ${COL}`}>
            <legend className={LEGEND}>Channel Name Blacklist</legend>
            <ListField
              name="channel_name_blacklist"
              label="Blocked Channel Name Patterns"
              tip="Channels whose name matches a pattern are skipped when cloning and their messages are not forwarded. * matches any characters, ? matches one. Plain text is a substring match. Examples: ticket-*, *-ticket, *-log-*"
              placeholder="Add pattern… e.g. ticket-*"
              mode="words"
              value={form.channel_name_blacklist}
              onChange={set}
            />
          </fieldset>

          <fieldset className={`filters-col filters-roles ${COL}`}>
            <legend className={LEGEND}>Role Blocks</legend>
            <ListField
              name="blocked_role_ids"
              inputId="blocked_role_ids_input"
              label="Blocked roles"
              tip="List of role IDs to block from being cloned."
              placeholder="Add role ID(s)…"
              mode="ids"
              value={form.blocked_role_ids}
              onChange={set}
              action={
                <>
                  <FetchButton id="filters-role-refresh" label="Fetch Discord roles" onClick={() => setRolesOpen(true)} />
                  <span id="filters-role-status" className="filters-roles-status text-xs text-muted" aria-live="polite">
                    {roleStatus}
                  </span>
                </>
              }
            />
          </fieldset>

          <fieldset className={`filters-col filters-words ${COL}`}>
            <legend className={LEGEND}>Word Filters</legend>
            <ListField
              name="blocked_words"
              label="Blocked Words / Phrases"
              tip="Any message that contains one of these words will NOT be forwarded for this guild mapping."
              placeholder="Add word or phrase…"
              mode="words"
              value={form.blocked_words}
              onChange={set}
            />
          </fieldset>

          <fieldset className={`filters-col filters-users ${COL}`}>
            <legend className={LEGEND}>User Filters</legend>
            <ListField
              name="wl_users"
              label="Whitelisted User IDs"
              tip="When set, ONLY messages from these user IDs will be cloned. Leave empty to clone all users (unless blacklisted)."
              placeholder="Add user ID(s)…"
              mode="ids"
              value={form.wl_users}
              onChange={set}
            />
            <ListField
              name="bl_users"
              label="Blacklisted User IDs"
              tip="Messages from these user IDs will be blocked from cloning."
              placeholder="Add user ID(s)…"
              mode="ids"
              value={form.bl_users}
              onChange={set}
            />
          </fieldset>

          <div className="btns modal-footer-row flex justify-end gap-2">
            <Button id="filters-save-btn" type="submit" variant="ghost" loading={saving} disabled={saving || load !== "ready"}>
              Save Filters
            </Button>
            {dirty ? (
              <Button id="btn-cancel-filters" type="button" variant="danger" onClick={() => void reload()}>
                Cancel
              </Button>
            ) : null}
          </div>
        </form>
      </Shell>

      {picker ? (
        <ObjectPicker
          mappingId={mid}
          listKey={picker}
          initial={form[picker]}
          onClose={() => setPicker(null)}
          onApply={(key, ids) => {
            set(key, ids);
            setPicker(null);
          }}
        />
      ) : null}
      {rolesOpen ? (
        <RolePicker
          mappingId={mid}
          initial={form.blocked_role_ids}
          onStatus={setRoleStatus}
          onClose={() => setRolesOpen(false)}
          onApply={(ids) => {
            set("blocked_role_ids", ids);
            setRolesOpen(false);
          }}
        />
      ) : null}
      <ConfirmDialog
        open={confirmOpen}
        title="Discard unsaved filter changes?"
        body="You have unsaved changes to these filters. If you close now, your changes will be lost."
        confirmLabel="Discard changes"
        danger
        onResult={(ok) => {
          setConfirmOpen(false);
          if (ok) onClosed();
        }}
      />
    </>
  );
}
