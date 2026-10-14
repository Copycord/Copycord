// The mapping Filters modal (#filters-modal / #form-filters): loads
// GET /filters/{id}, edits nine chip lists, browses categories, channels and
// roles of the host guild, and saves with POST /filters/{id}/save. The look is
// the old 960px #filters-modal: bordered fieldsets with a purple legend, 12px
// labels with the round "i" tip and the small purple picker circle.

import { useCallback, useEffect, useRef, useState } from "react";
import type { ReactNode } from "react";
import { TipLabel } from "../../../components/Modal";
import { useToast } from "../../../components/Toast";
import { Button, ChipInput, ConfirmDialog, cx } from "../../../components/ui";
import type { DashboardMapping } from "../../../types/dashboard-mappings";
import type { FilterKey, FiltersState, PickerKey } from "../../../types/dashboard-filters";
import { GhostButton } from "../../system/buttons";
import { EMPTY_FILTERS, fetchFilters, isDirty, saveFilters } from "./model";
import { ObjectPicker, RolePicker } from "./Pickers";
import { Shell } from "./Shell";

// #filters-modal fieldset.filters-col and its legend.
const LEGEND = "mb-2.5 px-1 text-[13px] leading-[1.3] font-semibold text-accent";
const COL = "mx-0.5 min-w-0 rounded-panel border border-border bg-control px-[18px] pt-4 pb-5";
// #filters-modal .chips, .chip and .chip-input over the shared ChipInput.
const CHIPS =
  "[&>div]:max-h-24 [&>div]:min-h-12 [&>div]:items-start [&>div]:gap-1.5 [&>div]:overflow-y-auto [&>div]:bg-[#0f0820] [&>div]:bg-none " +
  "[&>div]:px-2.5 [&>div]:pt-2.5 [&>div]:pb-1.5 [&>div]:[scrollbar-color:var(--color-accent)_transparent] [&>div]:[scrollbar-width:thin] " +
  "[&>div:focus-within]:shadow-none [&>div:focus-within]:ring-0 " +
  "[&_li>button]:rounded-input [&_li>button]:px-2.5 [&_li>button]:py-1.5 [&_li>button]:leading-[1.4] " +
  "[&_li>button:hover]:border-white/15 [&_li>button:hover]:bg-white/8 " +
  "[&_input]:w-[178px] [&_input]:min-w-[140px] [&_input]:flex-none [&_input]:px-0.5 [&_input]:pt-1 [&_input]:pb-2 " +
  "[&_input]:font-sans [&_input]:text-sm [&_input]:leading-[1.4]";

function SearchIcon() {
  return (
    <svg viewBox="0 0 16 16" aria-hidden="true" focusable="false" className="block size-3 translate-x-[0.7px] fill-white">
      <path d="M7 2a5 5 0 1 1-3.536 8.536l-1.95 1.95a.75.75 0 1 0 1.06 1.06l1.95-1.95A5 5 0 0 1 7 2zm0 2a3 3 0 1 0 0 6 3 3 0 0 0 0-6z" />
    </svg>
  );
}

/** .role-fetch-btn: the 16px purple circle with a white magnifier. */
function FetchButton({
  id,
  label,
  title,
  onClick,
  className,
  ...data
}: {
  id?: string;
  label: string;
  title?: string;
  onClick: () => void;
  className?: string;
  [data: `data-${string}`]: string | undefined;
}) {
  return (
    <button
      type="button"
      id={id}
      aria-label={label}
      title={title ?? label}
      onClick={onClick}
      className={cx(
        "role-fetch-btn group/rf m-0 inline-flex cursor-pointer items-center justify-center border-0 bg-transparent p-0 leading-none text-black outline-none",
        className,
      )}
      {...data}
    >
      <span className="role-fetch-circle relative -mt-0.5 ml-[3px] inline-flex size-4 items-center justify-center rounded-full border border-primary bg-primary text-[0px] text-white transition duration-150 group-hover/rf:-translate-y-[0.5px] group-hover/rf:border-accent group-hover/rf:bg-accent group-hover/rf:shadow-[0_0_0_1px_rgba(0,0,0,0.35)] group-focus-visible/rf:-translate-y-[0.5px] group-focus-visible/rf:border-accent group-focus-visible/rf:bg-accent">
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
  /** The short accessible name of the "i" button. */
  tipLabel: string;
  placeholder: string;
  mode: "ids" | "words";
  value: string[];
  onChange: (key: FilterKey, v: string[]) => void;
  /** Picker button (and status) after the label, in the old .filters-roles-label-row. */
  action?: ReactNode;
  /** The Role Blocks row: inline-flex with a 4px gap. */
  roles?: boolean;
}

function ListField({ name, inputId, label, tip, tipLabel, placeholder, mode, value, onChange, action, roles }: ListFieldProps) {
  const id = inputId ?? `${name}_input`;
  const tipEl = <TipLabel htmlFor={id} text={label} tip={tip} tipLabel={tipLabel} tipId={`tip-${name}`} align="wrap" />;
  return (
    <div className="field mt-4 flex flex-col gap-1.5 first-of-type:mt-0">
      {action ? (
        <div className={cx("filters-roles-label-row", roles && "inline-flex items-center gap-1")}>
          {tipEl}
          {roles ? null : " "}
          {action}
        </div>
      ) : (
        <TipLabel htmlFor={id} text={label} tip={tip} tipLabel={tipLabel} tipId={`tip-${name}`} align="wrap" className="m-0! flex! leading-[1.3]" />
      )}
      <ChipInput id={id} mode={mode} value={value} onChange={(v) => onChange(name, v)} placeholder={placeholder} className={CHIPS} />
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
    <FetchButton label={label} onClick={() => setPicker(key)} data-filter-target={key} data-filter-kind={kind} className="filter-fetch-btn [&>span]:top-px" />
  );

  return (
    <>
      <Shell
        id="filters-modal"
        closeId="filters-close"
        titleId="filters-title"
        title={`Filters – ${niceName}`}
        onClose={requestClose}
        cardClass="my-[5vh] max-h-[85vh] w-[min(960px,94vw)] rounded-[20px]"
        headerClass="flex items-center justify-between px-5 py-4"
        titleClass="m-0 text-[15px] leading-[1.3] font-semibold text-accent"
        layer={30}
      >
        <form
          id="form-filters"
          className="padded flex max-h-[calc(85vh-60px)] min-h-0 flex-col gap-5 overflow-y-auto px-5 pt-4 pb-5 [scrollbar-color:var(--color-accent)_transparent] [scrollbar-width:thin]" autoComplete="off" onSubmit={onSubmit}>
          <input type="hidden" id="filters_mapping_id" name="mapping_id" value={mid} readOnly />
          {load === "failed" ? (
            <p role="alert" className="text-sm text-danger">
              Failed to load filters. Saving is disabled so the current filters are not overwritten.
            </p>
          ) : null}

          <div className="filters-grid grid grid-cols-2 gap-6 max-[900px]:grid-cols-1">
            <fieldset className={`filters-col ${COL}`}>
              <legend className={LEGEND}>Whitelist (Allow-list)</legend>
              <ListField
                name="wl_categories"
                label="Allowed Category IDs"
                tip="Only these categories (and their channels) will sync. Leave empty to allow all categories."
                tipLabel="Only sync channels inside these category IDs."
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
                tipLabel="Only sync these specific channels or threads."
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
                tipLabel="Skip any channels in these categories."
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
                tipLabel="Never sync these specific channels or threads."
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
              tip="Channels whose name matches a pattern will be skipped during cloning and messages will not be forwarded. Supports wildcards: * matches any characters, ? matches one character. Plain text uses substring matching. Examples: ticket-*, *-ticket, *-log-*"
              tipLabel="Channels matching these patterns will not be cloned."
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
              tipLabel="Which roles does this affect?"
              placeholder="Add role ID(s)…"
              mode="ids"
              value={form.blocked_role_ids}
              onChange={set}
              roles
              action={
                <>
                  <FetchButton
                    id="filters-role-refresh"
                    label="Fetch roles from Discord"
                    title="Fetch Discord roles"
                    className="-translate-y-0.5"
                    onClick={() => setRolesOpen(true)}
                  />
                  <span id="filters-role-status" className="filters-roles-status text-[11px] text-muted opacity-80" aria-live="polite">
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
              tipLabel="Messages containing these words will NOT sync for this mapping."
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
              tipLabel="Only messages from these users will be cloned"
              placeholder="Add user ID(s)…"
              mode="ids"
              value={form.wl_users}
              onChange={set}
            />
            <ListField
              name="bl_users"
              label="Blacklisted User IDs"
              tip="Messages from these user IDs will be blocked from cloning."
              tipLabel="Messages from these users will NOT be cloned"
              placeholder="Add user ID(s)…"
              mode="ids"
              value={form.bl_users}
              onChange={set}
            />
          </fieldset>

          <div className="btns modal-footer-row mt-2 flex flex-wrap gap-2.5 pt-1">
            <Button id="filters-save-btn" type="submit" variant="ghost" loading={saving} disabled={saving || load !== "ready"}>
              Save Filters
            </Button>
            {dirty ? (
              <GhostButton id="btn-cancel-filters" danger onClick={() => void reload()}>
                Cancel
              </GhostButton>
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
