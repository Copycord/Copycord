// One mapping card, the old .guild-card: icon, name, host server, and the
// Settings, Filters, status, Clone and Delete buttons. Class names are kept
// for the e2e helpers.

import type { ReactNode } from "react";
import { cx } from "../../../components/ui";
import type { DashboardMapping } from "../../../types/dashboard-mappings";
import { CloneIcon, FiltersIcon, PausedBellIcon, PauseIcon, PlayIcon, SettingsIcon, TrashIcon } from "./icons";
import { DEFAULT_ICON, hostLabel, isPaused, statusLabel } from "./model";

const ICON_BTN =
  "inline-flex size-7 min-h-7 cursor-pointer items-center justify-center rounded-md border-0 bg-white/4 p-0 " +
  "text-muted transition hover:bg-accent/12 hover:text-accent active:translate-y-px disabled:cursor-default disabled:opacity-50";

function IconButton({ className, ...rest }: React.ButtonHTMLAttributes<HTMLButtonElement> & { children: ReactNode }) {
  return <button type="button" className={cx(ICON_BTN, className)} {...rest} />;
}

export interface MappingCardProps {
  mapping: DashboardMapping;
  /** Bots are running: the old page greyed the cards out and blocked clicks. */
  locked: boolean;
  busy: boolean;
  onEdit: (m: DashboardMapping) => void;
  onFilters: (m: DashboardMapping) => void;
  onToggle: (m: DashboardMapping) => void;
  onClone: (m: DashboardMapping) => void;
  onDelete: (m: DashboardMapping) => void;
}

export function MappingCard({ mapping: m, locked, busy, onEdit, onFilters, onToggle, onClone, onDelete }: MappingCardProps) {
  const paused = isPaused(m);
  const label = statusLabel(paused);
  return (
    <div
      className={cx(
        "guild-card relative flex min-w-0 flex-col overflow-hidden rounded-xl border p-3 text-left text-text shadow-[0_4px_12px_rgba(0,0,0,0.4)] transition",
        paused
          ? "is-paused border-transparent bg-slate-400/24 hover:bg-slate-400/30"
          : "is-active border-border bg-card hover:-translate-y-0.5 hover:border-accent hover:bg-[rgba(27,16,48,0.6)] hover:shadow-[0_8px_20px_rgba(0,0,0,0.6)]",
        locked && "locked cursor-not-allowed opacity-50 grayscale-[0.4]",
      )}
      data-id={m.mapping_id}
      data-status={(m.status || "active").toLowerCase()}
      title={locked ? "Stop the bots to edit mappings" : undefined}
    >
      <div className={cx("guild-card-inner flex h-full flex-col gap-2.5", locked && "pointer-events-none")}>
        <div className="guild-card-header flex items-center gap-2.5">
          <img
            src={m.original_guild_icon_url || DEFAULT_ICON}
            alt=""
            className={cx("guild-card-avatar size-9 min-w-9 shrink-0 rounded-full object-cover", paused && "opacity-50 grayscale-[0.6]")}
          />
          <div className="guild-card-info min-w-0 flex-1">
            <div className="guild-card-name-title truncate text-[13px] leading-[1.3] font-semibold" title={m.mapping_name || ""}>
              {m.mapping_name || ""}
            </div>
            <div className="guild-card-route mt-px truncate text-[11px] text-muted opacity-70">{hostLabel(m)}</div>
          </div>
          {paused ? (
            <span className="guild-card-paused-icon inline-flex shrink-0 text-muted opacity-40" title="Paused">
              <PausedBellIcon />
            </span>
          ) : null}
        </div>

        <div className="guild-card-actions mt-auto flex gap-1.5 border-t border-border pt-2">
          <IconButton className="edit-mapping-btn" data-id={m.mapping_id} aria-label="Edit mapping" title="Settings" onClick={() => onEdit(m)}>
            <SettingsIcon />
          </IconButton>
          <IconButton
            className="mapping-filters-btn"
            data-id={m.mapping_id}
            aria-label="Filters for this mapping"
            title="Filters"
            onClick={() => onFilters(m)}
          >
            <FiltersIcon />
          </IconButton>
          <IconButton
            className={cx("mapping-status-btn", paused ? "is-paused" : "is-active")}
            data-id={m.mapping_id}
            aria-pressed={paused}
            aria-label={label}
            title={label}
            disabled={busy}
            onClick={() => onToggle(m)}
          >
            {paused ? <PlayIcon /> : <PauseIcon />}
          </IconButton>
          <IconButton className="clone-mapping-btn" data-id={m.mapping_id} aria-label="Clone mapping" title="Clone mapping" onClick={() => onClone(m)}>
            <CloneIcon />
          </IconButton>
          <span className="guild-card-action-spacer flex-1" />
          <IconButton
            className="delete-mapping-btn"
            data-action="delete"
            data-id={m.mapping_id}
            aria-label="Delete mapping"
            title="Delete mapping"
            onClick={() => onDelete(m)}
          >
            <TrashIcon />
          </IconButton>
        </div>
      </div>
    </div>
  );
}
