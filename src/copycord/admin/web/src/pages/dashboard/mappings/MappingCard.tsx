// One mapping card (.guild-card): icon, name, host server, and the
// Settings, Filters, status, Clone and Delete buttons. Class names are kept
// stable; tests select by them.

import type { ReactNode } from "react";
import { cx } from "../../../components/ui";
import type { DashboardMapping } from "../../../types/dashboard-mappings";
import { CloneIcon, FiltersIcon, PausedBellIcon, PauseIcon, PlayIcon, SettingsIcon, TrashIcon } from "./icons";
import {
  MAPPING_ACTIONS,
  MAPPING_AVATAR,
  MAPPING_CARD,
  MAPPING_HEADER,
  MAPPING_ICON_BTN,
  MAPPING_INFO,
  MAPPING_INNER,
  MAPPING_NAME,
  MAPPING_ROUTE,
} from "../DashboardSkeleton";
import { DEFAULT_ICON, hostLabel, isPaused, statusLabel } from "./model";


function IconButton({ className, ...rest }: React.ButtonHTMLAttributes<HTMLButtonElement> & { children: ReactNode }) {
  return <button type="button" className={cx(MAPPING_ICON_BTN, className)} {...rest} />;
}

export interface MappingCardProps {
  mapping: DashboardMapping;
  /** Bots are running: the card is greyed out and blocks clicks. */
  locked: boolean;
  busy: boolean;
  onEdit: (m: DashboardMapping) => void;
  onFilters: (m: DashboardMapping) => void;
  onToggle: (m: DashboardMapping) => void;
  onClone: (m: DashboardMapping) => void;
  onDelete: (m: DashboardMapping) => void;
  /** The fade in as the card replaces a loading tile (useReveal). */
  className?: string;
}

export function MappingCard({ mapping: m, locked, busy, onEdit, onFilters, onToggle, onClone, onDelete, className }: MappingCardProps) {
  const paused = isPaused(m);
  const label = statusLabel(paused);
  return (
    <div
      className={cx(
        "guild-card",
        MAPPING_CARD,
        paused
          ? "is-paused border-transparent bg-slate-400/24 hover:bg-slate-400/30"
          : "is-active border-border bg-card hover:-translate-y-0.5 hover:border-accent hover:bg-card-hover hover:[box-shadow:0_8px_20px_var(--color-shadow-60)]",
        locked && "locked cursor-not-allowed opacity-50 grayscale-[0.4]",
        className,
      )}
      data-id={m.mapping_id}
      data-status={(m.status || "active").toLowerCase()}
      title={locked ? "Stop the bots to edit mappings" : undefined}
    >
      <div className={cx("guild-card-inner", MAPPING_INNER, locked && "pointer-events-none")}>
        <div className={cx("guild-card-header", MAPPING_HEADER)}>
          <img
            src={m.original_guild_icon_url || DEFAULT_ICON}
            alt=""
            className={cx("guild-card-avatar", MAPPING_AVATAR, paused && "opacity-50 grayscale-[0.6]")}
          />
          <div className={cx("guild-card-info", MAPPING_INFO)}>
            <div className={cx("guild-card-name-title", MAPPING_NAME)} title={m.mapping_name || ""}>
              {m.mapping_name || ""}
            </div>
            <div className={cx("guild-card-route", MAPPING_ROUTE)}>{hostLabel(m)}</div>
          </div>
          {paused ? (
            <span className="guild-card-paused-icon inline-flex shrink-0 text-muted opacity-40" title="Paused">
              <PausedBellIcon />
            </span>
          ) : null}
        </div>

        <div className={cx("guild-card-actions", MAPPING_ACTIONS)}>
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
