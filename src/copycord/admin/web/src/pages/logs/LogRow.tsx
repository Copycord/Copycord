// One event log row plus its expandable metadata row. Everything that came
// from Discord (details, names, extra values) is rendered as React text.

import type { KeyboardEvent } from "react";
import { cx } from "../../components/ui/cx";
import type { EventLog } from "../../types/logs";
import { LogsIcon } from "./icons";
import { BADGE, COLS, DELETE_BTN, DETAILS_TEXT, GUILD_TEXT, ROW, rowCells, TIMESTAMP_TEXT } from "./LogsSkeleton";
import { fmtTimestamp, guildText, hasMetadata, metadataRows, typeMeta } from "./model";
import type { TypeTone } from "./model";

const TONE: Record<TypeTone, string> = {
  created: "bg-good-a15 text-good-soft",
  deleted: "bg-danger-a15 text-danger-soft",
  renamed: "bg-caution-a15 text-caution",
  moved: "bg-info-soft-a15 text-info-soft",
  converted: "bg-accent-a15 text-accent",
  thread: "bg-kind-thread-a15 text-kind-thread",
  role: "bg-kind-role-a15 text-kind-role",
  emoji: "bg-kind-emoji-a15 text-kind-emoji",
  sticker: "bg-kind-sticker-a15 text-kind-sticker",
  guild: "bg-kind-guild-a15 text-kind-guild",
  permissions: "bg-kind-permissions-a15 text-kind-permissions",
  metadata: "bg-kind-metadata-a15 text-kind-metadata",
  webhook: "bg-primary-a15 text-accent",
  error: "bg-danger-a20 text-danger-light",
  default: "bg-slate-a15 text-slate",
};

export function TypeBadge({ type }: { type: string }) {
  const meta = typeMeta(type);
  return (
    <span data-tone={meta.tone} className={cx(BADGE, TONE[meta.tone])}>
      {meta.label}
    </span>
  );
}

export interface LogRowProps {
  log: EventLog;
  expanded: boolean;
  deleting: boolean;
  onToggle: (id: string) => void;
  onDelete: (log: EventLog) => void;
}

export function LogRow({ log, expanded, deleting, onToggle, onDelete }: LogRowProps) {
  const expandable = hasMetadata(log);
  const toggle = expandable ? () => onToggle(log.log_id) : undefined;
  const onKey = (e: KeyboardEvent<HTMLTableRowElement>) => {
    if (!toggle || e.target !== e.currentTarget) return;
    if (e.key === "Enter" || e.key === " ") {
      e.preventDefault();
      toggle();
    }
  };
  const cells = rowCells(expanded);

  return (
    <>
      <tr
        data-log-id={log.log_id}
        className={cx(ROW, expandable && "cursor-pointer outline-none focus-visible:bg-primary-a6")}
        onClick={toggle}
        onKeyDown={toggle ? onKey : undefined}
        tabIndex={expandable ? 0 : undefined}
        aria-expanded={expandable ? expanded : undefined}
      >
        <td className={cells.td}>
          <span className={TIMESTAMP_TEXT}>{fmtTimestamp(log.created_at)}</span>
        </td>
        <td className={cells.td}>
          <TypeBadge type={log.event_type} />
        </td>
        <td className={cells.guild}>
          <span className={GUILD_TEXT}>{guildText(log)}</span>
        </td>
        <td className={cells.td}>
          <span className={DETAILS_TEXT}>{log.details ?? ""}</span>
        </td>
        <td className={cells.actions}>
          <button
            type="button"
            title="Delete"
            aria-label="Delete log"
            disabled={deleting}
            className={DELETE_BTN}
            onClick={(e) => {
              e.stopPropagation();
              onDelete(log);
            }}
          >
            <LogsIcon name="x" size={14} />
          </button>
        </td>
      </tr>
      {expandable && expanded ? (
        <tr data-detail-for={log.log_id}>
          <td colSpan={COLS} className="p-0">
            <div className="grid grid-cols-[repeat(auto-fill,minmax(220px,1fr))] gap-x-6 gap-y-2 border-b border-border bg-well-a50 px-5 pt-3 pb-4">
              {metadataRows(log).map(([label, value], i) => (
                <div key={`${label}-${i}`} className="flex flex-col gap-0.5">
                  <span className="text-[11px] tracking-[0.5px] text-muted uppercase">{label}</span>
                  <span className="font-mono text-[13px] break-all text-white/85">{value}</span>
                </div>
              ))}
            </div>
          </td>
        </tr>
      ) : null}
    </>
  );
}
