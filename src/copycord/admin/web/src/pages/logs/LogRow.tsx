// One event log row plus its expandable metadata row. Everything that came
// from Discord (details, names, extra values) is rendered as React text.

import type { KeyboardEvent } from "react";
import { cx } from "../../components/ui/cx";
import type { EventLog } from "../../types/logs";
import { LogsIcon } from "./icons";
import { fmtTimestamp, guildText, hasMetadata, metadataRows, typeMeta } from "./model";
import type { TypeTone } from "./model";

const TONE: Record<TypeTone, string> = {
  created: "border-[rgba(16,185,129,0.25)] bg-[rgba(16,185,129,0.15)] text-[#34d399]",
  deleted: "border-[rgba(239,68,68,0.25)] bg-[rgba(239,68,68,0.15)] text-[#f87171]",
  renamed: "border-[rgba(251,191,36,0.25)] bg-[rgba(251,191,36,0.15)] text-[#fbbf24]",
  moved: "border-[rgba(96,165,250,0.25)] bg-[rgba(96,165,250,0.15)] text-[#60a5fa]",
  converted: "border-[rgba(167,139,250,0.25)] bg-[rgba(167,139,250,0.15)] text-[#a78bfa]",
  thread: "border-[rgba(244,114,182,0.25)] bg-[rgba(244,114,182,0.15)] text-[#f472b6]",
  role: "border-[rgba(251,146,60,0.25)] bg-[rgba(251,146,60,0.15)] text-[#fb923c]",
  emoji: "border-[rgba(34,211,238,0.25)] bg-[rgba(34,211,238,0.15)] text-[#22d3ee]",
  sticker: "border-[rgba(232,121,249,0.25)] bg-[rgba(232,121,249,0.15)] text-[#e879f9]",
  guild: "border-[rgba(192,132,252,0.25)] bg-[rgba(192,132,252,0.15)] text-[#c084fc]",
  permissions: "border-[rgba(253,186,116,0.25)] bg-[rgba(253,186,116,0.15)] text-[#fdba74]",
  metadata: "border-[rgba(56,189,248,0.25)] bg-[rgba(56,189,248,0.15)] text-[#7dd3fc]",
  webhook: "border-[rgba(139,92,246,0.25)] bg-[rgba(139,92,246,0.15)] text-[#a78bfa]",
  error: "border-[rgba(239,68,68,0.35)] bg-[rgba(239,68,68,0.2)] text-[#fca5a5]",
  default: "border-[rgba(148,163,184,0.25)] bg-[rgba(148,163,184,0.15)] text-[#94a3b8]",
};

export function TypeBadge({ type }: { type: string }) {
  const meta = typeMeta(type);
  return (
    <span
      data-tone={meta.tone}
      className={cx(
        "inline-block whitespace-nowrap rounded-md border px-2.5 py-[3px] text-[11px] font-semibold tracking-[0.2px] capitalize",
        TONE[meta.tone],
      )}
    >
      {meta.label}
    </span>
  );
}

// Cells pick their own horizontal padding so no two utilities fight over one property.
export const TD = "border-b py-2.5 align-middle text-[13px] text-text";
export const COLS = 6;

export interface LogRowProps {
  log: EventLog;
  selected: boolean;
  expanded: boolean;
  deleting: boolean;
  onSelect: (id: string, on: boolean) => void;
  onToggle: (id: string) => void;
  onDelete: (log: EventLog) => void;
}

export function LogRow({ log, selected, expanded, deleting, onSelect, onToggle, onDelete }: LogRowProps) {
  const expandable = hasMetadata(log);
  const toggle = expandable ? () => onToggle(log.log_id) : undefined;
  const onKey = (e: KeyboardEvent<HTMLTableRowElement>) => {
    if (!toggle || e.target !== e.currentTarget) return;
    if (e.key === "Enter" || e.key === " ") {
      e.preventDefault();
      toggle();
    }
  };
  const edge = cx(TD, expanded ? "border-b-transparent" : "border-white/4");
  const td = cx(edge, "px-3.5");

  return (
    <>
      <tr
        data-log-id={log.log_id}
        className={cx(
          "group transition-colors hover:bg-[rgba(139,92,246,0.06)]",
          expandable && "cursor-pointer outline-none focus-visible:bg-[rgba(139,92,246,0.06)]",
          selected && "bg-[rgba(139,92,246,0.08)]",
        )}
        onClick={toggle}
        onKeyDown={toggle ? onKey : undefined}
        tabIndex={expandable ? 0 : undefined}
        aria-expanded={expandable ? expanded : undefined}
      >
        <td className={cx(edge, "px-0 text-center")} onClick={(e) => e.stopPropagation()}>
          <input
            type="checkbox"
            className="size-3.5 cursor-pointer accent-primary"
            aria-label="Select log"
            checked={selected}
            onChange={(e) => onSelect(log.log_id, e.target.checked)}
          />
        </td>
        <td className={td}>
          <span className="whitespace-nowrap font-mono text-xs text-muted">{fmtTimestamp(log.created_at)}</span>
        </td>
        <td className={td}>
          <TypeBadge type={log.event_type} />
        </td>
        <td className={cx(td, "max-md:hidden")}>
          <span className="text-xs font-medium text-muted">{guildText(log)}</span>
        </td>
        <td className={td}>
          <span className="break-words text-[13px] text-white/80">{log.details ?? ""}</span>
        </td>
        <td className={cx(edge, "px-1 text-center")}>
          <button
            type="button"
            title="Delete"
            aria-label="Delete log"
            disabled={deleting}
            className={cx(
              "inline-flex cursor-pointer items-center justify-center rounded-md p-1 text-muted opacity-0 transition",
              "group-hover:opacity-100 focus-visible:opacity-100 max-md:opacity-100",
              "hover:bg-[rgba(239,68,68,0.1)] hover:text-[#f87171] disabled:cursor-wait",
            )}
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
            <div className="grid grid-cols-[repeat(auto-fill,minmax(220px,1fr))] gap-x-6 gap-y-2 border-b border-border bg-[rgba(15,8,30,0.5)] px-5 pt-3 pb-4">
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
