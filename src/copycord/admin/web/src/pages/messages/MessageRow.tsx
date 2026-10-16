// One message row: a cell per visible column and the Preview button. Every cell is one line that
// truncates with the full value in its title, so rows keep the height of the loading rows.

import { memo, useEffect, useState } from "react";
import { cx } from "../../components/ui/cx";
import type { MessageRow as Row } from "../../types/messages";
import { MessagesIcon } from "./icons";
import { CELL_TEXT, MONO, MUTED, PREVIEW_TD, ROW, TD } from "./MessagesSkeleton";
import { absoluteTime, authorDisplayName, relativeTime } from "./model";
import type { ColumnKey } from "./model";

const SMALL_BTN =
  "inline-flex shrink-0 cursor-pointer items-center justify-center rounded-md p-1 text-muted outline-none transition-colors " +
  "hover:bg-white/6 hover:text-text focus-visible:ring-2 focus-visible:ring-accent/60";
const LINK =
  "inline-flex items-center gap-1 rounded-md px-1.5 py-0.5 text-xs text-text-2 no-underline outline-none transition-colors " +
  "hover:bg-white/6 hover:text-text focus-visible:ring-2 focus-visible:ring-accent/60";
export const PREVIEW_BTN =
  "inline-flex cursor-pointer items-center rounded-lg border border-border-input bg-panel-a60 px-2.5 py-0.5 text-xs text-text " +
  "outline-none transition-colors hover:border-accent/50 hover:bg-primary-a10 focus-visible:ring-2 focus-visible:ring-accent/60";

const DASH = <span className={MUTED}>-</span>;

/** A monospace id with a copy button; the check shows for a moment after a copy. */
export function CopyId({ id, label }: { id: string | null; label: string }) {
  const [copied, setCopied] = useState(false);
  useEffect(() => {
    if (!copied) return;
    const t = setTimeout(() => setCopied(false), 1200);
    return () => clearTimeout(t);
  }, [copied]);
  if (!id) return DASH;
  return (
    <span className="flex min-w-0 items-center gap-1">
      <span className={cx(CELL_TEXT, MONO, "min-w-0 text-text-2")} title={id}>
        {id}
      </span>
      <button
        type="button"
        className={cx(SMALL_BTN, "opacity-0 group-hover:opacity-100 focus-visible:opacity-100 max-md:opacity-100", copied && "opacity-100")}
        aria-label={copied ? `${label} copied` : `Copy ${label}`}
        title={copied ? "Copied" : `Copy ${label}`}
        onClick={() => {
          void navigator.clipboard?.writeText(id).then(
            () => setCopied(true),
            () => undefined,
          );
        }}
      >
        <MessagesIcon name={copied ? "check" : "copy"} size={13} />
      </button>
    </span>
  );
}

export function DiscordLink({ href, label, text }: { href: string | null; label: string; text: string }) {
  if (!href) {
    return (
      <span className={cx(LINK, "cursor-default text-muted/50 hover:bg-transparent hover:text-muted/50")} title={`${label}: not available`}>
        {text}
      </span>
    );
  }
  return (
    <a className={LINK} href={href} target="_blank" rel="noopener noreferrer" aria-label={`${label} in Discord`} title={`${label} in Discord`}>
      {text}
      <MessagesIcon name="external" size={11} />
    </a>
  );
}

export function authorText(r: Row): { name: string | null; username: string | null } {
  return { name: authorDisplayName(r), username: r.author_username };
}

/** A name that truncates with the full text in its title; a dash when unknown. */
function Name({ text, className }: { text: string | null; className?: string }) {
  if (!text) return DASH;
  return (
    <span className={cx(CELL_TEXT, className)} title={text}>
      {text}
    </span>
  );
}

function Count({ n, icon, one, many }: { n: number | null; icon: "paperclip" | "embed"; one: string; many: string }) {
  if (n === null || n === undefined) return DASH;
  return (
    <span className={cx(CELL_TEXT, "flex items-center gap-1 text-xs text-text-2 tabular-nums", n === 0 && "text-muted/60")} title={`${n} ${n === 1 ? one : many}`}>
      <MessagesIcon name={icon} size={13} />
      {n}
    </span>
  );
}

function Cell({ k, r, now }: { k: ColumnKey; r: Row; now: number }) {
  switch (k) {
    case "time":
      return (
        <span className={cx(CELL_TEXT, "text-xs text-text-2 tabular-nums")} title={absoluteTime(r.created_at)}>
          {relativeTime(r.created_at, now)}
        </span>
      );
    case "created":
      return r.created_at === null ? DASH : <Name text={absoluteTime(r.created_at)} className="text-xs text-text-2 tabular-nums" />;
    case "author_name":
      return <Name text={authorDisplayName(r)} className="font-medium" />;
    case "username":
      return <Name text={r.author_username ? `@${r.author_username}` : null} className="text-text-2" />;
    case "author_id":
      return <CopyId id={r.author_id} label="author ID" />;
    case "original_channel":
      return <Name text={r.original_channel_name ? `#${r.original_channel_name}` : null} />;
    case "original_channel_id":
      return <CopyId id={r.original_channel_id} label="original channel ID" />;
    case "cloned_channel":
      return <Name text={r.cloned_channel_name ? `#${r.cloned_channel_name}` : null} />;
    case "cloned_channel_id":
      return <CopyId id={r.cloned_channel_id} label="cloned channel ID" />;
    case "original_guild":
      return <Name text={r.original_guild_name} />;
    case "original_guild_id":
      return <CopyId id={r.original_guild_id} label="original guild ID" />;
    case "cloned_guild":
      return <Name text={r.cloned_guild_name} />;
    case "cloned_guild_id":
      return <CopyId id={r.cloned_guild_id} label="cloned guild ID" />;
    case "mapping":
      return <Name text={r.mapping_name} className="text-text-2" />;
    case "attachments":
      return <Count n={r.attachment_count} icon="paperclip" one="attachment" many="attachments" />;
    case "embeds":
      return <Count n={r.embed_count} icon="embed" one="embed" many="embeds" />;
    case "links":
      return (
        <span className="flex items-center gap-0.5">
          <DiscordLink href={r.original_url} label="Open original" text="Original" />
          <DiscordLink href={r.cloned_url} label="Open clone" text="Clone" />
        </span>
      );
    case "message_id":
      return <CopyId id={r.original_message_id} label="message ID" />;
    case "cloned_message_id":
      return <CopyId id={r.cloned_message_id} label="cloned message ID" />;
  }
}

export const MessageRow = memo(function MessageRow({
  row,
  columns,
  now,
  onPreview,
}: {
  row: Row;
  columns: readonly ColumnKey[];
  now: number;
  onPreview: (row: Row) => void;
}) {
  return (
    <tr className={ROW} data-message-row="">
      {columns.map((k) => (
        <td key={k} className={cx(TD, "leading-6")}>
          <Cell k={k} r={row} now={now} />
        </td>
      ))}
      <td className={cx(TD, PREVIEW_TD, "leading-6")}>
        <button type="button" className={PREVIEW_BTN} onClick={() => onPreview(row)} aria-label={`Preview message ${row.original_message_id}`}>
          Preview
        </button>
      </td>
    </tr>
  );
});
