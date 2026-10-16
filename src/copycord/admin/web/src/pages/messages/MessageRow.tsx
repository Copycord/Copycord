// One message row: a cell per visible column and the Preview button. Every cell is one line that
// truncates with the full value in its title, so rows keep the height of the loading rows.

import { memo, useEffect, useState } from "react";
import { cx } from "../../components/ui/cx";
import type { MessageRow as Row } from "../../types/messages";
import { MessagesIcon } from "./icons";
import { CELL_TEXT, MONO, MUTED, ROW, TD } from "./MessagesSkeleton";
import { absoluteTime, channelLabel, relativeTime } from "./model";
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
  return { name: r.author_name ?? r.author_username, username: r.author_username };
}

function Cell({ k, r, now }: { k: ColumnKey; r: Row; now: number }) {
  switch (k) {
    case "time":
      return (
        <span className={cx(CELL_TEXT, "text-xs text-text-2 tabular-nums")} title={absoluteTime(r.created_at)}>
          {relativeTime(r.created_at, now)}
        </span>
      );
    case "author": {
      const { name, username } = authorText(r);
      if (!name) return DASH;
      const full = username && username !== name ? `${name} @${username}` : name;
      return (
        <span className={CELL_TEXT} title={full}>
          <span className="font-medium">{name}</span>
          {username && username !== name ? <span className={cx(MUTED, "ml-1.5")}>@{username}</span> : null}
        </span>
      );
    }
    case "channel": {
      const text = channelLabel(r.original_channel_name, r.original_channel_id);
      return (
        <span className={cx(CELL_TEXT, !r.original_channel_name && cx(MONO, "text-text-2"))} title={text}>
          {text}
        </span>
      );
    }
    case "mapping":
      return r.mapping_name ? (
        <span className={cx(CELL_TEXT, "text-text-2")} title={r.mapping_name}>
          {r.mapping_name}
        </span>
      ) : (
        DASH
      );
    case "counts": {
      if (r.attachment_count === null && r.embed_count === null) return DASH;
      const a = r.attachment_count ?? 0;
      const e = r.embed_count ?? 0;
      const title = [`${a} attachment${a === 1 ? "" : "s"}`, `${e} embed${e === 1 ? "" : "s"}`].join(", ");
      return (
        <span className={cx(CELL_TEXT, "flex items-center gap-2.5 text-xs text-text-2 tabular-nums")} title={title}>
          {a > 0 ? (
            <span className="inline-flex items-center gap-0.5">
              <MessagesIcon name="paperclip" size={13} />
              <span className="sr-only">Attachments</span>
              {a}
            </span>
          ) : null}
          {e > 0 ? (
            <span className="inline-flex items-center gap-0.5">
              <MessagesIcon name="embed" size={13} />
              <span className="sr-only">Embeds</span>
              {e}
            </span>
          ) : null}
        </span>
      );
    }
    case "links":
      return (
        <span className="flex items-center gap-0.5">
          <DiscordLink href={r.original_url} label="Open original" text="Original" />
          <DiscordLink href={r.cloned_url} label="Open clone" text="Clone" />
        </span>
      );
    case "message_id":
      return <CopyId id={r.original_message_id} label="message ID" />;
    case "author_id":
      return <CopyId id={r.author_id} label="author ID" />;
    case "original_channel_id":
      return <CopyId id={r.original_channel_id} label="original channel ID" />;
    case "cloned_message_id":
      return <CopyId id={r.cloned_message_id} label="cloned message ID" />;
    case "cloned_channel_id":
      return <CopyId id={r.cloned_channel_id} label="cloned channel ID" />;
    case "guild_ids":
      return (
        <span className="flex min-w-0 items-center gap-1">
          <span className="min-w-0 flex-1">
            <CopyId id={r.original_guild_id} label="original guild ID" />
          </span>
          <span className={MUTED} aria-label="cloned to">
            →
          </span>
          <span className="min-w-0 flex-1">
            <CopyId id={r.cloned_guild_id} label="cloned guild ID" />
          </span>
        </span>
      );
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
      <td className={cx(TD, "text-right leading-6")}>
        <button type="button" className={PREVIEW_BTN} onClick={() => onPreview(row)} aria-label={`Preview message ${row.original_message_id}`}>
          Preview
        </button>
      </td>
    </tr>
  );
});
