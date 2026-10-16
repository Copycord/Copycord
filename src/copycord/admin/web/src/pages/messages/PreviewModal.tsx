// The Preview dialog: who and when in the header, the row's ids and links in a grid, and the message
// itself fetched live through the server bot (GET /api/messages/{id}/content), a shimmer meanwhile.

import { useCallback, useEffect, useState } from "react";
import type { ReactNode } from "react";
import { Modal } from "../../components/Modal";
import { cx } from "../../components/ui/cx";
import { Skeleton } from "../../components/ui/Skeleton";
import { api } from "../../lib/api";
import type { LiveEmbed, MessageContentResponse, MessageRow } from "../../types/messages";
import { MessagesIcon } from "./icons";
import { CopyId, DiscordLink, PREVIEW_BTN, authorText } from "./MessageRow";
import { absoluteTime, channelLabel, contentUrl, previewError } from "./model";
import type { PreviewError } from "./model";

type State =
  | { kind: "loading" }
  | { kind: "ok"; data: MessageContentResponse }
  | { kind: "error"; error: PreviewError };

const DT = "text-[11px] font-semibold tracking-[0.4px] text-muted uppercase";
const DD = "m-0 min-w-0 text-[13px] text-text";

function embedText(v: LiveEmbed["author"] | LiveEmbed["footer"]): string | null {
  if (!v) return null;
  if (typeof v === "string") return v;
  return ("name" in v ? v.name : "text" in v ? v.text : null) ?? null;
}

const plural = (n: number, word: string) => `${n} ${word}${n === 1 ? "" : "s"}`;

function formatSize(n: number | null | undefined): string {
  if (!n) return "";
  if (n < 1024) return `${n} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`;
  return `${(n / 1024 / 1024).toFixed(1)} MB`;
}

export function PreviewModal({ row, onClose }: { row: MessageRow | null; onClose: () => void }) {
  const [state, setState] = useState<State>({ kind: "loading" });
  const [attempt, setAttempt] = useState(0);
  const [wait, setWait] = useState(0);

  useEffect(() => {
    if (!row) return;
    const ctl = new AbortController();
    setState({ kind: "loading" });
    api.get<MessageContentResponse>(contentUrl(row), { signal: ctl.signal }).then(
      (data) => !ctl.signal.aborted && setState({ kind: "ok", data }),
      (e: unknown) => {
        if (ctl.signal.aborted) return;
        const error = previewError(e);
        setState({ kind: "error", error });
        setWait(error.retryAfter ? Math.ceil(error.retryAfter) : 0);
      },
    );
    return () => ctl.abort();
  }, [row, attempt]);

  // A rate limit keeps Retry off until Discord's wait is over; no automatic retry.
  useEffect(() => {
    if (wait <= 0) return;
    const t = setTimeout(() => setWait((w) => w - 1), 1000);
    return () => clearTimeout(t);
  }, [wait]);

  const retry = useCallback(() => setAttempt((a) => a + 1), []);

  if (!row) return null;
  const { name, username } = authorText(row);
  const title = name ? (username && username !== name ? `${name} (@${username})` : name) : "Unknown author";

  return (
    <Modal
      open
      onClose={onClose}
      title={title}
      subtitle={<span className="text-xs text-muted">{absoluteTime(row.created_at)}</span>}
      className="w-[min(780px,94vw)]"
      bodyClassName="min-h-0 overflow-auto px-4 pb-4"
      id="message-preview"
    >
      <dl className="m-0 grid grid-cols-[repeat(auto-fill,minmax(210px,1fr))] gap-x-5 gap-y-2.5 rounded-lg border border-border bg-white/2 p-3">
        <Meta label="Message ID">
          <CopyId id={row.original_message_id} label="message ID" />
        </Meta>
        <Meta label="Cloned message ID">
          <CopyId id={row.cloned_message_id} label="cloned message ID" />
        </Meta>
        <Meta label="Author ID">
          <CopyId id={row.author_id} label="author ID" />
        </Meta>
        <Meta label="Channel">
          <span className="block truncate" title={channelLabel(row.original_channel_name, row.original_channel_id)}>
            {channelLabel(row.original_channel_name, row.original_channel_id)}
            {row.cloned_channel_name ? <span className="text-muted"> → #{row.cloned_channel_name}</span> : null}
          </span>
        </Meta>
        <Meta label="Mapping">
          <span className="block truncate" title={row.mapping_name ?? undefined}>
            {row.mapping_name ?? "-"}
          </span>
        </Meta>
        <Meta label="Attachments, embeds">
          {row.attachment_count === null && row.embed_count === null
            ? "-"
            : plural(row.attachment_count ?? 0, "attachment") + ", " + plural(row.embed_count ?? 0, "embed")}
        </Meta>
        <Meta label="Discord">
          <span className="-ml-1.5 flex items-center gap-1">
            <DiscordLink href={row.original_url} label="Open original" text="Original" />
            <DiscordLink href={row.cloned_url} label="Open clone" text="Clone" />
          </span>
        </Meta>
      </dl>

      <h3 className="mt-4 mb-2 text-[11px] font-semibold tracking-[0.4px] text-muted uppercase">Message</h3>
      <div aria-busy={state.kind === "loading" || undefined} aria-live="polite" className="rounded-lg border border-border bg-black/15 p-3">
        {state.kind === "loading" ? (
          <div role="status" aria-label="Loading message">
            {[92, 100, 76, 40].map((w, i) => (
              <Skeleton.Text key={i} block className="mb-1.5 text-[13px] leading-5" width={`${w}%`} />
            ))}
          </div>
        ) : state.kind === "error" ? (
          <div role="alert" className="flex flex-wrap items-center justify-between gap-3">
            <div>
              <p className="m-0 text-[13px] font-medium text-text">{state.error.title}</p>
              <p className="m-0 mt-0.5 text-xs text-muted">
                {state.error.retryAfter && wait > 0 ? `Try again in ${wait} s.` : state.error.hint}
              </p>
            </div>
            <button type="button" className={PREVIEW_BTN} onClick={retry} disabled={wait > 0}>
              Retry
            </button>
          </div>
        ) : (
          <LiveBody data={state.data} />
        )}
      </div>
    </Modal>
  );
}

function Meta({ label, children }: { label: string; children: ReactNode }) {
  return (
    <div className="group min-w-0">
      <dt className={DT}>{label}</dt>
      <dd className={DD}>{children}</dd>
    </div>
  );
}

function LiveBody({ data }: { data: MessageContentResponse }) {
  const m = data.message;
  const empty = !m.content && m.embeds.length === 0 && m.attachments.length === 0;
  return (
    <div className="flex flex-col gap-3">
      <p className="m-0 text-[11px] text-muted">
        {data.source === "clone" ? "Read from the clone" : "Read from the original"}
        {m.edited_at ? ", edited" : ""}
        {data.cached ? ", cached" : ""}
      </p>
      {m.content ? (
        <p data-message-content="" className="m-0 text-[13px] leading-5 break-words whitespace-pre-wrap text-text">
          {m.content}
        </p>
      ) : null}
      {empty ? <p className="m-0 text-[13px] text-muted">This message has no text, embeds or attachments.</p> : null}
      {m.embeds.length ? (
        <div className="flex flex-col gap-2">
          {m.embeds.map((e, i) => (
            <div key={i} className="border-l-2 border-l-border-strong pl-3">
              {embedText(e.author) ? <p className="m-0 text-xs text-muted">{embedText(e.author)}</p> : null}
              {e.title ? (
                e.url ? (
                  <a href={e.url} target="_blank" rel="noopener noreferrer" className="text-[13px] font-semibold text-accent-soft">
                    {e.title}
                  </a>
                ) : (
                  <p className="m-0 text-[13px] font-semibold text-text">{e.title}</p>
                )
              ) : null}
              {e.description ? (
                <p className="m-0 mt-0.5 text-[13px] leading-5 break-words whitespace-pre-wrap text-text-2">{e.description}</p>
              ) : null}
              {e.fields?.length ? (
                <dl className="m-0 mt-1 grid grid-cols-[repeat(auto-fill,minmax(160px,1fr))] gap-2">
                  {e.fields.map((f, j) => (
                    <div key={j} className="min-w-0">
                      <dt className="text-xs font-semibold text-text">{f.name}</dt>
                      <dd className="m-0 text-xs break-words whitespace-pre-wrap text-text-2">{f.value}</dd>
                    </div>
                  ))}
                </dl>
              ) : null}
              {embedText(e.footer) ? <p className="m-0 mt-1 text-[11px] text-muted">{embedText(e.footer)}</p> : null}
            </div>
          ))}
        </div>
      ) : null}
      {m.attachments.length ? (
        <ul className="m-0 flex list-none flex-col gap-1 p-0">
          {m.attachments.map((a, i) => (
            <li key={i} className="flex min-w-0 items-center gap-1.5 text-[13px]">
              <MessagesIcon name="paperclip" size={13} className="shrink-0 text-muted" />
              <a href={a.url} target="_blank" rel="noopener noreferrer" className={cx("truncate text-accent-soft")} title={a.filename}>
                {a.filename}
              </a>
              {a.size ? <span className="shrink-0 text-xs text-muted">{formatSize(a.size)}</span> : null}
            </li>
          ))}
        </ul>
      ) : null}
    </div>
  );
}
