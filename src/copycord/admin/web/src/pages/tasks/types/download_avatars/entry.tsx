// Download avatars in the task type registry: its form, inputs rows, summary words, hero count and the
// run page's download card (the zip link with its expiry, "Link expired" once past it).

import { useEffect, useState } from "react";
import type { InputRow, InputsRun, TaskTypeEntry } from "..";
import { Button } from "../../../../components/ui/Button";
import type { TaskRun } from "../../../../types/tasks";
import { formatCount } from "../../StepList";
import { formatWhen } from "../../RunList";
import { RUN_CARD } from "../../TasksSkeleton";
import { AvatarsForm } from "./AvatarsForm";

export interface AvatarDownload {
  url: string;
  filename: string;
  files: number;
  size: number;
  expires_at: string;
}

/** The run report's download, or null when the run made none. */
export function downloadOf(run: Pick<TaskRun, "report">): AvatarDownload | null {
  const d = run.report && typeof run.report === "object" ? (run.report as { download?: unknown }).download : null;
  if (!d || typeof d !== "object") return null;
  const v = d as Partial<AvatarDownload>;
  return typeof v.url === "string" && typeof v.expires_at === "string" ? (v as AvatarDownload) : null;
}

/** "12.3 MB", "512 bytes". */
export function sizeWords(n: number): string {
  let size = n;
  for (const unit of ["bytes", "KB", "MB"]) {
    if (size < 1024) return unit === "bytes" ? `${Math.round(size)} bytes` : `${size.toFixed(1)} ${unit}`;
    size /= 1024;
  }
  return `${size.toFixed(1)} GB`;
}

export function hoursWords(n: unknown): string {
  const h = typeof n === "number" ? n : Number(n);
  return Number.isFinite(h) ? `${formatCount(h)} ${h === 1 ? "hour" : "hours"}` : String(n ?? "");
}

export function describeAvatars(inputs: Record<string, unknown>, run?: InputsRun): InputRow[] {
  return [
    { label: "Server", value: run?.context?.label || String(inputs.guild_name || inputs.guild_id || "") },
    { label: "Link expires after", value: hoursWords(inputs.expires_hours) },
  ];
}

/** True from the expiry on; it flips by itself when the time comes while the page is open. */
function useExpired(expiresAt: string): boolean {
  const at = new Date(expiresAt).getTime();
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    const left = at - Date.now();
    if (!Number.isFinite(left) || left <= 0) return;
    // setTimeout holds at most about 24.8 days; a longer wait just checks again then.
    const t = window.setTimeout(() => setNow(Date.now()), Math.min(left + 50, 2 ** 31 - 1));
    return () => window.clearTimeout(t);
  }, [at, now]);
  return !Number.isFinite(at) || at <= now;
}

export function AvatarsDownloadCard({ run }: { run: TaskRun }) {
  const download = downloadOf(run);
  if (!download) return null;
  return <DownloadCard download={download} />;
}

function DownloadCard({ download }: { download: AvatarDownload }) {
  const expired = useExpired(download.expires_at);
  const save = () => {
    const a = document.createElement("a");
    a.href = download.url;
    a.download = download.filename;
    document.body.append(a);
    a.click();
    a.remove();
  };
  return (
    <section aria-label="Download" className={RUN_CARD}>
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div className="flex min-w-0 flex-col gap-0.5">
          <span className="truncate text-sm text-text">{download.filename}</span>
          <span className="text-xs text-muted">
            {formatCount(download.files)} {download.files === 1 ? "avatar" : "avatars"}, {sizeWords(download.size)}
          </span>
        </div>
        {expired ? (
          <p className="m-0 text-sm text-muted">Link expired {formatWhen(download.expires_at)}</p>
        ) : (
          <div className="flex flex-wrap items-center gap-3">
            <span className="text-xs text-muted">Expires {formatWhen(download.expires_at)}</span>
            <Button variant="primary" onClick={save}>
              Download zip
            </Button>
          </div>
        )}
      </div>
    </section>
  );
}

const AvatarsIcon = (
  <svg viewBox="0 0 24 24" className="size-4" fill="none" stroke="currentColor" strokeWidth="2" aria-hidden="true">
    <circle cx="12" cy="8" r="4" />
    <path d="M4 21c1.5-4 4.5-6 8-6s6.5 2 8 6" />
  </svg>
);

export const DOWNLOAD_AVATARS_ENTRY: TaskTypeEntry = {
  icon: AvatarsIcon,
  Form: AvatarsForm,
  describeInputs: describeAvatars,
  summaryLabels: { members: "Members found", downloaded: "Avatars downloaded", failed: "Avatars failed", skipped: "Avatars skipped" },
  hero: { key: "downloaded", label: "avatars downloaded" },
  RunCard: AvatarsDownloadCard,
};
