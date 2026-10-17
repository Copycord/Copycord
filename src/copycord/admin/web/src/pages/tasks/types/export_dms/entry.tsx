// Export DMs in the task type registry: its form, inputs rows, summary words, hero count and the
// run page's download card (the saved JSON file, "File no longer on disk" once it was deleted).

import { useEffect, useState } from "react";
import type { InputRow, TaskTypeEntry } from "..";
import { Button } from "../../../../components/ui/Button";
import { ApiError, api } from "../../../../lib/api";
import type { TaskRun } from "../../../../types/tasks";
import { formatCount } from "../../StepList";
import { RUN_CARD } from "../../TasksSkeleton";
import { sizeWords } from "../download_avatars/entry";
import { ExportDmsForm } from "./ExportDmsForm";

export const GONE = "File no longer on disk";

export interface ExportFile {
  filename: string;
  size: number | null;
  messages: number | null;
}

/** The saved file's path from the run report, or null when the run saved none. */
export function savedPathOf(run: Pick<TaskRun, "report">): string | null {
  const report = run.report && typeof run.report === "object" ? (run.report as { output?: unknown }) : null;
  const output = report?.output && typeof report.output === "object" ? (report.output as { json_path?: unknown }) : null;
  return typeof output?.json_path === "string" && output.json_path ? output.json_path : null;
}

const DMS_BASE = "/api/tasks/export-dms";

export function fileUrl(runId: string, base = DMS_BASE): string {
  return `${base}/${encodeURIComponent(runId)}/file`;
}

type FileState = { kind: "loading" } | { kind: "ok"; file: ExportFile } | { kind: "gone" } | { kind: "none" };

/** The saved JSON file's card on the run page, read from the type's file route under base. */
export function ExportFileCard({ run, base }: { run: TaskRun; base: string }) {
  const path = savedPathOf(run);
  const [state, setState] = useState<FileState>({ kind: "loading" });
  useEffect(() => {
    if (!path) return;
    let live = true;
    setState({ kind: "loading" });
    api
      .get<{ file: ExportFile }>(`${base}/${encodeURIComponent(run.id)}`)
      .then((res) => live && setState(res?.file ? { kind: "ok", file: res.file } : { kind: "none" }))
      .catch((e: unknown) => {
        if (!live) return;
        setState(e instanceof ApiError && e.status === 404 && e.message === GONE ? { kind: "gone" } : { kind: "none" });
      });
    return () => {
      live = false;
    };
  }, [run.id, path, base]);
  if (!path || state.kind === "loading" || state.kind === "none") return null;
  if (state.kind === "gone") {
    return (
      <section aria-label="Download" className={RUN_CARD}>
        <div className="flex flex-wrap items-center justify-between gap-3 px-4 py-3">
          <span className="text-sm text-text">JSON file</span>
          <p className="m-0 text-sm text-muted">{GONE}</p>
        </div>
      </section>
    );
  }
  const { file } = state;
  const save = () => {
    const a = document.createElement("a");
    a.href = fileUrl(run.id, base);
    a.download = file.filename;
    document.body.append(a);
    a.click();
    a.remove();
  };
  const facts = [
    file.messages != null ? `${formatCount(file.messages)} ${file.messages === 1 ? "message" : "messages"}` : null,
    file.size != null ? sizeWords(file.size) : null,
  ].filter(Boolean);
  return (
    <section aria-label="Download" className={RUN_CARD}>
      <div className="flex flex-wrap items-center justify-between gap-3 px-4 py-3">
        <div className="flex min-w-0 flex-col gap-0.5">
          <span className="truncate text-sm text-text">{file.filename}</span>
          {facts.length ? <span className="text-xs text-muted">{facts.join(", ")}</span> : null}
        </div>
        <Button variant="primary" onClick={save}>
          Download JSON
        </Button>
      </div>
    </section>
  );
}

export function ExportDmsDownloadCard({ run }: { run: TaskRun }) {
  return <ExportFileCard run={run} base={DMS_BASE} />;
}

/** The webhook without its token: "https://discord.com/api/webhooks/123/...". */
export function maskWebhook(url: unknown): string {
  if (typeof url !== "string" || !url) return "None";
  const cut = url.replace(/\/+$/, "").lastIndexOf("/");
  const head = cut > 0 ? url.slice(0, cut) : "";
  return (head.match(/\//g)?.length ?? 0) >= 3 ? `${head}/...` : url;
}

export function describeExportDms(inputs: Record<string, unknown>): InputRow[] {
  return [
    { label: "User ID", value: String(inputs.user_id ?? "") },
    { label: "JSON file", value: inputs.json_file === false ? "No" : "Yes" },
    { label: "Webhook", value: maskWebhook(inputs.webhook_url) },
  ];
}

const ExportDmsIcon = (
  <svg viewBox="0 0 24 24" className="size-4" fill="none" stroke="currentColor" strokeWidth="2" aria-hidden="true">
    <path d="M4 5h16v11H8l-4 4z" />
    <path d="M12 8v5M9.5 10.5 12 13l2.5-2.5" />
  </svg>
);

export const EXPORT_DMS_ENTRY: TaskTypeEntry = {
  icon: ExportDmsIcon,
  Form: ExportDmsForm,
  describeInputs: describeExportDms,
  summaryLabels: { exported: "Messages exported", forwarded: "Messages forwarded", failed: "Messages failed" },
  hero: { key: "exported", label: "messages exported" },
  RunCard: ExportDmsDownloadCard,
};
