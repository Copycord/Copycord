// Scrape members in the task type registry: its form, inputs rows, summary words, hero count and the
// run page's download card (the saved JSON file, "File no longer on disk" once it was deleted).

import { useEffect, useState } from "react";
import type { InputRow, InputsRun, TaskTypeEntry } from "..";
import { Button } from "../../../../components/ui/Button";
import { ApiError, api } from "../../../../lib/api";
import type { TaskRun } from "../../../../types/tasks";
import { formatCount } from "../../StepList";
import { RUN_CARD } from "../../TasksSkeleton";
import { sizeWords } from "../download_avatars/entry";
import { ScrapeMembersForm } from "./ScrapeMembersForm";

export const GONE = "File no longer on disk";

export interface ScrapeFile {
  filename: string;
  size: number | null;
  members: number | null;
}

/** The saved file's path under scrapes/ from the run report, or null when the run saved none. */
export function scrapePathOf(run: Pick<TaskRun, "report">): string | null {
  const report = run.report && typeof run.report === "object" ? (run.report as { output?: unknown }) : null;
  const output = report?.output && typeof report.output === "object" ? (report.output as { path?: unknown }) : null;
  return typeof output?.path === "string" && output.path ? output.path : null;
}

export function scrapeInfoUrl(runId: string): string {
  return `/api/tasks/scrape-members/${encodeURIComponent(runId)}`;
}

export function scrapeFileUrl(runId: string): string {
  return `${scrapeInfoUrl(runId)}/file`;
}

/** "1 member", "1,204 members". */
export function membersWords(n: number): string {
  return `${formatCount(n)} ${n === 1 ? "member" : "members"}`;
}

type FileState = { kind: "loading" } | { kind: "ok"; file: ScrapeFile } | { kind: "gone" } | { kind: "none" };

export function ScrapeDownloadCard({ run }: { run: TaskRun }) {
  const path = scrapePathOf(run);
  const [state, setState] = useState<FileState>({ kind: "loading" });
  useEffect(() => {
    if (!path) return;
    let live = true;
    setState({ kind: "loading" });
    api
      .get<{ file: ScrapeFile }>(scrapeInfoUrl(run.id))
      .then((res) => live && setState(res?.file ? { kind: "ok", file: res.file } : { kind: "none" }))
      .catch((e: unknown) => {
        if (!live) return;
        setState(e instanceof ApiError && e.status === 404 && e.message === GONE ? { kind: "gone" } : { kind: "none" });
      });
    return () => {
      live = false;
    };
  }, [run.id, path]);
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
    a.href = scrapeFileUrl(run.id);
    a.download = file.filename;
    document.body.append(a);
    a.click();
    a.remove();
  };
  const facts = [file.members != null ? membersWords(file.members) : null, file.size != null ? sizeWords(file.size) : null].filter(Boolean);
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

const INCLUDES: [string, string][] = [
  ["include_username", "Usernames"],
  ["include_avatar_url", "Avatar URLs"],
  ["include_bio", "Bios"],
  ["include_roles", "Roles"],
];

/** "Usernames, Avatar URLs", or "IDs only" when nothing extra was asked for. */
export function includeWords(inputs: Record<string, unknown>): string {
  const on = INCLUDES.filter(([k]) => inputs[k] === true).map(([, w]) => w);
  return on.length ? on.join(", ") : "IDs only";
}

export function describeScrape(inputs: Record<string, unknown>, run?: InputsRun): InputRow[] {
  const id = String(inputs.guild_id ?? "");
  const label = run?.context?.label;
  return [
    { label: "Server", value: label && label !== id ? `${label} (${id})` : id },
    { label: "Include", value: includeWords(inputs) },
    { label: "Proxies", value: inputs.use_proxies === false ? "No" : "Yes" },
  ];
}

const ScrapeIcon = (
  <svg viewBox="0 0 24 24" className="size-4" fill="none" stroke="currentColor" strokeWidth="2" aria-hidden="true">
    <circle cx="9" cy="8" r="3.5" />
    <path d="M3 20c0-3.3 2.7-6 6-6s6 2.7 6 6" />
    <path d="M17 11h4M17 15h4M17 7h4" />
  </svg>
);

export const SCRAPE_MEMBERS_ENTRY: TaskTypeEntry = {
  icon: ScrapeIcon,
  Form: ScrapeMembersForm,
  describeInputs: describeScrape,
  summaryLabels: { members: "Members found", tokens: "Tokens used", tokens_skipped: "Tokens skipped" },
  hero: { key: "members", label: "members found" },
  RunCard: ScrapeDownloadCard,
};
