// One run at /tasks/runs/:id: the header with its status, number, meta line and actions (Cancel while
// active, Re-run and Delete when finished), the Summary and step list on the left, and the Inputs
// card, the log and the "Report (JSON)" panel in the main column. Clicking a step filters the log.

import { useState } from "react";
import type { ReactNode } from "react";
import { Link, useNavigate } from "react-router";
import { Button } from "../../components/ui/Button";
import { ConfirmDialog } from "../../components/ui/ConfirmDialog";
import { EmptyState } from "../../components/ui/EmptyState";
import { Skeleton } from "../../components/ui/Skeleton";
import { api, errorText } from "../../lib/api";
import { ACTIVE_STATUSES } from "../../types/tasks";
import type { TaskRun } from "../../types/tasks";
import { LogView } from "./LogView";
import { ReportPanel, useRunToast } from "./ReportPanel";
import { ContextBadge, formatWhen, runDuration, triggerText, useNow } from "./RunList";
import { runUrl, useTaskRun } from "./runStore";
import { STATUS_LABEL, StatusIcon } from "./StatusIcon";
import { StepList, formatCount } from "./StepList";
import { FRAME } from "./TasksSkeleton";

export function RunSection({ title, children }: { title: string; children: ReactNode }) {
  return (
    <section aria-label={title} className={FRAME}>
      <h3 className="m-0 border-b border-border px-4 py-2.5 text-sm font-semibold text-text">{title}</h3>
      <div className="px-4 py-3 text-sm text-text-2">{children}</div>
    </section>
  );
}

const DAY = new Intl.DateTimeFormat(undefined, { month: "short", day: "numeric", year: "numeric" });

function day(v: unknown): string {
  if (typeof v !== "string" || !v) return "?";
  const d = new Date(v);
  return Number.isNaN(d.getTime()) ? v : DAY.format(d);
}

/** A backfill range in words: "All history", "Since Oct 3, 2026", "Oct 1, 2026 to Oct 3, 2026", "Last 500 messages". */
export function rangeWords(range: unknown): string {
  if (!range || typeof range !== "object") return String(range ?? "");
  const r = range as { mode?: string; since?: unknown; until?: unknown; last_n?: unknown };
  switch (r.mode) {
    case "all":
      return "All history";
    case "since":
      return `Since ${day(r.since)}`;
    case "between":
      return `${day(r.since)} to ${day(r.until)}`;
    case "last_n":
      return `Last ${typeof r.last_n === "number" ? formatCount(r.last_n) : String(r.last_n)} messages`;
    default:
      return JSON.stringify(range);
  }
}

function plural(n: number, one: string): string {
  return `${formatCount(n)} ${one}${n === 1 ? "" : "s"}`;
}

function rawValue(v: unknown): string {
  if (typeof v === "boolean") return v ? "Yes" : "No";
  if (typeof v === "string" || typeof v === "number") return String(v);
  if (v == null) return "";
  return JSON.stringify(v);
}

const MODE: Record<string, string> = { resume: "Resume previous", new: "Start new" };

/** The run's inputs as label and value rows; keys this does not know fall back to the raw key and value. */
export function inputRows(run: Pick<TaskRun, "inputs" | "context">): [string, string][] {
  const rows: [string, string][] = [];
  for (const [k, v] of Object.entries(run.inputs ?? {})) {
    switch (k) {
      case "mapping_id":
        rows.push(["Mapping", run.context?.label || rawValue(v)]);
        break;
      case "channel_ids":
        rows.push(["Channels", Array.isArray(v) ? plural(v.length, "channel") : rawValue(v)]);
        break;
      case "category_ids":
        rows.push(["Categories", Array.isArray(v) ? `${formatCount(v.length)} ${v.length === 1 ? "category" : "categories"}` : rawValue(v)]);
        break;
      case "range":
        rows.push(["Range", rangeWords(v)]);
        break;
      case "ignore_cloned":
        rows.push(["Ignore cloned", rawValue(v)]);
        break;
      case "mode":
        rows.push(["Mode", (typeof v === "string" && MODE[v]) || rawValue(v)]);
        break;
      default:
        rows.push([k, rawValue(v)]);
    }
  }
  return rows;
}

export function InputsCard({ run }: { run: TaskRun }) {
  const rows = inputRows(run);
  return (
    <section aria-label="Inputs" className={FRAME}>
      <h3 className="m-0 border-b border-border px-4 py-2.5 text-sm font-semibold text-text">Inputs</h3>
      {rows.length ? (
        <dl className="m-0 grid grid-cols-[minmax(110px,auto)_1fr] gap-x-4 gap-y-1 px-4 py-3 text-sm">
          {rows.map(([k, v]) => (
            <div key={k} className="contents">
              <dt className="text-muted">{k}</dt>
              <dd className="m-0 break-words text-text">{v}</dd>
            </div>
          ))}
        </dl>
      ) : (
        <p className="m-0 px-4 py-3 text-sm text-muted">No inputs</p>
      )}
    </section>
  );
}

type Ask = "cancel" | "delete" | null;

export function RunHeader({ run, typeTitle }: { run: TaskRun; typeTitle: string }) {
  const now = useNow(run.status === "running");
  const duration = runDuration(run, now);
  const active = ACTIVE_STATUSES.includes(run.status);
  const navigate = useNavigate();
  const toast = useRunToast();
  const [ask, setAsk] = useState<Ask>(null);
  const [busy, setBusy] = useState(false);
  const name = `${typeTitle} #${run.number}`;

  const onConfirm = async (ok: boolean) => {
    const what = ask;
    setAsk(null);
    if (!ok || !what) return;
    setBusy(true);
    try {
      if (what === "cancel") {
        await api.post(`${runUrl(run.id)}/cancel`);
        toast.show(`Cancelling ${name}`, { kind: "success" });
      } else {
        await api.del(runUrl(run.id));
        toast.show(`Deleted ${name}`, { kind: "success" });
        navigate("/tasks");
      }
    } catch (e) {
      toast.show(errorText(e, what === "cancel" ? "Could not cancel the run." : "Could not delete the run."), { kind: "error" });
    } finally {
      setBusy(false);
    }
  };

  return (
    <header className="mb-4">
      <nav aria-label="Breadcrumb" className="mb-2 text-sm text-muted">
        <Link to="/tasks" className="text-accent no-underline hover:underline">
          All tasks
        </Link>
        {" / "}
        <Link to={`/tasks?type=${encodeURIComponent(run.type)}`} className="text-accent no-underline hover:underline">
          {typeTitle}
        </Link>
      </nav>
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div className="min-w-0">
          <div className="flex items-center gap-2">
            <StatusIcon status={run.status} size={20} />
            <h2 className="m-0 text-[22px] font-bold text-text">{run.title}</h2>
            <span className="text-[22px] text-muted">#{run.number}</span>
          </div>
          <div className="mt-1 flex flex-wrap items-center gap-x-3 gap-y-1 text-sm text-muted">
            <span>{triggerText(run)}</span>
            <ContextBadge run={run} />
            <span title="Started">{formatWhen(run.started_at ?? run.created_at)}</span>
            {duration ? <span title="Duration">{duration}</span> : null}
            <span data-testid="run-status">{STATUS_LABEL[run.status]}</span>
          </div>
        </div>
        <div className="flex shrink-0 gap-2">
          {active ? (
            <Button variant="ghost-red" size="sm" loading={busy} disabled={busy} onClick={() => setAsk("cancel")}>
              Cancel run
            </Button>
          ) : (
            <>
              <Button
                variant="ghost"
                size="sm"
                onClick={() => navigate(`/tasks?type=${encodeURIComponent(run.type)}&rerun=${encodeURIComponent(run.id)}`)}
              >
                Re-run
              </Button>
              <Button variant="ghost-red" size="sm" loading={busy} disabled={busy} onClick={() => setAsk("delete")}>
                Delete
              </Button>
            </>
          )}
        </div>
      </div>
      {run.status === "failed" && run.error ? <p className="mt-2 text-sm text-danger">{run.error}</p> : null}
      <ConfirmDialog
        open={ask !== null}
        title={ask === "delete" ? `Delete ${name}?` : `Cancel ${name}?`}
        body={ask === "delete" ? "The run, its log and its report are removed. This cannot be undone." : "Running steps stop at the next page; queued steps are dropped."}
        confirmLabel={ask === "delete" ? "Delete" : "Cancel run"}
        cancelLabel={ask === "delete" ? "Keep" : "Keep running"}
        confirmVariant="ghost-red"
        onResult={(ok) => void onConfirm(ok)}
      />
    </header>
  );
}

export function RunDetail({ id, typeTitle }: { id: string; typeTitle: (key: string) => string }) {
  const { run, loading, error } = useTaskRun(id);
  const [step, setStep] = useState<string | null>(null);
  if (!run) {
    if (loading) return <Skeleton.Block height={120} />;
    return <EmptyState title="Run not found" description={error ? errorText(error, "Could not load the run.") : undefined} />;
  }
  return (
    <div className="flex flex-col">
      <RunHeader run={run} typeTitle={typeTitle(run.type)} />
      <div className="grid grid-cols-[260px_minmax(0,1fr)] gap-4 max-md:grid-cols-1">
        <StepList run={run} selected={step} onSelect={setStep} />
        <div className="flex min-w-0 flex-col gap-4">
          <InputsCard run={run} />
          <LogView run={run} step={step} onClearStep={() => setStep(null)} />
          <ReportPanel run={run} />
        </div>
      </div>
    </div>
  );
}
