// One run at /tasks/runs/:id, the same page for every task type: the header with its status, number,
// meta line and actions (Cancel while active, Re-run and Delete when finished), then one column: the
// summary card (the counts the type reports as stat tiles, then its Inputs), the Steps card and the
// collapsed "Report (JSON)" panel. Everything type specific comes from the registry in ./types.

import { useState } from "react";
import { Link, useNavigate } from "react-router";
import { Button } from "../../components/ui/Button";
import { ConfirmDialog } from "../../components/ui/ConfirmDialog";
import { EmptyState } from "../../components/ui/EmptyState";
import { api, errorText } from "../../lib/api";
import { ACTIVE_STATUSES } from "../../types/tasks";
import type { TaskRun, TaskType } from "../../types/tasks";
import { ReportPanel, useRunToast } from "./ReportPanel";
import { ContextBadge, formatWhen, runDuration, triggerText, useNow } from "./RunList";
import { runUrl, useTaskRun } from "./runStore";
import { STATUS_LABEL, StatusIcon } from "./StatusIcon";
import { StepList, formatCount } from "./StepList";
import {
  FRAME,
  INPUTS_BOX,
  INPUTS_LIST,
  INPUTS_TITLE,
  RUN_BODY,
  RUN_BREADCRUMB,
  RUN_CRUMB,
  RUN_HEADER,
  RUN_META,
  RUN_TITLE,
  RUN_TITLE_ROW,
  RunDetailSkeleton,
} from "./TasksSkeleton";
import { describeRunInputs, summaryStats } from "./types";

/** The counts as stat tiles over the run's Inputs; the tiles show once the run reports counts. */
export function RunSummary({ run, schema }: { run: TaskRun; schema?: Record<string, unknown> }) {
  const stats = summaryStats(run);
  const rows = describeRunInputs(run, schema);
  return (
    <section aria-label="Summary" className={FRAME}>
      {stats.length ? (
        <dl aria-label="Counts" className="m-0 flex flex-wrap gap-x-8 gap-y-3 border-b border-border px-4 py-3">
          {stats.map((s) => (
            <div key={s.key} className="min-w-0">
              <dt className="text-xs text-muted">{s.label}</dt>
              <dd className="m-0 text-lg font-semibold tabular-nums text-text">{formatCount(s.value)}</dd>
            </div>
          ))}
        </dl>
      ) : null}
      <div className={INPUTS_BOX}>
        <h3 className={INPUTS_TITLE}>Inputs</h3>
        {rows.length ? (
          <dl aria-label="Inputs" className={INPUTS_LIST}>
            {rows.map((r, i) => (
              <div key={`${i}:${r.label}`} className="contents">
                <dt className="text-muted">{r.label}</dt>
                <dd className="m-0 break-words text-text">{r.value}</dd>
              </div>
            ))}
          </dl>
        ) : (
          <p className="m-0 text-sm text-muted">No inputs</p>
        )}
      </div>
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
    <header className={RUN_HEADER}>
      <nav aria-label="Breadcrumb" className={RUN_BREADCRUMB}>
        <Link to="/tasks" className={RUN_CRUMB}>
          All tasks
        </Link>
        {" / "}
        <Link to={`/tasks?type=${encodeURIComponent(run.type)}`} className={RUN_CRUMB}>
          {typeTitle}
        </Link>
      </nav>
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div className="min-w-0">
          <div className={RUN_TITLE_ROW}>
            <StatusIcon status={run.status} size={20} />
            <h2 className={RUN_TITLE} title={run.title}>
              {typeTitle}
            </h2>
            <span className="text-[22px] text-muted">#{run.number}</span>
          </div>
          <div className={RUN_META}>
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

/** The run page body for a held run: header, summary and inputs, steps, report. */
export function RunView({ run, type, refreshing = false }: { run: TaskRun; type?: TaskType; refreshing?: boolean }) {
  return (
    <div className="flex flex-col" aria-busy={refreshing || undefined}>
      <RunHeader run={run} typeTitle={type?.title ?? run.type} />
      <div className={RUN_BODY}>
        <RunSummary run={run} schema={type?.inputs_schema} />
        <StepList run={run} />
        <ReportPanel run={run} />
      </div>
    </div>
  );
}

export function RunDetail({ id, typeOf }: { id: string; typeOf: (key: string) => TaskType | undefined }) {
  // Live over task.run.updated; a refetch keeps the held run on screen (no shimmer after the first load).
  const { run, loading, refreshing, error } = useTaskRun(id);
  if (!run) {
    if (loading) return <RunDetailSkeleton />;
    return <EmptyState title="Run not found" description={error ? errorText(error, "Could not load the run.") : undefined} />;
  }
  return <RunView run={run} type={typeOf(run.type)} refreshing={refreshing} />;
}
