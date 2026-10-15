// One run at /tasks/runs/:id, the same page for every task type: the header with its status glyph, number,
// status pill, the "Run actions" menu (Copy run ID, Download report, Cancel while active, Delete when
// finished) and a row of display chips (mapping, trigger, actor, started, duration, run ID), then one column: the
// summary card (the counts the type reports as stat tiles, then its Inputs), the Steps card and the
// collapsed "Report (JSON)" panel. Everything type specific comes from the registry in ./types.

import { useEffect, useState } from "react";
import { Link, useNavigate } from "react-router";
import { Badge } from "../../components/ui/Badge";
import { ConfirmDialog } from "../../components/ui/ConfirmDialog";
import { cx } from "../../components/ui/cx";
import { EmptyState } from "../../components/ui/EmptyState";
import { useLastLabels } from "../../components/ui/lastCount";
import { api, errorText } from "../../lib/api";
import { ACTIVE_STATUSES } from "../../types/tasks";
import type { TaskRun, TaskType } from "../../types/tasks";
import { RowMenu, type MenuItem } from "../channels/actions/RowMenu";
import { ReportPanel, reportUrl, useRunToast } from "./ReportPanel";
import { ContextBadge, formatAgo, formatWhen, runConfirm, runDuration, triggerLabel, useNow } from "./RunList";
import { runUrl, useTaskRun } from "./runStore";
import { RunStatusPill, StatusIcon } from "./StatusIcon";
import { StepList, formatCount } from "./StepList";
import {
  BUSY,
  FADE_IN,
  INPUT_CHIPS,
  INPUT_CHIPS_MAX,
  INPUTS_BOX,
  INPUTS_LABELS_KEY,
  INPUTS_LIST,
  INPUTS_TITLE,
  MetaChip,
  RUN_BODY,
  RUN_BREADCRUMB,
  RUN_CARD,
  RUN_CRUMB,
  RUN_ERROR,
  RUN_HEADER,
  RUN_HEAD_MAIN,
  RUN_HEAD_ROW,
  RUN_META,
  RUN_NUMBER,
  RUN_PILL,
  RUN_TITLE,
  RUN_TITLE_ROW,
  RunDetailSkeleton,
  RunIdChip,
  STATS_LABELS_KEY,
  STATS_LIST,
  STAT_LABEL,
  STAT_TILE,
  STAT_VALUE,
  SummaryHead,
  typeUrl,
} from "./TasksSkeleton";
import { rememberRunTypes, runTypeOf, typeLabel } from "./typeLabels";
import { describeRunInputs, entryFor, summaryStats } from "./types";

/** A list input as small display chips on one line, the first few and "+N more"; the count in the title. */
export function InputChips({ chips, title }: { chips: string[]; title: string }) {
  const more = chips.length - INPUT_CHIPS_MAX;
  return (
    <span className={INPUT_CHIPS} title={title}>
      {chips.slice(0, INPUT_CHIPS_MAX).map((c, i) => (
        <Badge key={`${i}:${c}`} size="sm" className="shrink-0 font-normal">
          {c}
        </Badge>
      ))}
      {more > 0 ? <span className="shrink-0 text-xs text-muted">+{formatCount(more)} more</span> : null}
    </span>
  );
}

/** The counts as stat tiles over the run's Inputs; the tiles show once the run reports counts. */
export function RunSummary({ run, schema }: { run: TaskRun; schema?: Record<string, unknown> }) {
  const stats = summaryStats(run);
  const rows = describeRunInputs(run, schema);
  // The skeleton of the next run page prints these labels and stands as tall.
  useLastLabels(STATS_LABELS_KEY, stats.map((s) => s.label));
  useLastLabels(INPUTS_LABELS_KEY, rows.map((r) => r.label));
  return (
    <section aria-label="Summary" className={RUN_CARD}>
      <SummaryHead />
      {stats.length ? (
        <dl aria-label="Counts" className={cx(STATS_LIST, FADE_IN)}>
          {stats.map((s) => (
            <div key={s.key} className={STAT_TILE}>
              <dt className={STAT_LABEL}>{s.label}</dt>
              <dd className={STAT_VALUE}>{formatCount(s.value)}</dd>
            </div>
          ))}
        </dl>
      ) : null}
      <div className={INPUTS_BOX}>
        <h3 className={INPUTS_TITLE}>Inputs</h3>
        {rows.length ? (
          <dl aria-label="Inputs" className={cx(INPUTS_LIST, FADE_IN)}>
            {rows.map((r, i) => (
              <div key={`${i}:${r.label}`} className="contents">
                <dt className="text-muted">{r.label}</dt>
                <dd className="m-0 min-w-0 break-words text-text">{r.chips?.length ? <InputChips chips={r.chips} title={r.value} /> : r.value}</dd>
              </div>
            ))}
          </dl>
        ) : (
          <p className={cx("m-0 text-sm text-muted", FADE_IN)}>No inputs</p>
        )}
      </div>
    </section>
  );
}

type Ask = "cancel" | "delete" | null;

/** The header's three-dot menu: Copy run ID, Download report, Cancel run while active, Delete once finished. */
export function runMenuItems(run: TaskRun, on: { copyId: () => void; download: () => void; ask: (a: "cancel" | "delete") => void }): MenuItem[] {
  const items: MenuItem[] = [{ act: "copy-id", label: "Copy run ID", onSelect: on.copyId }];
  if (run.report != null) items.push({ act: "download", label: "Download report", onSelect: on.download });
  if (ACTIVE_STATUSES.includes(run.status)) items.push({ act: "cancel", label: "Cancel run", onSelect: () => on.ask("cancel") });
  else items.push({ act: "delete", label: "Delete", danger: true, separator: true, onSelect: () => on.ask("delete") });
  return items;
}

export function RunHeader({ run, typeTitle }: { run: TaskRun; typeTitle: string }) {
  const now = useNow(run.status === "running");
  const duration = runDuration(run, now);
  const started = run.started_at ?? run.created_at;
  const navigate = useNavigate();
  const toast = useRunToast();
  const [ask, setAsk] = useState<Ask>(null);
  const [busy, setBusy] = useState(false);
  const name = `${typeTitle} #${run.number}`;

  const copyId = async () => {
    try {
      await navigator.clipboard.writeText(run.id);
      toast.show("Run ID copied", { kind: "success" });
    } catch {
      toast.show("Could not copy the run ID.", { kind: "error" });
    }
  };

  const download = () => {
    const a = document.createElement("a");
    a.href = reportUrl(run.id);
    a.download = "";
    a.click();
  };

  const onConfirm = async (ok: boolean) => {
    const what = ask;
    setAsk(null);
    if (!ok || !what) return;
    setBusy(true);
    try {
      // The same toasts as the list's menu.
      if (what === "cancel") {
        await api.post(`${runUrl(run.id)}/cancel`);
        toast.show(`Cancelling ${name}`, { kind: "info" });
      } else {
        await api.del(runUrl(run.id));
        toast.show(`Deleted ${name}`, { kind: "success" });
        navigate("/tasks");
      }
    } catch (e) {
      toast.show(errorText(e, what === "cancel" ? `Could not cancel ${name}` : `Could not delete ${name}`), { kind: "error" });
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
        <Link to={typeUrl(run.type)} className={RUN_CRUMB}>
          {typeTitle}
        </Link>
      </nav>
      <div className={RUN_HEAD_ROW}>
        {/* The type's title is a label the skeleton already showed; the run's own parts fade in. */}
        <div className={RUN_HEAD_MAIN}>
          <StatusIcon status={run.status} size={20} className={FADE_IN} />
          <div className={RUN_TITLE_ROW}>
            <h2 className={RUN_TITLE} title={run.title}>
              {typeTitle}
            </h2>
            <span className={cx(RUN_NUMBER, FADE_IN)}>#{run.number}</span>
            <RunStatusPill status={run.status} testId="run-status" className={cx(RUN_PILL, FADE_IN)} />
            {/* On the title line, not a line of its own, so the page stands as tall as its skeleton. */}
            {run.status === "failed" && run.error ? (
              <span data-testid="run-error" title={run.error} className={cx(RUN_ERROR, FADE_IN)}>
                {run.error}
              </span>
            ) : null}
          </div>
        </div>
        <div className={cx("shrink-0", busy && BUSY)} aria-busy={busy || undefined}>
          <RowMenu
            kind="channel"
            label="Run actions"
            disabled={busy}
            items={runMenuItems(run, { copyId: () => void copyId(), download, ask: setAsk })}
          />
        </div>
      </div>
      <div className={cx(RUN_META, FADE_IN)}>
        <ContextBadge run={run} title="Mapping" />
        <MetaChip glyph="trigger" title="Trigger">
          {triggerLabel(run)}
        </MetaChip>
        <MetaChip glyph="actor" title="Started by">
          {run.actor}
        </MetaChip>
        <MetaChip glyph="started" testId="run-started" title={`Started ${formatWhen(started)}`}>
          {formatAgo(started, now)}
        </MetaChip>
        {duration ? (
          <MetaChip glyph="duration" testId="run-duration" title="Duration">
            {duration}
          </MetaChip>
        ) : null}
        <RunIdChip id={run.id} onCopy={() => void copyId()} />
      </div>
      <ConfirmDialog open={ask !== null} {...runConfirm(ask ?? "cancel", name)} onResult={(ok) => void onConfirm(ok)} />
    </header>
  );
}

/** The run page body for a held run: header, summary and inputs, steps, report. */
export function RunView({ run, type, refreshing = false }: { run: TaskRun; type?: TaskType; refreshing?: boolean }) {
  return (
    <div className={cx("flex flex-col transition-opacity motion-reduce:transition-none", refreshing && BUSY)} aria-busy={refreshing || undefined}>
      <RunHeader run={run} typeTitle={type?.title ?? typeLabel(run.type).title} />
      <div className={RUN_BODY}>
        <RunSummary run={run} schema={type?.inputs_schema} />
        <StepList run={run} noteOf={entryFor(run.type).stepNote} />
        <ReportPanel run={run} />
      </div>
    </div>
  );
}

export function RunDetail({ id, typeOf }: { id: string; typeOf: (key: string) => TaskType | undefined }) {
  // Live over task.run.updated; a refetch keeps the held run on screen (no shimmer after the first load).
  const { run, loading, refreshing, error } = useTaskRun(id);
  useEffect(() => {
    if (run) rememberRunTypes([run]);
  }, [run]);
  if (!run) {
    if (loading) return <RunDetailSkeleton runType={runTypeOf(id)} />;
    return <EmptyState plain title="Run not found" description={error ? errorText(error, "Could not load the run.") : undefined} />;
  }
  return <RunView run={run} type={typeOf(run.type)} refreshing={refreshing} />;
}
