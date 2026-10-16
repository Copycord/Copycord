// One run at /tasks/runs/:id, the same page for every task type: the header with the status glyph (it alone
// carries the status), title and number, Cancel run while it is active and the "Run actions" menu (Copy run
// ID, Download report, Delete when finished) on one line, the meta line under it ("Manual · admin · 5h ago ·
// 1m 55s · Clone-1 · 0d88ac5a"), then one column: the summary card (the hero count, the segmented step bar,
// the counts in one muted line, then its Inputs collapsed), the Steps card and the collapsed "Report (JSON)"
// panel. Everything type specific comes from the registry in ./types.

import { useEffect, useState } from "react";
import { Link, useNavigate } from "react-router";
import { Badge } from "../../components/ui/Badge";
import { Button } from "../../components/ui/Button";
import { ConfirmDialog } from "../../components/ui/ConfirmDialog";
import { cx } from "../../components/ui/cx";
import { EmptyState } from "../../components/ui/EmptyState";
import { api, errorText } from "../../lib/api";
import { ACTIVE_STATUSES } from "../../types/tasks";
import type { StepState, TaskRun, TaskType } from "../../types/tasks";
import { RowMenu, type MenuItem } from "../channels/actions/RowMenu";
import { SegBar, stepCounts } from "./Outcome";
import { ReportPanel, reportUrl, useRunToast } from "./ReportPanel";
import { formatAgo, formatWhen, runConfirm, runDuration, triggeredText, useNow } from "./RunList";
import { runUrl, useTaskRun } from "./runStore";
import { StatusIcon } from "./StatusIcon";
import { StepList, formatCount } from "./StepList";
import {
  BUSY,
  FADE_IN,
  HERO_LABEL,
  HERO_VALUE,
  INPUT_CHIPS,
  INPUT_CHIPS_MAX,
  INPUTS_LIST,
  InputsDisclosure,
  OVERVIEW_BAR,
  ROW_MAPPING,
  RUN_ACTIONS,
  RUN_BODY,
  RUN_BREADCRUMB,
  RUN_CARD,
  RUN_CRUMB,
  RUN_ERROR,
  RUN_HEADER,
  RUN_HEAD_ROW,
  RUN_NUMBER,
  RUN_TITLE,
  RUN_TITLE_ROW,
  RunDetailSkeleton,
  RunIdValue,
  RunMetaLine,
  RunOverview,
  typeUrl,
} from "./TasksSkeleton";
import { rememberRunTypes, runTypeOf, typeLabel } from "./typeLabels";
import { describeRunInputs, entryFor, heroStat, summaryStats } from "./types";

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

/** Copies the run's full id to the clipboard and says so in a toast; the meta line's copy button and the Run actions menu. */
export function useCopyRunId(id: string): () => Promise<void> {
  const toast = useRunToast();
  return async () => {
    try {
      await navigator.clipboard.writeText(id);
      toast.show("Run ID copied", { kind: "success" });
    } catch {
      toast.show("Could not copy the run ID.", { kind: "error" });
    }
  };
}

/** The meta line under the title, plain values between dots: how and by whom, when (the date in its title), how long, the mapping, the ID. */
export function RunMeta({ run }: { run: TaskRun }) {
  const active = ACTIVE_STATUSES.includes(run.status);
  const now = useNow(run.status === "running");
  const duration = runDuration(run, now);
  const started = run.started_at ?? run.created_at;
  const when = formatWhen(started);
  const mapping = run.context?.label;
  const copyId = useCopyRunId(run.id);
  return (
    <RunMetaLine
      values={{
        trigger: <span className={FADE_IN}>{triggeredText(run)}</span>,
        started: (
          <span data-testid="run-started" title={when ? `Started ${when}` : undefined} className={cx("tabular-nums", FADE_IN)}>
            {when ? formatAgo(started, now) : "-"}
          </span>
        ),
        ran: (
          <span data-testid="run-duration" title={duration == null ? undefined : `${active ? "Running for" : "Ran for"} ${duration}`} className={cx("tabular-nums", FADE_IN)}>
            {duration ?? (active ? "not started" : "-")}
          </span>
        ),
        mapping: mapping ? (
          <span className={cx(ROW_MAPPING, FADE_IN)} title={`Mapping ${mapping}`}>
            {mapping}
          </span>
        ) : null,
        id: <RunIdValue id={run.id} onCopy={() => void copyId()} />,
      }}
    />
  );
}

/** The counts line's words for each step state, in the order it reads them. */
const COUNT_WORDS: [StepState, string][] = [
  ["running", "running"],
  ["queued", "queued"],
  ["success", "done"],
  ["failed", "failed"],
  ["cancelled", "cancelled"],
  ["skipped", "skipped"],
];
const STEP_KEYS = new Set<string>(COUNT_WORDS.map(([state]) => state));

/**
 * "4 done · 1 failed · 1 cancelled": the steps by state (from the run's steps, else its summary's step
 * counts), then any other count the summary reports but the hero; a zero is left out.
 */
export function countsText(run: TaskRun, heroKey?: string): string {
  const stats = summaryStats(run);
  const byState = new Map<string, number>();
  if (run.steps?.length) for (const s of run.steps) byState.set(s.state, (byState.get(s.state) ?? 0) + 1);
  else for (const s of stats) if (STEP_KEYS.has(s.key)) byState.set(s.key, s.value);
  const parts = COUNT_WORDS.filter(([state]) => byState.get(state)).map(([state, word]) => `${formatCount(byState.get(state)!)} ${word}`);
  for (const s of stats) if (!STEP_KEYS.has(s.key) && s.key !== heroKey && s.value) parts.push(`${formatCount(s.value)} ${s.label.toLowerCase()}`);
  return parts.join(" · ");
}

/** The hero count over the segmented bar, the counts in one muted line under it, then the run's Inputs collapsed. */
export function RunSummary({ run, schema }: { run: TaskRun; schema?: Record<string, unknown> }) {
  const hero = heroStat(run);
  const counts = stepCounts(run);
  const rows = describeRunInputs(run, schema);
  const line = countsText(run, hero?.key);
  return (
    <section aria-label="Summary" className={RUN_CARD}>
      <RunOverview
        hero={
          hero ? (
            <>
              <span className={cx(HERO_VALUE, FADE_IN)}>{formatCount(hero.value)}</span>
              <span className={cx(HERO_LABEL, FADE_IN)}>{hero.label}</span>
            </>
          ) : null
        }
        bar={
          counts ? (
            <SegBar counts={counts} className={cx(OVERVIEW_BAR, FADE_IN)} />
          ) : (
            <div aria-hidden="true" className={cx(OVERVIEW_BAR, "rounded-full bg-line-1")} />
          )
        }
        counts={line ? <span className={FADE_IN}>{line}</span> : null}
      />
      <InputsDisclosure>
        {rows.length ? (
          <dl aria-label="Inputs" className={INPUTS_LIST}>
            {rows.map((r, i) => (
              <div key={`${i}:${r.label}`} className="contents">
                <dt className="text-muted">{r.label}</dt>
                <dd className="m-0 min-w-0 break-words text-text">{r.chips?.length ? <InputChips chips={r.chips} title={r.value} /> : r.value}</dd>
              </div>
            ))}
          </dl>
        ) : (
          <p className="m-0 text-sm text-muted">No inputs</p>
        )}
      </InputsDisclosure>
    </section>
  );
}

type Ask = "cancel" | "delete" | null;

/** The header's three-dot menu: Copy run ID, Download report, Delete once finished. Cancel run is its own button. */
export function runMenuItems(run: TaskRun, on: { copyId: () => void; download: () => void; ask: (a: "cancel" | "delete") => void }): MenuItem[] {
  const items: MenuItem[] = [{ act: "copy-id", label: "Copy run ID", onSelect: on.copyId }];
  if (run.report != null) items.push({ act: "download", label: "Download report", onSelect: on.download });
  if (!ACTIVE_STATUSES.includes(run.status)) items.push({ act: "delete", label: "Delete", danger: true, separator: true, onSelect: () => on.ask("delete") });
  return items;
}

export function RunHeader({ run, typeTitle }: { run: TaskRun; typeTitle: string }) {
  const navigate = useNavigate();
  const toast = useRunToast();
  const [ask, setAsk] = useState<Ask>(null);
  const [busy, setBusy] = useState(false);
  const name = `${typeTitle} #${run.number}`;
  const active = ACTIVE_STATUSES.includes(run.status);

  const copyId = useCopyRunId(run.id);

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
        {/* The type's title is a label the skeleton already showed; the run's own parts fade in. The glyph alone
            carries the status, its word in its title and label. */}
        <div className={RUN_TITLE_ROW}>
          <StatusIcon status={run.status} size={20} testId="run-status" className={FADE_IN} />
          <h2 className={RUN_TITLE} title={run.title}>
            {typeTitle}
          </h2>
          <span className={cx(RUN_NUMBER, FADE_IN)}>#{run.number}</span>
          {/* On the title line, not a line of its own, so the page stands as tall as its skeleton. */}
          {run.status === "failed" && run.error ? (
            <span data-testid="run-error" title={run.error} className={cx(RUN_ERROR, FADE_IN)}>
              {run.error}
            </span>
          ) : null}
        </div>
        <div className={cx(RUN_ACTIONS, busy && BUSY)} aria-busy={busy || undefined}>
          {active ? (
            <Button variant="ghost-red" size="sm" disabled={busy} onClick={() => setAsk("cancel")} className={FADE_IN}>
              Cancel run
            </Button>
          ) : null}
          <RowMenu
            kind="channel"
            label="Run actions"
            disabled={busy}
            items={runMenuItems(run, { copyId: () => void copyId(), download, ask: setAsk })}
          />
        </div>
      </div>
      <RunMeta run={run} />
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
