// Tasks: every long job the app runs, in the layout of GitHub Actions "All workflows". The rail picks a
// task type (?type=), the list shows its runs live, and /tasks/runs/:id opens one run.

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useNavigate, useParams, useSearchParams } from "react-router";
import { useToast } from "../components/Toast";
import { Button } from "../components/ui/Button";
import { useConfirm } from "../components/ui/ConfirmDialog";
import { EmptyState } from "../components/ui/EmptyState";
import { useLastCount } from "../components/ui/lastCount";
import { useApi } from "../hooks/useApi";
import { ApiError, api, errorText } from "../lib/api";
import type { TaskRun, TaskRunResponse, TaskType, TaskTypesResponse } from "../types/tasks";
import { RunDetail } from "./tasks/RunDetail";
import { RunFilterSelects, RunSearch, useRunFilters } from "./tasks/RunFilters";
import { RunList, RunMenu, runConfirm, type RunAction } from "./tasks/RunList";
import { PER_PAGE, runUrl, useTaskRuns } from "./tasks/runStore";
import {
  ALL_TASKS_DESCRIPTION,
  MAIN,
  PAGE,
  RUNS_COUNT_KEY,
  RUNS_EMPTY,
  RUN_PAGES_KEY,
  RunPager,
  RunsEmptyIcon,
  StatusTabs,
  TasksHeader,
  TasksHeading,
  readPagerShown,
} from "./tasks/TasksSkeleton";
import { TypeRail } from "./tasks/TypeRail";
import { rememberRunTypes, rememberTypes, typeLabel } from "./tasks/typeLabels";
import { useBotsRunning } from "./tasks/useBotsRunning";
import { TaskTypeForm } from "./tasks/types";

export interface RunTaskButtonProps {
  type: TaskType;
  onRun: (type: TaskType) => void;
  /** For a requires_bots type: whether both bots run (null while the first status check is out). */
  botsUp?: boolean | null;
}

export const BOTS_DOWN = "Start the bots first";

/**
 * "Run task" on a type page opens that type's form. All tasks has none: the rail picks the type first.
 * A type that needs the bots keeps the button off, with the reason beside it, until both bots run.
 */
export function RunTaskButton({ type, onRun, botsUp = null }: RunTaskButtonProps) {
  const blocked = Boolean(type.requires_bots) && botsUp !== true;
  const down = blocked && botsUp === false;
  return (
    <div className="flex shrink-0 items-center gap-2.5">
      {down ? <span className="text-sm text-muted">{BOTS_DOWN}</span> : null}
      <Button variant="primary" disabled={blocked} title={down ? BOTS_DOWN : undefined} onClick={() => onRun(type)}>
        Run task
      </Button>
    </div>
  );
}

interface OpenForm {
  type: TaskType;
  /** Remounts the form so each open starts from a clean state. */
  nonce: number;
}

export function Tasks() {
  const { id } = useParams<{ id: string }>();
  const [params] = useSearchParams();
  const typeKey = params.get("type") ?? "";
  const { data, loading: typesLoading, reload: reloadTypes } = useApi<TaskTypesResponse>("/api/tasks/types");
  const types = useMemo(() => data?.types ?? [], [data]);
  const selected = types.find((t) => t.key === typeKey);
  // A type page names its type from the first paint, as the skeleton does: the remembered title, or the
  // id in words on a first visit. A type the loaded list does not have falls back to All tasks.
  const label = !typeKey ? null : (selected ?? (data ? null : typeLabel(typeKey)));
  useEffect(() => {
    if (data?.types) rememberTypes(data.types);
  }, [data]);
  const typeOf = useCallback((key: string) => types.find((t) => t.key === key), [types]);
  const typeTitle = useCallback((key: string) => typeOf(key)?.title ?? typeLabel(key).title, [typeOf]);
  const [form, setForm] = useState<OpenForm | null>(null);
  const openForm = useCallback((type: TaskType) => {
    setForm((f) => ({ type, nonce: (f?.nonce ?? 0) + 1 }));
  }, []);

  const botsUp = useBotsRunning(Boolean(selected?.requires_bots));
  const runAction = selected ? <RunTaskButton type={selected} botsUp={botsUp} onRun={(t) => openForm(t)} /> : null;

  return (
    <section className="w-full">
      <TasksHeading />
      <div className={PAGE}>
        <TypeRail types={types} selected={id ? "" : typeKey} loading={!data && typesLoading} />
        <div className={MAIN}>
          {id ? (
            <RunDetail id={id} typeOf={typeOf} onStatusChange={reloadTypes} />
          ) : (
            <>
              <TasksHeader
                title={label?.title ?? "All tasks"}
                description={label ? label.description : ALL_TASKS_DESCRIPTION}
                action={runAction}
              />
              <RunsView
                types={types}
                typeTitle={typeTitle}
                onCountsChange={reloadTypes}
              />
            </>
          )}
        </div>
      </div>
      {form ? <TaskTypeForm key={form.nonce} type={form.type} open onClose={() => setForm(null)} /> : null}
    </section>
  );
}

/** Poll this often while the socket is down, so the list still moves; while it is up the frames keep it live. */
const SOCKET_DOWN_POLL_S = 30;

interface RunsViewProps {
  types: TaskType[];
  typeTitle: (key: string) => string;
  /** A live frame started a run or moved one to another status: the rail's counts are stale. */
  onCountsChange?: () => void;
}

function RunsView({ types, typeTitle, onCountsChange }: RunsViewProps) {
  const { filters, active, set, setPage, clear } = useRunFilters();
  const { runs, total, counts, loading, refreshing, settled, connected, reload, actors, contexts, patchRun, removeRun } = useTaskRuns(
    filters,
    onCountsChange,
  );
  const navigate = useNavigate();
  const confirm = useConfirm();
  const toast = useToast();
  const page = filters.page ?? 1;
  const pages = Math.max(1, Math.ceil(total / PER_PAGE));
  const onSearch = useCallback((q: string) => set("q", q), [set]);
  useLastCount(RUNS_COUNT_KEY, settled ? runs.length : undefined);
  useLastCount(RUN_PAGES_KEY, settled ? pages : undefined);
  // So a run opened from this list names its type before it loads.
  useEffect(() => rememberRunTypes(runs), [runs]);

  useEffect(() => {
    if (connected) return;
    const t = window.setInterval(reload, SOCKET_DOWN_POLL_S * 1000);
    return () => window.clearInterval(t);
  }, [connected, reload]);

  const onAction = async (act: RunAction, run: TaskRun) => {
    const name = `${typeTitle(run.type)} #${run.number}`;
    if (act === "view") {
      navigate(`/tasks/runs/${encodeURIComponent(run.id)}`);
    } else if (act === "cancel") {
      const ok = await confirm(runConfirm("cancel", name));
      if (!ok) return;
      try {
        const res = await api.post<TaskRunResponse>(`${runUrl(run.id)}/cancel`);
        if (res?.run) patchRun(res.run);
        toast.show(`Cancelling ${name}`, { kind: "info" });
      } catch (e) {
        const held = e instanceof ApiError ? (e.body as { run?: TaskRun } | null)?.run : undefined;
        if (held) patchRun(held);
        toast.show(errorText(e, `Could not cancel ${name}`), { kind: "error" });
      }
    } else {
      const ok = await confirm(runConfirm("delete", name));
      if (!ok) return;
      try {
        await api.del(runUrl(run.id));
        removeRun(run.id);
        toast.show(`Deleted ${name}`, { kind: "success" });
      } catch (e) {
        toast.show(errorText(e, `Could not delete ${name}`), { kind: "error" });
      }
    }
  };

  // Inside the runs frame, both in one shape (icon, title, a line) so the loading list can stand in for
  // either; only the filtered one adds Clear filters. Which one shows follows the filters the held page answers, so it changes only
  // when a fetch settles.
  const emptyFor = useRef({ active });
  if (settled) emptyFor.current = { active };
  const empty = emptyFor.current.active ? (
    <EmptyState
      plain
      className={RUNS_EMPTY}
      icon={<RunsEmptyIcon />}
      title="No runs match these filters"
      description="Try other filters, or clear them to see every run"
      action={
        <Button variant="ghost" onClick={clear}>
          Clear filters
        </Button>
      }
    />
  ) : (
    <EmptyState
      plain
      className={RUNS_EMPTY}
      icon={<RunsEmptyIcon />}
      title="No task runs yet"
      description="Runs show up here once a task starts"
    />
  );

  // One page needs no pager; while the first page loads, the pager stands only if the last list had more.
  const pagerShown = loading ? readPagerShown() : pages > 1;
  const footer = pagerShown ? <RunPager page={page} pages={loading ? null : pages} onPage={setPage} /> : null;

  return (
    <>
      <RunSearch value={filters.q ?? ""} onSearch={onSearch} />
      <StatusTabs counts={loading ? null : (counts ?? { all: total })} value={filters.status ?? ""} onSelect={(v) => set("status", v)} />
      <RunList
        runs={runs}
        total={total}
        loading={loading}
        refreshing={refreshing}
        typeTitle={typeTitle}
        filters={
          <RunFilterSelects
            filters={filters}
            types={types}
            actors={actors}
            contexts={contexts}
            onChange={set}
            active={active}
            onClear={clear}
          />
        }
        empty={empty}
        footer={footer}
        rowActions={(r) => <RunMenu run={r} onAction={(act, run) => void onAction(act, run)} />}
      />
    </>
  );
}
