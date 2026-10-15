// Tasks: every long job the app runs, in the layout of GitHub Actions "All workflows". The rail picks a
// task type (?type=), the list shows its runs live, and /tasks/runs/:id opens one run.

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useNavigate, useParams, useSearchParams } from "react-router";
import { useToast } from "../components/Toast";
import { Button } from "../components/ui/Button";
import { useConfirm } from "../components/ui/ConfirmDialog";
import { EmptyState } from "../components/ui/EmptyState";
import { useApi } from "../hooks/useApi";
import { ApiError, api, errorText } from "../lib/api";
import type { TaskRun, TaskRunResponse, TaskType, TaskTypesResponse } from "../types/tasks";
import { RunDetail } from "./tasks/RunDetail";
import { RunFilterSelects, RunSearch, useRunFilters } from "./tasks/RunFilters";
import { RunList, RunMenu, type RunAction } from "./tasks/RunList";
import { PER_PAGE, runUrl, useTaskRuns } from "./tasks/runStore";
import { ALL_TASKS_DESCRIPTION, AUTO_REFRESH_KEY, MAIN, PAGE, RunPager, TasksHeader, TasksHeading, readAutoRefresh } from "./tasks/TasksSkeleton";
import { TypeRail } from "./tasks/TypeRail";
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
  prefill: TaskRun | null;
  /** Remounts the form so each open starts from a clean state. */
  nonce: number;
}

export function Tasks() {
  const { id } = useParams<{ id: string }>();
  const [params, setParams] = useSearchParams();
  const typeKey = params.get("type") ?? "";
  const rerun = params.get("rerun") ?? "";
  const { data, loading: typesLoading } = useApi<TaskTypesResponse>("/api/tasks/types");
  const types = useMemo(() => data?.types ?? [], [data]);
  const selected = types.find((t) => t.key === typeKey);
  // A type page before the types land: bars in the header rather than All tasks, as in the skeleton.
  const typePending = Boolean(typeKey) && !data && typesLoading;
  const typeOf = useCallback((key: string) => types.find((t) => t.key === key), [types]);
  const typeTitle = useCallback((key: string) => typeOf(key)?.title ?? key, [typeOf]);
  const toast = useToast();
  const toastRef = useRef(toast);
  toastRef.current = toast;
  const [form, setForm] = useState<OpenForm | null>(null);
  const openForm = useCallback((type: TaskType, prefill: TaskRun | null = null) => {
    setForm((f) => ({ type, prefill, nonce: (f?.nonce ?? 0) + 1 }));
  }, []);

  // ?rerun=<id> opens the run's type form prefilled from the run, then leaves the URL.
  useEffect(() => {
    if (!rerun || types.length === 0) return;
    let live = true;
    api
      .get<TaskRunResponse>(runUrl(rerun))
      .then((res) => {
        if (!live) return;
        const type = types.find((t) => t.key === res.run.type);
        if (type) openForm(type, res.run);
        else toastRef.current.show(`Unknown task type ${res.run.type}`, { kind: "error" });
      })
      .catch((e) => {
        if (live) toastRef.current.show(errorText(e, "Could not load the run to re-run"), { kind: "error" });
      })
      .finally(() => {
        if (!live) return;
        setParams(
          (p) => {
            const next = new URLSearchParams(p);
            next.delete("rerun");
            return next;
          },
          { replace: true },
        );
      });
    return () => {
      live = false;
    };
  }, [rerun, types, openForm, setParams]);

  const botsUp = useBotsRunning(Boolean(selected?.requires_bots));
  const runAction = selected ? <RunTaskButton type={selected} botsUp={botsUp} onRun={(t) => openForm(t)} /> : null;

  return (
    <section className="w-full">
      <TasksHeading />
      <div className={PAGE}>
        <TypeRail types={types} selected={id ? "" : typeKey} loading={!data && typesLoading} />
        <div className={MAIN}>
          {id ? (
            <RunDetail id={id} typeOf={typeOf} />
          ) : (
            <>
              <TasksHeader
                title={typePending ? null : (selected?.title ?? "All tasks")}
                description={typePending ? null : (selected?.description ?? ALL_TASKS_DESCRIPTION)}
                action={runAction}
                fade={Boolean(typeKey)}
              />
              <RunsView types={types} typeTitle={typeTitle} />
            </>
          )}
        </div>
      </div>
      {form ? <TaskTypeForm key={form.nonce} type={form.type} prefill={form.prefill} open onClose={() => setForm(null)} /> : null}
    </section>
  );
}

/** Poll this often while the socket is down and auto-refresh is Off, so the list still moves. */
const SOCKET_DOWN_POLL_S = 30;

function RunsView({ types, typeTitle }: { types: TaskType[]; typeTitle: (key: string) => string }) {
  const { filters, active, set, setPage, clear } = useRunFilters();
  const { runs, total, loading, refreshing, settled, connected, reload, actors, contexts, patchRun, removeRun } = useTaskRuns(filters);
  const [autoRefresh, setAutoRefresh] = useState(readAutoRefresh);
  const navigate = useNavigate();
  const confirm = useConfirm();
  const toast = useToast();
  const page = filters.page ?? 1;
  const pages = Math.max(1, Math.ceil(total / PER_PAGE));
  const onSearch = useCallback((q: string) => set("q", q), [set]);

  useEffect(() => {
    const secs = Number(autoRefresh) || (connected ? 0 : SOCKET_DOWN_POLL_S);
    if (!secs) return;
    const t = window.setInterval(reload, secs * 1000);
    return () => window.clearInterval(t);
  }, [autoRefresh, connected, reload]);

  const onAction = async (act: RunAction, run: TaskRun) => {
    const name = `${typeTitle(run.type)} #${run.number}`;
    if (act === "view") {
      navigate(`/tasks/runs/${encodeURIComponent(run.id)}`);
    } else if (act === "rerun") {
      // Tasks reads ?rerun= and opens the type's form prefilled from this run.
      navigate(`/tasks?type=${encodeURIComponent(run.type)}&rerun=${encodeURIComponent(run.id)}`);
    } else if (act === "cancel") {
      const ok = await confirm({
        title: `Cancel ${name}?`,
        body: "Work already done is kept; the run stops at its next checkpoint.",
        confirmLabel: "Cancel run",
        cancelLabel: "Keep running",
        confirmVariant: "ghost-red",
      });
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
      const ok = await confirm({
        title: `Delete ${name}?`,
        body: "Its log and report are deleted with it. This cannot be undone.",
        confirmLabel: "Delete",
        confirmVariant: "ghost-red",
      });
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

  const onAutoRefresh = (v: string) => {
    setAutoRefresh(v);
    try {
      localStorage.setItem(AUTO_REFRESH_KEY, v);
    } catch {
      // Storage blocked: keep it for this visit only.
    }
  };

  // Plain text inside the runs frame; the header's Run task is the only one on the page. Which one
  // shows follows the filters the held page answers, so it changes only when a fetch settles.
  const emptyFor = useRef({ active, scoped: Boolean(filters.type) });
  if (settled) emptyFor.current = { active, scoped: Boolean(filters.type) };
  const empty = emptyFor.current.active ? (
    <EmptyState
      plain
      title="No runs match"
      action={
        <Button variant="ghost" onClick={clear}>
          Clear filters
        </Button>
      }
    />
  ) : (
    <EmptyState
      plain
      title="No task runs yet"
      description={emptyFor.current.scoped ? "Run task above to start one" : "Pick a task type on the left to run one"}
    />
  );

  const footer = <RunPager page={page} pages={loading ? null : pages} onPage={setPage} />;

  return (
    <>
      <RunSearch value={filters.q ?? ""} onSearch={onSearch} />
      <RunList
        runs={runs}
        total={total}
        loading={loading}
        refreshing={refreshing}
        typeTitle={typeTitle}
        autoRefresh={autoRefresh}
        onAutoRefresh={onAutoRefresh}
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
