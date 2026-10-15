// Tasks: every long job the app runs, in the layout of GitHub Actions "All workflows". The rail picks a
// task type (?type=), the list shows its runs live, and /tasks/runs/:id opens one run.

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import type { ReactNode } from "react";
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
import { AUTO_REFRESH_KEY, AUTO_REFRESH_OPTIONS, RunList, RunMenu, type RunAction } from "./tasks/RunList";
import { PER_PAGE, runUrl, useTaskRuns } from "./tasks/runStore";
import { MAIN, PAGE, TasksHeading } from "./tasks/TasksSkeleton";
import { TypeRail } from "./tasks/TypeRail";
import { TaskTypeForm } from "./tasks/types";

function readAutoRefresh(): string {
  try {
    const v = localStorage.getItem(AUTO_REFRESH_KEY);
    if (v && AUTO_REFRESH_OPTIONS.some((o) => o.value === v)) return v;
  } catch {
    // Storage blocked: use the default.
  }
  return "30";
}

export interface RunTaskButtonProps {
  type: TaskType;
  onRun: (type: TaskType) => void;
}

/** "Run task" on a type page opens that type's form. All tasks has none: the rail picks the type first. */
export function RunTaskButton({ type, onRun }: RunTaskButtonProps) {
  return (
    <Button variant="primary" onClick={() => onRun(type)}>
      Run task
    </Button>
  );
}

export interface TasksHeaderProps {
  title: string;
  description: string;
  action: ReactNode;
}

export function TasksHeader({ title, description, action }: TasksHeaderProps) {
  return (
    <div className="mb-4 flex items-start justify-between gap-3">
      <div className="min-w-0">
        <h2 className="m-0 text-[22px] font-bold text-text">{title}</h2>
        <p className="mt-1 mb-0 text-sm text-muted">{description}</p>
      </div>
      {action}
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
  const { data } = useApi<TaskTypesResponse>("/api/tasks/types");
  const types = useMemo(() => data?.types ?? [], [data]);
  const selected = types.find((t) => t.key === typeKey);
  const typeTitle = useCallback((key: string) => types.find((t) => t.key === key)?.title ?? key, [types]);
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

  const runAction = selected ? <RunTaskButton type={selected} onRun={(t) => openForm(t)} /> : null;

  return (
    <section className="w-full">
      <TasksHeading />
      <div className={PAGE}>
        <TypeRail types={types} selected={id ? "" : typeKey} />
        <div className={MAIN}>
          {id ? (
            <RunDetail id={id} typeTitle={typeTitle} />
          ) : (
            <>
              <TasksHeader
                title={selected?.title ?? "All tasks"}
                description={selected?.description ?? "Showing runs from all task types"}
                action={runAction}
              />
              <RunsView types={types} typeTitle={typeTitle} runAction={runAction} />
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

function RunsView({
  types,
  typeTitle,
  runAction,
}: {
  types: TaskType[];
  typeTitle: (key: string) => string;
  runAction: ReactNode;
}) {
  const { filters, active, set, setPage, clear } = useRunFilters();
  const { runs, total, loading, connected, reload, actors, contexts, patchRun, removeRun } = useTaskRuns(filters);
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

  const empty = active ? (
    <EmptyState
      title="No runs match"
      className="m-4"
      action={
        <Button variant="ghost" onClick={clear}>
          Clear filters
        </Button>
      }
    />
  ) : (
    <EmptyState
      title="No task runs yet"
      className="m-4"
      description={filters.type ? undefined : "Pick a task type on the left to run one"}
      action={runAction}
    />
  );

  const footer =
    total > PER_PAGE || page > 1 ? (
      <nav aria-label="Pages" className="flex items-center justify-between gap-3 border-t border-border px-4 py-3 text-sm text-muted">
        <Button variant="ghost" size="sm" disabled={page <= 1} onClick={() => setPage(page - 1)}>
          Previous
        </Button>
        <span>
          Page {page} of {pages}
        </span>
        <Button variant="ghost" size="sm" disabled={page >= pages} onClick={() => setPage(page + 1)}>
          Next
        </Button>
      </nav>
    ) : null;

  return (
    <>
      <RunSearch value={filters.q ?? ""} onSearch={onSearch} />
      <RunList
        runs={runs}
        total={total}
        loading={loading}
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
