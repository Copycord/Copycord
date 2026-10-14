// Tasks: every long job the app runs, in the layout of GitHub Actions "All workflows". The rail picks a
// task type (?type=), the list shows its runs live, and /tasks/runs/:id opens one run.

import { useCallback, useEffect, useState } from "react";
import { useParams, useSearchParams } from "react-router";
import { Button } from "../components/ui/Button";
import { useApi } from "../hooks/useApi";
import type { TaskType, TaskTypesResponse } from "../types/tasks";
import { RunDetail } from "./tasks/RunDetail";
import { AUTO_REFRESH_KEY, AUTO_REFRESH_OPTIONS, RunList } from "./tasks/RunList";
import { useTaskRuns } from "./tasks/runStore";
import { MAIN, PAGE, TasksHeading } from "./tasks/TasksSkeleton";
import { TypeRail } from "./tasks/TypeRail";

function readAutoRefresh(): string {
  try {
    const v = localStorage.getItem(AUTO_REFRESH_KEY);
    if (v && AUTO_REFRESH_OPTIONS.some((o) => o.value === v)) return v;
  } catch {
    // Storage blocked: use the default.
  }
  return "30";
}

export interface TasksHeaderProps {
  title: string;
  description: string;
  /** Start forms exist for these types; with none the button stays disabled. */
  runnable: TaskType[];
}

export function TasksHeader({ title, description, runnable }: TasksHeaderProps) {
  const none = runnable.length === 0;
  return (
    <div className="mb-4 flex items-start justify-between gap-3">
      <div className="min-w-0">
        <h2 className="m-0 text-[22px] font-bold text-text">{title}</h2>
        <p className="mt-1 mb-0 text-sm text-muted">{description}</p>
      </div>
      <Button variant="primary" disabled={none} title={none ? "No task type has a start form yet" : undefined}>
        Run task
      </Button>
    </div>
  );
}

export function Tasks() {
  const { id } = useParams<{ id: string }>();
  const [params] = useSearchParams();
  const typeKey = params.get("type") ?? "";
  const { data } = useApi<TaskTypesResponse>("/api/tasks/types");
  const types = data?.types ?? [];
  const selected = types.find((t) => t.key === typeKey);
  const typeTitle = useCallback((key: string) => types.find((t) => t.key === key)?.title ?? key, [types]);

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
                runnable={[]}
              />
              <RunsView typeKey={typeKey} typeTitle={typeTitle} />
            </>
          )}
        </div>
      </div>
    </section>
  );
}

function RunsView({ typeKey, typeTitle }: { typeKey: string; typeTitle: (key: string) => string }) {
  const { runs, total, loading, reload } = useTaskRuns({ type: typeKey || undefined });
  const [autoRefresh, setAutoRefresh] = useState(readAutoRefresh);

  useEffect(() => {
    const secs = Number(autoRefresh);
    if (!secs) return;
    const t = window.setInterval(reload, secs * 1000);
    return () => window.clearInterval(t);
  }, [autoRefresh, reload]);

  const onAutoRefresh = (v: string) => {
    setAutoRefresh(v);
    try {
      localStorage.setItem(AUTO_REFRESH_KEY, v);
    } catch {
      // Storage blocked: keep it for this visit only.
    }
  };

  return (
    <RunList
      runs={runs}
      total={total}
      loading={loading}
      typeTitle={typeTitle}
      autoRefresh={autoRefresh}
      onAutoRefresh={onAutoRefresh}
    />
  );
}
